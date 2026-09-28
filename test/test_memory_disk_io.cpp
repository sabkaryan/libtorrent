/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "disk_io_test.hpp" // for memory_file_disk_io()
#include "memory_gates.hpp"
#include "setup_transfer.hpp" // for generate_piece, remove_all

#include "libtorrent/disk_interface.hpp"
#include "libtorrent/disk_observer.hpp"
#include "libtorrent/disk_buffer_holder.hpp"
#include "libtorrent/session_params.hpp" // for disk_io_constructor_type
#include "libtorrent/settings_pack.hpp" // for default_settings
#include "libtorrent/storage_defs.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/peer_request.hpp"
#include "libtorrent/pread_disk_io.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/sha1_hash.hpp"
#include "libtorrent/aux_/vector.hpp"
#include "libtorrent/aux_/time.hpp"
#include "libtorrent/aux_/memory_hasher.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp" // for memory_place_for_test

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

struct counting_observer final : lt::disk_observer
{
	void on_disk() override { ++calls; }
	int calls = 0;
};

// the write queue holds one block: the first async_write reaches the limit,
// returns true and keeps the observer; once the block is flushed the backend
// must call on_disk() on it. A backend that loses the observer on the way
// leaves a peer throttled for good
void observer_woken_after_back_pressure(lt::disk_io_constructor_type const& disk_io)
{
	lt::io_context ios;
	lt::counters cnt;
	lt::settings_pack sett = lt::default_settings();
	sett.set_int(lt::settings_pack::max_queued_disk_bytes, lt::default_block_size);
	sett.set_int(lt::settings_pack::aio_threads, 1);
	sett.set_int(lt::settings_pack::hashing_threads, 1);
	std::unique_ptr<lt::disk_interface> disk = disk_io(ios, sett, cnt);

	lt::file_storage fs;
	fs.set_piece_length(lt::default_block_size);
	fs.add_file("back_pressure/file", lt::default_block_size, {});
	fs.set_num_pieces(1);
	lt::aux::vector<lt::download_priority_t, lt::file_index_t> priorities;
	lt::renamed_files rf;
	lt::storage_params const params{fs, rf, "back_pressure_torrent", {}
		, lt::storage_mode_t::storage_mode_sparse, priorities, lt::sha1_hash{}, true, false};
	lt::storage_holder storage = disk->new_torrent(params, std::shared_ptr<void>());

	std::vector<char> const buf(std::size_t(lt::default_block_size), 'x');
	// back_pressure keeps a weak_ptr: the observer lives as long as a peer
	auto const obs = std::make_shared<counting_observer>();
	lt::peer_request req;
	req.piece = lt::piece_index_t{0};
	req.start = 0;
	req.length = lt::default_block_size;
	bool written = false;
	bool const pressure = disk->async_write(storage, req, buf.data(), obs
		, [&written](lt::storage_error const& ec) { TEST_CHECK(!ec); written = true; });
	TEST_CHECK(pressure);

	// the piece is complete: hashing it lets the backend flush it
	bool hashed = false;
	disk->async_hash(storage, lt::piece_index_t{0}, {}, lt::disk_interface::v1_hash
		, [&hashed](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& ec)
		{ TEST_CHECK(!ec); hashed = true; });
	disk->submit_jobs();

	auto const start_time = lt::aux::time_now();
	while (!written || !hashed || obs->calls == 0)
	{
		ios.restart();
		if (ios.run_for(std::chrono::milliseconds(5)) == 0)
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		if (lt::aux::time_now() - start_time > lt::seconds(10))
		{
			TEST_ERROR("timeout: on_disk() not called");
			break;
		}
	}
	TEST_CHECK(written);
	TEST_CHECK(hashed);
	TEST_EQUAL(obs->calls, 1);

	disk->abort(true);
}

// a pool that keeps every piece in memory
std::shared_ptr<lt::memory_storage_pool> memory_pool()
{
	auto pool = std::make_shared<lt::memory_storage_pool>();
	pool->set_default_policy(lt::memory_policy::memory);
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	return pool;
}

// a disk_interface and the storages of one test. The storages are removed
// after abort(), before the disk_interface goes, as the tests of
// test_disk_io.cpp do
struct disk_env
{
	disk_env(lt::disk_io_constructor_type const& ctor, int const hashing_threads)
	{
		sett = lt::default_settings();
		sett.set_int(lt::settings_pack::hashing_threads, hashing_threads);
		sett.set_int(lt::settings_pack::aio_threads, 1);
		disk = ctor(ios, sett, cnt);
	}

	~disk_env()
	{
		disk->abort(true);
		storages.clear();
		disk.reset();
		for (auto const& p : paths)
		{
			lt::error_code ec;
			remove_all(p, ec);
		}
	}

	disk_env(disk_env const&) = delete;
	disk_env& operator=(disk_env const&) = delete;

	// fs must outlive the environment
	lt::storage_index_t add(lt::file_storage const& fs, std::string const& path
		, bool const v1, bool const v2)
	{
		lt::error_code ec;
		remove_all(path, ec);
		paths.push_back(path);
		lt::aux::vector<lt::download_priority_t, lt::file_index_t> priorities;
		lt::renamed_files rf;
		lt::storage_params const params{fs, rf, path, {}
			, lt::storage_mode_t::storage_mode_sparse, priorities, lt::sha1_hash{}, v1, v2};
		storages.push_back(disk->new_torrent(params, std::shared_ptr<void>()));
		return static_cast<lt::storage_index_t>(storages.back());
	}

	// runs the handlers until pred() is true, for at most 10 seconds
	template <typename Pred>
	bool run_until(Pred pred)
	{
		auto const end = lt::aux::time_now() + lt::seconds(10);
		while (!pred())
		{
			if (lt::aux::time_now() > end) return false;
			ios.restart();
			if (ios.run_for(std::chrono::milliseconds(2)) == 0)
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return true;
	}

	// writes `data` (the bytes of the piece from offset 0) block by block,
	// except the blocks in `skip`. pread_disk_io completes a write once it
	// flushed it, which may take the piece's hash: hash() waits for them
	void write(lt::storage_index_t const st, lt::piece_index_t const piece
		, std::vector<char> const& data, std::vector<int> const& skip = {})
	{
		int const len = int(data.size());
		for (int off = 0; off < len; off += lt::default_block_size)
		{
			int const block = off / lt::default_block_size;
			if (std::find(skip.begin(), skip.end(), block) != skip.end()) continue;
			lt::peer_request const r{piece, off, std::min(lt::default_block_size, len - off)};
			// as the tests of test_disk_io.cpp do: pread_disk_io flushes the
			// piece once it is hashed
			bool const last = off + lt::default_block_size >= len;
			disk->async_write(st, r, data.data() + off, {}
				, [this](lt::storage_error const& e) { TEST_CHECK(!e); ++writes_done; }
				, last ? lt::disk_interface::flush_piece : lt::disk_job_flags_t{});
			++writes_issued;
		}
		disk->submit_jobs();
	}

	struct hash_result
	{
		lt::sha1_hash v1;
		std::vector<lt::sha256_hash> v2;
		lt::storage_error error;
	};

	hash_result hash(lt::storage_index_t const st, lt::piece_index_t const piece
		, bool const v1, int const v2_blocks)
	{
		auto v2 = std::make_shared<std::vector<lt::sha256_hash>>(std::size_t(v2_blocks));
		hash_result ret;
		bool done = false;
		disk->async_hash(st, piece, *v2
			, (v1 ? lt::disk_interface::v1_hash : lt::disk_job_flags_t{}) | lt::disk_interface::flush_piece
			, [&ret, &done, v2](lt::piece_index_t, lt::sha1_hash const& h, lt::storage_error const& e)
			{
				ret.v1 = h;
				ret.v2 = *v2;
				ret.error = e;
				done = true;
			});
		disk->submit_jobs();
		TEST_CHECK(run_until([&] { return done; }));
		TEST_CHECK(run_until([&] { return writes_done == writes_issued; }));
		return ret;
	}

	lt::sha256_hash hash2(lt::storage_index_t const st, lt::piece_index_t const piece, int const offset)
	{
		lt::sha256_hash ret;
		bool done = false;
		disk->async_hash2(st, piece, offset, {}
			, [&ret, &done](lt::piece_index_t, lt::sha256_hash const& h, lt::storage_error const& e)
			{
				TEST_CHECK(!e);
				ret = h;
				done = true;
			});
		disk->submit_jobs();
		TEST_CHECK(run_until([&] { return done; }));
		return ret;
	}

	// the bytes of r (empty on an error, which is returned in `error`)
	std::vector<char> read(lt::storage_index_t const st, lt::peer_request const& r
		, lt::storage_error& error)
	{
		std::vector<char> ret;
		bool done = false;
		disk->async_read(st, r, [&](lt::disk_buffer_holder b, lt::storage_error const& e)
			{
				error = e;
				if (!e) ret.assign(b.data(), b.data() + r.length);
				done = true;
			}, {});
		disk->submit_jobs();
		TEST_CHECK(run_until([&] { return done; }));
		return ret;
	}

	lt::io_context ios;
	lt::counters cnt;
	lt::settings_pack sett;
	std::unique_ptr<lt::disk_interface> disk;
	std::vector<lt::storage_holder> storages;
	std::vector<std::string> paths;
	int writes_issued = 0;
	int writes_done = 0;
};

// pieces of 2 blocks. File a ends inside piece 1 and a pad file fills the
// piece: block 0 of piece 1 is part data, part pad, block 1 is all pad. File
// b fills piece 2 and ends in the short piece 3
lt::file_storage pad_layout()
{
	lt::file_storage fs;
	int const piece_size = 2 * lt::default_block_size;
	fs.set_piece_length(piece_size);
	fs.add_file("pad_layout/a", 40000, {});
	fs.add_file("pad_layout/.pad/25536", 2 * piece_size - 40000, lt::file_storage::flag_pad_file);
	fs.add_file("pad_layout/b", 50000, {});
	fs.set_num_pieces(int((fs.total_size() + piece_size - 1) / piece_size));
	return fs;
}

// the bytes libtorrent writes to a piece: the v1 piece (v1 and hybrid) or
// the v2 piece (v2 only). Pad ranges are zero
std::vector<char> piece_data(lt::file_storage const& fs, lt::piece_index_t const piece
	, bool const v1)
{
	int const len = v1 ? fs.piece_size(piece) : fs.piece_size2(piece);
	std::vector<char> ret = generate_piece(piece, len);
	std::ptrdiff_t pos = 0;
	for (auto const& s : fs.map_block(piece, 0, len))
	{
		auto const size = static_cast<std::ptrdiff_t>(s.size);
		if (fs.pad_file_at(s.file_index))
			std::fill(ret.begin() + pos, ret.begin() + pos + size, char(0));
		pos += size;
	}
	return ret;
}

// the blocks entirely in pad files. libtorrent does not write them
std::vector<int> pad_blocks(lt::file_storage const& fs, lt::piece_index_t const piece)
{
	std::vector<int> ret;
	int const len = fs.piece_size(piece);
	for (int off = 0; off < len; off += lt::default_block_size)
	{
		auto const slices = fs.map_block(piece, off, std::min(lt::default_block_size, len - off));
		if (std::all_of(slices.begin(), slices.end()
			, [&fs](lt::file_slice const& s) { return fs.pad_file_at(s.file_index); }))
			ret.push_back(off / lt::default_block_size);
	}
	return ret;
}

lt::sha1_hash sha1(std::vector<char> const& v)
{
	return lt::hasher(v.data(), int(v.size())).final();
}

// one file of `blocks` blocks, one piece
lt::file_storage one_piece(char const* name, int const blocks)
{
	lt::file_storage fs;
	fs.set_piece_length(blocks * lt::default_block_size);
	fs.add_file(name, blocks * lt::default_block_size, {});
	fs.set_num_pieces(1);
	return fs;
}

} // anonymous namespace

// the reference: pread_disk_io itself wakes the observer
TORRENT_TEST(observer_woken_after_back_pressure_pread)
{
	observer_woken_after_back_pressure(lt::pread_disk_io_constructor);
}

// the pass-through hands the original observer to the default backend
TORRENT_TEST(observer_woken_after_back_pressure_memory_file)
{
	observer_woken_after_back_pressure(memory_file_disk_io());
}

// the same blocks, written to memory and to pread_disk_io, give the same
// SHA-1, the same v2 block hashes and the same async_hash2 of each block, for
// v1, v2 and hybrid torrents, with and without hashing threads
TORRENT_TEST(memory_piece_hash_matches_pread)
{
	lt::file_storage const fs = pad_layout();
	struct mode { bool v1; bool v2; };
	for (mode const m : {mode{true, false}, mode{false, true}, mode{true, true}})
	{
		for (int const threads : {0, 2})
		{
			std::printf("v1: %d v2: %d hashing threads: %d\n", int(m.v1), int(m.v2), threads);
			disk_env mem(lt::memory_disk_io_constructor(memory_pool()), threads);
			disk_env ref(lt::pread_disk_io_constructor, threads);
			lt::storage_index_t const ms = mem.add(fs, "hash_matches_memory", m.v1, m.v2);
			lt::storage_index_t const rs = ref.add(fs, "hash_matches_pread", m.v1, m.v2);
			for (auto const p : fs.piece_range())
			{
				std::vector<char> const data = piece_data(fs, p, m.v1);
				std::vector<int> const skip = pad_blocks(fs, p);
				mem.write(ms, p, data, skip);
				ref.write(rs, p, data, skip);
				int const v2_blocks = m.v2 ? fs.blocks_in_piece2(p) : 0;
				auto const mh = mem.hash(ms, p, m.v1, v2_blocks);
				auto const rh = ref.hash(rs, p, m.v1, v2_blocks);
				TEST_CHECK(!mh.error);
				TEST_CHECK(!rh.error);
				if (m.v1)
				{
					TEST_CHECK(mh.v1 == rh.v1);
					TEST_CHECK(mh.v1 == sha1(data));
				}
				TEST_CHECK(mh.v2 == rh.v2);
				for (int b = 0; b < v2_blocks; ++b)
				{
					int const off = b * lt::default_block_size;
					lt::sha256_hash const expected = lt::hasher256(data.data() + off
						, std::min(lt::default_block_size, fs.piece_size2(p) - off)).final();
					TEST_CHECK(mh.v2[std::size_t(b)] == expected);
					lt::sha256_hash const m2 = mem.hash2(ms, p, off);
					TEST_CHECK(m2 == ref.hash2(rs, p, off));
					TEST_CHECK(m2 == expected);
				}
			}
		}
	}
}

// a block that is part data, part pad, written with non-zero bytes in its
// pad range: SHA-1 covers the pad as zeros, as pread_disk_io (which does not
// write pad files) computes it, and a read returns zeros there
TORRENT_TEST(memory_pad_zeroed)
{
	lt::file_storage const fs = pad_layout();
	lt::piece_index_t const p{1};
	std::vector<char> const clean = piece_data(fs, p, true);
	std::vector<char> dirty = clean;
	// file a ends at 40000: piece 1 is pad from byte 7232 on
	int const data_len = 40000 - fs.piece_length();
	std::fill(dirty.begin() + data_len, dirty.end(), char(0x5a));

	for (int const threads : {0, 2})
	{
		disk_env mem(lt::memory_disk_io_constructor(memory_pool()), threads);
		disk_env ref(lt::pread_disk_io_constructor, threads);
		lt::storage_index_t const ms = mem.add(fs, "pad_zeroed_memory", true, true);
		lt::storage_index_t const rs = ref.add(fs, "pad_zeroed_pread", true, true);
		std::vector<int> const skip = pad_blocks(fs, p);
		TEST_EQUAL(int(skip.size()), 1);
		mem.write(ms, p, dirty, skip);
		ref.write(rs, p, clean, skip);
		int const v2_blocks = fs.blocks_in_piece2(p);
		auto const mh = mem.hash(ms, p, true, v2_blocks);
		auto const rh = ref.hash(rs, p, true, v2_blocks);
		TEST_CHECK(!mh.error);
		TEST_CHECK(mh.v1 == rh.v1);
		TEST_CHECK(mh.v1 == sha1(clean));
		TEST_CHECK(mh.v2 == rh.v2);

		lt::storage_error error;
		std::vector<char> const block = mem.read(ms, lt::peer_request{p, 0, lt::default_block_size}, error);
		TEST_CHECK(!error);
		TEST_CHECK(block.size() == std::size_t(lt::default_block_size)
			&& std::equal(block.begin(), block.end(), clean.begin()));
	}
}

// the last piece is shorter than the others and ends inside a block (a file
// of 6.5 blocks, pieces of 4): reads give the 2.5 blocks of the piece, a
// read past its end is refused, and its hash is pread_disk_io's
TORRENT_TEST(last_piece_short)
{
	lt::file_storage fs;
	fs.set_piece_length(4 * lt::default_block_size);
	fs.add_file("last_piece_short/file", 6 * lt::default_block_size + lt::default_block_size / 2, {});
	fs.set_num_pieces(2);
	lt::piece_index_t const last{1};
	TEST_EQUAL(fs.piece_size(last), 2 * lt::default_block_size + lt::default_block_size / 2);

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	disk_env ref(lt::pread_disk_io_constructor, 1);
	lt::storage_index_t const ms = mem.add(fs, "last_piece_memory", true, false);
	lt::storage_index_t const rs = ref.add(fs, "last_piece_pread", true, false);
	std::vector<char> const data = piece_data(fs, last, true);
	mem.write(ms, last, data);
	ref.write(rs, last, data);

	std::vector<char> read_back;
	for (int off = 0; off < int(data.size()); off += lt::default_block_size)
	{
		lt::storage_error error;
		int const len = std::min(lt::default_block_size, int(data.size()) - off);
		std::vector<char> const b = mem.read(ms, lt::peer_request{last, off, len}, error);
		TEST_CHECK(!error);
		read_back.insert(read_back.end(), b.begin(), b.end());
	}
	TEST_CHECK(read_back == data);

	// an unaligned read across two blocks
	{
		lt::storage_error error;
		int const off = lt::default_block_size + 100;
		std::vector<char> const b = mem.read(ms, lt::peer_request{last, off, lt::default_block_size}, error);
		TEST_CHECK(!error);
		TEST_CHECK(b.size() == std::size_t(lt::default_block_size)
			&& std::equal(b.begin(), b.end(), data.begin() + off));
	}
	// past the end of the piece
	{
		lt::storage_error error;
		int const off = 2 * lt::default_block_size;
		mem.read(ms, lt::peer_request{last, off, lt::default_block_size}, error);
		TEST_CHECK(error);
	}

	auto const mh = mem.hash(ms, last, true, 0);
	auto const rh = ref.hash(rs, last, true, 0);
	TEST_CHECK(!mh.error);
	TEST_CHECK(mh.v1 == rh.v1);
	TEST_CHECK(mh.v1 == sha1(data));
}

// with hashing threads the answer of a hash job is computed on a hashing
// thread, not on the thread that runs the io_context; without them it is
// computed inline, on that thread
TORRENT_TEST(hash_off_network_thread)
{
	lt::file_storage const fs = one_piece("off_network/file", 4);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	for (int const threads : {1, 0})
	{
		disk_env mem(lt::memory_disk_io_constructor(memory_pool()), threads);
		lt::storage_index_t const ms = mem.add(fs, "off_network", true, false);
		std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
		mem.write(ms, p, data);
		// the background hash lets go of the entry first: a hash job that
		// found it held would be parked, and answered from a hashing thread
		// even if async_hash hashed inline
		if (threads > 0)
		{
			TEST_CHECK(mem.run_until([&] {
				return lt::aux::memory_hasher_background_blocks_for_test() - background0 == 4; }));
		}
		auto const h = mem.hash(ms, p, true, 0);
		TEST_CHECK(!h.error);
		TEST_CHECK(h.v1 == sha1(data));
		std::thread::id const hashed_on = lt::aux::memory_hasher_answer_thread_for_test();
		if (threads > 0) TEST_CHECK(hashed_on != std::this_thread::get_id());
		else TEST_CHECK(hashed_on == std::this_thread::get_id());
	}
}

// blocks written in order are hashed as they arrive: once the hasher is
// idle, async_hash hashes no block of its own
TORRENT_TEST(hash_incremental)
{
	int const blocks = 8;
	lt::file_storage const fs = one_piece("incremental/file", blocks);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::storage_index_t const ms = mem.add(fs, "incremental", true, false);
	std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();

	// the gate is the synchronisation point: the hashing thread reaches it
	// at the first block, and hashes every block once released
	memory_hasher_gate gate(p);
	mem.write(ms, p, data);
	TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));
	gate.release();
	TEST_CHECK(mem.run_until([&] {
		return memory_hasher_gate::waiting() == 0
			&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == blocks; }));

	std::int64_t const tail0 = lt::aux::memory_hasher_tail_blocks_for_test();
	auto const h = mem.hash(ms, p, true, 0);
	TEST_CHECK(!h.error);
	TEST_CHECK(h.v1 == sha1(data));
	TEST_EQUAL(lt::aux::memory_hasher_tail_blocks_for_test() - tail0, 0);
}

// a piece with blocks missing hashes them as zeros, answers without an
// error and counts them in hash_missing_blocks()
TORRENT_TEST(missing_block_hashes_as_zero)
{
	lt::file_storage const fs = one_piece("missing/file", 4);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);
	std::vector<char> zeros_for_missing = data;
	for (int const b : {1, 3})
		std::fill_n(zeros_for_missing.begin() + b * lt::default_block_size, lt::default_block_size, char(0));

	for (int const threads : {0, 1})
	{
		auto const pool = memory_pool();
		disk_env mem(lt::memory_disk_io_constructor(pool), threads);
		lt::storage_index_t const ms = mem.add(fs, "missing", true, false);
		mem.write(ms, p, data, {1, 3});
		auto const h = mem.hash(ms, p, true, 0);
		TEST_CHECK(!h.error);
		TEST_CHECK(h.v1 != sha1(data));
		TEST_CHECK(h.v1 == sha1(zeros_for_missing));
		TEST_EQUAL(pool->hash_missing_blocks(), 2);
	}
}

// what force_recheck does to a piece that is half in memory: the check of
// the files, then a hash of the piece. The hash answers without an error
// (an error would pause the torrent) and does not match; the piece stays in
// memory, and its next block is written there
TORRENT_TEST(recheck_partial_in_memory)
{
	lt::file_storage fs;
	fs.set_piece_length(4 * lt::default_block_size);
	fs.add_file("recheck_partial/file", 8 * lt::default_block_size, {});
	fs.set_num_pieces(2);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	auto const pool = memory_pool();
	disk_env mem(lt::memory_disk_io_constructor(pool), 1);
	lt::storage_index_t const ms = mem.add(fs, "recheck_partial", true, false);
	mem.write(ms, p, data, {2, 3});
	TEST_EQUAL(pool->held_bytes(), 2 * lt::default_block_size);

	bool checked = false;
	mem.disk->async_check_files(ms, nullptr, {}
		, [&checked](lt::status_t, lt::storage_error const&) { checked = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([&] { return checked; }));

	auto const h = mem.hash(ms, p, true, 0);
	TEST_CHECK(!h.error);
	TEST_CHECK(h.v1 != sha1(data));
	TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::memory);

	// the next block goes to memory too
	std::vector<char> const block2(data.begin() + 2 * lt::default_block_size
		, data.begin() + 3 * lt::default_block_size);
	bool written = false;
	mem.disk->async_write(ms, lt::peer_request{p, 2 * lt::default_block_size, lt::default_block_size}
		, block2.data(), {}, [&written](lt::storage_error const& e) { TEST_CHECK(!e); written = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([&] { return written; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::memory);
	TEST_EQUAL(pool->held_bytes(), 3 * lt::default_block_size);
	lt::storage_error error;
	std::vector<char> const b = mem.read(ms
		, lt::peer_request{p, 2 * lt::default_block_size, lt::default_block_size}, error);
	TEST_CHECK(!error);
	TEST_CHECK(b == block2);
}
