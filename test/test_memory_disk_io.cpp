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
#include "libtorrent/session_handle.hpp" // for delete_files
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
#include "libtorrent/aux_/memory_storage.hpp" // for memory_storages_destroyed_pinned_for_test
#include "libtorrent/aux_/path.hpp" // for create_directories, combine_path

// interposes pwrite() for this binary: include it from this file only
#include "write_gate.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
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
	storage.reset();
	disk.reset();
	lt::error_code ec;
	remove_all("back_pressure_torrent", ec);
}

// a pool that keeps every piece in memory
std::shared_ptr<lt::memory_storage_pool> memory_pool()
{
	auto pool = std::make_shared<lt::memory_storage_pool>();
	pool->set_default_policy(lt::memory_policy::memory);
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	return pool;
}

// the default limit of disk_env::run_until(). A build that slows every hash
// down (simulate-slow=hash) needs longer, as test_disk_io.cpp does
#ifdef TORRENT_SIMULATE_SLOW_HASH
lt::time_duration const run_limit = lt::seconds(60);
#else
lt::time_duration const run_limit = lt::seconds(10);
#endif

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

	// runs the handlers until pred() is true, for at most `limit`
	template <typename Pred>
	bool run_until(Pred pred, lt::time_duration const limit = run_limit)
	{
		auto const end = lt::aux::time_now() + limit;
		while (!pred())
		{
			if (lt::aux::time_now() > end) return false;
			ios.restart();
			if (ios.run_for(std::chrono::milliseconds(2)) == 0)
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return true;
	}

	// runs the handlers for `d`: long enough for the default backend to
	// answer a job it has no reason to hold
	void run_for(lt::time_duration const d)
	{
		run_until([] { return false; }, d);
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
			disk->async_write(st, r, data.data() + off, {}
				, [this](lt::storage_error const& e) { TEST_CHECK(!e); ++writes_done; }
				, lt::disk_job_flags_t{});
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
		// flush_piece: see write()
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

// a null pool is refused when the constructor is made, not dereferenced
// when the session creates its disk backend
TORRENT_TEST(constructor_refuses_null_pool)
{
	bool thrown = false;
	try
	{
		lt::memory_disk_io_constructor(nullptr);
	}
	catch (std::invalid_argument const&)
	{
		thrown = true;
	}
	TEST_CHECK(thrown);
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

// blocks written out of order, 1, 2 and 3, then 0: the background hash
// feeds SHA-1 in block order, from block 0, and once block 0 is there it
// hashes the blocks that were waiting behind it. async_hash then hashes no
// block of its own, and its hash is pread_disk_io's. The hybrid torrent's
// v2 block hashes are computed as the blocks arrive: the background hash
// of blocks 1 to 3 is done before block 0 is written
TORRENT_TEST(hash_out_of_order)
{
	int const blocks = 4;
	lt::file_storage const fs = one_piece("out_of_order/file", blocks);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	for (bool const v2 : {false, true})
	{
		std::printf("v2: %d\n", int(v2));
		disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
		disk_env ref(lt::pread_disk_io_constructor, 1);
		lt::storage_index_t const ms = mem.add(fs, "out_of_order_memory", true, v2);
		lt::storage_index_t const rs = ref.add(fs, "out_of_order_pread", true, v2);
		std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();

		mem.write(ms, p, data, {0});
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued; }));
		if (v2)
		{
			TEST_CHECK(mem.run_until([&] {
				return lt::aux::memory_hasher_background_blocks_for_test() - background0 >= blocks - 1; }));
		}
		mem.write(ms, p, data, {1, 2, 3});
		// every block once for SHA-1, and once for SHA-256 with v2
		std::int64_t const all = v2 ? 2 * blocks : blocks;
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
			&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == all; }));

		std::int64_t const tail0 = lt::aux::memory_hasher_tail_blocks_for_test();
		int const v2_blocks = v2 ? blocks : 0;
		auto const mh = mem.hash(ms, p, true, v2_blocks);
		TEST_EQUAL(lt::aux::memory_hasher_tail_blocks_for_test() - tail0, 0);

		ref.write(rs, p, data);
		auto const rh = ref.hash(rs, p, true, v2_blocks);
		TEST_CHECK(!mh.error);
		TEST_CHECK(!rh.error);
		TEST_CHECK(mh.v1 == rh.v1);
		TEST_CHECK(mh.v1 == sha1(data));
		TEST_CHECK(mh.v2 == rh.v2);
	}
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
// memory, and its next block is written there. Then every block comes again,
// out of order, some of them already held: the entry fills up (a block it
// holds is kept, it does not start the piece over), and the hash is right
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

	auto const write_block = [&](int const block)
	{
		std::vector<char> const bytes(data.begin() + block * lt::default_block_size
			, data.begin() + (block + 1) * lt::default_block_size);
		bool done = false;
		mem.disk->async_write(ms, lt::peer_request{p, block * lt::default_block_size, lt::default_block_size}
			, bytes.data(), {}, [&done](lt::storage_error const& e) { TEST_CHECK(!e); done = true; });
		mem.disk->submit_jobs();
		TEST_CHECK(mem.run_until([&] { return done; }));
	};
	// the peers send the blocks again, out of order: 0 (held), 1 (held),
	// 3 (missing). A held block is kept, it does not start the piece over
	for (int const block : {0, 1, 3}) write_block(block);
	TEST_EQUAL(pool->held_bytes(), 4 * lt::default_block_size);
	// the entry is complete now, but the hash answered for it (the recheck)
	// had blocks missing: another copy of a held block is kept too
	write_block(2);
	TEST_EQUAL(pool->held_bytes(), 4 * lt::default_block_size);
	// the hash is pread's, and nothing is missing
	std::int64_t const missing_before = pool->hash_missing_blocks();
	auto const full = mem.hash(ms, p, true, 0);

	disk_env ref(lt::pread_disk_io_constructor, 1);
	lt::storage_index_t const rs = ref.add(fs, "recheck_partial_pread", true, false);
	ref.write(rs, p, data);
	auto const rh = ref.hash(rs, p, true, 0);

	TEST_CHECK(!full.error);
	TEST_CHECK(full.v1 == rh.v1);
	TEST_CHECK(full.v1 == sha1(data));
	TEST_EQUAL(pool->hash_missing_blocks(), missing_before);
}

// a fence waits for our own jobs issued before it. Every block is written
// and hashed in the background; the gate holds the async_hash that follows
// at its tail stage. async_stop_torrent (a fence) answers only once the
// gate is released, after the hash answered
TORRENT_TEST(stop_waits_for_async_hash)
{
	int const blocks = 4;
	lt::file_storage const fs = one_piece("stop_waits/file", blocks);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::storage_index_t const ms = mem.add(fs, "stop_waits", true, false);
	std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
	mem.write(ms, p, data);
	// the background hash let go of the entry: the gate holds the hash job
	// at its tail stage (the tail is empty)
	TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
		&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == blocks; }));

	memory_hasher_gate gate(p);
	std::vector<std::string> order;
	mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash
		, [&](lt::piece_index_t, lt::sha1_hash const& h, lt::storage_error const& e)
		{
			TEST_CHECK(!e);
			TEST_CHECK(h == sha1(data));
			order.emplace_back("hash");
		});
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));

	mem.disk->async_stop_torrent(ms, [&] { order.emplace_back("stop"); });
	mem.disk->submit_jobs();
	// the default backend answers its stop at once
	mem.run_for(lt::milliseconds(500));
	TEST_CHECK(order.empty());

	gate.release();
	TEST_CHECK(mem.run_until([&] { return order.size() == 2; }));
	TEST_CHECK(order == (std::vector<std::string>{"hash", "stop"}));
}

// the background hash of written blocks is not a job a fence waits for. The
// gate holds it, no async_hash is issued, and async_stop_torrent answers
TORRENT_TEST(stop_does_not_wait_for_background_hasher)
{
	lt::file_storage const fs = one_piece("stop_background/file", 4);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::storage_index_t const ms = mem.add(fs, "stop_background", true, false);
	memory_hasher_gate gate(p);
	mem.write(ms, p, data);
	TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
		&& memory_hasher_gate::waiting() == 1; }));

	bool stopped = false;
	mem.disk->async_stop_torrent(ms, [&] { stopped = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([&] { return stopped; }, lt::seconds(5)));
	// the background hash is still held
	TEST_EQUAL(memory_hasher_gate::waiting(), 1);
	gate.release();
}

// jobs behind a fence are parked in posting order. The gate holds the
// hashing thread while an async_hash2 of the piece is issued (our job, on a
// pinned entry); then async_clear_piece of the piece, a write of its block
// 0 with new bytes and a read of that block. While the gate holds, none of
// the three answers. Once released they answer in posting order, and the
// read sees the new bytes, copied when the write was called
TORRENT_TEST(write_after_clear_is_parked)
{
	lt::file_storage const fs = one_piece("parked/file", 4);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, false);

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::storage_index_t const ms = mem.add(fs, "parked", false, true);
	memory_hasher_gate gate(p);
	mem.write(ms, p, data);
	TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
		&& memory_hasher_gate::waiting() == 1; }));

	bool hashed2 = false;
	mem.disk->async_hash2(ms, p, 0, {}
		, [&](lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const& e)
		{
			TEST_CHECK(!e);
			hashed2 = true;
		});

	std::vector<std::string> order;
	mem.disk->async_clear_piece(ms, p, [&](lt::piece_index_t) { order.emplace_back("clear"); });

	std::vector<char> const fresh(std::size_t(lt::default_block_size), 'N');
	lt::peer_request const block0{p, 0, lt::default_block_size};
	{
		std::vector<char> buf = fresh;
		mem.disk->async_write(ms, block0, buf.data(), {}
			, [&](lt::storage_error const& e) { TEST_CHECK(!e); order.emplace_back("write"); });
		// the caller's buffer lives only until the call returns
		std::fill(buf.begin(), buf.end(), 'X');
	}
	std::vector<char> read_back;
	mem.disk->async_read(ms, block0, [&](lt::disk_buffer_holder b, lt::storage_error const& e)
		{
			TEST_CHECK(!e);
			if (!e) read_back.assign(b.data(), b.data() + lt::default_block_size);
			order.emplace_back("read");
		}, {});
	mem.disk->submit_jobs();

	mem.run_for(lt::milliseconds(500));
	TEST_CHECK(order.empty());
	TEST_CHECK(!hashed2);

	gate.release();
	TEST_CHECK(mem.run_until([&] { return order.size() == 3 && hashed2; }));
	TEST_CHECK(order == (std::vector<std::string>{"clear", "write", "read"}));
	TEST_CHECK(read_back == fresh);
	TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::memory);
}

// async_clear_piece of a piece in memory (its hash failed) leaves it
// without an entry and not in the file. The next write decides its place
// again: with the memory policy the block goes to memory, and the piece
// written again hashes right
TORRENT_TEST(clear_piece_new_entry)
{
	int const blocks = 4;
	lt::file_storage const fs = one_piece("clear_new_entry/file", blocks);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);
	std::vector<char> bad = data;
	std::size_t const corrupt = std::size_t(lt::default_block_size) + 10;
	bad[corrupt] = char(bad[corrupt] ^ 0x55);

	for (int const threads : {0, 1})
	{
		auto const pool = memory_pool();
		disk_env mem(lt::memory_disk_io_constructor(pool), threads);
		lt::storage_index_t const ms = mem.add(fs, "clear_new_entry", true, false);
		mem.write(ms, p, bad);
		auto const failed = mem.hash(ms, p, true, 0);
		TEST_CHECK(!failed.error);
		TEST_CHECK(failed.v1 != sha1(data));
		TEST_EQUAL(pool->held_bytes(), blocks * lt::default_block_size);
		lt::aux::memory_set_in_file_for_test(*mem.disk, ms, p, true);

		bool cleared = false;
		mem.disk->async_clear_piece(ms, p, [&](lt::piece_index_t const i)
			{
				TEST_CHECK(i == p);
				cleared = true;
			});
		mem.disk->submit_jobs();
		TEST_CHECK(mem.run_until([&] { return cleared; }));
		TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::none);
		TEST_CHECK(!lt::aux::memory_in_file_for_test(*mem.disk, ms, p));
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(pool->retired_bytes(), 0);

		// block 0 goes to memory again
		mem.write(ms, p, data, {1, 2, 3});
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued; }));
		TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::memory);
		TEST_EQUAL(pool->held_bytes(), lt::default_block_size);

		mem.write(ms, p, data, {0});
		auto const passed = mem.hash(ms, p, true, 0);
		TEST_CHECK(!passed.error);
		TEST_CHECK(passed.v1 == sha1(data));
		TEST_EQUAL(pool->held_bytes(), blocks * lt::default_block_size);
	}
}

// async_delete_files and remove_torrent free the memory of the torrent. An
// entry a hash job holds (the gate holds the job) is freed when the job
// lets go of it, the others at once
TORRENT_TEST(delete_and_remove_free_memory)
{
	int const blocks = 4;
	lt::file_storage fs;
	fs.set_piece_length(blocks * lt::default_block_size);
	fs.add_file("free_memory/file", 2 * blocks * lt::default_block_size, {});
	fs.set_num_pieces(2);
	lt::piece_index_t const p0{0};
	lt::piece_index_t const p1{1};
	std::int64_t const piece_bytes = blocks * lt::default_block_size;

	auto const pool = memory_pool();
	disk_env mem(lt::memory_disk_io_constructor(pool), 1);

	// writes both pieces and waits for their background hash
	auto const fill = [&](lt::storage_index_t const st)
	{
		std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
		mem.write(st, p0, piece_data(fs, p0, true));
		mem.write(st, p1, piece_data(fs, p1, true));
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
			&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == 2 * blocks; }));
		TEST_EQUAL(pool->held_bytes(), 2 * piece_bytes);
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), 2 * blocks);
	};
	// an async_hash of piece 0, held by a gate on piece 0: it pins the
	// entry. Returns the flag its handler sets
	auto const pin = [&](lt::storage_index_t const st)
	{
		auto hashed = std::make_shared<bool>(false);
		mem.disk->async_hash(st, p0, {}, lt::disk_interface::v1_hash
			, [hashed](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&) { *hashed = true; });
		mem.disk->submit_jobs();
		TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));
		return hashed;
	};

	// async_delete_files: its part in memory is done when it is raised
	{
		lt::storage_index_t const ms = mem.add(fs, "delete_free", true, false);
		fill(ms);
		lt::aux::memory_set_in_file_for_test(*mem.disk, ms, p1, true);
		memory_hasher_gate gate(p0);
		auto const hashed = pin(ms);
		bool deleted = false;
		mem.disk->async_delete_files(ms, lt::session_handle::delete_files
			, [&](lt::storage_error const&) { deleted = true; });
		mem.disk->submit_jobs();
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(pool->retired_bytes(), piece_bytes);
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), blocks);
		TEST_CHECK(!lt::aux::memory_in_file_for_test(*mem.disk, ms, p1));
		gate.release();
		TEST_CHECK(mem.run_until([&] { return deleted && *hashed; }));
		TEST_EQUAL(pool->retired_bytes(), 0);
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), 0);
	}

	// remove_torrent
	{
		lt::storage_index_t const ms = mem.add(fs, "remove_free", true, false);
		fill(ms);
		memory_hasher_gate gate(p0);
		auto const hashed = pin(ms);
		mem.storages.back().reset();
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), blocks);
		gate.release();
		TEST_CHECK(mem.run_until([&] { return *hashed; }));
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), 0);
	}
}

namespace {

// issues one of the eight fences of disk_interface on `st`. `done` is called
// by its handler, with its status and its error (none for a handler without
// them)
using fence_done = std::function<void(lt::status_t, lt::storage_error const&)>;
using fence_call = std::function<void(lt::disk_interface&, lt::storage_index_t, fence_done done)>;

struct fence_case
{
	char const* name;
	fence_call issue;
	// the fence is async_clear_piece of the piece the gate holds a hash of
	bool clears_hashed_piece;
	// its handler takes an error: on a removed storage it is
	// operation_aborted
	bool aborted_when_removed;
	// its handler takes a status: on a removed storage it is
	// fatal_disk_error
	bool fatal_when_removed;
	// the default backend closes the files of the storage for it
	bool closes_files;
};

std::vector<fence_case> every_fence(lt::piece_index_t const hashed)
{
	return {
		{"clear_piece", [hashed](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{ d.async_clear_piece(st, hashed, [done](lt::piece_index_t) { done({}, {}); }); }
			, true, false, false, false},
		{"move_storage", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{
				d.async_move_storage(st, "every_fence_moved", lt::move_flags_t::always_replace_files
					, [done](lt::status_t const st, std::string const&, lt::storage_error const& e) { done(st, e); });
			}, false, true, true, false},
		{"release_files", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{ d.async_release_files(st, [done] { done({}, {}); }); }
			, false, false, false, true},
		{"stop_torrent", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{ d.async_stop_torrent(st, [done] { done({}, {}); }); }
			, false, false, false, true},
		{"rename_file", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{
				d.async_rename_file(st, lt::file_index_t{0}, "every_fence/renamed"
					, [done](std::string const&, lt::file_index_t, lt::storage_error const& e) { done({}, e); });
			}, false, true, false, false},
		{"delete_files", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{
				d.async_delete_files(st, lt::session_handle::delete_files
					, [done](lt::storage_error const& e) { done({}, e); });
			}, false, true, false, false},
		{"set_file_priority", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{
				lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio;
				prio.push_back(lt::default_priority);
				d.async_set_file_priority(st, std::move(prio)
					, [done](lt::storage_error const& e, lt::aux::vector<lt::download_priority_t, lt::file_index_t>)
					{ done({}, e); });
			}, false, true, false, false},
		{"check_files", [](lt::disk_interface& d, lt::storage_index_t const st, fence_done done)
			{
				d.async_check_files(st, nullptr, {}
					, [done](lt::status_t const st, lt::storage_error const& e) { done(st, e); });
			}, false, true, true, false},
	};
}

// writes the file of a storage made by disk_env::add() of `fs` (one file,
// under `path`): `bytes` from its start
void write_file(std::string const& path, lt::file_storage const& fs, std::vector<char> const& bytes)
{
	std::string const file = lt::combine_path(path, fs.file_path(lt::file_index_t{0}));
	lt::error_code ec;
	lt::create_directories(lt::parent_path(file), ec);
	TEST_CHECK(!ec);
	std::ofstream f(file, std::ios::binary);
	f.write(bytes.data(), std::streamsize(bytes.size()));
}

} // anonymous namespace

// every fence of disk_interface is a fence on the memory path. Piece 0 is
// written and hashed in the background, and the gate holds an async_hash of
// it (our job). Then the fence, a write of a block of a piece in memory
// (piece 1, whose last block is missing; for async_clear_piece, piece 0
// itself) and a read of that block. Nothing answers while the gate holds;
// once released the handlers run hash -> fence -> write -> read, and the read
// returns the written bytes.
//
// The fences the default backend answers alone (release_files and
// stop_torrent: their answers carry nothing of it) show that they reach it:
// a read of piece 2, which is in the file and has no entry, makes the
// default backend open the file first, and the fence closes it
TORRENT_TEST(every_fence_parks_jobs)
{
	int const blocks = 4;
	lt::file_storage fs;
	fs.set_piece_length(blocks * lt::default_block_size);
	fs.add_file("every_fence/file", 3 * blocks * lt::default_block_size, {});
	fs.set_num_pieces(3);
	lt::piece_index_t const p{0};
	lt::piece_index_t const q{1};
	lt::piece_index_t const in_file{2};

	for (fence_case const& fc : every_fence(p))
	{
		std::printf("fence: %s\n", fc.name);
		disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
		mem.paths.push_back("every_fence_moved");
		std::string const path = std::string("every_fence_") + fc.name;
		lt::storage_index_t const ms = mem.add(fs, path, true, false);
		if (fc.closes_files)
		{
			std::vector<char> bytes(std::size_t(fs.total_size()), 0);
			std::vector<char> const third = piece_data(fs, in_file, true);
			std::copy(third.begin(), third.end(), bytes.begin() + std::ptrdiff_t(2 * fs.piece_length()));
			write_file(path, fs, bytes);
			lt::storage_error error;
			std::vector<char> const b = mem.read(ms, lt::peer_request{in_file, 0, lt::default_block_size}, error);
			TEST_CHECK(!error);
			TEST_CHECK(b.size() == std::size_t(lt::default_block_size)
				&& std::equal(b.begin(), b.end(), third.begin()));
			TEST_CHECK(!mem.disk->get_status(ms).empty());
		}
		std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
		mem.write(ms, p, piece_data(fs, p, true));
		mem.write(ms, q, piece_data(fs, q, true), {blocks - 1});
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
			&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == 2 * blocks - 1; }));

		memory_hasher_gate gate(p);
		std::vector<std::string> order;
		mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash
			, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&) { order.emplace_back("hash"); });
		mem.disk->submit_jobs();
		TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));

		fc.issue(*mem.disk, ms, [&](lt::status_t, lt::storage_error const&) { order.emplace_back("fence"); });
		lt::peer_request const r = fc.clears_hashed_piece
			? lt::peer_request{p, 0, lt::default_block_size}
			: lt::peer_request{q, (blocks - 1) * lt::default_block_size, lt::default_block_size};
		std::vector<char> const fresh(std::size_t(lt::default_block_size), 'N');
		mem.disk->async_write(ms, r, fresh.data(), {}
			, [&](lt::storage_error const& e) { TEST_CHECK(!e); order.emplace_back("write"); });
		std::vector<char> read_back;
		mem.disk->async_read(ms, r, [&](lt::disk_buffer_holder b, lt::storage_error const& e)
			{
				TEST_CHECK(!e);
				if (!e) read_back.assign(b.data(), b.data() + lt::default_block_size);
				order.emplace_back("read");
			}, {});
		mem.disk->submit_jobs();

		mem.run_for(lt::milliseconds(500));
		TEST_CHECK(order.empty());

		gate.release();
		TEST_CHECK(mem.run_until([&] { return order.size() == 4; }));
		TEST_CHECK(order == (std::vector<std::string>{"hash", "fence", "write", "read"}));
		TEST_CHECK(read_back == fresh);
		// the default backend closed the file it had open
		if (fc.closes_files) TEST_CHECK(mem.disk->get_status(ms).empty());
	}
}

// async_clear_piece waits for our jobs of its piece only: the gate holds an
// async_hash of piece 1, and a clear of piece 0 (in memory) answers
TORRENT_TEST(clear_piece_waits_for_its_piece_only)
{
	int const blocks = 4;
	lt::file_storage fs;
	fs.set_piece_length(blocks * lt::default_block_size);
	fs.add_file("clear_scope/file", 2 * blocks * lt::default_block_size, {});
	fs.set_num_pieces(2);
	lt::piece_index_t const p{0};
	lt::piece_index_t const q{1};

	disk_env mem(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::storage_index_t const ms = mem.add(fs, "clear_scope", true, false);
	std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
	mem.write(ms, p, piece_data(fs, p, true));
	mem.write(ms, q, piece_data(fs, q, true));
	TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
		&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == 2 * blocks; }));

	memory_hasher_gate gate(q);
	bool hashed = false;
	mem.disk->async_hash(ms, q, {}, lt::disk_interface::v1_hash
		, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&) { hashed = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));

	bool cleared = false;
	mem.disk->async_clear_piece(ms, p, [&](lt::piece_index_t) { cleared = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([&] { return cleared; }, lt::seconds(5)));
	TEST_CHECK(!hashed);
	TEST_EQUAL(memory_hasher_gate::waiting(), 1);
	gate.release();
	TEST_CHECK(mem.run_until([&] { return hashed; }));
}

#if defined TORRENT_LINUX
// async_clear_piece of a piece in the file goes to the default backend: that
// clear is a fence there, so it waits for the backend's write of the piece,
// which the write gate holds
TORRENT_TEST(clear_piece_in_file_goes_to_default_backend)
{
	lt::file_storage const fs = one_piece("clear_file_piece/file", 4);
	lt::piece_index_t const p{0};
	// the file policy: every piece goes to the file
	auto const pool = std::make_shared<lt::memory_storage_pool>();
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	disk_env mem(lt::memory_disk_io_constructor(pool), 1);
	lt::storage_index_t const ms = mem.add(fs, "clear_file_piece", true, false);

	set_hold_writes(true);
	mem.write(ms, p, piece_data(fs, p, true));
	bool hashed = false;
	mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash | lt::disk_interface::flush_piece
		, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&) { hashed = true; });
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([] {
		std::lock_guard<std::mutex> l(gate_mutex);
		return writes_waiting > 0; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::file);

	bool cleared = false;
	mem.disk->async_clear_piece(ms, p, [&](lt::piece_index_t) { cleared = true; });
	mem.disk->submit_jobs();
	mem.run_for(lt::milliseconds(500));
	TEST_CHECK(!cleared);

	set_hold_writes(false);
	TEST_CHECK(mem.run_until([&] { return cleared && hashed
		&& mem.writes_done == mem.writes_issued; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p) == lt::piece_place::none);
}
#endif

// jobs parked behind a fence of a storage that is removed answer
// operation_aborted when the fence lowers, and reach no backend. The fence
// that is up (a clear of a piece in memory) waits for an async_hash the gate
// holds. Behind it: a write, a read, a second async_hash, an async_hash2 and
// a fence, each of the eight in turn. Started on the removed storage, the
// fence answers without the default backend: with operation_aborted if its
// handler takes an error, and fatal_disk_error if it takes a status
TORRENT_TEST(parked_jobs_of_removed_storage_answer_aborted)
{
	lt::file_storage const fs = one_piece("removed_parked/file", 4);
	lt::piece_index_t const p{0};

	for (fence_case const& fc : every_fence(p))
	{
		std::printf("fence: %s\n", fc.name);
		auto const pool = memory_pool();
		disk_env mem(lt::memory_disk_io_constructor(pool), 1);
		mem.paths.push_back("every_fence_moved");
		lt::storage_index_t const ms = mem.add(fs, std::string("removed_parked_") + fc.name, true, true);
		std::int64_t const background0 = lt::aux::memory_hasher_background_blocks_for_test();
		mem.write(ms, p, piece_data(fs, p, true));
		// a v1 and a v2 hash of each block
		TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
			&& lt::aux::memory_hasher_background_blocks_for_test() - background0 == 8; }));

		memory_hasher_gate gate(p);
		std::vector<std::string> order;
		std::vector<std::string> aborted;
		auto const record = [&](char const* name, lt::storage_error const& e)
		{
			order.emplace_back(name);
			if (e.ec == boost::asio::error::operation_aborted) aborted.emplace_back(name);
		};
		mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash
			, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& e) { record("hash", e); });
		mem.disk->submit_jobs();
		TEST_CHECK(mem.run_until([] { return memory_hasher_gate::waiting() == 1; }));

		mem.disk->async_clear_piece(ms, p, [&](lt::piece_index_t) { order.emplace_back("clear"); });
		std::vector<char> const fresh(std::size_t(lt::default_block_size), 'N');
		lt::peer_request const r{p, 0, lt::default_block_size};
		mem.disk->async_write(ms, r, fresh.data(), {}
			, [&](lt::storage_error const& e) { record("write", e); });
		mem.disk->async_read(ms, r, [&](lt::disk_buffer_holder, lt::storage_error const& e)
			{ record("read", e); }, {});
		mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash
			, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& e) { record("rehash", e); });
		mem.disk->async_hash2(ms, p, 0, {}
			, [&](lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const& e) { record("hash2", e); });
		lt::status_t fence_status{};
		fc.issue(*mem.disk, ms, [&](lt::status_t const st, lt::storage_error const& e)
		{
			fence_status = st;
			record("fence", e);
		});
		mem.disk->submit_jobs();

		mem.storages.back().reset();
		gate.release();
		TEST_CHECK(mem.run_until([&] { return order.size() == 7; }));
		// nothing answers twice
		mem.run_for(lt::milliseconds(200));
		TEST_CHECK(order == (std::vector<std::string>{"hash", "clear", "write", "read", "rehash", "hash2", "fence"}));
		std::vector<std::string> expected{"write", "read", "rehash", "hash2"};
		if (fc.aborted_when_removed) expected.emplace_back("fence");
		TEST_CHECK(aborted == expected);
		TEST_EQUAL(bool(fence_status & lt::disk_status::fatal_disk_error), fc.fatal_when_removed);
		TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*mem.disk), 0);
	}
}

// a storage removed while a hashing thread holds an entry of it is retired,
// not destroyed: the pool keeps it (with the torrent object, here none)
// until the last pin drops, and frees it then. The held bytes leave at the
// removal, the pinned ones are counted as retired until the pin goes. The
// hash job answers, but its entry is no longer current: a late answer
// changes no counter of the pool (its missing block is not counted)
TORRENT_TEST(storage_removed_while_hasher_holds_entry)
{
	using access = lt::aux::memory_pool_test_access;
	lt::file_storage const fs = one_piece("removed_held/file", 4);
	lt::piece_index_t const p{0};

	auto const pool = memory_pool();
	int const destroyed0 = lt::aux::memory_storages_destroyed_pinned_for_test();
	std::int64_t const missing0 = pool->hash_missing_blocks();
	disk_env mem(lt::memory_disk_io_constructor(pool), 1);
	lt::storage_index_t const ms = mem.add(fs, "removed_held", true, false);

	memory_hasher_gate gate(p);
	// block 3 missing: the background hash of the first blocks holds the
	// entry at the gate
	mem.write(ms, p, piece_data(fs, p, true), {3});
	TEST_CHECK(mem.run_until([&] { return mem.writes_done == mem.writes_issued
		&& memory_hasher_gate::waiting() == 1; }));
	bool hashed = false;
	mem.disk->async_hash(ms, p, {}, lt::disk_interface::v1_hash
		, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&) { hashed = true; });
	mem.disk->submit_jobs();

	mem.storages.back().reset();
	TEST_EQUAL(access::retired_storages(*pool), 1);
	TEST_EQUAL(access::storages_retired_pinned(*pool), 1);
	TEST_EQUAL(pool->held_bytes(), 0);
	TEST_EQUAL(pool->retired_bytes(), 3 * lt::default_block_size);
	TEST_EQUAL(access::blocks_in_use(*pool), 3);

	gate.release();
	TEST_CHECK(mem.run_until([&] { return hashed && access::retired_storages(*pool) == 0; }));
	TEST_EQUAL(pool->retired_bytes(), 0);
	TEST_EQUAL(access::blocks_in_use(*pool), 0);
	TEST_EQUAL(access::storages_retired_pinned(*pool), 1);
	TEST_EQUAL(pool->hash_missing_blocks(), missing0);
	TEST_EQUAL(lt::aux::memory_storages_destroyed_pinned_for_test(), destroyed0);
}

#if defined TORRENT_LINUX
namespace {
	int reads_held()
	{
		std::lock_guard<std::mutex> l(gate_mutex);
		return reads_waiting_before;
	}
}

// an answer of the default backend to a hash of a piece without an entry
// (a full recheck of pieces that are in the file) carries no entry to
// compare. async_delete_files clears "in file" when its fence is raised,
// but the default backend answers a hash issued before the fence after
// that. The answer applies only if the piece was not retired since: here
// "in file" stays cleared. Piece 0, hashed before the delete, shows the
// answer does set it otherwise
TORRENT_TEST(inner_hash_answer_after_delete_files)
{
	int const blocks = 4;
	lt::file_storage fs;
	fs.set_piece_length(blocks * lt::default_block_size);
	fs.add_file("recheck/file", 2 * blocks * lt::default_block_size, {});
	fs.set_num_pieces(2);
	lt::piece_index_t const p0{0};
	lt::piece_index_t const p1{1};

	// the file policy: the pieces are the default backend's
	auto const pool = std::make_shared<lt::memory_storage_pool>();
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	disk_env mem(lt::memory_disk_io_constructor(pool), 1);
	lt::storage_index_t const ms = mem.add(fs, "recheck", true, false);

	// the file of an earlier run: the storage has no entry for its pieces
	std::vector<char> content = piece_data(fs, p0, true);
	std::vector<char> const second = piece_data(fs, p1, true);
	content.insert(content.end(), second.begin(), second.end());
	{
		lt::error_code ec;
		lt::create_directories(lt::combine_path("recheck", "recheck"), ec);
		TEST_CHECK(!ec);
		std::ofstream f(lt::combine_path("recheck", lt::combine_path("recheck", "file")), std::ios::binary);
		f.write(content.data(), std::streamsize(content.size()));
	}

	{
		auto const h = mem.hash(ms, p0, true, 0);
		TEST_CHECK(!h.error);
		TEST_CHECK(h.v1 == sha1(piece_data(fs, p0, true)));
		TEST_CHECK(lt::aux::memory_in_file_for_test(*mem.disk, ms, p0));
		TEST_CHECK(lt::aux::memory_place_for_test(*mem.disk, ms, p0) == lt::piece_place::none);
	}

	set_hold_reads(true, false);
	bool hashed = false;
	lt::storage_error hash_error;
	mem.disk->async_hash(ms, p1, {}, lt::disk_interface::v1_hash
		, [&](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& e)
		{
			hash_error = e;
			hashed = true;
		});
	mem.disk->submit_jobs();
	TEST_CHECK(mem.run_until([] { return reads_held() > 0; }));

	bool deleted = false;
	mem.disk->async_delete_files(ms, lt::session_handle::delete_files
		, [&](lt::storage_error const&) { deleted = true; });
	mem.disk->submit_jobs();
	mem.run_for(lt::milliseconds(200));
	TEST_CHECK(!hashed);
	TEST_CHECK(!deleted);
	// cleared when the fence was raised
	TEST_CHECK(!lt::aux::memory_in_file_for_test(*mem.disk, ms, p0));

	set_hold_reads(false, false);
	TEST_CHECK(mem.run_until([&] { return hashed && deleted; }));
	TEST_CHECK(!hash_error);
	TEST_CHECK(!lt::aux::memory_in_file_for_test(*mem.disk, ms, p1));
	TEST_CHECK(!lt::aux::memory_in_file_for_test(*mem.disk, ms, p0));
}
#endif

namespace {

#if defined TORRENT_LINUX
// the writes held at the write gate now
int held_writes()
{
	std::lock_guard<std::mutex> l(gate_mutex);
	return writes_waiting;
}
#endif

// the answers of the jobs waiting on a tail
struct tail_answers
{
	int answered = 0;
	int aborted = 0;
	void operator()(lt::storage_error const& e)
	{
		if (e.ec == boost::asio::error::operation_aborted) ++aborted;
		++answered;
	}
};

#if defined TORRENT_LINUX
// a piece held in memory is moved to the file (persist set of owner 1). The
// write gate holds the default backend's writes of the move, so its step
// does not end. The piece is forgotten, and a write, a read, a hash and a
// block hash of it go to the default backend: they wait on the tail of the
// transfer. A fence (async_rename_file) issued after them waits behind them.
// Then `end` runs (it removes the storage or aborts the disk_interface)
// while the step's writes are still held: the jobs on the tail answer
// operation_aborted at once. The fence waits for the step, one of our jobs:
// it answers once the writes are released (on a removed storage with
// operation_aborted, without the default backend)
void parked_on_tail_answered(std::function<void(disk_env&, lt::storage_index_t)> const& end
	, bool const removed)
{
	disk_env env(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::file_storage fs;
	fs.set_piece_length(2 * lt::default_block_size);
	fs.add_file("tail/a", 4 * lt::default_block_size, {});
	fs.set_num_pieces(2);
	lt::storage_index_t const st = env.add(fs, "tail_torrent", true, false);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);
	env.write(st, p, data);
	TEST_CHECK(env.run_until([&] { return env.writes_done == env.writes_issued; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*env.disk, st, p) == lt::piece_place::memory);

	set_hold_writes(true);
	lt::piece_index_t const set[] = {p};
	lt::aux::memory_set_persist_for_test(*env.disk, st, set);
	TEST_CHECK(env.run_until([] { return held_writes() > 0; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*env.disk, st, p) == lt::piece_place::transfer);
	lt::aux::memory_forget_for_test(*env.disk, st, p);

	tail_answers a;
	lt::peer_request const r{p, 0, lt::default_block_size};
	env.disk->async_write(st, r, data.data(), {}, [&a](lt::storage_error const& e) { a(e); });
	env.disk->async_read(st, r, [&a](lt::disk_buffer_holder, lt::storage_error const& e) { a(e); });
	env.disk->async_hash(st, p, {}, lt::disk_interface::v1_hash
		, [&a](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& e) { a(e); });
	env.disk->async_hash2(st, p, 0, {}
		, [&a](lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const& e) { a(e); });
	bool fenced = false;
	lt::storage_error fence_error;
	env.disk->async_rename_file(st, lt::file_index_t{0}, "tail/renamed"
		, [&](std::string const&, lt::file_index_t, lt::storage_error const& e)
		{
			fenced = true;
			fence_error = e;
		});
	env.disk->submit_jobs();
	TEST_EQUAL(lt::aux::memory_tail_jobs_for_test(*env.disk, st, p), 4);
	TEST_EQUAL(lt::aux::memory_fence_jobs_for_test(*env.disk, st), 1);

	end(env, st);
	TEST_CHECK(env.run_until([&] { return a.answered == 4; }));
	TEST_EQUAL(a.aborted, 4);
	// the step's writes are still held
	TEST_CHECK(held_writes() > 0);
	TEST_CHECK(!fenced);

	set_hold_writes(false);
	TEST_CHECK(env.run_until([&] { return fenced; }));
	if (removed) TEST_CHECK(fence_error.ec == boost::asio::error::operation_aborted);
	env.run_for(lt::milliseconds(200));
	TEST_EQUAL(a.answered, 4);
}
#endif

} // anonymous namespace

#if defined TORRENT_LINUX
TORRENT_TEST(transfer_parked_answered_on_remove)
{
	parked_on_tail_answered([](disk_env& env, lt::storage_index_t const st)
	{
		for (auto& h : env.storages)
			if (static_cast<lt::storage_index_t>(h) == st) h.reset();
	}, true);
}

TORRENT_TEST(transfer_parked_answered_on_abort)
{
	parked_on_tail_answered([](disk_env& env, lt::storage_index_t)
	{
		env.disk->abort(false);
	}, false);
}
#endif

namespace {

// a v1 storage of two pieces of two blocks each
lt::file_storage two_block_pieces(std::string const& name)
{
	lt::file_storage fs;
	fs.set_piece_length(2 * lt::default_block_size);
	fs.add_file(name + "/a", 4 * lt::default_block_size, {});
	fs.set_num_pieces(2);
	return fs;
}

} // anonymous namespace

// a one-shot persist() is spent when a partial piece's move ends in the
// file too: the piece forgotten and written again goes to memory.
//
// The move writes the blocks of the partial piece with flush_piece, which
// trips pread_disk_io's invariant check (disk_cache.cpp, check_invariant)
// in builds with invariant checks (open): the test runs in builds without
#if !TORRENT_USE_INVARIANT_CHECKS
TORRENT_TEST(partial_transfer_spends_one_shot)
{
	disk_env env(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::file_storage const fs = two_block_pieces("partial_one_shot");
	lt::storage_index_t const st = env.add(fs, "partial_one_shot_torrent", true, false);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);

	// block 0 only: a partial piece in memory
	env.write(st, p, data, {1});
	TEST_CHECK(env.run_until([&] { return env.writes_done == env.writes_issued; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*env.disk, st, p) == lt::piece_place::memory);

	lt::piece_index_t const set[] = {p};
	lt::aux::memory_persist_for_test(*env.disk, st, set);
	// the step runs on the network thread (here: this io_context)
	TEST_CHECK(env.run_until([&] { return lt::aux::memory_steps_issued_for_test(*env.disk, st) == 1; }));
	// block 1 goes to the default backend; the piece completes and is
	// flushed, which answers the move's write
	env.write(st, p, data, {0});
	TEST_CHECK(env.run_until([&] { return env.writes_done == env.writes_issued; }));
	TEST_CHECK(env.run_until([&] {
		return lt::aux::memory_place_for_test(*env.disk, st, p) == lt::piece_place::file; }));
	TEST_EQUAL(lt::aux::memory_blocks_in_use_for_test(*env.disk), 0);

	lt::aux::memory_forget_for_test(*env.disk, st, p);
	env.write(st, p, data);
	TEST_CHECK(env.run_until([&] { return env.writes_done == env.writes_issued; }));
	TEST_CHECK(lt::aux::memory_place_for_test(*env.disk, st, p) == lt::piece_place::memory);
}
#endif

#if defined TORRENT_LINUX
// a transfer can start after the torrent's last fence, and the storage be
// removed while its writes are in the default backend (held here at the
// write gate). The default backend's storage stays until the step is
// answered, then goes: no job of the default backend outlives its storage,
// and the transfer's entry is freed. The storage is drained from the
// removal until the step is answered (finish_step() lets it go, not the
// destructor of the disk_interface)
TORRENT_TEST(transfer_keeps_inner_after_remove)
{
	disk_env env(lt::memory_disk_io_constructor(memory_pool()), 1);
	lt::file_storage const fs = two_block_pieces("transfer_remove");
	lt::storage_index_t const st = env.add(fs, "transfer_remove_torrent", true, false);
	lt::piece_index_t const p{0};
	std::vector<char> const data = piece_data(fs, p, true);
	env.write(st, p, data);
	TEST_CHECK(env.run_until([&] { return env.writes_done == env.writes_issued; }));

	set_hold_writes(true);
	lt::piece_index_t const set[] = {p};
	lt::aux::memory_set_persist_for_test(*env.disk, st, set);
	TEST_CHECK(env.run_until([] { return held_writes() > 0; }));
	TEST_EQUAL(lt::aux::memory_draining_for_test(*env.disk), 0);

	for (auto& h : env.storages)
		if (static_cast<lt::storage_index_t>(h) == st) h.reset();
	TEST_EQUAL(lt::aux::memory_draining_for_test(*env.disk), 1);
	env.run_for(lt::milliseconds(200));
	// the retired entry is pinned by the step
	TEST_CHECK(lt::aux::memory_blocks_in_use_for_test(*env.disk) > 0);
	TEST_EQUAL(lt::aux::memory_draining_for_test(*env.disk), 1);

	set_hold_writes(false);
	TEST_CHECK(env.run_until([&] { return lt::aux::memory_blocks_in_use_for_test(*env.disk) == 0
		&& lt::aux::memory_draining_for_test(*env.disk) == 0; }));
	env.run_for(lt::milliseconds(200));
}
#endif
