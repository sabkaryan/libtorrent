/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "settings.hpp" // for settings()
#include "setup_transfer.hpp" // for remove_all
#include "memory_gates.hpp"

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/torrent_status.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/create_torrent.hpp"
#include "libtorrent/load_torrent.hpp"
#include "libtorrent/bencode.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/pread_disk_io.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/address.hpp"
#include "libtorrent/peer_request.hpp"
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/aux_/random.hpp"
#include "libtorrent/aux_/session_impl.hpp"
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp"
#include "libtorrent/aux_/memory_storage.hpp" // for memory_storages_destroyed_pinned_for_test
#include "libtorrent/torrent_info.hpp"
#include "libtorrent/socket.hpp"

// interposes pwrite() for this binary: include it from this file only
#include "write_gate.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <sys/stat.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

using namespace lt;

namespace {

int const piece_size = 64 * 1024;
int const num_pieces = 7;
// two files, the boundary inside piece 1, the last piece short
std::int64_t const file_a = 100000;
std::int64_t const file_b = std::int64_t(num_pieces) * piece_size - 1000 - file_a;

std::vector<char> random_content()
{
	std::vector<char> ret(static_cast<std::size_t>(file_a + file_b));
	aux::random_bytes(ret);
	return ret;
}

// a v1 torrent over the content, every piece with its own bytes
add_torrent_params make_torrent(std::vector<char> const& content)
{
	std::vector<create_file_entry> fs;
	fs.emplace_back("unregistered/a", file_a);
	fs.emplace_back("unregistered/b", file_b);
	lt::create_torrent t(std::move(fs), piece_size, create_torrent::v1_only);
	for (auto const p : t.piece_range())
	{
		std::size_t const start = std::size_t(static_cast<int>(p)) * piece_size;
		std::size_t const len = std::min(content.size() - start, std::size_t(piece_size));
		t.set_hash(p, hasher(content.data() + start, int(len)).final());
	}
	return load_torrent_buffer(bencode(t.generate()));
}

settings_pack session_settings()
{
	settings_pack p = settings();
	p.set_int(settings_pack::alert_mask, alert_category::status
		| alert_category::storage | alert_category::piece_progress
		| alert_category::error);
	p.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	return p;
}

// pops alerts until one of type T satisfies pred, for at most 10 seconds
template <typename T, typename Pred>
bool wait_alert(lt::session& ses, Pred pred)
{
	auto const end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (std::chrono::steady_clock::now() < end)
	{
		ses.wait_for_alert(std::chrono::milliseconds(100));
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
		{
			if (auto const* e = alert_cast<torrent_error_alert>(a))
				std::printf("torrent error: %s\n", e->message().c_str());
			auto const* t = alert_cast<T>(a);
			if (t != nullptr && pred(*t)) return true;
		}
	}
	return false;
}

std::vector<char> read_file(std::string const& path)
{
	std::ifstream f(path, std::ios::binary);
	return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

struct run_result
{
	add_torrent_params resume;
	// the files of the torrent, concatenated
	std::vector<char> files;
	// memory_storage_pool::held_bytes() once every piece passed
	std::int64_t held = -1;
};

// adds the torrent, writes every piece with add_piece(), waits for it to
// finish and saves its resume data. The files are read back after the
// session is gone
run_result run(disk_io_constructor_type disk_io, memory_storage_pool const* pool
	, std::string const& save_path, std::vector<char> const& content)
{
	error_code ec;
	remove_all(save_path, ec);
	run_result ret;
	{
		session_params sp(session_settings());
		sp.disk_io_constructor = std::move(disk_io);
		lt::session ses(std::move(sp));

		add_torrent_params atp = make_torrent(content);
		atp.save_path = save_path;
		atp.flags &= ~torrent_flags::auto_managed;
		atp.flags &= ~torrent_flags::paused;
		torrent_handle const th = ses.add_torrent(atp);
		for (int i = 0; i < 200 && th.status().state != torrent_status::downloading; ++i)
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		TEST_EQUAL(th.status().state, torrent_status::downloading);

		for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
			th.add_piece(p, content.data() + std::size_t(static_cast<int>(p)) * piece_size);

		// the resume data lists the pieces written to the files, which may
		// be later than their hash check: wait for both
		bool finished = false;
		std::set<piece_index_t> flushed;
		auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while ((!finished || int(flushed.size()) < num_pieces)
			&& std::chrono::steady_clock::now() < deadline)
		{
			ses.wait_for_alert(std::chrono::milliseconds(100));
			std::vector<alert*> alerts;
			ses.pop_alerts(&alerts);
			for (alert* a : alerts)
			{
				if (alert_cast<torrent_finished_alert>(a)) finished = true;
				if (auto const* f = alert_cast<piece_flushed_alert>(a))
					flushed.insert(f->piece_index);
			}
		}
		TEST_CHECK(finished);
		TEST_EQUAL(int(flushed.size()), num_pieces);
		if (pool) ret.held = pool->held_bytes();

		th.save_resume_data(torrent_handle::flush_disk_cache);
		auto const end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		bool saved = false;
		while (!saved && std::chrono::steady_clock::now() < end)
		{
			ses.wait_for_alert(std::chrono::milliseconds(100));
			std::vector<alert*> alerts;
			ses.pop_alerts(&alerts);
			for (alert* a : alerts)
			{
				if (auto const* r = alert_cast<save_resume_data_alert>(a))
				{
					ret.resume = r->params;
					saved = true;
				}
			}
		}
		TEST_CHECK(saved);
		ses.remove_torrent(th);
	}
	ret.files = read_file(combine_path(save_path, combine_path("unregistered", "a")));
	std::vector<char> const b = read_file(combine_path(save_path, combine_path("unregistered", "b")));
	ret.files.insert(ret.files.end(), b.begin(), b.end());
	remove_all(save_path, ec);
	return ret;
}

} // anonymous namespace

// a torrent that is not registered with the pool, under the default policy,
// goes to the file: the bytes and the resume data are those of pread_disk_io
TORRENT_TEST(unregistered_torrent_goes_to_file)
{
	std::vector<char> const content = random_content();

	// the pool has room: the torrent goes to the file for its policy alone
	auto pool = std::make_shared<memory_storage_pool>();
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	run_result const mem = run(memory_disk_io_constructor(pool), pool.get()
		, complete("unregistered_memory"), content);
	run_result const ref = run(pread_disk_io_constructor, nullptr
		, complete("unregistered_pread"), content);

	TEST_CHECK(mem.files == content);
	TEST_CHECK(ref.files == content);
	TEST_EQUAL(mem.held, 0);

	TEST_EQUAL(mem.resume.have_pieces.size(), num_pieces);
	TEST_EQUAL(mem.resume.have_pieces.count(), num_pieces);
	TEST_CHECK(mem.resume.have_pieces == ref.resume.have_pieces);
	TEST_CHECK(mem.resume.verified_pieces == ref.resume.verified_pieces);
	TEST_EQUAL(mem.resume.unfinished_pieces.size(), ref.resume.unfinished_pieces.size());
	TEST_CHECK(mem.resume.file_priorities == ref.resume.file_priorities);
	TEST_CHECK(mem.resume.piece_priorities == ref.resume.piece_priorities);
	TEST_EQUAL(mem.resume.total_downloaded, ref.resume.total_downloaded);
	TEST_CHECK(mem.resume.flags == ref.resume.flags);
}

// two torrents live in one session at the same time: each one's writes,
// hashes and file operations reach the storage the default backend created
// for it, not the other one's
TORRENT_TEST(two_torrents_keep_their_own_storage)
{
	std::vector<char> const content_a = random_content();
	std::vector<char> const content_b = random_content();
	std::string const path_a = complete("two_torrents_a");
	std::string const path_b = complete("two_torrents_b");
	error_code ec;
	remove_all(path_a, ec);
	remove_all(path_b, ec);
	{
		session_params sp(session_settings());
		sp.disk_io_constructor = memory_disk_io_constructor(std::make_shared<memory_storage_pool>());
		lt::session ses(std::move(sp));

		std::vector<torrent_handle> handles;
		for (auto const& [content, path] : {std::make_pair(&content_a, &path_a)
			, std::make_pair(&content_b, &path_b)})
		{
			add_torrent_params atp = make_torrent(*content);
			atp.save_path = *path;
			atp.flags &= ~torrent_flags::auto_managed;
			atp.flags &= ~torrent_flags::paused;
			handles.push_back(ses.add_torrent(atp));
		}
		for (auto const& th : handles)
		{
			for (int i = 0; i < 200 && th.status().state != torrent_status::downloading; ++i)
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			TEST_EQUAL(th.status().state, torrent_status::downloading);
		}
		// the second torrent first, then the first one
		for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
			handles[1].add_piece(p, content_b.data() + std::size_t(static_cast<int>(p)) * piece_size);
		for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
			handles[0].add_piece(p, content_a.data() + std::size_t(static_cast<int>(p)) * piece_size);

		int finished = 0;
		TEST_CHECK(wait_alert<torrent_finished_alert>(ses
			, [&finished](torrent_finished_alert const&) { return ++finished == 2; }));
		for (auto const& th : handles) ses.remove_torrent(th);
		// both alerts may come in one batch: count them in one wait
		int removed = 0;
		TEST_CHECK(wait_alert<torrent_removed_alert>(ses
			, [&removed](torrent_removed_alert const&) { return ++removed == 2; }));
	}
	for (auto const& [content, path] : {std::make_pair(&content_a, &path_a)
		, std::make_pair(&content_b, &path_b)})
	{
		std::vector<char> files = read_file(combine_path(*path, combine_path("unregistered", "a")));
		std::vector<char> const b = read_file(combine_path(*path, combine_path("unregistered", "b")));
		files.insert(files.end(), b.begin(), b.end());
		TEST_CHECK(files == *content);
		remove_all(*path, ec);
	}
}

// a piece kept in memory is "flushed" (in the pool) once it passed its hash
// check, not before: piece_flushed_alert follows piece_finished_alert. The
// bytes the pool holds are the piece's
TORRENT_TEST(piece_flushed_after_hash)
{
	std::vector<char> const content = random_content();
	std::string const save_path = complete("piece_flushed_after_hash");
	error_code ec;
	remove_all(save_path, ec);
	auto pool = std::make_shared<memory_storage_pool>();
	pool->set_default_policy(memory_policy::memory);
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	{
		session_params sp(session_settings());
		sp.disk_io_constructor = memory_disk_io_constructor(pool);
		lt::session ses(std::move(sp));

		add_torrent_params atp = make_torrent(content);
		atp.save_path = save_path;
		atp.flags &= ~torrent_flags::auto_managed;
		atp.flags &= ~torrent_flags::paused;
		torrent_handle const th = ses.add_torrent(atp);
		for (int i = 0; i < 200 && th.status().state != torrent_status::downloading; ++i)
			std::this_thread::sleep_for(std::chrono::milliseconds(50));
		TEST_EQUAL(th.status().state, torrent_status::downloading);

		piece_index_t const p{0};
		th.add_piece(p, content.data());

		// the order of the two alerts of piece 0
		std::vector<char const*> order;
		auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (order.size() < 2 && std::chrono::steady_clock::now() < deadline)
		{
			ses.wait_for_alert(std::chrono::milliseconds(100));
			std::vector<alert*> alerts;
			ses.pop_alerts(&alerts);
			for (alert* a : alerts)
			{
				if (auto const* f = alert_cast<piece_finished_alert>(a))
					if (f->piece_index == p) order.push_back("finished");
				if (auto const* f = alert_cast<piece_flushed_alert>(a))
					if (f->piece_index == p) order.push_back("flushed");
				if (auto const* f = alert_cast<hash_failed_alert>(a))
					if (f->piece_index == p) order.push_back("hash failed");
			}
		}
		TEST_EQUAL(int(order.size()), 2);
		if (order.size() == 2)
		{
			TEST_EQUAL(std::string(order[0]), "finished");
			TEST_EQUAL(std::string(order[1]), "flushed");
		}

		std::vector<char> buf(static_cast<std::size_t>(piece_size));
		TEST_EQUAL(pool->read(th, p, 0, buf), piece_size);
		TEST_CHECK(std::equal(buf.begin(), buf.end(), content.begin()));
		TEST_EQUAL(pool->held_bytes(), piece_size);

		ses.remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(ses
			, [](torrent_removed_alert const&) { return true; }));
	}
	// nothing reached the file
	std::vector<char> const a = read_file(combine_path(save_path, combine_path("unregistered", "a")));
	TEST_CHECK(std::all_of(a.begin(), a.end(), [](char const c) { return c == 0; }));
	remove_all(save_path, ec);
}

namespace {

using access = aux::memory_pool_test_access;

// a pool with the given default policy and no limit
std::shared_ptr<memory_storage_pool> make_pool(memory_policy const policy)
{
	auto pool = std::make_shared<memory_storage_pool>();
	pool->set_default_policy(policy);
	pool->set_limit(std::numeric_limits<std::int64_t>::max());
	return pool;
}

// a session on the pool, with one hashing thread
std::unique_ptr<lt::session> make_session(std::shared_ptr<memory_storage_pool> const& pool)
{
	settings_pack sett = session_settings();
	sett.set_int(settings_pack::hashing_threads, 1);
	session_params sp(sett);
	sp.disk_io_constructor = memory_disk_io_constructor(pool);
	return std::make_unique<lt::session>(std::move(sp));
}

bool wait_state(torrent_handle const& th, torrent_status::state_t const st)
{
	for (int i = 0; i < 200; ++i)
	{
		if (th.status().state == st) return true;
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	return false;
}

template <typename Pred>
bool wait_for(Pred pred)
{
	for (int i = 0; i < 500; ++i)
	{
		if (pred()) return true;
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
	return pred();
}

// adds the torrent, not paused, into an empty save_path
torrent_handle add(lt::session& ses, add_torrent_params atp, std::string const& save_path)
{
	error_code ec;
	remove_all(save_path, ec);
	atp.save_path = save_path;
	atp.flags &= ~torrent_flags::auto_managed;
	atp.flags &= ~torrent_flags::paused;
	return ses.add_torrent(std::move(atp));
}

int piece_len(std::vector<char> const& content, piece_index_t const p)
{
	std::size_t const start = std::size_t(static_cast<int>(p)) * piece_size;
	return int(std::min(content.size() - start, std::size_t(piece_size)));
}

char const* piece_ptr(std::vector<char> const& content, piece_index_t const p)
{
	return content.data() + std::size_t(static_cast<int>(p)) * piece_size;
}

// the piece with one byte of its block `block` changed
std::vector<char> corrupt_piece(std::vector<char> const& content, piece_index_t const p, int const block)
{
	std::vector<char> ret(piece_ptr(content, p), piece_ptr(content, p) + piece_len(content, p));
	ret[std::size_t(block) * default_block_size] ^= 0x55;
	return ret;
}

// adds the pieces with add_piece() and waits until each one is flushed
bool add_pieces(lt::session& ses, torrent_handle const& th, std::vector<char> const& content
	, std::vector<piece_index_t> const& pieces)
{
	for (auto const p : pieces) th.add_piece(p, piece_ptr(content, p));
	std::set<piece_index_t> left(pieces.begin(), pieces.end());
	return wait_alert<piece_flushed_alert>(ses, [&left](piece_flushed_alert const& a)
		{
			left.erase(a.piece_index);
			return left.empty();
		});
}

bool hash_failed(lt::session& ses, piece_index_t const p)
{
	return wait_alert<hash_failed_alert>(ses, [p](hash_failed_alert const& a) { return a.piece_index == p; });
}

// the pool's bytes of the piece are the content's
bool reads(memory_storage_pool const& pool, torrent_handle const& th
	, std::vector<char> const& content, piece_index_t const p)
{
	int const len = piece_len(content, p);
	std::vector<char> buf(std::size_t(len), '\0');
	if (pool.read(th, p, 0, buf) != len) return false;
	return std::equal(buf.begin(), buf.end(), piece_ptr(content, p));
}

int read_code(memory_storage_pool const& pool, torrent_handle const& th, piece_index_t const p)
{
	std::vector<char> buf(std::size_t(piece_size), '\0');
	return pool.read(th, p, 0, buf);
}

// the "in file" flag of the piece, read on the network thread
bool in_file(lt::session& ses, torrent_handle const& th, piece_index_t const p)
{
	std::shared_ptr<aux::session_impl> const s = ses.native_handle();
	std::shared_ptr<aux::torrent> const t = th.native_handle();
	std::promise<bool> result;
	boost::asio::dispatch(s->get_context(), [&result, s, t, p]
		{ result.set_value(aux::memory_in_file_for_test(s->disk_thread(), t->storage(), p)); });
	return result.get_future().get();
}

// a torrent over one file of the content, hashed from the file itself
// (v2 and hybrid torrents need the merkle trees of the file)
add_torrent_params torrent_from_file(std::vector<char> const& content, std::string const& dir
	, create_flags_t const flags)
{
	error_code ec;
	remove_all(dir, ec);
	lt::create_directories(combine_path(dir, "gen"), ec);
	TEST_CHECK(!ec);
	{
		std::ofstream f(combine_path(dir, combine_path("gen", "a")), std::ios::binary);
		f.write(content.data(), std::streamsize(content.size()));
	}
	std::vector<create_file_entry> fs;
	fs.emplace_back("gen/a", std::int64_t(content.size()));
	lt::create_torrent t(std::move(fs), piece_size, flags);
	set_piece_hashes(t, dir, ec);
	TEST_CHECK(!ec);
	add_torrent_params ret = load_torrent_buffer(bencode(t.generate()));
	remove_all(dir, ec);
	return ret;
}

std::vector<char> four_pieces()
{
	std::vector<char> ret(std::size_t(4 * piece_size));
	aux::random_bytes(ret);
	return ret;
}

} // anonymous namespace

// property 2: pool.forget_piece() of a piece in memory answers 0 and the
// place it had; its bytes are gone (read: not_in_memory, held 0). Written
// again, the piece is read back
TORRENT_TEST(forget_frees_memory)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("forget_frees_memory");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_EQUAL(pool->held_bytes(), piece_size);

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 0);
		TEST_CHECK(r.place == piece_place::memory);
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(pool->retired_bytes(), 0);
		TEST_CHECK(!th.have_piece(p));

		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_EQUAL(pool->held_bytes(), piece_size);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 3: pool.forget_piece() that libtorrent answers with a code other
// than 0 drops nothing. Code 1: a plain torrent_handle::forget_piece() made
// libtorrent forget the piece, whose bytes the pool still holds. Code 3: a
// piece in the file that passed its hash but whose blocks are not written
// yet (the write gate holds them); its entry keeps its place
TORRENT_TEST(forget_nonzero_keeps_memory)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("forget_nonzero");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_EQUAL(th.forget_piece(p), 0);

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 1);
		TEST_CHECK(r.place == piece_place::memory);
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_EQUAL(pool->held_bytes(), piece_size);

#if defined TORRENT_LINUX
		// piece 4 lies in file 1, whose claim is the file
		piece_index_t const q{4};
		file_index_t const files[] = {file_index_t{1}};
		pool->set_policy(th, 1, memory_policy::file, files);
		set_hold_writes(true);
		th.add_piece(q, piece_ptr(content, q));
		TEST_CHECK(wait_alert<piece_finished_alert>(*ses
			, [q](piece_finished_alert const& a) { return a.piece_index == q; }));
		memory_forget_result const busy = pool->forget_piece(th, q);
		TEST_EQUAL(busy.code, 3);
		TEST_CHECK(busy.place == piece_place::file);
		set_hold_writes(false);
		TEST_CHECK(wait_alert<piece_flushed_alert>(*ses
			, [q](piece_flushed_alert const& a) { return a.piece_index == q; }));
		memory_forget_result const later = pool->forget_piece(th, q);
		TEST_EQUAL(later.code, 0);
		TEST_CHECK(later.place == piece_place::file);
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_EQUAL(pool->held_bytes(), piece_size);
#endif
	}
	error_code ec;
	remove_all(path, ec);
}

// property 4: a plain torrent_handle::forget_piece() leaves the piece in
// memory; downloaded again, it starts over: new bytes with a corrupt block
// fail the hash (they are not merged into the old entry, whose hash would
// pass). Written again right, it passes and the pool holds it once
TORRENT_TEST(plain_forget_then_rewrite)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("plain_forget_rewrite");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_EQUAL(th.forget_piece(p), 0);

		std::vector<char> const bad = corrupt_piece(content, p, 1);
		th.add_piece(p, bad.data());
		TEST_CHECK(hash_failed(*ses, p));

		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_EQUAL(pool->held_bytes(), piece_size);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 5: generations. A v2 torrent; the gate holds the hashing thread
// on an async_hash2 of a piece it has (its entry pinned). Under it,
// pool.forget_piece() answers 0 and the piece is written again with a
// corrupt block 0: the pool holds one generation, the old one is retired.
// Released, the old job neither frees the new entry's blocks nor stores
// its (right) hash of block 0 in the new entry: the new piece fails its
// hash, and once cleared nothing is left
TORRENT_TEST(generations_hash2_pin)
{
	std::vector<char> const content = four_pieces();
	std::string const path = complete("generations_hash2");
	add_torrent_params const atp = torrent_from_file(content, complete("generations_src")
		, create_torrent::v2_only);
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, atp, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		memory_hasher_gate gate(p);
		std::shared_ptr<aux::session_impl> const s = ses->native_handle();
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		std::atomic<bool> hashed2{false};
		boost::asio::dispatch(s->get_context(), [&hashed2, s, t, p]
		{
			s->disk_thread().async_hash2(t->storage(), p, 0, {}
				, [&hashed2](piece_index_t, sha256_hash const&, storage_error const&) { hashed2 = true; });
			s->disk_thread().submit_jobs();
		});
		TEST_CHECK(wait_for([] { return memory_hasher_gate::waiting() == 1; }));

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 0);
		TEST_CHECK(r.place == piece_place::memory);
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(pool->retired_bytes(), piece_size);

		std::vector<char> const bad = corrupt_piece(content, p, 0);
		th.add_piece(p, bad.data());
		TEST_CHECK(wait_for([&] { return pool->held_bytes() == piece_size; }));
		TEST_EQUAL(pool->retired_bytes(), piece_size);
		TEST_CHECK(!hashed2);

		gate.release();
		TEST_CHECK(hash_failed(*ses, p));
		TEST_CHECK(wait_for([&] { return hashed2.load(); }));
		TEST_CHECK(wait_for([&] { return pool->held_bytes() == 0 && pool->retired_bytes() == 0; }));
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_EQUAL(access::blocks_in_use(*pool), 0);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 16: a piece that starts while the pool holds its limit goes to
// the file and counts as spilled
TORRENT_TEST(limit_spills_to_file)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("limit_spills");
	auto const pool = make_pool(memory_policy::memory);
	pool->set_limit(2 * piece_size);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		std::vector<piece_index_t> const pieces{piece_index_t{0}, piece_index_t{1}
			, piece_index_t{2}, piece_index_t{3}};
		TEST_CHECK(add_pieces(*ses, th, content, pieces));
		TEST_EQUAL(pool->held_bytes(), 2 * piece_size);
		TEST_EQUAL(pool->spilled_pieces(), 2);
		TEST_CHECK(reads(*pool, th, content, piece_index_t{0}));
		TEST_CHECK(reads(*pool, th, content, piece_index_t{1}));
		TEST_EQUAL(read_code(*pool, th, piece_index_t{2}), memory_storage_pool::not_in_memory);
		TEST_EQUAL(read_code(*pool, th, piece_index_t{3}), memory_storage_pool::not_in_memory);
		memory_pieces const m = pool->in_memory(th);
		TEST_EQUAL(m.complete.count(), 2);
		TEST_EQUAL(m.partial.count(), 0);
		memory_held const h = pool->held_bytes(th);
		TEST_EQUAL(h.complete, 2 * piece_size);
		TEST_EQUAL(h.partial, 0);
	}
	error_code ec;
	remove_all(path, ec);
}

// a limit lowered below what the pool holds drops nothing; new pieces go
// to the file
TORRENT_TEST(limit_lowered_below_held)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("limit_lowered");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		TEST_CHECK(add_pieces(*ses, th, content, {piece_index_t{0}, piece_index_t{1}, piece_index_t{2}}));
		TEST_EQUAL(pool->held_bytes(), 3 * piece_size);

		pool->set_limit(piece_size);
		TEST_EQUAL(pool->held_bytes(), 3 * piece_size);
		for (int i = 0; i < 3; ++i) TEST_CHECK(reads(*pool, th, content, piece_index_t{i}));

		TEST_CHECK(add_pieces(*ses, th, content, {piece_index_t{3}}));
		TEST_EQUAL(read_code(*pool, th, piece_index_t{3}), memory_storage_pool::not_in_memory);
		TEST_EQUAL(pool->spilled_pieces(), 1);
		TEST_EQUAL(pool->held_bytes(), 3 * piece_size);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 18a: a hybrid torrent added from a magnet link with its v1
// info-hash gets its metadata later; its storage (keyed by the truncated
// v2 info-hash) still matches the registration by v1
TORRENT_TEST(key_magnet_v1_hybrid)
{
	std::vector<char> const content = four_pieces();
	std::string const path = complete("key_magnet_hybrid");
	add_torrent_params const full = torrent_from_file(content, complete("key_magnet_src"), {});
	TEST_CHECK(full.ti->info_hashes().has_v1() && full.ti->info_hashes().has_v2());
	auto const pool = make_pool(memory_policy::file);
	pool->set_policy(info_hash_t(full.ti->info_hashes().v1), memory_policy::memory);
	{
		auto ses = make_session(pool);
		add_torrent_params magnet;
		magnet.info_hashes = info_hash_t(full.ti->info_hashes().v1);
		torrent_handle const th = add(*ses, magnet, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading_metadata));
		TEST_CHECK(th.set_metadata(full.ti->info_section()));
		TEST_CHECK(wait_state(th, torrent_status::downloading));

		piece_index_t const p{0};
		th.add_piece(p, piece_ptr(content, p));
		TEST_CHECK(wait_for([&] { return pool->held_bytes(th).complete == piece_size; }));
		TEST_CHECK(reads(*pool, th, content, p));
	}
	error_code ec;
	remove_all(path, ec);
}

// property 18b: one torrent in two sessions on one pool has two storages;
// a handle reads its own
TORRENT_TEST(two_sessions_one_pool)
{
	std::vector<char> const content = random_content();
	std::string const path1 = complete("two_sessions_1");
	std::string const path2 = complete("two_sessions_2");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses1 = make_session(pool);
		auto ses2 = make_session(pool);
		torrent_handle const th1 = add(*ses1, make_torrent(content), path1);
		torrent_handle const th2 = add(*ses2, make_torrent(content), path2);
		TEST_CHECK(wait_state(th1, torrent_status::downloading));
		TEST_CHECK(wait_state(th2, torrent_status::downloading));
		piece_index_t const p0{0};
		piece_index_t const p1{1};
		TEST_CHECK(add_pieces(*ses1, th1, content, {p0}));
		TEST_CHECK(add_pieces(*ses2, th2, content, {p1}));

		TEST_CHECK(reads(*pool, th1, content, p0));
		TEST_EQUAL(read_code(*pool, th1, p1), memory_storage_pool::not_in_memory);
		TEST_CHECK(reads(*pool, th2, content, p1));
		TEST_EQUAL(read_code(*pool, th2, p0), memory_storage_pool::not_in_memory);
		TEST_EQUAL(pool->held_bytes(), 2 * piece_size);
		TEST_EQUAL(pool->held_bytes(th1).complete, piece_size);
		TEST_EQUAL(pool->held_bytes(th2).complete, piece_size);
	}
	error_code ec;
	remove_all(path1, ec);
	remove_all(path2, ec);
}

// property 22: the persist sets of two owners add up: owner 2's set does
// not replace owner 1's
TORRENT_TEST(claims_two_owners_persist)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("claims_persist");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		// the head and the tail of file 1, and a piece of file 0
		piece_index_t const set1[] = {piece_index_t{1}, piece_index_t{num_pieces - 1}};
		piece_index_t const set2[] = {piece_index_t{0}};
		pool->set_persist(th, 1, set1);
		pool->set_persist(th, 2, set2);
		std::vector<piece_index_t> all;
		for (int i = 0; i < num_pieces; ++i) all.emplace_back(i);
		TEST_CHECK(add_pieces(*ses, th, content, all));

		for (int i = 0; i < num_pieces; ++i)
		{
			piece_index_t const p{i};
			bool const persisted = i == 0 || i == 1 || i == num_pieces - 1;
			if (persisted) TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
			else TEST_CHECK(reads(*pool, th, content, p));
		}
	}
	error_code ec;
	remove_all(path, ec);
}

// property 23: owner 1 claims file 0 for memory, owner 2 file 1 for the
// file: the pieces of file 0 are in memory, those of file 1 in the file,
// and the piece on their boundary in the file
TORRENT_TEST(claims_policy_per_file)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("claims_policy");
	auto const pool = make_pool(memory_policy::file);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		file_index_t const f0[] = {file_index_t{0}};
		file_index_t const f1[] = {file_index_t{1}};
		pool->set_policy(th, 1, memory_policy::memory, f0);
		pool->set_policy(th, 2, memory_policy::file, f1);
		std::vector<piece_index_t> all;
		for (int i = 0; i < num_pieces; ++i) all.emplace_back(i);
		TEST_CHECK(add_pieces(*ses, th, content, all));

		TEST_CHECK(reads(*pool, th, content, piece_index_t{0}));
		for (int i = 1; i < num_pieces; ++i)
			TEST_EQUAL(read_code(*pool, th, piece_index_t{i}), memory_storage_pool::not_in_memory);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 24: a piece in the file ("in file" once its blocks are written)
// forgotten by a plain torrent_handle::forget_piece() while a claim asks
// for memory starts over when it is downloaded again: it goes to memory
// and is not "in file" any more
TORRENT_TEST(plain_forget_file_piece_replaced)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("plain_forget_file");
	auto const pool = make_pool(memory_policy::file);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
		TEST_CHECK(in_file(*ses, th, p));

		pool->set_policy(th, 1, memory_policy::memory);
		TEST_EQUAL(th.forget_piece(p), 0);
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_CHECK(!in_file(*ses, th, p));
	}
	error_code ec;
	remove_all(path, ec);
}

// the calls by handle of a removed torrent answer not_managed
TORRENT_TEST(pool_calls_on_removed_handle)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("removed_handle");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		ses->remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(*ses, [](torrent_removed_alert const&) { return true; }));

		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_managed);
		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, memory_storage_pool::not_managed);
		TEST_CHECK(r.place == piece_place::none);
		memory_held const h = pool->held_bytes(th);
		TEST_EQUAL(h.complete, memory_storage_pool::not_managed);
		TEST_EQUAL(h.partial, memory_storage_pool::not_managed);
		TEST_EQUAL(pool->in_memory(th).complete.size(), 0);
		TEST_EQUAL(pool->held_bytes(), 0);
	}
	TEST_EQUAL(access::blocks_in_use(*pool), 0);
	error_code ec;
	remove_all(path, ec);
}

// a claim made while the torrent has no metadata (no storage) waits for
// its storage: once the metadata arrives, its pieces go to memory
TORRENT_TEST(claim_before_metadata)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("claim_before_metadata");
	add_torrent_params const full = make_torrent(content);
	auto const pool = make_pool(memory_policy::file);
	{
		auto ses = make_session(pool);
		add_torrent_params magnet;
		magnet.info_hashes = full.ti->info_hashes();
		torrent_handle const th = add(*ses, magnet, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading_metadata));
		pool->set_policy(th, 1, memory_policy::memory);
		TEST_EQUAL(access::pending_claims(*pool), 1);
		TEST_EQUAL(read_code(*pool, th, piece_index_t{0}), memory_storage_pool::not_managed);

		TEST_CHECK(th.set_metadata(full.ti->info_section()));
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		TEST_EQUAL(access::pending_claims(*pool), 0);
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
	}
	error_code ec;
	remove_all(path, ec);
}

// the session is destroyed while a hash of a piece in memory is issued and
// not answered: the gate holds the hashing thread and is released by
// another thread during the destruction. Every block goes back and no
// storage outlives its pins (run under ASan and TSan)
TORRENT_TEST(session_destroyed_while_hash_issued)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("session_destroyed_hash");
	auto const pool = make_pool(memory_policy::memory);
	int const destroyed0 = aux::memory_storages_destroyed_pinned_for_test();
	{
		memory_hasher_gate gate(piece_index_t{0});
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		th.add_piece(piece_index_t{0}, content.data());
		TEST_CHECK(wait_for([] { return memory_hasher_gate::waiting() == 1; }));
		TEST_CHECK(wait_for([&] { return pool->held_bytes() == piece_size; }));
		// libtorrent issues the hash once the write handlers ran
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		TEST_CHECK(!th.have_piece(piece_index_t{0}));

		std::thread releaser([&gate]
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			gate.release();
		});
		ses.reset();
		releaser.join();
	}
	TEST_EQUAL(pool->held_bytes(), 0);
	TEST_EQUAL(pool->retired_bytes(), 0);
	TEST_EQUAL(access::blocks_in_use(*pool), 0);
	TEST_EQUAL(access::retired_storages(*pool), 0);
	TEST_EQUAL(aux::memory_storages_destroyed_pinned_for_test(), destroyed0);
	error_code ec;
	remove_all(path, ec);
}

// a removed torrent whose storage a background hash still pins: the pool
// keeps the torrent object until the last pin drops, which happens on the
// hashing thread, and then releases it on the network thread. The torrent
// has peers in its list: ~torrent frees them into the session's allocator,
// which only the network thread may touch (run under TSan)
TORRENT_TEST(removed_torrent_released_on_network_thread)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("removed_released");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		add_torrent_params atp = make_torrent(content);
		// nothing listens there: they stay in the peer list
		for (int i = 0; i < 5; ++i)
			atp.peers.emplace_back(make_address_v4("127.0.0.1"), std::uint16_t(1 + i));
		torrent_handle const th = add(*ses, atp, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));

		std::shared_ptr<aux::session_impl> const s = ses->native_handle();
		std::thread::id network;
		{
			std::promise<std::thread::id> id;
			boost::asio::dispatch(s->get_context(), [&id] { id.set_value(std::this_thread::get_id()); });
			network = id.get_future().get();
		}

		// two blocks of piece 0, written through the session's
		// disk_interface: libtorrent does not know them and issues no hash.
		// Their background hash holds the entry at the gate
		piece_index_t const p{0};
		memory_hasher_gate gate(p);
		{
			std::shared_ptr<aux::torrent> const t = th.native_handle();
			std::promise<void> issued;
			boost::asio::dispatch(s->get_context(), [&issued, &content, s, t, p]
			{
				for (int b = 0; b < 2; ++b)
				{
					peer_request const r{p, b * default_block_size, default_block_size};
					s->disk_thread().async_write(t->storage(), r
						, content.data() + std::size_t(b) * default_block_size, {}
						, [](storage_error const& e) { TEST_CHECK(!e); });
				}
				s->disk_thread().submit_jobs();
				issued.set_value();
			});
			issued.get_future().wait();
		}
		TEST_CHECK(wait_for([] { return memory_hasher_gate::waiting() == 1; }));
		TEST_CHECK(th.status().list_peers > 0);

		std::weak_ptr<aux::torrent> const weak = th.native_handle();
		aux::memory_retired_torrent_release_reset_for_test();
		ses->remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(*ses, [](torrent_removed_alert const&) { return true; }));
		TEST_EQUAL(access::retired_storages(*pool), 1);
		// the pool keeps it: its storage refers to its file_storage
		TEST_CHECK(!weak.expired());

		gate.release();
		TEST_CHECK(wait_for([&weak] { return weak.expired(); }));
		TEST_CHECK(aux::memory_retired_torrent_release_thread_for_test() == network);
		TEST_EQUAL(access::retired_storages(*pool), 0);
		TEST_EQUAL(access::blocks_in_use(*pool), 0);
	}
	error_code ec;
	remove_all(path, ec);
}

// the teardown twin: the session is destroyed while a background hash
// still pins the storage of a removed torrent with peers. The hashing
// thread keeps the network thread running until it is done, so the torrent
// object is released there, while the session and its allocators exist
// (run under ASan and TSan)
TORRENT_TEST(removed_torrent_released_at_session_teardown)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("removed_teardown");
	auto const pool = make_pool(memory_policy::memory);
	std::weak_ptr<aux::torrent> weak;
	std::thread::id network;
	{
		piece_index_t const p{0};
		memory_hasher_gate gate(p);
		auto ses = make_session(pool);
		add_torrent_params atp = make_torrent(content);
		// nothing listens there: they stay in the peer list
		for (int i = 0; i < 5; ++i)
			atp.peers.emplace_back(make_address_v4("127.0.0.1"), std::uint16_t(1 + i));
		torrent_handle const th = add(*ses, atp, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));

		{
			// not held past this block: the session must go with ses.reset()
			std::shared_ptr<aux::session_impl> const s = ses->native_handle();
			std::promise<std::thread::id> id;
			boost::asio::dispatch(s->get_context(), [&id] { id.set_value(std::this_thread::get_id()); });
			network = id.get_future().get();

			std::shared_ptr<aux::torrent> const t = th.native_handle();
			std::promise<void> issued;
			boost::asio::dispatch(s->get_context(), [&issued, &content, s, t, p]
			{
				for (int b = 0; b < 2; ++b)
				{
					peer_request const r{p, b * default_block_size, default_block_size};
					s->disk_thread().async_write(t->storage(), r
						, content.data() + std::size_t(b) * default_block_size, {}
						, [](storage_error const& e) { TEST_CHECK(!e); });
				}
				s->disk_thread().submit_jobs();
				issued.set_value();
			});
			issued.get_future().wait();
		}
		TEST_CHECK(wait_for([] { return memory_hasher_gate::waiting() == 1; }));
		TEST_CHECK(th.status().list_peers > 0);

		weak = th.native_handle();
		aux::memory_retired_torrent_release_reset_for_test();
		ses->remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(*ses, [](torrent_removed_alert const&) { return true; }));
		TEST_EQUAL(access::retired_storages(*pool), 1);
		TEST_CHECK(!weak.expired());

		std::thread releaser([&gate]
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			gate.release();
		});
		ses.reset();
		releaser.join();
	}
	TEST_CHECK(weak.expired());
	TEST_CHECK(aux::memory_retired_torrent_release_thread_for_test() == network);
	TEST_EQUAL(access::retired_storages(*pool), 0);
	TEST_EQUAL(access::blocks_in_use(*pool), 0);
	error_code ec;
	remove_all(path, ec);
}

namespace {

// a session on the pool, with one hashing thread and the given settings
std::unique_ptr<lt::session> make_session(std::shared_ptr<memory_storage_pool> const& pool
	, settings_pack sett)
{
	sett.set_int(settings_pack::hashing_threads, 1);
	session_params sp(sett);
	sp.disk_io_constructor = memory_disk_io_constructor(pool);
	return std::make_unique<lt::session>(std::move(sp));
}

// waits for pred for up to `ms` milliseconds
template <typename Pred>
bool wait_for_ms(int const ms, Pred pred)
{
	auto const end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
	while (std::chrono::steady_clock::now() < end)
	{
		if (pred()) return true;
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
	return pred();
}

// the resume data of the torrent
add_torrent_params save_resume(lt::session& ses, torrent_handle const& th)
{
	add_torrent_params ret;
	th.save_resume_data(torrent_handle::flush_disk_cache);
	bool const saved = wait_alert<save_resume_data_alert>(ses, [&ret](save_resume_data_alert const& a)
		{
			ret = a.params;
			return true;
		});
	TEST_CHECK(saved);
	return ret;
}

bool has(add_torrent_params const& atp, piece_index_t const p)
{
	return p < atp.have_pieces.end_index() && atp.have_pieces.get_bit(p);
}

// runs f on the network thread of the session and waits for it
template <typename F>
void on_network(lt::session& ses, F f)
{
	std::shared_ptr<aux::session_impl> const s = ses.native_handle();
	std::promise<void> done;
	boost::asio::dispatch(s->get_context(), [&done, &f, s]
	{
		f(*s);
		done.set_value();
	});
	done.get_future().wait();
}

#if defined TORRENT_LINUX
// the writes held at the write gate now
int held_writes()
{
	std::lock_guard<std::mutex> l(gate_mutex);
	return writes_waiting;
}
#endif

// the answers of disk jobs a test issued itself
struct answers
{
	std::atomic<int> answered{0};
	std::atomic<int> aborted{0};
	void operator()(storage_error const& e)
	{
		if (e.ec == boost::asio::error::operation_aborted) ++aborted;
		++answered;
	}
};

} // anonymous namespace

#if defined TORRENT_LINUX
// property 6: generations of a transfer. The write gate holds the transfer
// of a piece (the claim of owner 1 sends it to the file). Under it the
// piece is forgotten (code 0), the claim dropped and the piece written
// again, into memory. Released, the old transfer changes nothing of the new
// generation: the piece stays in memory and is not "in file", so the resume
// data loses it
TORRENT_TEST(transfer_generation_check)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("transfer_generation");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		set_hold_writes(true);
		file_index_t const f0[] = {file_index_t{0}};
		pool->set_policy(th, 1, memory_policy::file, f0);
		TEST_CHECK(wait_for([] { return held_writes() > 0; }));

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 0);
		TEST_CHECK(r.place == piece_place::transfer);
		pool->drop_owner(th, 1);
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_EQUAL(pool->retired_bytes(), piece_size);

		set_hold_writes(false);
		// the old transfer let go of its entry
		TEST_CHECK(wait_for([&] { return pool->retired_bytes() == 0; }));
		TEST_CHECK(reads(*pool, th, content, p));
		TEST_CHECK(pool->in_memory(th).complete.get_bit(p));
		TEST_EQUAL(pool->held_bytes(), piece_size);
		TEST_CHECK(!in_file(*ses, th, p));
		add_torrent_params rd = save_resume(*ses, th);
		TEST_CHECK(has(rd, p));
		pool->filter_resume(th, rd);
		TEST_CHECK(!has(rd, p));
		TEST_EQUAL(pool->persist_failures(th), 0);
	}
	error_code ec;
	remove_all(path, ec);
}

// property 6a: the tail of a transfer. The write gate holds the transfer
// of a piece in a persist set. Under it the piece is forgotten (code 0) and
// written again with a corrupt block: the set sends it to the file, and
// libtorrent asks for its hash as soon as the writes are issued. Neither
// the writes nor the hash may reach the default backend's cache entry of
// the transfer: a write there would be refused, and the hash would answer
// the transfer's (right) hash. They wait for the transfer and the piece
// fails its hash
TORRENT_TEST(transfer_tail_parks_piece_jobs)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("transfer_tail");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		set_hold_writes(true);
		piece_index_t const set[] = {p};
		pool->set_persist(th, 1, set);
		TEST_CHECK(wait_for([] { return held_writes() > 0; }));

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 0);
		std::vector<char> const bad = corrupt_piece(content, p, 1);
		th.add_piece(p, bad.data());
		// libtorrent issues the hash with the writes
		std::this_thread::sleep_for(std::chrono::milliseconds(300));
		TEST_CHECK(!th.have_piece(p));

		set_hold_writes(false);
		TEST_CHECK(hash_failed(*ses, p));
	}
	error_code ec;
	remove_all(path, ec);
}

// property 7: a piece is read from memory for the whole transfer; its
// memory is freed only once the default backend answered, and then it is
// "in file" and stays in the filtered resume data
TORRENT_TEST(transfer_reads_without_gap)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("transfer_reads");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{2};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		set_hold_writes(true);
		pool->set_policy(th, 1, memory_policy::file);
		TEST_CHECK(wait_for([] { return held_writes() > 0; }));
		for (int i = 0; i < 5; ++i)
		{
			TEST_CHECK(reads(*pool, th, content, p));
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		TEST_EQUAL(pool->held_bytes(), piece_size);
		TEST_EQUAL(pool->pending_persist_bytes(th), piece_size);
		TEST_CHECK(pool->in_memory(th).complete.get_bit(p));

		set_hold_writes(false);
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_CHECK(wait_for([&] { return access::blocks_in_use(*pool) == 0; }));
		TEST_CHECK(in_file(*ses, th, p));
		add_torrent_params rd = save_resume(*ses, th);
		pool->filter_resume(th, rd);
		TEST_CHECK(has(rd, p));
		TEST_EQUAL(pool->persist_failures(th), 0);
	}
	std::vector<char> const b = read_file(combine_path(path, combine_path("unregistered", "b")));
	std::size_t const at = std::size_t(2 * piece_size - file_a);
	TEST_CHECK(b.size() >= at + std::size_t(piece_size));
	if (b.size() >= at + std::size_t(piece_size))
		TEST_CHECK(std::equal(b.begin() + std::ptrdiff_t(at), b.begin() + std::ptrdiff_t(at + piece_size)
			, piece_ptr(content, piece_index_t{2})));
	error_code ec;
	remove_all(path, ec);
}
#endif

// property 8: a transfer leaves no cache entry of the default backend
// without its hash returned. A v2-only torrent: pread_disk_io flushes the
// blocks of a v2-only piece only after its async_hash (or under pressure),
// so a transfer that skipped the hash would never end (with v1 hashes the
// blocks are flushed once pread's hasher reaches the end, and the skip
// would only show in the next generation). The transfer ends within 5
// seconds; the piece, forgotten and written again into the file with a
// corrupt block, fails its hash (an entry left without its hash returned
// would take the new blocks and answer the old hash)
TORRENT_TEST(transfer_leaves_no_pread_entry)
{
	std::vector<char> const content = four_pieces();
	std::string const path = complete("transfer_no_entry");
	add_torrent_params const atp = torrent_from_file(content, complete("transfer_no_entry_src")
		, create_torrent::v2_only);
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, atp, path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{1};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		piece_index_t const set[] = {p};
		pool->persist(th, set);
		TEST_CHECK(wait_for_ms(5000, [&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_CHECK(in_file(*ses, th, p));
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
		TEST_EQUAL(pool->persist_failures(th), 0);

		memory_forget_result const r = pool->forget_piece(th, p);
		TEST_EQUAL(r.code, 0);
		TEST_CHECK(r.place == piece_place::file);
		pool->persist(th, set);
		std::vector<char> const bad = corrupt_piece(content, p, 1);
		th.add_piece(p, bad.data());
		TEST_CHECK(hash_failed(*ses, p));
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
	}
	error_code ec;
	remove_all(path, ec);
}

// a one-shot persist() is spent once its piece is moved to the file:
// forgotten and written again, the piece goes to memory
TORRENT_TEST(persist_spent_by_transfer)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("persist_spent");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{2};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		piece_index_t const set[] = {p};
		pool->persist(th, set);
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_CHECK(in_file(*ses, th, p));

		TEST_EQUAL(pool->forget_piece(th, p).code, 0);
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		TEST_CHECK(reads(*pool, th, content, p));
	}
	error_code ec;
	remove_all(path, ec);
}

#if defined TORRENT_LINUX
// property 9: back-pressure between pieces. The default backend's queue
// holds two blocks and the write gate holds its writes: the first piece's
// step hands over all its blocks and its hash, the second one waits for
// the queue. Between them the first piece is forgotten (code 0) and the
// storage is stopped: the stop answers only after the step's writes. The
// piece, written again into the file with a corrupt block, fails its hash;
// the second piece is moved after the stop
TORRENT_TEST(transfer_backpressure_between_pieces)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("transfer_backpressure");
	auto const pool = make_pool(memory_policy::memory);
	{
		settings_pack sett = session_settings();
		sett.set_int(settings_pack::max_queued_disk_bytes, 2 * default_block_size);
		auto ses = make_session(pool, sett);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p0{0};
		piece_index_t const p1{2};
		TEST_CHECK(add_pieces(*ses, th, content, {p0, p1}));

		set_hold_writes(true);
		piece_index_t const set[] = {p0, p1};
		pool->set_persist(th, 1, set);
		TEST_CHECK(wait_for([] { return held_writes() > 0; }));
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		TEST_EQUAL(pool->pending_persist_bytes(th), 2 * piece_size);

		memory_forget_result const r = pool->forget_piece(th, p0);
		TEST_EQUAL(r.code, 0);
		std::atomic<bool> stopped{false};
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		on_network(*ses, [&stopped, t](aux::session_impl& s)
		{
			s.disk_thread().async_stop_torrent(t->storage(), [&stopped] { stopped = true; });
			s.disk_thread().submit_jobs();
		});
		std::vector<char> const bad = corrupt_piece(content, p0, 1);
		th.add_piece(p0, bad.data());
		std::this_thread::sleep_for(std::chrono::milliseconds(300));
		// the step's writes are held
		TEST_CHECK(!stopped);

		set_hold_writes(false);
		TEST_CHECK(wait_for([&stopped] { return stopped.load(); }));
		TEST_CHECK(hash_failed(*ses, p0));
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_CHECK(in_file(*ses, th, p1));
		TEST_EQUAL(pool->persist_failures(th), 0);
	}
	error_code ec;
	remove_all(path, ec);
}
#endif

// property 10: filter_resume() drops a piece that is not in the file. The
// race: resume data saved while the piece was in memory, then the piece is
// forgotten. And the path file -> forgotten -> in memory again: the resume
// data saved last names the piece, which is in memory only
TORRENT_TEST(filter_resume_race)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("filter_resume_race");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));

		piece_index_t const p{0};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));
		add_torrent_params rd = save_resume(*ses, th);
		TEST_CHECK(has(rd, p));
		TEST_EQUAL(pool->forget_piece(th, p).code, 0);
		pool->filter_resume(th, rd);
		TEST_CHECK(!has(rd, p));

		// piece 3 goes to the file, is forgotten, and comes back in memory
		piece_index_t const q{3};
		pool->set_policy(th, 1, memory_policy::file);
		TEST_CHECK(add_pieces(*ses, th, content, {q}));
		TEST_CHECK(wait_for([&] { return in_file(*ses, th, q); }));
		add_torrent_params in_the_file = save_resume(*ses, th);
		pool->filter_resume(th, in_the_file);
		TEST_CHECK(has(in_the_file, q));

		pool->set_policy(th, 1, memory_policy::memory);
		memory_forget_result const fq = pool->forget_piece(th, q);
		TEST_EQUAL(fq.code, 0);
		TEST_CHECK(fq.place == piece_place::file);
		TEST_CHECK(add_pieces(*ses, th, content, {q}));
		TEST_CHECK(reads(*pool, th, content, q));
		add_torrent_params again = save_resume(*ses, th);
		TEST_CHECK(has(again, q));
		pool->filter_resume(th, again);
		TEST_CHECK(!has(again, q));
	}
	error_code ec;
	remove_all(path, ec);
}

// property 11: a claim for the file moves a partial piece: its blocks in
// memory are written to the file (and leave memory), and its later blocks
// go to the file too. Once all are there the piece is "in file".
//
// The move writes the blocks of the partial piece with flush_piece.
// pread_disk_io's invariant check (disk_cache.cpp, check_invariant, in
// builds with invariant checks) wants every block of a piece flagged for
// flushing to be written, flushed or hashed, which a partial piece is not
// (open): the test runs in builds without invariant checks
#if !TORRENT_USE_INVARIANT_CHECKS
TORRENT_TEST(policy_file_migrates_partial)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("policy_file_partial");
	auto const pool = make_pool(memory_policy::memory);
	piece_index_t const p{3};
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		std::shared_ptr<aux::torrent> const t = th.native_handle();

		// blocks [first, last) of the piece, through the session's
		// disk_interface (libtorrent does not know them)
		auto const write_blocks = [&](int const first, int const last, disk_job_flags_t const flags)
		{
			auto done = std::make_shared<answers>();
			on_network(*ses, [&](aux::session_impl& s)
			{
				for (int b = first; b < last; ++b)
				{
					peer_request const r{p, b * default_block_size, default_block_size};
					s.disk_thread().async_write(t->storage(), r
						, piece_ptr(content, p) + std::size_t(b) * default_block_size, {}
						, [done](storage_error const& e) { TEST_CHECK(!e); (*done)(e); }, flags);
				}
				s.disk_thread().submit_jobs();
			});
			TEST_CHECK(wait_for([&] { return done->answered == last - first; }));
		};

		write_blocks(0, 2, {});
		TEST_CHECK(pool->in_memory(th).partial.get_bit(p));
		TEST_EQUAL(pool->held_bytes(th).partial, 2 * default_block_size);

		pool->set_policy(th, 1, memory_policy::file);
		TEST_EQUAL(pool->pending_persist_bytes(th), 2 * default_block_size);
		// the rest of the piece, after the move's step (the network thread
		// runs the move first). pread_disk_io flushes the blocks of a
		// partial piece when a thread of its wakes up (the piece completes,
		// the cache is full, a fence), not when they come, flush_piece or
		// not: the move's writes are answered then
		write_blocks(2, 4, {});
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_EQUAL(read_code(*pool, th, p), memory_storage_pool::not_in_memory);
		TEST_CHECK(wait_for([&] { return in_file(*ses, th, p); }));
		TEST_EQUAL(pool->held_bytes(), 0);
		TEST_CHECK(wait_for([&] { return access::blocks_in_use(*pool) == 0; }));

		std::vector<char> const b = read_file(combine_path(path, combine_path("unregistered", "b")));
		std::size_t const at = std::size_t(3 * piece_size - file_a);
		TEST_CHECK(b.size() >= at + std::size_t(piece_size));
		if (b.size() >= at + std::size_t(piece_size))
			TEST_CHECK(std::equal(b.begin() + std::ptrdiff_t(at), b.begin() + std::ptrdiff_t(at + piece_size)
				, piece_ptr(content, p)));
	}
	error_code ec;
	remove_all(path, ec);
}
#endif

// property 12: pending_persist_bytes() counts the bytes in memory only. A
// paused torrent with a persist set of four pieces, two of them held:
// once those two are moved it is 0, though the other two never arrive
TORRENT_TEST(pending_zero_when_paused)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("pending_paused");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		TEST_CHECK(add_pieces(*ses, th, content, {piece_index_t{0}, piece_index_t{2}}));
		th.pause();
		TEST_CHECK(wait_for([&] { return bool(th.flags() & torrent_flags::paused); }));

		piece_index_t const set[] = {piece_index_t{0}, piece_index_t{2}, piece_index_t{3}, piece_index_t{4}};
		pool->set_persist(th, 1, set);
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_CHECK(in_file(*ses, th, piece_index_t{0}));
		TEST_CHECK(in_file(*ses, th, piece_index_t{2}));
		TEST_EQUAL(pool->held_bytes(), 0);
	}
	error_code ec;
	remove_all(path, ec);
}

// pending_persist_bytes() grows in the call that asks for the move, before
// the network thread runs it: here the network thread is busy
TORRENT_TEST(pending_counted_in_the_call)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("pending_in_call");
	auto const pool = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool);
		torrent_handle const th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const p{2};
		TEST_CHECK(add_pieces(*ses, th, content, {p}));

		std::shared_ptr<aux::session_impl> const s = ses->native_handle();
		std::promise<void> go;
		std::shared_future<void> const busy = go.get_future().share();
		std::promise<void> entered;
		boost::asio::post(s->get_context(), [&entered, busy]
		{
			entered.set_value();
			busy.wait();
		});
		entered.get_future().wait();

		piece_index_t const set[] = {p};
		pool->set_persist(th, 1, set);
		TEST_EQUAL(pool->pending_persist_bytes(th), piece_size);
		go.set_value();
		TEST_CHECK(wait_for([&] { return pool->pending_persist_bytes(th) == 0; }));
		TEST_CHECK(in_file(*ses, th, p));
	}
	error_code ec;
	remove_all(path, ec);
}

// the pool outlives the session: resume data saved before, filtered after,
// by the info-hashes against what the pool keeps of the removed storage.
// Calls by handle answer not_managed. forget_record() drops the record
TORRENT_TEST(pool_outlives_session)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("pool_outlives");
	auto const pool = make_pool(memory_policy::memory);
	add_torrent_params rd;
	torrent_handle th;
	piece_index_t const mem{0};
	piece_index_t const file{2};
	{
		auto ses = make_session(pool);
		th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const set[] = {file};
		pool->set_persist(th, 1, set);
		TEST_CHECK(add_pieces(*ses, th, content, {mem, file}));
		TEST_CHECK(wait_for([&] { return in_file(*ses, th, file); }));
		rd = save_resume(*ses, th);
		TEST_CHECK(has(rd, mem));
		TEST_CHECK(has(rd, file));
	}
	TEST_EQUAL(access::residues(*pool), 1);

	add_torrent_params by_hash = rd;
	pool->filter_resume(by_hash);
	TEST_CHECK(!has(by_hash, mem));
	TEST_CHECK(has(by_hash, file));
	add_torrent_params by_handle = rd;
	pool->filter_resume(th, by_handle);
	TEST_CHECK(by_handle.have_pieces == by_hash.have_pieces);

	TEST_EQUAL(read_code(*pool, th, mem), memory_storage_pool::not_managed);
	TEST_EQUAL(pool->pending_persist_bytes(th), memory_storage_pool::not_managed);
	TEST_EQUAL(pool->persist_failures(th), memory_storage_pool::not_managed);
	TEST_EQUAL(pool->held_bytes(th).complete, memory_storage_pool::not_managed);
	TEST_EQUAL(pool->forget_piece(th, mem).code, memory_storage_pool::not_managed);

	pool->forget_record(rd.info_hashes);
	TEST_EQUAL(access::residues(*pool), 0);
	add_torrent_params unknown = rd;
	pool->filter_resume(unknown);
	TEST_CHECK(unknown.have_pieces == rd.have_pieces);
	TEST_EQUAL(access::blocks_in_use(*pool), 0);
	error_code ec;
	remove_all(path, ec);
}

// a torrent removed, added again with its filtered resume data, and
// removed again: the pool keeps what the last lifetime left, not the
// intersection with the first one. A piece moved to the file in the second
// lifetime stays in the resume data filtered after the removal
TORRENT_TEST(residues_trimmed_on_readd)
{
	std::vector<char> const content = random_content();
	std::string const path = complete("residues_trimmed");
	auto const pool = make_pool(memory_policy::memory);
	piece_index_t const first{2};
	piece_index_t const second{3};
	add_torrent_params rd;
	{
		auto ses = make_session(pool);
		torrent_handle th = add(*ses, make_torrent(content), path);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		piece_index_t const set1[] = {first};
		pool->set_persist(th, 1, set1);
		TEST_CHECK(add_pieces(*ses, th, content, {first}));
		TEST_CHECK(wait_for([&] { return in_file(*ses, th, first); }));
		rd = save_resume(*ses, th);
		pool->filter_resume(th, rd);
		TEST_CHECK(has(rd, first));
		ses->remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(*ses, [](torrent_removed_alert const&) { return true; }));
		TEST_EQUAL(access::residues(*pool), 1);

		// the second lifetime, from the filtered resume data
		rd.ti = make_torrent(content).ti;
		rd.save_path = path;
		rd.flags &= ~torrent_flags::auto_managed;
		rd.flags &= ~torrent_flags::paused;
		th = ses->add_torrent(rd);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		TEST_EQUAL(access::residues(*pool), 0);
		TEST_CHECK(in_file(*ses, th, first));
		piece_index_t const set2[] = {second};
		pool->set_persist(th, 1, set2);
		TEST_CHECK(add_pieces(*ses, th, content, {second}));
		TEST_CHECK(wait_for([&] { return in_file(*ses, th, second); }));
		rd = save_resume(*ses, th);
		ses->remove_torrent(th);
		TEST_CHECK(wait_alert<torrent_removed_alert>(*ses, [](torrent_removed_alert const&) { return true; }));
	}
	TEST_EQUAL(access::residues(*pool), 1);
	add_torrent_params filtered = rd;
	pool->filter_resume(filtered);
	TEST_CHECK(has(filtered, first));
	TEST_CHECK(has(filtered, second));
	error_code ec;
	remove_all(path, ec);
}

namespace {

int const e2e_piece_size = piece_size;
int const e2e_pieces = 9; // setup_transfer's torrent

// a seeder session and one on the pool, the torrent of setup_transfer() in
// both (the files of the seeder in "tmp1<suffix>", of the other one in
// "tmp2<suffix>"). Before the peers connect, `before` runs with the
// handle of the downloader
struct e2e
{
	lt::session seeder;
	std::unique_ptr<lt::session> ses;
	torrent_handle th;
	std::vector<char> content;

	e2e(std::shared_ptr<memory_storage_pool> const& pool, std::string const& suffix
		, std::function<void(torrent_handle const&)> const& before)
		: seeder(session_settings())
		, ses(make_session(pool))
	{
		error_code ec;
		remove_all("tmp1" + suffix, ec);
		remove_all("tmp2" + suffix, ec);
		torrent_handle seed;
		std::tie(seed, th, std::ignore) = setup_transfer(&seeder, ses.get(), nullptr
			, true, false, false, suffix, e2e_piece_size);
		content = read_file(combine_path("tmp1" + suffix, "temporary"));
		TEST_EQUAL(int(content.size()), e2e_pieces * e2e_piece_size);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		if (before) before(th);
		seed.connect_peer(tcp::endpoint(make_address("127.0.0.1"), std::uint16_t(ses->listen_port())));
	}

	// sanitizer builds are slow
	bool finished() const
	{
		bool const ret = wait_for_ms(90000, [this] { return th.status().is_finished; });
		if (!ret)
		{
			torrent_status const st = th.status();
			std::printf("not finished: pieces %d peers %d state %d rate %d\n"
				, st.num_pieces, st.num_peers, int(st.state), st.download_rate);
		}
		return ret;
	}
};

// the blocks the file takes on disk, -1 if it does not exist
long long disk_blocks(std::string const& file)
{
	struct stat st{};
	if (::stat(file.c_str(), &st) != 0) return -1;
	return static_cast<long long>(st.st_blocks);
}

} // anonymous namespace

// end to end: a torrent in memory is downloaded from a local peer. The
// pool reads it back, and its file takes no block on disk
TORRENT_TEST(e2e_download_in_memory)
{
	std::string const suffix = "_e2e_memory";
	auto const pool = make_pool(memory_policy::memory);
	{
		e2e t(pool, suffix, {});
		TEST_CHECK(t.finished());
		for (int i = 0; i < e2e_pieces; ++i)
			TEST_CHECK(reads(*pool, t.th, t.content, piece_index_t{i}));
		TEST_EQUAL(pool->held_bytes(), std::int64_t(e2e_pieces) * e2e_piece_size);
		// the file of the torrent where the session keeps it: no block on
		// disk, or not created at all (-1)
		long long const blocks = disk_blocks(combine_path(t.th.status().save_path, "temporary"));
		TEST_CHECK(blocks == 0 || blocks == -1);
	}
	error_code ec;
	remove_all("tmp1" + suffix, ec);
	remove_all("tmp2" + suffix, ec);
}

namespace {
std::vector<piece_index_t> const e2e_set{piece_index_t{1}, piece_index_t{4}, piece_index_t{8}};

bool in_set(piece_index_t const p)
{
	return std::find(e2e_set.begin(), e2e_set.end(), p) != e2e_set.end();
}
}

// end to end: the persist set is in the file, the rest only in memory
TORRENT_TEST(e2e_persist_set_on_disk)
{
	std::string const suffix = "_e2e_persist";
	auto const pool = make_pool(memory_policy::memory);
	{
		e2e t(pool, suffix, [&pool](torrent_handle const& th) { pool->set_persist(th, 1, e2e_set); });
		TEST_CHECK(t.finished());
		for (auto const p : e2e_set) TEST_CHECK(wait_for([&] { return in_file(*t.ses, t.th, p); }));
		std::vector<char> const f = read_file(combine_path("tmp2" + suffix, "temporary"));
		for (int i = 0; i < e2e_pieces; ++i)
		{
			piece_index_t const p{i};
			std::size_t const at = std::size_t(i) * e2e_piece_size;
			bool const on_disk = f.size() >= at + std::size_t(e2e_piece_size)
				&& std::equal(f.begin() + std::ptrdiff_t(at), f.begin() + std::ptrdiff_t(at + e2e_piece_size)
					, t.content.begin() + std::ptrdiff_t(at));
			TEST_EQUAL(on_disk, in_set(p));
			if (in_set(p)) TEST_EQUAL(read_code(*pool, t.th, p), memory_storage_pool::not_in_memory);
			else TEST_CHECK(reads(*pool, t.th, t.content, p));
		}
	}
	error_code ec;
	remove_all("tmp1" + suffix, ec);
	remove_all("tmp2" + suffix, ec);
}

// end to end: the filtered resume data of a torrent with a persist set,
// added to a new session: the torrent has exactly the pieces in the file.
// The check of that resume data makes them "in file" in the new pool
TORRENT_TEST(e2e_restart_with_filtered_resume)
{
	std::string const suffix = "_e2e_restart";
	auto const pool = make_pool(memory_policy::memory);
	add_torrent_params rd;
	std::vector<char> content;
	{
		e2e t(pool, suffix, [&pool](torrent_handle const& th) { pool->set_persist(th, 1, e2e_set); });
		TEST_CHECK(t.finished());
		for (auto const p : e2e_set) TEST_CHECK(wait_for([&] { return in_file(*t.ses, t.th, p); }));
		rd = save_resume(*t.ses, t.th);
		TEST_EQUAL(rd.have_pieces.count(), e2e_pieces);
		pool->filter_resume(t.th, rd);
		rd.ti = std::make_shared<torrent_info>(*t.th.torrent_file());
		content = t.content;
	}
	TEST_EQUAL(rd.have_pieces.count(), int(e2e_set.size()));

	auto const pool2 = make_pool(memory_policy::memory);
	{
		auto ses = make_session(pool2);
		rd.save_path = "tmp2" + suffix;
		rd.flags &= ~torrent_flags::auto_managed;
		rd.flags &= ~torrent_flags::paused;
		torrent_handle const th = ses->add_torrent(rd);
		TEST_CHECK(wait_state(th, torrent_status::downloading));
		for (int i = 0; i < e2e_pieces; ++i)
			TEST_EQUAL(th.have_piece(piece_index_t{i}), in_set(piece_index_t{i}));
		add_torrent_params again = save_resume(*ses, th);
		pool2->filter_resume(th, again);
		TEST_EQUAL(again.have_pieces.count(), int(e2e_set.size()));
		for (auto const p : e2e_set) TEST_CHECK(has(again, p));
	}
	error_code ec;
	remove_all("tmp1" + suffix, ec);
	remove_all("tmp2" + suffix, ec);
}
