/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "settings.hpp" // for settings()
#include "setup_transfer.hpp" // for remove_all

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
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/aux_/random.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <thread>
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

	auto pool = std::make_shared<memory_storage_pool>();
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
		for (int i = 0; i < 2; ++i)
			TEST_CHECK(wait_alert<torrent_removed_alert>(ses
				, [](torrent_removed_alert const&) { return true; }));
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
