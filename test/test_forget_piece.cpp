/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "setup_transfer.hpp" // for create_torrent
#include "settings.hpp" // for settings()
#include "test_utils.hpp" // for _piece

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/create_torrent.hpp"
#include "libtorrent/session_stats.hpp"
#include "libtorrent/aux_/path.hpp"
#include "libtorrent/extensions.hpp"
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/session_impl.hpp"
#include "libtorrent/pread_disk_io.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/storage_defs.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/aux_/vector.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <string>
#include <vector>


#if defined TORRENT_LINUX
#include <fcntl.h>
#include <unistd.h>
#include <linux/falloc.h>
#endif
#include "write_gate.hpp"

using namespace lt;

namespace {

int const piece_size = 8 * 1024 * 1024;
int const num_pieces = 12;

std::vector<char> piece_data()
{
	// create_torrent() in setup_transfer gives every piece this content
	std::vector<char> ret(static_cast<std::size_t>(piece_size));
	for (std::size_t i = 0; i < ret.size(); ++i) ret[i] = char((i % 26) + 'A');
	return ret;
}

settings_pack forget_settings()
{
	settings_pack p = settings();
	p.set_int(settings_pack::alert_mask, alert_category::status
		| alert_category::storage | alert_category::piece_progress
		| alert_category::error | alert_category::stats);
	p.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	p.set_int(settings_pack::aio_threads, 1);
	p.set_int(settings_pack::hashing_threads, 1);
	return p;
}

// adds a v1 torrent whose file does not exist yet, waits until it is checked
torrent_handle add_empty(lt::session& ses, std::string const& save_path
	, int const pieces = num_pieces)
{
	add_torrent_params atp = ::create_torrent(nullptr, "forget_piece", piece_size
		, pieces, false, create_torrent::v1_only);
	atp.save_path = save_path;
	atp.flags &= ~torrent_flags::auto_managed;
	atp.flags &= ~torrent_flags::paused;
	torrent_handle const th = ses.add_torrent(atp);
	for (int i = 0; i < 100; ++i)
	{
		torrent_status const st = th.status();
		if (st.state == torrent_status::downloading) return th;
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	TEST_ERROR("torrent did not reach the downloading state");
	return th;
}

// waits for piece_finished_alert for the piece, i.e. the piece passed the
// hash check. With a write-back disk cache its blocks may still be unflushed
bool wait_passed(lt::session& ses, piece_index_t const piece)
{
	auto const end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (std::chrono::steady_clock::now() < end)
	{
		ses.wait_for_alert(std::chrono::milliseconds(100));
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
		{
			if (auto const* pf = alert_cast<piece_finished_alert>(a))
				if (pf->piece_index == piece) return true;
		}
	}
	return false;
}

// calls forget_piece() until it stops answering 3 (still downloading).
// Returns its final answer; *busy counts the 3s
int forget_when_idle(torrent_handle const& th, piece_index_t const piece, int* busy)
{
	for (int i = 0; i < 2000; ++i)
	{
		int const r = th.forget_piece(piece);
		if (r != 3) return r;
		if (busy) ++*busy;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return 3;
}

std::int64_t stats_counter(lt::session& ses, char const* name)
{
	int const idx = find_metric_idx(name);
	TORRENT_ASSERT(idx >= 0);
	ses.post_session_stats();
	auto const end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (std::chrono::steady_clock::now() < end)
	{
		ses.wait_for_alert(std::chrono::milliseconds(100));
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
			if (auto const* s = alert_cast<session_stats_alert>(a))
				return s->counters()[std::size_t(idx)];
	}
	TEST_ERROR("no session_stats_alert");
	return -1;
}

} // anonymous namespace

TORRENT_TEST(forget_piece_return_codes)
{
	std::string const save_path = complete("forget_piece_codes");
	error_code ec;
	remove_all(save_path, ec);
	lt::session ses(forget_settings());
	torrent_handle const th = add_empty(ses, save_path);

	TEST_EQUAL(th.forget_piece(piece_index_t(0)), 1); // we don't have it
	TEST_EQUAL(th.forget_piece(piece_index_t(-1)), 4);
	TEST_EQUAL(th.forget_piece(piece_index_t(num_pieces)), 4);

	std::vector<char> const data = piece_data();
	th.add_piece(piece_index_t(0), data.data());
	TEST_CHECK(wait_passed(ses, piece_index_t(0)));
	std::int64_t const have_before = stats_counter(ses, "ses.num_have_pieces");

	TEST_EQUAL(forget_when_idle(th, piece_index_t(0), nullptr), 0);
	TEST_CHECK(!th.have_piece(piece_index_t(0)));
	TEST_EQUAL(th.forget_piece(piece_index_t(0)), 1); // already forgotten

	// num_have_pieces counts pieces ever completed; it does not go down
	TEST_EQUAL(stats_counter(ses, "ses.num_have_pieces"), have_before);

	ses.remove_torrent(th);
	remove_all(save_path, ec);
}

#if defined TORRENT_LINUX
namespace {

bool punch_piece(std::string const& file, piece_index_t const p)
{
	int const fd = ::open(file.c_str(), O_RDWR);
	if (fd < 0) return false;
	int const r = ::fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
		, std::int64_t(static_cast<int>(p)) * piece_size, piece_size);
	::close(fd);
	return r == 0;
}

struct forget_state
{
	std::mutex mutex;
	std::vector<piece_index_t> retry; // forget_piece() answered 3
	int passed = 0;
	int punched = 0;
	int busy = 0;
	int unexpected = 0;
};

// forgets every piece as soon as it passes the hash check, on the network
// thread, and punches it out right away if forget_piece() accepted it. This is
// the earliest a client could possibly do it.
struct forget_on_pass final : torrent_plugin
{
	forget_on_pass(std::shared_ptr<aux::torrent> t, io_context& ioc
		, std::string file, std::shared_ptr<forget_state> s)
		: m_torrent(std::move(t)), m_ioc(ioc), m_file(std::move(file)), m_state(std::move(s))
	{}

	void on_piece_pass(piece_index_t const p) override
	{
		// on_piece_pass() is called from within torrent::we_have(); run
		// forget_piece() right after it returns instead of re-entering it
		post(m_ioc, [t = m_torrent, p, file = m_file, state = m_state]
		{
			int const r = t->forget_piece(p);
			std::lock_guard<std::mutex> l(state->mutex);
			++state->passed;
			if (r == 0)
			{
				if (punch_piece(file, p)) ++state->punched;
				else ++state->unexpected;
			}
			else if (r == 3)
			{
				++state->busy;
				state->retry.push_back(p);
			}
			else ++state->unexpected;
		});
	}

	std::shared_ptr<aux::torrent> m_torrent;
	io_context& m_ioc;
	std::string m_file;
	std::shared_ptr<forget_state> m_state;
};

} // anonymous namespace

// The documented contract: once forget_piece() returned 0, the caller may
// release the piece's bytes on disk (e.g. punch a hole). With a write-back
// disk cache, a piece passes the hash check before all its blocks reach the
// disk; if forget_piece() accepted such a piece, a later flush would write
// the old bytes back into the hole.
TORRENT_TEST(forget_piece_leaves_nothing_to_flush)
{
	std::string const save_path = complete("forget_piece_flush");
	error_code ec;
	remove_all(save_path, ec);
	lt::session ses(forget_settings());
	std::string const file = combine_path(save_path, "forget_piece");
	auto const state = std::make_shared<forget_state>();
	slow_writes = true;
	io_context& ioc = ses.native_handle()->get_context();
	ses.add_extension([&ioc, file, state](torrent_handle const& h, client_data_t)
		-> std::shared_ptr<torrent_plugin>
		{ return std::make_shared<forget_on_pass>(h.native_handle(), ioc, file, state); });
	// one piece more than is added: if every piece passed before it was
	// written, a torrent made of just those would finish and drop its piece
	// picker, and forget_piece() would answer 2 to the retries
	torrent_handle const th = add_empty(ses, save_path, num_pieces + 1);

	// one piece at a time, so the network thread is idle when a piece passes
	// and the forget_piece() posted by the plugin runs right away
	std::vector<char> const data = piece_data();
	for (int p = 0; p < num_pieces; ++p)
	{
		th.add_piece(piece_index_t(p), data.data());
		auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
		for (;;)
		{
			{
				std::lock_guard<std::mutex> l(state->mutex);
				if (state->passed == p + 1) break;
			}
			if (std::chrono::steady_clock::now() > deadline) { TEST_ERROR("piece did not pass"); break; }
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}

	// the pieces forget_piece() turned down: retry the way a client would
	std::vector<piece_index_t> retry;
	{
		std::lock_guard<std::mutex> l(state->mutex);
		retry.swap(state->retry);
	}
	int late_punched = 0;
	for (piece_index_t const p : retry)
	{
		int const r = forget_when_idle(th, p, nullptr);
		TEST_EQUAL(r, 0);
		if (r == 0 && punch_piece(file, p)) ++late_punched;
	}

	// write out whatever the disk cache still holds
	th.flush_cache();
	auto const end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	bool flushed = false;
	while (!flushed && std::chrono::steady_clock::now() < end)
	{
		ses.wait_for_alert(std::chrono::milliseconds(100));
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts) if (alert_cast<cache_flushed_alert>(a)) flushed = true;
	}
	TEST_CHECK(flushed);

	slow_writes = false;

	// every punched piece must still be a hole
	int resurrected = 0;
	int const fd = ::open(file.c_str(), O_RDONLY);
	TEST_CHECK(fd >= 0);
	std::vector<char> buf(static_cast<std::size_t>(piece_size));
	for (int p = 0; fd >= 0 && p < num_pieces; ++p)
	{
		auto const n = ::pread(fd, buf.data(), buf.size(), off_t(p) * piece_size);
		if (n > 0 && std::any_of(buf.begin(), buf.begin() + n, [](char c) { return c != 0; }))
			++resurrected;
	}
	if (fd >= 0) ::close(fd);

	std::lock_guard<std::mutex> l(state->mutex);
	std::printf("passed: %d punched on pass: %d answered 3: %d punched after retry: %d"
		" unexpected: %d resurrected: %d\n"
		, state->passed, state->punched, state->busy, late_punched, state->unexpected, resurrected);
	TEST_EQUAL(state->punched + late_punched, num_pieces);
	TEST_EQUAL(state->unexpected, 0);
	TEST_EQUAL(resurrected, 0);
	// the test only proves something if some pieces passed before they were
	// on disk; if the slow-write interposer stopped working, none would
	TEST_CHECK(state->busy > 0);

	ses.remove_torrent(th);
	remove_all(save_path, ec);
}
#endif

// With suggest_mode = suggest_read_cache a finished torrent keeps its piece
// picker but also marks itself as having every piece. Forgetting a piece must
// still make the torrent download it again.
TORRENT_TEST(forget_piece_on_finished_torrent_keeping_its_picker)
{
	std::string const save_path = complete("forget_piece_read_cache");
	error_code ec;
	remove_all(save_path, ec);
	settings_pack pack = forget_settings();
	pack.set_int(settings_pack::suggest_mode, settings_pack::suggest_read_cache);
	lt::session ses(pack);
	torrent_handle const th = add_empty(ses, save_path);

	std::vector<char> const data = piece_data();
	for (int p = 0; p < num_pieces; ++p)
		th.add_piece(piece_index_t(p), data.data());
	bool seeding = false;
	for (int i = 0; i < 200 && !seeding; ++i)
	{
		seeding = th.status().is_seeding;
		if (!seeding) std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	TEST_CHECK(seeding);

	TEST_EQUAL(forget_when_idle(th, piece_index_t(0), nullptr), 0);
	torrent_status st;
	for (int i = 0; i < 100; ++i)
	{
		st = th.status();
		if (!st.is_seeding && !st.is_finished) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	TEST_CHECK(!th.have_piece(piece_index_t(0)));
	TEST_CHECK(!st.is_seeding);
	TEST_CHECK(!st.is_finished);

	ses.remove_torrent(th);
	remove_all(save_path, ec);
}

#if defined TORRENT_LINUX
// the disk side of forget_piece(): a piece that passed and was forgotten is
// downloaded again. With pread_disk_io a block of it may arrive while the old
// blocks of the piece are still being flushed, holding the piece's previous
// cache entry. That write must be failed with operation_aborted (the peer
// requests the block again) instead of going into the old entry, and
// counted. Once the flush is done, the piece starts over and is hashed anew
TORRENT_TEST(rewrite_aborted_while_old_blocks_are_flushing)
{
	using namespace std::chrono_literals;
	std::string const save_path = complete("forget_piece_rewrite");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	io_context ioc;
	counters cnt;
	settings_pack pack = settings();
	pack.set_int(settings_pack::aio_threads, 1);
	pack.set_int(settings_pack::hashing_threads, 1);
	std::unique_ptr<disk_interface> disk = pread_disk_io_constructor(ioc, pack, cnt);

	int const blocks = 2;
	int const len = blocks * default_block_size;
	file_storage fs;
	fs.set_piece_length(len);
	fs.add_file("rewrite", len);
	fs.set_num_pieces(1);
	aux::vector<download_priority_t, file_index_t> prios;
	renamed_files rf;
	storage_params params(fs, rf, save_path, "", storage_mode_sparse, prios
		, sha1_hash("01234567890123456789"), true, false);
	storage_holder st = disk->new_torrent(params, {});

	auto run_until = [&](auto done)
	{
		auto const end = std::chrono::steady_clock::now() + 10s;
		while (!done() && std::chrono::steady_clock::now() < end)
		{
			disk->submit_jobs();
			ioc.run_for(5ms);
			ioc.restart();
		}
		return done();
	};

	bool checked = false;
	add_torrent_params atp;
	disk->async_check_files(st, &atp, aux::vector<std::string, file_index_t>{}
		, [&](status_t, storage_error const&) { checked = true; });
	TEST_CHECK(run_until([&] { return checked; }));

	auto piece_hash = [&](char const fill)
	{
		std::vector<char> const buf(static_cast<std::size_t>(len), fill);
		return hasher(buf).final();
	};
	std::vector<char> const a(static_cast<std::size_t>(default_block_size), 'A');
	std::vector<char> const b(static_cast<std::size_t>(default_block_size), 'B');

	// the piece arrives and passes; its flush is held at the disk
	set_hold_writes(true);
	int a_written = 0;
	for (int i = 0; i < blocks; ++i)
	{
		disk->async_write(st, peer_request{0_piece, i * default_block_size, default_block_size}
			, a.data(), {}, [&](storage_error const& e) { TEST_CHECK(!e); ++a_written; });
	}
	sha1_hash first;
	disk->async_hash(st, 0_piece, {}, disk_interface::v1_hash
		, [&](piece_index_t, sha1_hash const& h, storage_error const&) { first = h; });
	TEST_CHECK(run_until([&] {
		std::lock_guard<std::mutex> l(gate_mutex);
		return !first.is_all_zeros() && writes_waiting > 0;
	}));
	TEST_CHECK(first == piece_hash('A'));
	TEST_EQUAL(a_written, 0);

	// the piece was forgotten and a block of it arrives again
	bool b_done = false;
	storage_error b_error;
	disk->async_write(st, peer_request{0_piece, 0, default_block_size}
		, b.data(), {}, [&](storage_error const& e) { b_error = e; b_done = true; });
	TEST_CHECK(run_until([&] { return b_done; }));
	TEST_CHECK(b_error.ec == boost::asio::error::operation_aborted);
	TEST_EQUAL(cnt[counters::num_rejected_piece_rewrites], 1);

	// the old blocks reach the disk
	set_hold_writes(false);
	TEST_CHECK(run_until([&] { return a_written == blocks; }));

	// the piece is downloaded again and hashed from the new blocks
	int b_written = 0;
	for (int i = 0; i < blocks; ++i)
	{
		disk->async_write(st, peer_request{0_piece, i * default_block_size, default_block_size}
			, b.data(), {}, [&](storage_error const& e) { TEST_CHECK(!e); ++b_written; });
	}
	sha1_hash second;
	disk->async_hash(st, 0_piece, {}, disk_interface::v1_hash
		, [&](piece_index_t, sha1_hash const& h, storage_error const&) { second = h; });
	TEST_CHECK(run_until([&] { return !second.is_all_zeros(); }));
	TEST_CHECK(second == piece_hash('B'));
	TEST_EQUAL(cnt[counters::num_rejected_piece_rewrites], 1);

	// tear down the way a torrent does
	bool stopped = false;
	disk->async_stop_torrent(st, [&] { stopped = true; });
	TEST_CHECK(run_until([&] { return stopped && b_written == blocks; }));
	st.reset();
	disk->abort(true);
	remove_all(save_path, ec);
}
#endif
