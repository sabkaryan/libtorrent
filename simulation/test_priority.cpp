/*

Copyright (c) 2026, Arvid Norberg
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/torrent_status.hpp"
#include "libtorrent/torrent_flags.hpp"
#include "libtorrent/settings_pack.hpp"

#include <cstdint>
#include <cstdio>
#include <functional>
#include <vector>

#include "simulator/simulator.hpp"
#include "simulator/utils.hpp"

#include "test.hpp"
#include "settings.hpp"
#include "setup_transfer.hpp" // for addr()
#include "utils.hpp"
#include "disk_io.hpp"

using namespace lt;
using namespace std::chrono_literals;

namespace {

// test_disk_io::async_set_file_priority() unconditionally fails with
// operation_not_supported, exercising on_file_priority()'s error path: a
// deferred priority update must still apply once the failing job completes.
template <typename Setup, typename Check>
void run_test(int const num_files, Setup const& setup, Check const& check)
{
	lt::address const peer0 = addr("50.0.0.1");

	sim::default_config network_cfg;
	sim::simulation sim{network_cfg};
	sim::asio::io_context ios0{sim, peer0};

	lt::session_proxy zombie;

	lt::session_params params;
	params.settings = settings();
	params.disk_io_constructor = test_disk().set_seed();

	auto ses = std::make_shared<lt::session>(params, ios0);

	// v1_only keeps the file count exactly num_files (no pad files), so
	// priorities can be compared against a plain vector
	lt::add_torrent_params atp =
		::create_test_torrent(lt::default_block_size, 4, lt::create_torrent::v1_only, num_files);
	atp.save_path = ".";
	atp.flags &= ~lt::torrent_flags::paused;
	atp.flags &= ~lt::torrent_flags::auto_managed;

	torrent_handle h;
	bool got_file_error_alert = false;
	print_alerts(*ses, [&](lt::session&, lt::alert const* a) {
		if (auto const* ta = alert_cast<add_torrent_alert>(a))
			h = ta->handle;
		else if (alert_cast<file_error_alert>(a))
			got_file_error_alert = true;
	});

	ses->async_add_torrent(atp);

	sim::timer t0(sim, 1s, [&](boost::system::error_code const&) { setup(h); });

	sim::timer t1(sim, 3s, [&](boost::system::error_code const&) {
		// confirms the disk-error path in on_file_priority() actually ran
		TEST_CHECK(got_file_error_alert);

		check(h);

		zombie = ses->abort();
		ses.reset();
	});

	sim.run();
}

// simulated seconds at which the downloader of run_finished_reconnect()
// reached each step, -1 if it never did
struct reconnect_times
{
	double finished = -1;
	double dropped = -1;
	double resumed = -1;
	double reconnected = -1;
	// finished again, with every piece it wants
	double complete = -1;
	// connect candidates and known peers right after `resume`
	int candidates = -1;
	int list_peers = -1;
	// the pieces it had when it was finished again, of num_pieces
	int pieces = -1;
	int num_pieces = -1;
};

// how the connection to the seed is closed once the downloader is finished
enum class drop_by : std::uint8_t
{
	// the downloader closes it (close_redundant_connections)
	close_redundant,
	// close_redundant_connections is off, and the client pauses the torrent
	// and resumes it at once, still finished
	pause
};

// One seed and one downloader that at first wants only half of the pieces.
// The downloader finishes and the connection to the seed is closed (`drop`).
// `resume_delay` seconds after that, `resume` makes it want more pieces: it
// must connect to the seed it knows again and finish again. That is one
// cycle; `cycles` of them run one after the other. Nothing else tells it
// about the seed (no tracker, DHT or incoming connection), and adding the
// same address again would not change the peer list either
template <typename Resume>
std::vector<reconnect_times> run_finished_reconnect(int const min_reconnect_time
	, int const resume_delay, int const window, Resume const& resume
	, drop_by const drop = drop_by::close_redundant, int const cycles = 1)
{
	sim::default_config network_cfg;
	sim::simulation sim{network_cfg};
	sim::asio::io_context seed_ios{sim, addr("60.0.0.1")};
	sim::asio::io_context dl_ios{sim, addr("50.0.0.1")};
	lt::session_proxy zombie[2];

	lt::add_torrent_params atp = ::create_test_torrent(4 * lt::default_block_size, 8
		, lt::create_torrent::v1_only);
	atp.save_path = ".";
	atp.flags &= ~(lt::torrent_flags::paused | lt::torrent_flags::auto_managed);
	int const num_pieces = atp.ti->num_pieces();

	auto pack = [](char const* listen) {
		lt::settings_pack p = settings();
		p.set_int(lt::settings_pack::alert_mask, lt::alert_category::status
			| lt::alert_category::error | lt::alert_category::peer | lt::alert_category::connect);
		p.set_str(lt::settings_pack::listen_interfaces, listen);
		return p;
	};

	lt::session_params sp;
	sp.settings = pack("60.0.0.1:6881");
	sp.disk_io_constructor = test_disk().set_seed();
	auto seed = std::make_shared<lt::session>(sp, seed_ios);
	lt::add_torrent_params seed_atp = atp;
	seed_atp.flags |= lt::torrent_flags::seed_mode;
	seed->async_add_torrent(seed_atp);

	lt::session_params dp;
	dp.settings = pack("50.0.0.1:6881");
	dp.settings.set_bool(lt::settings_pack::close_redundant_connections
		, drop == drop_by::close_redundant);
	dp.settings.set_int(lt::settings_pack::min_reconnect_time, min_reconnect_time);
	dp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(dp, dl_ios);

	atp.piece_priorities.assign(std::size_t(num_pieces), lt::default_priority);
	for (int i = num_pieces / 2; i < num_pieces; ++i)
		atp.piece_priorities[std::size_t(i)] = lt::dont_download;

	torrent_handle h;
	print_alerts(*ses, [&](lt::session&, lt::alert const* a) {
		if (auto const* ta = alert_cast<add_torrent_alert>(a))
		{
			h = ta->handle;
			h.connect_peer(tcp::endpoint(addr("60.0.0.1"), 6881));
		}
	});
	ses->async_add_torrent(atp);

	std::vector<reconnect_times> rs(1);
	rs.back().num_pieces = num_pieces;
	// drop_by::pause: the torrent of this cycle was paused
	bool paused = false;
	lt::time_point const start = lt::clock_type::now();
	auto const seconds = [&] {
		return lt::duration_cast<lt::milliseconds>(lt::clock_type::now() - start).count() / 1000.0;
	};
	sim::asio::high_resolution_timer timer(dl_ios);
	std::function<void(boost::system::error_code const&)> on_tick;
	on_tick = [&](boost::system::error_code const&) {
		double const now = seconds();
		if (rs.back().complete >= 0 && int(rs.size()) < cycles)
		{
			rs.emplace_back();
			rs.back().num_pieces = num_pieces;
			rs.back().finished = now;
			paused = false;
		}
		reconnect_times& r = rs.back();
		if (h.is_valid())
		{
			lt::torrent_status const st = h.status();
			if (r.finished < 0 && st.is_finished) r.finished = now;
			if (drop == drop_by::pause && r.finished >= 0 && !paused && st.num_peers > 0)
			{
				h.pause();
				paused = true;
			}
			if (r.finished >= 0 && r.dropped < 0 && st.num_peers == 0
				&& (drop != drop_by::pause || paused))
			{
				r.dropped = now;
				if (drop == drop_by::pause) h.resume();
			}
			if (r.dropped >= 0 && r.resumed < 0 && now - r.dropped >= resume_delay)
			{
				resume(h, num_pieces, int(rs.size()) - 1);
				r.resumed = now;
				lt::torrent_status const after = h.status();
				r.candidates = after.connect_candidates;
				r.list_peers = after.list_peers;
			}
			else if (r.resumed >= 0)
			{
				if (r.reconnected < 0 && st.num_peers > 0) r.reconnected = now;
				if (r.reconnected >= 0 && st.is_finished)
				{
					r.complete = now;
					r.pieces = st.num_pieces;
				}
			}
		}
		if ((r.complete >= 0 && int(rs.size()) == cycles)
			|| (r.resumed >= 0 && now - r.resumed > window) || now > 600)
		{
			zombie[0] = ses->abort();
			zombie[1] = seed->abort();
			ses.reset();
			seed.reset();
			return;
		}
		timer.expires_after(lt::milliseconds(100));
		timer.async_wait(on_tick);
	};
	timer.expires_after(lt::milliseconds(100));
	timer.async_wait(on_tick);
	sim.run();

	// on stderr: the series scripts read it from there
	for (std::size_t i = 0; i < rs.size(); ++i)
	{
		reconnect_times const& r = rs[i];
		std::fprintf(stderr, "RESULT cycle=%d finished=%.1f dropped=%.1f resumed=%.1f candidates=%d"
			" list_peers=%d reconnected=%.1f complete=%.1f pieces=%d/%d\n", int(i), r.finished
			, r.dropped, r.resumed, r.candidates, r.list_peers, r.reconnected, r.complete, r.pieces
			, r.num_pieces);
	}
	rs.resize(std::size_t(cycles));
	return rs;
}

void resume_by_priority(torrent_handle const& h, int const num_pieces, int)
{
	h.prioritize_pieces(std::vector<download_priority_t>(std::size_t(num_pieces)
		, lt::default_priority));
}

} // anonymous namespace

// a prioritize_files() call issued while one is already outstanding is
// deferred rather than dispatched as a second concurrent disk job, and
// must still apply once the outstanding job completes, even on error.
TORRENT_TEST(prioritize_files_disk_error)
{
	int const num_files = 4;
	std::vector<download_priority_t> const first(std::size_t(num_files), lt::dont_download);
	std::vector<download_priority_t> const last(std::size_t(num_files), lt::top_priority);

	run_test(
		num_files,
		[&](torrent_handle h) {
			// the second call, issued before the first job's completion
			// handler runs, merges into m_deferred_file_priorities
			h.prioritize_files(first);
			h.prioritize_files(last);
		},
		[&](torrent_handle h) { TEST_CHECK(h.get_file_priorities() == last); });
}

// same as above, exercised through set_file_priority(), which shares
// on_file_priority() with prioritize_files()
TORRENT_TEST(set_file_priority_disk_error)
{
	int const num_files = 4;

	run_test(
		num_files,
		[](torrent_handle h) {
			h.file_priority(file_index_t(0), lt::dont_download);
			h.file_priority(file_index_t(0), lt::top_priority);
		},
		[](torrent_handle h) { TEST_EQUAL(h.file_priority(file_index_t(0)), lt::top_priority); });
}

// leaving the finished state brings back the peers the torrent dropped
// while it was finished: the peer list counted no seed as a connect
// candidate then, and with no candidates the torrent did not try to connect
// at all, so the count was never updated
TORRENT_TEST(finished_reconnect_after_priority)
{
	reconnect_times const r = run_finished_reconnect(1, 5, 30, &resume_by_priority).front();
	TEST_CHECK(r.finished >= 0);
	TEST_CHECK(r.dropped >= 0);
	TEST_CHECK(r.resumed >= 0);
	TEST_EQUAL(r.list_peers, 1);
	TEST_EQUAL(r.candidates, 1);
	TEST_CHECK(r.reconnected >= 0);
	TEST_CHECK(r.reconnected - r.resumed <= 3);
	TEST_CHECK(r.complete >= 0);
	TEST_EQUAL(r.pieces, r.num_pieces);
}

// the same, when the torrent leaves the finished state because a piece it
// wants is forgotten
TORRENT_TEST(finished_reconnect_after_forget_piece)
{
	int forgot = -1;
	reconnect_times const r = run_finished_reconnect(1, 5, 30
		, [&](torrent_handle const& h, int, int) { forgot = h.forget_piece(piece_index_t(0)); }).front();
	TEST_EQUAL(forgot, 0);
	TEST_CHECK(r.resumed >= 0);
	TEST_EQUAL(r.list_peers, 1);
	TEST_EQUAL(r.candidates, 1);
	TEST_CHECK(r.reconnected >= 0);
	TEST_CHECK(r.reconnected - r.resumed <= 3);
	// it downloads the forgotten piece again; the other half is still not wanted
	TEST_CHECK(r.complete >= 0);
	TEST_EQUAL(r.pieces, r.num_pieces / 2);
}

// a dropped peer is still not tried again before min_reconnect_time has
// passed since it was dropped, finished or not
TORRENT_TEST(finished_reconnect_waits_min_reconnect_time)
{
	reconnect_times const r = run_finished_reconnect(60, 5, 90, &resume_by_priority).front();
	TEST_CHECK(r.resumed >= 0);
	TEST_EQUAL(r.candidates, 1);
	TEST_CHECK(r.reconnected >= 0);
	TEST_CHECK(r.reconnected - r.dropped >= 59);
	TEST_CHECK(r.reconnected - r.dropped <= 63);
	TEST_CHECK(r.complete >= 0);
	TEST_EQUAL(r.pieces, r.num_pieces);
}

// the same, when the torrent leaves the finished state on its way through a
// check of its files (here the disk has none of them), not through
// resume_download()
TORRENT_TEST(finished_reconnect_after_recheck)
{
	reconnect_times const r = run_finished_reconnect(1, 5, 30
		, [](torrent_handle const& h, int, int) { h.force_recheck(); }).front();
	TEST_CHECK(r.resumed >= 0);
	TEST_EQUAL(r.list_peers, 1);
	TEST_CHECK(r.reconnected >= 0);
	TEST_CHECK(r.reconnected - r.resumed <= 3);
	// it downloads the wanted half again
	TEST_CHECK(r.complete >= 0);
	TEST_EQUAL(r.pieces, r.num_pieces / 2);
}

// three cycles in a row with a seed that has every piece from the start,
// and a downloader that does not close redundant connections. Each cycle
// the finished torrent is paused and resumed, which closes the connection,
// then forgets one more of the pieces it has and finishes again
TORRENT_TEST(finished_reconnect_after_pause_three_cycles)
{
	std::vector<int> forgot;
	std::vector<reconnect_times> const rs = run_finished_reconnect(1, 5, 30
		, [&](torrent_handle const& h, int, int const cycle) {
			forgot.push_back(h.forget_piece(piece_index_t(cycle)));
		}, drop_by::pause, 3);
	TEST_EQUAL(int(forgot.size()), 3);
	for (int const f : forgot) TEST_EQUAL(f, 0);
	for (reconnect_times const& r : rs)
	{
		TEST_CHECK(r.dropped >= 0);
		TEST_CHECK(r.resumed >= 0);
		TEST_EQUAL(r.list_peers, 1);
		TEST_EQUAL(r.candidates, 1);
		TEST_CHECK(r.reconnected >= 0);
		TEST_CHECK(r.reconnected - r.resumed <= 3);
		TEST_CHECK(r.complete >= 0);
		TEST_EQUAL(r.pieces, r.num_pieces / 2);
	}
}
