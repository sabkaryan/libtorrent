/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/torrent_flags.hpp"
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/peer_connection.hpp"
#include "libtorrent/aux_/piece_picker.hpp"
#include "simulator/simulator.hpp"
#include "simulator/utils.hpp"
#include "settings.hpp"
#include "test.hpp"
#include "disk_io.hpp"
#include "setup_swarm.hpp"

using namespace lt;

namespace {

struct outcome
{
	// from setting the deadline until the piece is downloaded, -1: never
	int done_ms = -1;
	// the most blocks of the piece requested from two peers at once, until it
	// is downloaded. Nothing else asks a second peer that early: busy-mode
	// requests wait for the average download time of a time-critical piece,
	// and none has been downloaded yet
	int extra_copies = 0;
};

// One slow and one other seed. Once the downloader has requested blocks of a
// piece from the slow seed, that piece gets a deadline of deadline_ms.
outcome late_holder(int const slow_kib, int const fast_kib, int const deadline_ms)
{
	int const piece_size = 0x40000; // 16 blocks
	// more than the fast seed sends in the 60 s: no end-game requests
	int const num_pieces = 400;

	// the default config caps every host's upload at 200 kB/s, which would
	// hide the seeds' own limits
	dsl_config network_cfg(100000, 1000000, lt::milliseconds(10));
	sim::simulation sim{network_cfg};

	lt::add_torrent_params atp = ::create_test_torrent(piece_size, num_pieces, {});
	atp.save_path = ".";
	atp.flags &= ~(torrent_flags::auto_managed | torrent_flags::paused);

	auto pack = [] {
		settings_pack p = settings();
		p.set_int(settings_pack::alert_mask, alert_category::status
			| alert_category::piece_progress | alert_category::error);
		p.set_bool(settings_pack::enable_outgoing_utp, false);
		p.set_bool(settings_pack::enable_incoming_utp, false);
		return p;
	};

	int const rates[] = {slow_kib, fast_kib};
	std::vector<std::unique_ptr<sim::asio::io_context>> seed_ios;
	std::vector<std::shared_ptr<lt::session>> seeds;
	std::vector<lt::session_proxy> zombies;
	for (int i = 0; i < 2; ++i)
	{
		char ip[32];
		std::snprintf(ip, sizeof(ip), "60.0.0.%d", i + 1);
		seed_ios.push_back(std::make_unique<sim::asio::io_context>(sim, make_address_v4(ip)));
		lt::session_params sp;
		sp.settings = pack();
		sp.settings.set_str(settings_pack::listen_interfaces, std::string(ip) + ":6881");
		sp.settings.set_int(settings_pack::upload_rate_limit, rates[i] * 1024);
		sp.disk_io_constructor = test_disk().set_seed(true);
		seeds.push_back(std::make_shared<lt::session>(sp, *seed_ios.back()));
		auto seed_atp = atp;
		seed_atp.flags |= torrent_flags::seed_mode;
		seeds.back()->async_add_torrent(seed_atp);
	}

	sim::asio::io_context dl_ios(sim, make_address_v4("50.0.0.1"));
	lt::session_params dp;
	dp.settings = pack();
	dp.settings.set_str(settings_pack::listen_interfaces, "50.0.0.1:6881");
	dp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(dp, dl_ios);

	torrent_handle h;
	time_point deadline_set{};
	piece_index_t critical{-1};
	outcome ret;
	ses->set_alert_notify([&] {
		post(ses->get_context(), [&] {
			std::vector<lt::alert*> alerts;
			ses->pop_alerts(&alerts);
			for (lt::alert* a : alerts)
			{
				if (auto const* at = alert_cast<add_torrent_alert>(a))
				{
					h = at->handle;
					for (int i = 0; i < 2; ++i)
					{
						char ip[32];
						std::snprintf(ip, sizeof(ip), "60.0.0.%d", i + 1);
						h.connect_peer(tcp::endpoint(make_address_v4(ip), 6881));
					}
				}
				else if (auto const* pf = alert_cast<piece_finished_alert>(a))
				{
					if (pf->piece_index == critical && ret.done_ms < 0)
						ret.done_ms = int(total_milliseconds(pf->timestamp() - deadline_set));
				}
			}
		});
	});
	ses->async_add_torrent(atp);

	// the piece of which the slow seed holds the most blocks gets a deadline,
	// once the peers' download rates are settled (10 s after they unchoke us)
	sim::timer t1(sim, seconds(15), [&](error_code const&) {
		auto const tor = h.native_handle();
		TEST_CHECK(tor);
		if (!tor) return;
		std::map<piece_index_t, int> held;
		for (auto* p : *tor)
		{
			if (p->remote().address() != make_address_v4("60.0.0.1")) continue;
			for (auto const& b : p->download_queue())
				if (!tor->have_piece(b.block.piece_index)) ++held[b.block.piece_index];
		}
		TEST_CHECK(!held.empty());
		if (held.empty()) return;
		critical = std::max_element(held.begin(), held.end()
			, [](auto const& a, auto const& b) { return a.second < b.second; })->first;
		std::printf("critical piece %d, %d blocks at the slow seed\n"
			, static_cast<int>(critical), held[critical]);
		for (auto* p : *tor)
			std::printf("peer %s rate %d outstanding %d requests %d\n"
				, p->remote().address().to_string().c_str(), p->download_rate_estimate()
				, p->outstanding_bytes(), int(p->download_queue().size()));
		deadline_set = clock_type::now();
		h.set_piece_deadline(critical, deadline_ms);
	});

	// every 100 ms until the piece is downloaded: blocks requested twice
	sim::asio::high_resolution_timer sample(dl_ios);
	std::function<void(error_code const&)> on_sample = [&](error_code const&) {
		auto const tor = h.native_handle();
		if (ret.done_ms >= 0 || !tor || !tor->has_picker()) return;
		int copies = 0;
		for (auto const& dp : tor->picker().get_download_queue())
		{
			if (dp.index != critical) continue;
			for (auto const& b : tor->picker().blocks_for_piece(dp))
				if (b.state == lt::aux::piece_picker::block_info::state_requested && b.num_peers > 1) ++copies;
		}
		ret.extra_copies = std::max(ret.extra_copies, copies);
		sample.expires_after(milliseconds(100));
		sample.async_wait(on_sample);
	};
	sample.expires_after(milliseconds(15050));
	sample.async_wait(on_sample);

	sim::timer t2(sim, seconds(60), [&](error_code const&) {
		sample.cancel();
		zombies.push_back(ses->abort());
		ses.reset();
		for (auto& s : seeds) zombies.push_back(s->abort());
		seeds.clear();
	});

	sim.run();
	std::printf("critical piece done %d ms after its deadline was set, %d blocks requested twice\n"
		, ret.done_ms, ret.extra_copies);
	return ret;
}

} // anonymous namespace

// A peer that keeps sending, but slowly, is not snubbed. When it holds blocks
// of a piece that becomes time-critical, a faster peer is asked for them as
// well, without waiting for the average piece download time.
TORRENT_TEST(late_holder_blocks_requested_from_a_faster_peer)
{
	// the slow seed's blocks of the piece come 4 s apart: without the extra
	// requests the piece takes ~11 s, with them under a second. Where a
	// candidate's own requests put the extra one is the unit test's
	// (test_late_blocks)
	outcome const r = late_holder(4, 400, 0);
	TEST_CHECK(r.done_ms >= 0);
	TEST_CHECK(r.done_ms < 4000);
	TEST_CHECK(r.extra_copies > 0);
}

// The same slow peer, but the deadline leaves it enough time: no other peer
// is asked for its blocks
TORRENT_TEST(holder_in_time_for_the_deadline_keeps_its_blocks)
{
	outcome const r = late_holder(4, 400, 40000);
	TEST_CHECK(r.done_ms >= 0);
	TEST_EQUAL(r.extra_copies, 0);
}

// The holder is late for the deadline, but the other peer is slower: a block
// asked of it now arrives in no less than 4 s, the holder's within ~3.4 s
// (request_queue_time). Asking it would only cost bytes that arrive twice
TORRENT_TEST(late_holder_keeps_its_blocks_without_a_faster_peer)
{
	outcome const r = late_holder(64, 4, 0);
	TEST_CHECK(r.done_ms >= 0);
	TEST_EQUAL(r.extra_copies, 0);
}
