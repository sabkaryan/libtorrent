/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <limits>
#include <string>
#include <vector>

#include <sys/stat.h>

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/torrent_status.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/piece_picker.hpp"
#include "libtorrent/aux_/peer_connection.hpp"
#include "libtorrent/torrent_flags.hpp"
#include "simulator/simulator.hpp"
#include "simulator/utils.hpp"
#include "settings.hpp"
#include "utils.hpp"
#include "test.hpp"
#include "disk_io.hpp"
#include "setup_swarm.hpp"

namespace {

int env_int(char const* name, int def)
{
	char const* v = std::getenv(name);
	return v ? std::atoi(v) : def;
}

// blocks whose wanted requests (sent or queued, of every peer) differ from
// the picker's count of them, as torrent::check_invariant() compares
int count_mismatches(lt::aux::torrent const& t)
{
	if (!t.has_picker()) return 0;
	std::map<lt::piece_block, int> num;
	for (auto const* p : t)
	{
		for (auto const& b : p->request_queue())
			if (!b.not_wanted && !b.timed_out) ++num[b.block];
		for (auto const& b : p->download_queue())
			if (!b.not_wanted && !b.timed_out) ++num[b.block];
	}
	int ret = 0;
	for (auto const& [b, n] : num)
	{
		if (t.picker().is_downloaded(b) || !t.picker().is_downloading(b.piece_index)) continue;
		if (t.picker().num_peers(b) != n) ++ret;
	}
	return ret;
}

bool path_exists(std::string const& path)
{
	struct ::stat st{};
	return ::stat(path.c_str(), &st) == 0;
}

} // anonymous namespace

// A torrent under memory_policy::memory_only downloads from two seeds while
// the pool holds only a few of its pieces: the pieces over the limit are
// declined, every one of them while blocks of it are still arriving from
// both seeds. Nothing may reach the files, the torrent must not fail, no
// piece may fail its hash, no seed may be banned or dropped, and the
// picker must count the requests the peers hold. Once the limit is
// raised the torrent completes in memory.
// MO_PIECE_KIB, MO_PIECES, MO_LIMIT (pieces), MO_PHASE (seconds at the
// limit), MO_V1, MO_POLICY
TORRENT_TEST(memory_only_swarm)
{
	using namespace lt;

	int const piece_kib = env_int("MO_PIECE_KIB", 256);
	int const req_pieces = env_int("MO_PIECES", 40);
	int const limit_pieces = env_int("MO_LIMIT", 4);
	int const phase = env_int("MO_PHASE", 20);
	// MO_V1=0: a hybrid torrent (default v1 only); MO_POLICY=1: plain memory
	bool const v1_only = env_int("MO_V1", 1) != 0;
	auto const policy = env_int("MO_POLICY", 2) == 1 ? memory_policy::memory : memory_policy::memory_only;
	// MO_FLUSH=<ms>: at the limit, flush_cache() that often. It raises a fence
	// over the whole storage until the default back end answers, and the
	// writes and clears of declined pieces that come meanwhile wait behind it
	int const flush_ms = env_int("MO_FLUSH", 0);

	dsl_config network_cfg(100000, 1000000, lt::milliseconds(10));
	sim::simulation sim{network_cfg};

	int const piece_size = piece_kib * 1024;
	lt::add_torrent_params atp = ::create_test_torrent(piece_size, req_pieces
		, v1_only ? lt::create_torrent::v1_only : lt::create_flags_t{});
	int const num_pieces = atp.ti->num_pieces();
	// the torrent ends with a short piece (1234 bytes) whose hash
	// create_test_torrent() computes over a whole piece: the test disk agrees
	// with it, a real hash of the piece never does. It is not downloaded
	piece_index_t const last_piece = atp.ti->last_piece();
	std::int64_t const wanted_bytes = atp.ti->total_size() - atp.ti->piece_size(last_piece);
	std::string const save_path = "memory_only_swarm";
	atp.save_path = save_path;
	atp.flags &= ~(lt::torrent_flags::auto_managed | lt::torrent_flags::paused);

	auto base_pack = [] {
		settings_pack p = settings();
		p.set_int(settings_pack::alert_mask, alert_category::status
			| alert_category::error | alert_category::peer);
		p.set_bool(settings_pack::enable_outgoing_utp, false);
		p.set_bool(settings_pack::enable_incoming_utp, false);
		p.set_bool(settings_pack::close_redundant_connections, false);
		return p;
	};

	std::vector<std::unique_ptr<sim::asio::io_context>> ios;
	std::vector<std::shared_ptr<lt::session>> seeds;
	char const* ips[] = {"60.0.0.1", "60.0.0.2"};
	for (char const* ip : ips)
	{
		ios.push_back(std::make_unique<sim::asio::io_context>(sim, make_address_v4(ip)));
		lt::session_params sp;
		sp.settings = base_pack();
		sp.settings.set_str(settings_pack::listen_interfaces, std::string(ip) + ":6881");
		sp.settings.set_int(settings_pack::upload_rate_limit, 2000 * 1024);
		sp.disk_io_constructor = test_disk().set_seed(true);
		seeds.push_back(std::make_shared<lt::session>(sp, *ios.back()));
		auto seed_atp = atp;
		seed_atp.flags |= lt::torrent_flags::seed_mode;
		seeds.back()->async_add_torrent(seed_atp);
	}

	auto const pool = std::make_shared<memory_storage_pool>();
	pool->set_limit(std::int64_t(limit_pieces) * piece_size);
	pool->set_policy(atp.ti->info_hashes(), policy);

	sim::asio::io_context dl_ios(sim, make_address_v4("50.0.0.1"));
	lt::session_params dp;
	dp.settings = base_pack();
	dp.settings.set_str(settings_pack::listen_interfaces, "50.0.0.1:6881");
	dp.disk_io_constructor = memory_disk_io_constructor(pool);
	auto ses = std::make_shared<lt::session>(dp, dl_ios);

	lt::torrent_handle h;
	int hash_failed = 0;
	int errors = 0;
	int bans = 0;
	int disconnects = 0;
	ses->set_alert_notify([&] {
		post(ses->get_context(), [&] {
			std::vector<lt::alert*> alerts;
			ses->pop_alerts(&alerts);
			for (lt::alert* a : alerts)
			{
				if (auto const* at = alert_cast<add_torrent_alert>(a))
				{
					h = at->handle;
					h.piece_priority(last_piece, dont_download);
					for (char const* ip : ips)
						h.connect_peer(tcp::endpoint(make_address_v4(ip), 6881));
				}
				else if (alert_cast<hash_failed_alert>(a)) ++hash_failed;
				else if (alert_cast<file_error_alert>(a) || alert_cast<torrent_error_alert>(a))
				{
					std::printf("error: %s\n", a->message().c_str());
					++errors;
				}
				else if (alert_cast<peer_ban_alert>(a)) ++bans;
				else if (auto const* pd = alert_cast<peer_disconnected_alert>(a))
				{
					// a seed drops a seed: that is the end, not a failure
					if (pd->error == errors::upload_upload_connection) continue;
					std::printf("disconnect: %s\n", a->message().c_str());
					++disconnects;
				}
			}
		});
	});
	ses->async_add_torrent(atp);

	// 0: at the limit; 1: the limit raised, until the torrent is complete
	int stage = 0;
	int ticks = 0;
	int mismatches = 0;
	std::int64_t declined_at_limit = 0;
	std::int64_t held_at_limit = 0;
	bool files_at_limit = false;
	bool healthy_at_limit = false;
	bool complete = false;
	sim::asio::high_resolution_timer timer(dl_ios);
	std::function<void(boost::system::error_code const&)> on_tick;
	on_tick = [&](boost::system::error_code const&) {
		++ticks;
		auto tor = h.is_valid() ? h.native_handle() : nullptr;
		if (tor) mismatches += count_mismatches(*tor);
		if (tor && stage == 0 && ticks >= phase * 10)
		{
			declined_at_limit = pool->declined_pieces();
			held_at_limit = pool->held_bytes();
			files_at_limit = path_exists(save_path);
			healthy_at_limit = !tor->has_error() && !tor->is_paused();
			pool->set_limit(std::numeric_limits<std::int64_t>::max());
			stage = 1;
		}
		else if (tor && stage == 1 && tor->is_finished())
		{
			complete = true;
			sim.stop();
			return;
		}
		if (ticks > (phase + 300) * 10) { sim.stop(); return; }
		timer.expires_after(lt::milliseconds(100));
		timer.async_wait(on_tick);
	};
	timer.expires_after(lt::milliseconds(100));
	timer.async_wait(on_tick);
	int flushes = 0;
	sim::asio::high_resolution_timer flush_timer(dl_ios);
	std::function<void(boost::system::error_code const&)> on_flush;
	on_flush = [&](boost::system::error_code const&) {
		if (stage != 0) return;
		if (h.is_valid()) { h.flush_cache(); ++flushes; }
		flush_timer.expires_after(lt::milliseconds(flush_ms));
		flush_timer.async_wait(on_flush);
	};
	if (flush_ms > 0)
	{
		flush_timer.expires_after(lt::milliseconds(flush_ms));
		flush_timer.async_wait(on_flush);
	}
	sim.run();

	// on stderr: the test main captures stdout, and _Exit() below drops it
	std::fprintf(stderr, "RESULT declined_at_limit=%lld held_at_limit=%lld files_at_limit=%d"
		" healthy_at_limit=%d complete=%d held=%lld hash_failed=%d errors=%d bans=%d"
		" disconnects=%d mismatches=%d files=%d flushes=%d\n"
		, (long long)declined_at_limit, (long long)held_at_limit, int(files_at_limit)
		, int(healthy_at_limit), int(complete), (long long)pool->held_bytes(), hash_failed
		, errors, bans, disconnects, mismatches, int(path_exists(save_path)), flushes);

	// the scenario happened: pieces were declined at the limit
	TEST_CHECK(declined_at_limit > 0);
	// the limit is soft, checked when a piece starts
	TEST_CHECK(held_at_limit >= std::int64_t(limit_pieces) * piece_size);
	TEST_CHECK(held_at_limit < std::int64_t(limit_pieces + 2) * piece_size);
	TEST_CHECK(!files_at_limit);
	TEST_CHECK(healthy_at_limit);
	TEST_CHECK(complete);
	TEST_EQUAL(pool->held_bytes(), wanted_bytes);
	TEST_EQUAL(hash_failed, 0);
	TEST_EQUAL(errors, 0);
	TEST_EQUAL(bans, 0);
	TEST_EQUAL(disconnects, 0);
	TEST_EQUAL(mismatches, 0);
	TEST_CHECK(!path_exists(save_path));
	TEST_EQUAL(num_pieces, req_pieces + 1);
	// tearing the simulation down while the peers are connected trips an
	// unrelated invariant (the seed count); the result is already known
	std::fflush(stdout);
	std::_Exit(unit_test::g_test_failures > 0 ? 1 : 0);
}
