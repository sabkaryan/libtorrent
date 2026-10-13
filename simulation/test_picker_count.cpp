/*
Stand: look for a torrent::check_invariant() "picker count discrepancy"
(peers' request/download queues vs piece_picker::num_peers()) under a
streaming-reader workload.

One downloader, several seeds. A simulated player reads at a fixed bitrate;
every planner pass writes piece priorities as a list (prioritize_piece_list):
behind the play position 0, the window 7, then 4 or 1, beyond that 0, with an
optional brake (every non-window piece 0). The window gets deadlines. Random
seeks clear the deadlines and move the window, pieces behind the reader are
forgotten, file priorities change, and the network misbehaves: seeds choke,
slow down, reject (small request queue), peers get disconnected.

Every random decision comes from PC_SEED, so a failing seed replays. Requires
asserts=on invariant-checks=on; the check also runs on every tick.
Output: action lines on stderr with PC_LOG=1, one RESULT line on stdout.
*/

#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <vector>
#include <algorithm>

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/alert_types.hpp"
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

} // anonymous namespace

TORRENT_TEST(picker_count)
{
	using namespace lt;

	unsigned const seed = unsigned(env_int("PC_SEED", 1));
	int const nseeds = env_int("PC_SEEDS", 4);
	int const piece_kib = env_int("PC_PIECE_KIB", 256);
	int const req_pieces = env_int("PC_PIECES", 1200);
	int const num_files = env_int("PC_FILES", 3);
	int const bitrate = env_int("PC_BITRATE", 1500);     // KiB/s
	int const window = env_int("PC_WINDOW", 24);
	int const desired = env_int("PC_DESIRED", 80);       // pieces ahead at 4/1
	int const duration = env_int("PC_DURATION", 600);    // simulated seconds
	int const chaos = env_int("PC_CHAOS", 1);
	int const corrupt = env_int("PC_CORRUPT", 0);        // seed 0 sends corrupt data after N blocks
	int const log = env_int("PC_LOG", 0);
	// switches for single actions (1 = on)
	int const do_pause = env_int("PC_PAUSE", 1);
	int const do_forget = env_int("PC_FORGET", 1);
	int const do_files = env_int("PC_FILES_PRIO", 1);
	int const do_seed_pause = env_int("PC_SEED_PAUSE", 1);

	std::mt19937 rng(seed);
	auto chance = [&](int per_10k) { return int(rng() % 10000) < per_10k; };
	auto pick = [&](int n) { return n <= 0 ? 0 : int(rng() % unsigned(n)); };

	dsl_config network_cfg(100000, 1000000, lt::milliseconds(10));
	sim::simulation sim{network_cfg};

	int const piece_size = piece_kib * 1024;
	lt::add_torrent_params atp = ::create_test_torrent(piece_size, req_pieces, {}, num_files);
	// files are aligned to pieces: the torrent has more pieces than asked for
	int const num_pieces = atp.ti->num_pieces();
	atp.save_path = ".";
	atp.flags &= ~lt::torrent_flags::auto_managed;
	atp.flags &= ~lt::torrent_flags::paused;

	auto base_pack = [] {
		settings_pack p = settings();
		p.set_int(settings_pack::alert_mask, alert_category::status
			| alert_category::piece_progress | alert_category::error);
		p.set_bool(settings_pack::enable_outgoing_utp, false);
		p.set_bool(settings_pack::enable_incoming_utp, false);
		return p;
	};

	std::vector<std::unique_ptr<sim::asio::io_context>> seed_ios;
	std::vector<std::shared_ptr<lt::session>> seeds;
	std::vector<address> seed_addr;
	std::vector<int> seed_rate;
	for (int i = 0; i < nseeds; ++i)
	{
		char ip[32];
		std::snprintf(ip, sizeof(ip), "60.0.0.%d", i + 1);
		seed_addr.push_back(make_address_v4(ip));
		seed_ios.push_back(std::make_unique<sim::asio::io_context>(sim, seed_addr.back()));
		seed_rate.push_back(200 + pick(1400));
		lt::session_params sp;
		sp.settings = base_pack();
		sp.settings.set_str(settings_pack::listen_interfaces, std::string(ip) + ":6881");
		sp.settings.set_int(settings_pack::upload_rate_limit, seed_rate.back() * 1024);
		auto disk = test_disk().set_seed(true);
		if (i == 0 && corrupt > 0) disk = disk.send_corrupt_data(corrupt);
		sp.disk_io_constructor = disk;
		seeds.push_back(std::make_shared<lt::session>(sp, *seed_ios.back()));
		auto seed_atp = atp;
		seed_atp.flags |= lt::torrent_flags::seed_mode;
		seeds.back()->async_add_torrent(seed_atp);
	}

	sim::asio::io_context dl_ios(sim, make_address_v4("50.0.0.1"));
	lt::session_params dp;
	dp.settings = base_pack();
	dp.settings.set_str(settings_pack::listen_interfaces, "50.0.0.1:6881");
	dp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(dp, dl_ios);

	lt::torrent_handle h;
	std::vector<char> have(std::size_t(num_pieces), 0);
	std::vector<std::uint8_t> written(std::size_t(num_pieces), 4);
	long long n_prio = 0, n_deadline = 0, n_reset = 0, n_clear = 0, n_seek = 0
		, n_forget = 0, n_forget3 = 0, n_files = 0, n_pause = 0, n_choke = 0
		, n_slow = 0, n_disc = 0, n_done = 0, n_failed = 0, n_inflight = 0;
	lt::time_point const t0 = lt::clock_type::now();
	auto now_ms = [&] { return (long long)lt::total_milliseconds(lt::clock_type::now() - t0); };

	ses->set_alert_notify([&] {
		post(ses->get_context(), [&] {
			std::vector<lt::alert*> alerts;
			ses->pop_alerts(&alerts);
			for (lt::alert* a : alerts)
			{
				if (auto const* at = alert_cast<add_torrent_alert>(a))
				{
					h = at->handle;
					for (auto const& s : seed_addr) h.connect_peer(tcp::endpoint(s, 6881));
				}
				else if (auto const* pf = alert_cast<piece_finished_alert>(a))
				{
					have[std::size_t(static_cast<int>(pf->piece_index))] = 1;
					++n_done;
				}
				else if (alert_cast<hash_failed_alert>(a)) ++n_failed;
			}
		});
	});
	ses->async_add_torrent(atp);

	double pos = 0;
	bool brake = false;
	bool paused = false;
	long long resume_at = 0;
	std::vector<long long> seed_back(std::size_t(nseeds), 0);
	int tick_count = 0;
	auto const tick = lt::milliseconds(50);
	sim::asio::io_context tick_ios(sim, make_address_v4("40.0.0.1"));
	sim::asio::high_resolution_timer timer(tick_ios);

	auto cur_piece = [&] { return std::min(num_pieces - 1, int(pos / piece_size)); };

	// one planner pass, in the order a streaming reader applies it:
	// deadlines of pieces leaving the window are reset, then priorities are
	// written as a list of changes, then the window gets its deadlines
	std::vector<int> deadlined;
	auto plan = [&] {
		int const cur = cur_piece();
		std::vector<std::uint8_t> want(std::size_t(num_pieces), 0);
		for (int j = cur; j < std::min(num_pieces, cur + window); ++j) want[std::size_t(j)] = 7;
		for (int j = cur + window; j < std::min(num_pieces, cur + window + desired); ++j)
			want[std::size_t(j)] = brake ? 0 : (chance(1000) ? 1 : 4);
		std::vector<int> keep;
		for (int p : deadlined)
		{
			if (want[std::size_t(p)] == 7) { keep.push_back(p); continue; }
			h.reset_piece_deadline(piece_index_t(p));
			++n_reset;
		}
		deadlined.swap(keep);
		std::vector<std::pair<piece_index_t, download_priority_t>> list;
		for (int j = 0; j < num_pieces; ++j)
		{
			// a reset deadline leaves 1 behind; the reader rewrites everything it disagrees with
			if (want[std::size_t(j)] == written[std::size_t(j)] && !chance(50)) continue;
			list.emplace_back(piece_index_t(j), download_priority_t(want[std::size_t(j)]));
			written[std::size_t(j)] = want[std::size_t(j)];
		}
		if (!list.empty()) { h.prioritize_pieces(list); ++n_prio; }
		int index = 0;
		for (int j = cur; j < std::min(num_pieces, cur + window); ++j)
		{
			if (have[std::size_t(j)]) continue;
			int const ms = index < 2 ? 0 : index < 5 ? 500 : (index - 2) * 400;
			++index;
			h.set_piece_deadline(piece_index_t(j), ms);
			if (std::find(deadlined.begin(), deadlined.end(), j) == deadlined.end()) deadlined.push_back(j);
			++n_deadline;
		}
		if (log) std::fprintf(stderr, "ACT t=%lld plan cur=%d list=%d brake=%d\n"
			, now_ms(), cur, int(list.size()), int(brake));
	};

	std::function<void(boost::system::error_code const&)> on_tick;
	on_tick = [&](boost::system::error_code const&) {
		++tick_count;
		long long const t = now_ms();
		auto tor = h.is_valid() ? h.native_handle() : nullptr;
		if (tor && tor->has_picker())
		{
			// runs torrent::check_invariant() on entry and exit
			aux::vector<int, piece_index_t> avail;
			tor->piece_availability(avail);
			for (auto* p : *tor) n_inflight += long(p->download_queue().size() + p->request_queue().size());
		}

		if (tor)
		{
			int const cur = cur_piece();
			if (have[std::size_t(cur)] && pos < double(num_pieces - 1) * piece_size)
				pos += bitrate * 1024.0 * lt::total_milliseconds(tick) / 1000.0;

			if (chance(2) && !paused)
			{
				// seek: deadlines go first, then the new window
				h.clear_piece_deadlines();
				deadlined.clear();
				++n_clear;
				pos = double(pick(num_pieces - window)) * piece_size;
				++n_seek;
				if (log) std::fprintf(stderr, "ACT t=%lld seek to=%d\n", t, cur_piece());
				plan();
			}
			else if (tick_count % 10 == 1 && !paused)
			{
				if (chance(500)) brake = !brake;
				plan();
			}

			if (chance(200) && !deadlined.empty())
			{
				int const k = pick(int(deadlined.size()));
				h.reset_piece_deadline(piece_index_t(deadlined[std::size_t(k)]));
				if (log) std::fprintf(stderr, "ACT t=%lld reset %d\n", t, deadlined[std::size_t(k)]);
				deadlined.erase(deadlined.begin() + k);
				++n_reset;
			}

			// forget pieces behind the reader
			if (do_forget && chance(1000) && cur > 2)
			{
				int const p = pick(cur - 1);
				if (have[std::size_t(p)])
				{
					// requests for this piece still in the peers' queues
					if (log) for (auto* pc : *tor)
					{
						for (auto const& b : pc->download_queue())
							if (static_cast<int>(b.block.piece_index) == p)
								std::fprintf(stderr, "ACT t=%lld forget-pending %d dq (%d,%d)%s%s%s %s\n", t, p, p
									, b.block.block_index, b.busy ? " busy" : "", b.not_wanted ? " not-wanted" : ""
									, b.timed_out ? " timed-out" : "", pc->remote().address().to_string().c_str());
						for (auto const& b : pc->request_queue())
							if (static_cast<int>(b.block.piece_index) == p)
								std::fprintf(stderr, "ACT t=%lld forget-pending %d rq (%d,%d)%s %s\n", t, p, p
									, b.block.block_index, b.busy ? " busy" : ""
									, pc->remote().address().to_string().c_str());
					}
					int const r = tor->forget_piece(piece_index_t(p));
					if (r == 0) { have[std::size_t(p)] = 0; ++n_forget; }
					else if (r == 3) ++n_forget3;
					if (log) std::fprintf(stderr, "ACT t=%lld forget %d r=%d\n", t, p, r);
				}
			}

			if (do_files && chance(30))
			{
				int const f = int(static_cast<std::int64_t>(pos) * num_files
					/ std::max<std::int64_t>(1, std::int64_t(num_pieces) * piece_size));
				std::vector<download_priority_t> fp(std::size_t(num_files), dont_download);
				fp[std::size_t(std::min(f, num_files - 1))] = default_priority;
				if (chance(3000)) std::fill(fp.begin(), fp.end(), default_priority);
				h.prioritize_files(fp);
				std::fill(written.begin(), written.end(), std::uint8_t(255));
				++n_files;
				if (log) std::fprintf(stderr, "ACT t=%lld files cur=%d\n", t, f);
			}

			// a short pause, as when the reader closes and reopens the torrent
			if (paused ? t >= resume_at : do_pause && chance(5))
			{
				paused = !paused;
				resume_at = t + 1000 + pick(5000);
				if (paused) h.pause(); else h.resume();
				if (!paused) for (auto const& s : seed_addr) h.connect_peer(tcp::endpoint(s, 6881));
				++n_pause;
				if (log) std::fprintf(stderr, "ACT t=%lld pause=%d\n", t, int(paused));
			}
		}

		if (chaos && t > 2000)
		{
			for (int i = 0; i < nseeds; ++i)
			{
				if (chance(40))
				{
					settings_pack p;
					p.set_int(settings_pack::unchoke_slots_limit, chance(2500) ? 0 : 8);
					p.set_int(settings_pack::max_allowed_in_request_queue, chance(2500) ? 8 : 2000);
					seeds[std::size_t(i)]->apply_settings(p);
					++n_choke;
				}
				if (chance(40))
				{
					settings_pack p;
					p.set_int(settings_pack::upload_rate_limit
						, (chance(2500) ? 2 + pick(20) : seed_rate[std::size_t(i)]) * 1024);
					seeds[std::size_t(i)]->apply_settings(p);
					++n_slow;
				}
				// a seed drops us: it pauses its torrent and comes back
				if (do_seed_pause && chance(20))
				{
					auto ts = seeds[std::size_t(i)]->get_torrents();
					if (!ts.empty() && seed_back[std::size_t(i)] == 0)
					{
						ts[0].pause();
						seed_back[std::size_t(i)] = t + 1000 + pick(4000);
						++n_disc;
					}
				}
				if (seed_back[std::size_t(i)] != 0 && t >= seed_back[std::size_t(i)])
				{
					auto ts = seeds[std::size_t(i)]->get_torrents();
					if (!ts.empty()) ts[0].resume();
					seed_back[std::size_t(i)] = 0;
				}
			}
			if (tick_count % 20 == 0 && h.is_valid() && !paused)
				for (auto const& s : seed_addr) h.connect_peer(tcp::endpoint(s, 6881));
		}

		if (t >= duration * 1000LL)
		{
			std::printf("RESULT seed=%u t=%lld done=%lld failed=%lld prio=%lld deadline=%lld reset=%lld"
				" clear=%lld seek=%lld forget=%lld forget3=%lld files=%lld pause=%lld choke=%lld"
				" slow=%lld disc=%lld inflight_avg=%lld\n", seed, t, n_done, n_failed, n_prio, n_deadline, n_reset
				, n_clear, n_seek, n_forget, n_forget3, n_files, n_pause, n_choke, n_slow, n_disc, n_inflight / std::max(1, tick_count));
			std::fflush(stdout);
			std::_Exit(0);
		}
		timer.expires_after(tick);
		timer.async_wait(on_tick);
	};
	timer.expires_after(tick);
	timer.async_wait(on_tick);

	sim.run();
}

namespace {

// the comparison torrent::check_invariant() makes, without aborting: blocks a
// peer still waits for (not timed out, not cancelled) against the picker's
// count, for blocks of pieces being downloaded. Returns the number of blocks
// that disagree.
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

// wanted (not cancelled, not timed out) requests for blocks of piece p, all
// peers: sent (download queue) or still queued to be sent (request queue)
int wanted_requests(lt::aux::torrent const& t, lt::piece_index_t const p, bool const sent)
{
	int ret = 0;
	for (auto const* pc : t)
		for (auto const& b : sent ? pc->download_queue() : pc->request_queue())
			if (b.block.piece_index == p && !b.not_wanted && !b.timed_out) ++ret;
	return ret;
}

} // anonymous namespace

// A slow peer holds requests for blocks of a piece; a deadline makes a fast
// peer fetch the same blocks (busy requests). The piece passes while the slow
// peer still has its requests queued, then it is forgotten and downloaded
// again: the slow peer's requests count in its queue but not in the picker.
// One more block waits in the slow peer's request queue, unsent while its
// download queue is full (as when its desired queue size shrinks).
TORRENT_TEST(forget_after_busy_duplicate)
{
	using namespace lt;

	dsl_config network_cfg(100000, 1000000, lt::milliseconds(10));
	sim::simulation sim{network_cfg};

	int const num_pieces = 8;
	lt::add_torrent_params atp = ::create_test_torrent(16 * 0x4000, num_pieces, {});
	atp.save_path = ".";
	atp.flags &= ~(lt::torrent_flags::auto_managed | lt::torrent_flags::paused);
	piece_index_t const piece{0};

	std::vector<std::unique_ptr<sim::asio::io_context>> ios;
	std::vector<std::shared_ptr<lt::session>> seeds;
	char const* ips[] = {"60.0.0.1", "60.0.0.2"}; // slow, fast
	int const rates[] = {2, 1000}; // KiB/s
	for (int i = 0; i < 2; ++i)
	{
		ios.push_back(std::make_unique<sim::asio::io_context>(sim, make_address_v4(ips[i])));
		lt::session_params sp;
		sp.settings = settings();
		sp.settings.set_str(settings_pack::listen_interfaces, std::string(ips[i]) + ":6881");
		sp.settings.set_int(settings_pack::upload_rate_limit, rates[i] * 1024);
		sp.settings.set_bool(settings_pack::close_redundant_connections, false);
		sp.disk_io_constructor = test_disk().set_seed(true);
		seeds.push_back(std::make_shared<lt::session>(sp, *ios.back()));
		auto seed_atp = atp;
		seed_atp.flags |= lt::torrent_flags::seed_mode;
		seeds.back()->async_add_torrent(seed_atp);
	}

	sim::asio::io_context dl_ios(sim, make_address_v4("50.0.0.1"));
	lt::session_params dp;
	dp.settings = settings();
	dp.settings.set_str(settings_pack::listen_interfaces, "50.0.0.1:6881");
	// only piece 0 is wanted, so the fast peer runs out of free blocks and
	// duplicates the slow peer's (end game); keep both peers once finished
	dp.settings.set_bool(settings_pack::close_redundant_connections, false);
	dp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(dp, dl_ios);

	lt::torrent_handle h;
	bool passed = false;
	ses->set_alert_notify([&] {
		post(ses->get_context(), [&] {
			std::vector<lt::alert*> alerts;
			ses->pop_alerts(&alerts);
			for (lt::alert* a : alerts)
			{
				if (auto const* at = alert_cast<add_torrent_alert>(a))
				{
					h = at->handle;
					std::vector<download_priority_t> prio(std::size_t(num_pieces), dont_download);
					prio[0] = top_priority;
					h.prioritize_pieces(prio);
					h.connect_peer(tcp::endpoint(make_address_v4(ips[0]), 6881));
				}
				else if (auto const* pf = alert_cast<piece_finished_alert>(a))
				{
					if (pf->piece_index == piece) passed = true;
				}
			}
		});
	});
	ses->async_add_torrent(atp);

	// 0: the slow peer gets requests; 1: the fast peer and the deadline;
	// 2: the piece passed, forget it; 3: downloading again, compare
	int stage = 0;
	int stale_at_forget = -1;
	int stale_unsent = -1;
	int forget_result = -1;
	int refused = 0;
	int mismatches = 0;
	int ticks = 0;
	sim::asio::high_resolution_timer timer(dl_ios);
	std::function<void(boost::system::error_code const&)> on_tick;
	on_tick = [&](boost::system::error_code const&) {
		++ticks;
		auto tor = h.is_valid() ? h.native_handle() : nullptr;
		// after the deadline, which makes queued requests time critical
		if (tor && stage == 1 && ticks == 30)
			for (auto* pc : *tor)
				if (pc->remote().address() == make_address_v4(ips[0]))
					pc->add_request(piece_block(piece, 15), {});
		if (tor && stage == 0 && ticks >= 20)
		{
			h.connect_peer(tcp::endpoint(make_address_v4(ips[1]), 6881));
			h.set_piece_deadline(piece, 0);
			stage = 1;
		}
		else if (tor && stage == 1 && passed)
		{
			if (stale_at_forget < 0)
			{
				stale_at_forget = wanted_requests(*tor, piece, true);
				stale_unsent = wanted_requests(*tor, piece, false);
			}
			forget_result = tor->forget_piece(piece);
			if (forget_result == 3) ++refused;
			else stage = 2;
		}
		else if (tor && stage == 2 && tor->has_picker() && tor->picker().is_downloading(piece))
		{
			mismatches = count_mismatches(*tor);
			stage = 3;
		}
		if (stage == 3 || ticks > 600) { sim.stop(); return; }
		timer.expires_after(lt::milliseconds(100));
		timer.async_wait(on_tick);
	};
	timer.expires_after(lt::milliseconds(100));
	timer.async_wait(on_tick);
	sim.run();

	std::printf("RESULT stage=%d stale_at_forget=%d stale_unsent=%d forget=%d refused=%d mismatches=%d\n"
		, stage, stale_at_forget, stale_unsent, forget_result, refused, mismatches);
	// the scenario happened: the piece was downloaded again after the forget
	TEST_EQUAL(stage, 3);
	TEST_EQUAL(mismatches, 0);
	// tearing the simulation down aborts; the result is already known
	std::fflush(stdout);
	std::_Exit(stage == 3 && mismatches == 0 ? 0 : 1);
}
