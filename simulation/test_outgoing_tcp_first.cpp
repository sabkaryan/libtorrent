/*

Copyright (c) 2026, the libtorrent contributors
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "utils.hpp"
#include "settings.hpp"
#include "disk_io.hpp"
#include "fake_peer.hpp"
#include "simulator/simulator.hpp"
#include "simulator/sink.hpp"
#include "simulator/packet.hpp"
#include "simulator/utils.hpp"
#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/socket_type.hpp"
#include "libtorrent/aux_/deadline_timer.hpp"

#include <functional>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace {

using lt::settings_pack;

// sits on the way into one host and drops TCP connection attempts, UDP
// datagrams (uTP), or both
struct firewall : sim::sink
{
	bool drop_tcp = false;
	bool drop_udp = false;

	void incoming_packet(sim::aux::packet p) override
	{
		bool const syn = p.type == sim::aux::packet::type_t::syn;
		bool const udp = !p.channel && p.type == sim::aux::packet::type_t::payload;
		if ((drop_tcp && syn) || (drop_udp && udp)) return;
		sim::forward_packet(std::move(p));
	}
	std::string label() const override { return "firewall"; }
};

struct config : sim::default_config
{
	std::map<lt::address, std::shared_ptr<firewall>> walls;

	sim::route incoming_route(lt::address ip) override
	{
		auto const it = walls.find(ip);
		if (it == walls.end()) return sim::default_config::incoming_route(ip);
		return sim::route().append(it->second).append(sim::default_config::incoming_route(ip));
	}
};

struct attempt
{
	lt::milliseconds at;
	bool utp;
	int port;
};

struct outcome
{
	std::vector<attempt> attempts; // outgoing, as peer_connect_alert reports them
	std::vector<std::string> ends; // transport and operation: "tcp connect", "utp connect", ...
	std::vector<lt::milliseconds> end_at;
	bool established_utp = false;
	bool established_tcp = false;
};

bool is_utp(lt::socket_type_t const t)
{
	return t == lt::socket_type_t::utp || t == lt::socket_type_t::utp_ssl;
}

struct run_opts
{
	bool tcp_first = true;
	bool outgoing_tcp = true;
	bool outgoing_utp = true;
	bool drop_tcp = false;
	bool drop_udp = false;
	int duration = 60;
	// after the first uTP connection is up: the seed closes it, and the
	// downloader is told the seed's port is new_port (0: unchanged)
	bool reconnect = false;
	int new_port = 0;
	// the firewall starts dropping UDP when the reconnect starts
	bool drop_udp_on_reconnect = false;
};

// a downloader at 50.0.0.1 is told about a seed at 50.0.0.2:6881 and nothing
// else; the seed accepts TCP and uTP unless the firewall in front of it drops them
outcome run(run_opts const o)
{
	config cfg;
	auto const seed_ip = lt::make_address_v4("50.0.0.2");
	auto wall = std::make_shared<firewall>();
	wall->drop_tcp = o.drop_tcp;
	wall->drop_udp = o.drop_udp;
	cfg.walls[seed_ip] = wall;

	sim::simulation sim{cfg};
	auto ios_d = std::make_unique<sim::asio::io_context>(sim, lt::make_address_v4("50.0.0.1"));
	auto ios_s = std::make_unique<sim::asio::io_context>(sim, seed_ip);
	lt::session_proxy zombie_d;
	lt::session_proxy zombie_s;

	lt::session_params ps;
	ps.settings = settings();
	ps.settings.set_int(settings_pack::alert_mask
		, lt::alert_category::all & ~lt::alert_category::stats);
	ps.settings.set_bool(settings_pack::enable_dht, false);
	ps.settings.set_bool(settings_pack::enable_lsd, false);
	ps.settings.set_bool(settings_pack::enable_outgoing_tcp, false);
	ps.settings.set_bool(settings_pack::enable_outgoing_utp, false);
	ps.settings.set_str(settings_pack::peer_fingerprint, "-SE0001-");
	ps.disk_io_constructor = test_disk().set_seed(true);
	auto seed = std::make_shared<lt::session>(ps, *ios_s);

	lt::session_params pd;
	pd.settings = settings();
	pd.settings.set_int(settings_pack::alert_mask
		, lt::alert_category::all & ~lt::alert_category::stats);
	pd.settings.set_bool(settings_pack::enable_dht, false);
	pd.settings.set_bool(settings_pack::enable_lsd, false);
	pd.settings.set_bool(settings_pack::outgoing_tcp_first, o.tcp_first);
	pd.settings.set_bool(settings_pack::enable_outgoing_tcp, o.outgoing_tcp);
	pd.settings.set_bool(settings_pack::enable_outgoing_utp, o.outgoing_utp);
	pd.settings.set_int(settings_pack::min_reconnect_time, 10);
	pd.settings.set_str(settings_pack::peer_fingerprint, "-DL0001-");
	// a finished torrent stops connecting to the seed: keep it unfinished for
	// the whole run (9 pieces of 16 kiB take minutes at this rate)
	pd.settings.set_int(settings_pack::download_rate_limit, 500);
	pd.disk_io_constructor = test_disk();
	auto dl = std::make_shared<lt::session>(pd, *ios_d);

	lt::add_torrent_params seed_params = ::create_test_torrent(0x4000, 9, {});
	seed_params.flags &= ~lt::torrent_flags::auto_managed;
	seed_params.flags &= ~lt::torrent_flags::paused;
	seed_params.save_path = ".";
	lt::add_torrent_params dl_params = seed_params;
	seed->async_add_torrent(std::move(seed_params));

	outcome r;
	bool reconnect_done = false;
	bool port_sent = false;
	bool stopped = false;
	lt::torrent_handle seed_handle;
	lt::torrent_handle dl_handle;
	auto const start = lt::clock_type::now();

	print_alerts(*seed, [&](lt::session&, lt::alert const* a)
	{
		if (auto* at = lt::alert_cast<lt::add_torrent_alert>(a))
		{
			seed_handle = at->handle;
			dl->async_add_torrent(dl_params);
		}
	}, 1);

	print_alerts(*dl, [&](lt::session&, lt::alert const* a)
	{
		if (auto* at = lt::alert_cast<lt::add_torrent_alert>(a))
		{
			dl_handle = at->handle;
			dl_handle.connect_peer(lt::tcp::endpoint(seed_ip, 6881));
		}
		if (auto* pc = lt::alert_cast<lt::peer_connect_alert>(a))
		{
			if (pc->direction == lt::peer_connect_alert::direction_t::out)
			{
				r.attempts.push_back({lt::duration_cast<lt::milliseconds>(lt::clock_type::now() - start)
					, is_utp(pc->socket_type), std::get<lt::peer_alert::ip_endpoint>(pc->ep).port()});
			}
		}
		if (auto* pd = lt::alert_cast<lt::peer_disconnected_alert>(a))
		{
			r.ends.push_back(std::string(is_utp(pd->socket_type) ? "utp " : "tcp ")
				+ lt::operation_name(pd->op));
			r.end_at.push_back(lt::duration_cast<lt::milliseconds>(lt::clock_type::now() - start));
		}
	}, 0);

	// once a second: which transport the established connection to the seed
	// uses, and the reconnect step of the scenario
	lt::aux::deadline_timer every(*ios_d);
	std::function<void(lt::error_code const&)> tick = [&](lt::error_code const& ec)
	{
		if (ec || stopped) return;
		if (dl_handle.is_valid())
		{
			std::vector<lt::peer_info> peers;
			dl_handle.get_peer_info(peers);
			for (auto const& p : peers)
			{
				if (p.flags & (lt::peer_info::connecting | lt::peer_info::handshake)) continue;
				if (p.flags & lt::peer_info::utp_socket) r.established_utp = true;
				else r.established_tcp = true;
			}
			if (o.reconnect && r.established_utp && !reconnect_done && (!peers.empty() || port_sent))
			{
				if (o.new_port != 0 && !port_sent)
				{
					// a source names the same IP with a new port while the
					// connection is up; one entry per IP, so the entry takes it.
					// The connection is dropped on the next tick, after that.
					port_sent = true;
					dl_handle.connect_peer(lt::tcp::endpoint(seed_ip, std::uint16_t(o.new_port)));
				}
				else
				{
					reconnect_done = true;
					// the seed drops the connection; the peer list entry stays
					seed_handle.pause();
					seed_handle.resume();
					r.attempts.push_back({lt::milliseconds(-1), false, 0}); // marker: reconnect starts here
					if (o.drop_udp_on_reconnect) wall->drop_udp = true;
				}
			}
		}
		every.expires_after(lt::seconds(1));
		every.async_wait(tick);
	};
	every.expires_after(lt::seconds(1));
	every.async_wait(tick);

	sim::timer done(sim, lt::seconds(o.duration), [&](boost::system::error_code const&)
	{
		stopped = true;
		every.cancel();
		zombie_d = dl->abort();
		zombie_s = seed->abort();
		dl.reset();
		seed.reset();
	});

	sim.run();
	return r;
}

std::string transports(outcome const& r)
{
	std::string ret;
	for (auto const& a : r.attempts)
		ret += a.at.count() < 0 ? std::string("| ") : std::string(a.utp ? "utp " : "tcp ");
	return ret;
}

std::vector<attempt> after_marker(outcome const& r)
{
	std::vector<attempt> ret;
	bool seen = false;
	for (auto const& a : r.attempts)
	{
		if (a.at.count() < 0) { seen = true; continue; }
		if (seen) ret.push_back(a);
	}
	return ret;
}

} // anonymous namespace

// a seed that answers over TCP is connected over TCP, once; uTP is not tried
TORRENT_TEST(tcp_first_reachable_over_tcp)
{
	auto const r = run({});
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(!r.attempts.empty());
	TEST_CHECK(!r.attempts.empty() && !r.attempts.front().utp);
	for (auto const& a : r.attempts) TEST_CHECK(!a.utp);
	TEST_CHECK(r.established_tcp);
	TEST_CHECK(!r.established_utp);
}

// the TCP attempt times out; the next attempt to the same endpoint is uTP,
// and it does not wait for the reconnect backoff a failure would cost
TORRENT_TEST(tcp_first_falls_back_to_utp)
{
	run_opts o;
	o.drop_tcp = true;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(r.attempts.size() >= 2);
	if (r.attempts.size() < 2) return;
	TEST_CHECK(!r.attempts[0].utp);
	TEST_CHECK(r.attempts[1].utp);
	TEST_CHECK(!r.ends.empty() && r.ends.front() == "tcp connect");
	// no reconnect backoff between the TCP failure and the uTP attempt
	if (!r.end_at.empty())
	{
		std::printf("tcp failed at %d ms, utp attempt at %d ms\n"
			, int(r.end_at.front().count()), int(r.attempts[1].at.count()));
		TEST_CHECK(r.attempts[1].at - r.end_at.front() < lt::seconds(2));
	}
	TEST_CHECK(r.established_utp);
}

// without the setting both transports enabled means uTP first, as before
TORRENT_TEST(tcp_first_off_starts_with_utp)
{
	run_opts o;
	o.tcp_first = false;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(!r.attempts.empty() && r.attempts.front().utp);
	TEST_CHECK(r.established_utp);
}

// outgoing uTP disabled: the setting never turns a TCP failure into uTP
TORRENT_TEST(tcp_first_without_outgoing_utp_stays_tcp)
{
	run_opts o;
	o.outgoing_utp = false;
	o.drop_tcp = true;
	o.duration = 120;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(!r.attempts.empty());
	for (auto const& a : r.attempts) TEST_CHECK(!a.utp);
	TEST_CHECK(!r.established_utp);
}

// outgoing TCP disabled: uTP right away
TORRENT_TEST(tcp_first_without_outgoing_tcp_is_utp_at_once)
{
	run_opts o;
	o.outgoing_tcp = false;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(!r.attempts.empty() && r.attempts.front().utp);
	for (auto const& a : r.attempts) TEST_CHECK(a.utp);
}

// both outgoing transports disabled: no outgoing attempt at all
TORRENT_TEST(tcp_first_without_outgoing_transports_makes_no_attempt)
{
	run_opts o;
	o.outgoing_tcp = false;
	o.outgoing_utp = false;
	auto const r = run(o);
	TEST_EQUAL(r.attempts.size(), 0);
}

// a new peer that answers neither: TCP, then uTP, then the uTP failure clears
// the peer's uTP support and later cycles are TCP only
TORRENT_TEST(tcp_first_new_peer_failing_both_goes_tcp_only)
{
	run_opts o;
	o.drop_tcp = true;
	o.drop_udp = true;
	o.duration = 300;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	TEST_CHECK(r.attempts.size() >= 3);
	if (r.attempts.size() < 3) return;
	TEST_CHECK(!r.attempts[0].utp);
	TEST_CHECK(r.attempts[1].utp);
	for (std::size_t i = 2; i < r.attempts.size(); ++i) TEST_CHECK(!r.attempts[i].utp);
	// the failed cycle costs one failure: the next attempt waits
	// min_reconnect_time * (failcount + 1) = 20 s after the uTP failure, not 0 s
	// (a retry right away) and not 30 s (both failures counted)
	TEST_CHECK(r.end_at.size() >= 2);
	if (r.end_at.size() < 2) return;
	auto const gap = r.attempts[2].at - r.end_at[1];
	std::printf("utp failed at %d ms, next attempt at %d ms\n"
		, int(r.end_at[1].count()), int(r.attempts[2].at.count()));
	TEST_CHECK(gap >= lt::seconds(10));
	TEST_CHECK(gap < lt::seconds(25));
}

// a peer whose uTP connection worked and whose uTP now fails as well: the
// failure ends the cycle, the next one starts with TCP again
TORRENT_TEST(tcp_first_confirmed_peer_whose_utp_fails_starts_again_with_tcp)
{
	run_opts o;
	o.drop_tcp = true;
	o.reconnect = true;
	o.drop_udp_on_reconnect = true;
	o.duration = 200;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	auto const after = after_marker(r);
	TEST_CHECK(after.size() >= 2);
	if (after.size() < 2) return;
	TEST_CHECK(after[0].utp);
	TEST_CHECK(!after[1].utp);
}

// a peer whose TCP failed and whose uTP connection worked: when that
// connection goes away, the next attempt is uTP again (confirmed support)
TORRENT_TEST(tcp_first_reconnects_over_utp_after_utp_worked)
{
	run_opts o;
	o.drop_tcp = true;
	o.reconnect = true;
	o.duration = 120;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	auto const after = after_marker(r);
	TEST_CHECK(!after.empty());
	if (after.empty()) return;
	TEST_CHECK(after.front().utp);
}

// the same peer announced on a new port: a TCP failure on the old port says
// nothing about the new one, so the first attempt there is TCP
TORRENT_TEST(tcp_first_new_port_starts_with_tcp)
{
	run_opts o;
	o.drop_tcp = true;
	o.reconnect = true;
	o.new_port = 6882;
	o.duration = 120;
	auto const r = run(o);
	std::printf("attempts: %s\n", transports(r).c_str());
	auto const after = after_marker(r);
	TEST_CHECK(!after.empty());
	if (after.empty()) return;
	TEST_EQUAL(after.front().port, 6882);
	TEST_CHECK(!after.front().utp);
}

namespace {

struct punch_result
{
	std::vector<attempt> attempts; // outgoing attempts to the punched endpoint
};

// a fake peer that supports the hole punch extension connects to the session
// at 50.0.0.1 and asks it to connect to 50.0.0.2:6881, an endpoint nothing
// answers at; the session has never tried that endpoint over TCP
punch_result run_punch()
{
	sim::default_config cfg;
	sim::simulation sim{cfg};
	auto ios = std::make_unique<sim::asio::io_context>(sim, lt::make_address_v4("50.0.0.1"));
	lt::session_proxy zombie;

	lt::session_params sp;
	sp.settings = settings();
	sp.settings.set_int(settings_pack::alert_mask
		, lt::alert_category::all & ~lt::alert_category::stats);
	sp.settings.set_bool(settings_pack::enable_dht, false);
	sp.settings.set_bool(settings_pack::enable_lsd, false);
	sp.settings.set_bool(settings_pack::outgoing_tcp_first, true);
	sp.settings.set_bool(settings_pack::enable_outgoing_tcp, true);
	sp.settings.set_bool(settings_pack::enable_outgoing_utp, true);
	sp.settings.set_int(settings_pack::min_reconnect_time, 10);
	sp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(sp, *ios);

	lt::add_torrent_params params = ::create_test_torrent(0x4000, 9, {});
	params.flags &= ~lt::torrent_flags::auto_managed;
	params.flags &= ~lt::torrent_flags::paused;
	params.save_path = ".";
	lt::sha1_hash const ih = params.ti->info_hash();
	ses->async_add_torrent(std::move(params));

	auto introducer = std::make_unique<fake_peer>(sim, "60.0.0.1");
	introducer->set_reserved_bits({0, 0, 0, 0, 0, 0x10, 0, 0});
	auto const target = lt::tcp::endpoint(lt::make_address_v4("50.0.0.2"), 6881);

	punch_result r;
	auto const start = lt::clock_type::now();
	print_alerts(*ses, [&](lt::session&, lt::alert const* a)
	{
		if (lt::alert_cast<lt::add_torrent_alert>(a))
		{
			introducer->connect_to(lt::tcp::endpoint(lt::make_address_v4("50.0.0.1"), 6881), ih);
			introducer->send_extended(0, std::string("d1:md12:ut_holepunchi4eee"));
			// connect (1), IPv4 (0), 50.0.0.2, port 6881
			char const msg[] = {1, 0, 50, 0, 0, 2, char(6881 >> 8), char(6881 & 0xff)};
			introducer->send_extended(4, lt::span<char const>(msg, sizeof(msg)));
		}
		if (auto* pc = lt::alert_cast<lt::peer_connect_alert>(a))
		{
			auto const e = std::get<lt::peer_alert::ip_endpoint>(pc->ep);
			if (pc->direction == lt::peer_connect_alert::direction_t::out && e == target)
				r.attempts.push_back({lt::duration_cast<lt::milliseconds>(lt::clock_type::now() - start)
					, is_utp(pc->socket_type), e.port()});
		}
	});

	sim::timer t(sim, lt::seconds(90), [&](boost::system::error_code const&)
	{
		zombie = ses->abort();
		ses.reset();
	});
	sim.run();
	return r;
}

// outgoing attempts per second when six endpoints that nothing answers at are
// known: each fails over TCP and then gets its uTP attempt
std::vector<int> run_starts_per_second(int const connection_speed)
{
	sim::default_config cfg;
	sim::simulation sim{cfg};
	auto ios = std::make_unique<sim::asio::io_context>(sim, lt::make_address_v4("50.0.0.1"));
	lt::session_proxy zombie;

	lt::session_params sp;
	sp.settings = settings();
	sp.settings.set_int(settings_pack::alert_mask
		, lt::alert_category::all & ~lt::alert_category::stats);
	sp.settings.set_bool(settings_pack::enable_dht, false);
	sp.settings.set_bool(settings_pack::enable_lsd, false);
	sp.settings.set_bool(settings_pack::outgoing_tcp_first, true);
	sp.settings.set_int(settings_pack::connection_speed, connection_speed);
	sp.settings.set_int(settings_pack::torrent_connect_boost, 0);
	sp.disk_io_constructor = test_disk();
	auto ses = std::make_shared<lt::session>(sp, *ios);

	lt::add_torrent_params params = ::create_test_torrent(0x4000, 9, {});
	params.flags &= ~lt::torrent_flags::auto_managed;
	params.flags &= ~lt::torrent_flags::paused;
	params.save_path = ".";
	ses->async_add_torrent(std::move(params));

	std::vector<int> per_second(120, 0);
	int utp = 0;
	auto const start = lt::clock_type::now();
	print_alerts(*ses, [&](lt::session&, lt::alert const* a)
	{
		if (auto* at = lt::alert_cast<lt::add_torrent_alert>(a))
		{
			for (int i = 1; i <= 6; ++i)
			{
				char ip[20];
				std::snprintf(ip, sizeof(ip), "60.0.0.%d", i);
				at->handle.connect_peer(lt::tcp::endpoint(lt::make_address_v4(ip), 6881));
			}
		}
		if (auto* pc = lt::alert_cast<lt::peer_connect_alert>(a))
		{
			if (pc->direction != lt::peer_connect_alert::direction_t::out) return;
			auto const s = lt::duration_cast<lt::seconds>(lt::clock_type::now() - start).count();
			if (s >= 0 && s < int(per_second.size())) ++per_second[std::size_t(s)];
			if (is_utp(pc->socket_type)) ++utp;
		}
	});

	sim::timer t(sim, lt::seconds(120), [&](boost::system::error_code const&)
	{
		zombie = ses->abort();
		ses.reset();
	});
	sim.run();
	std::printf("utp attempts: %d\n", utp);
	TEST_CHECK(utp > 0);
	return per_second;
}

int max_starts_in_a_second(std::vector<int> const& v)
{
	int ret = 0;
	for (int n : v) ret = std::max(ret, n);
	return ret;
}

} // anonymous namespace

// a hole punch asks for uTP even though the endpoint never failed over TCP;
// when that attempt fails, the next ordinary attempt follows TCP first
TORRENT_TEST(tcp_first_hole_punch_is_utp_and_does_not_stick)
{
	auto const r = run_punch();
	std::string t;
	for (auto const& a : r.attempts) t += a.utp ? "utp " : "tcp ";
	std::printf("attempts to the punched endpoint: %s\n", t.c_str());
	TEST_CHECK(r.attempts.size() >= 2);
	if (r.attempts.size() < 2) return;
	TEST_CHECK(r.attempts[0].utp);
	TEST_CHECK(!r.attempts[1].utp);
}

// the uTP attempts after TCP failures go through the connection scheduler:
// with connection_speed 1 and no boost, never more than one start per second
// (two when a tick lands on a second boundary)
TORRENT_TEST(tcp_first_fallback_attempts_respect_connection_speed)
{
	auto const v = run_starts_per_second(1);
	TEST_CHECK(max_starts_in_a_second(v) <= 2);
}

// the check above can fire: a high connection_speed starts several in one second
TORRENT_TEST(tcp_first_fallback_attempts_control_high_connection_speed)
{
	auto const v = run_starts_per_second(50);
	TEST_CHECK(max_starts_in_a_second(v) > 2);
}
