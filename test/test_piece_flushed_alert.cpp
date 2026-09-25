/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "setup_transfer.hpp" // for create_torrent
#include "settings.hpp" // for settings()
#include "test_utils.hpp"

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/torrent_status.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/create_torrent.hpp"
#include "libtorrent/posix_disk_io.hpp"
#include "libtorrent/aux_/path.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "write_gate.hpp"

using namespace lt;
using namespace std::chrono_literals;

namespace {

int const piece_size = 64 * 1024;
int const num_pieces = 6;
char const* const name = "piece_flushed";

settings_pack flushed_settings()
{
	settings_pack p = settings();
	p.set_int(settings_pack::alert_mask, alert_category::status
		| alert_category::storage | alert_category::piece_progress
		| alert_category::error);
	p.set_str(settings_pack::listen_interfaces, "127.0.0.1:0");
	p.set_int(settings_pack::aio_threads, 1);
	p.set_int(settings_pack::hashing_threads, 1);
	return p;
}

// create_torrent() in setup_transfer gives every piece this content
std::vector<char> piece_data()
{
	std::vector<char> ret(static_cast<std::size_t>(piece_size));
	for (std::size_t i = 0; i < ret.size(); ++i) ret[i] = char((i % 26) + 'A');
	return ret;
}

add_torrent_params make_params(std::string const& save_path)
{
	add_torrent_params atp = ::create_torrent(nullptr, name, piece_size
		, num_pieces, false, create_torrent::v1_only);
	atp.save_path = save_path;
	atp.flags &= ~torrent_flags::auto_managed;
	atp.flags &= ~torrent_flags::paused;
	return atp;
}

void write_whole_file(std::string const& file)
{
	std::ofstream f(file, std::ios::binary);
	std::vector<char> const data = piece_data();
	for (int i = 0; i < num_pieces; ++i)
		f.write(data.data(), std::streamsize(data.size()));
}

// what the file holds for the piece right now, read without libtorrent. Bytes
// past the end of the file (or of a missing file) read as zeros
std::vector<char> piece_in_file(std::string const& file, piece_index_t const piece)
{
	std::vector<char> ret(static_cast<std::size_t>(piece_size), 0);
	std::ifstream f(file, std::ios::binary);
	if (!f) return ret;
	f.seekg(std::streamoff(static_cast<int>(piece)) * piece_size);
	f.read(ret.data(), std::streamsize(ret.size()));
	return ret;
}

// the piece_finished_alerts and piece_flushed_alerts of one torrent
struct piece_alerts
{
	std::vector<int> finished = std::vector<int>(num_pieces, 0);
	std::vector<int> flushed = std::vector<int>(num_pieces, 0);
	// piece_flushed_alert came before the piece's piece_finished_alert
	std::vector<bool> flushed_first = std::vector<bool>(num_pieces, false);
	// when piece_flushed_alert was handled, the file held the piece's bytes
	std::vector<bool> in_file = std::vector<bool>(num_pieces, false);

	int count(std::vector<int> const& v) const
	{
		int n = 0;
		for (int c : v) n += (c > 0);
		return n;
	}
	bool all_finished() const { return count(finished) == num_pieces; }
	bool all_flushed() const { return count(flushed) == num_pieces; }
};

// handles the session's alerts until done() or the timeout
void collect(lt::session& ses, piece_alerts& rec, std::string const& file
	, std::function<bool()> const& done, std::chrono::milliseconds const timeout)
{
	std::vector<char> const expected = piece_data();
	auto const end = std::chrono::steady_clock::now() + timeout;
	while (!done() && std::chrono::steady_clock::now() < end)
	{
		ses.wait_for_alert(100ms);
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
		{
			if (auto const* pf = alert_cast<piece_finished_alert>(a))
			{
				++rec.finished[std::size_t(static_cast<int>(pf->piece_index))];
			}
			else if (auto const* fl = alert_cast<piece_flushed_alert>(a))
			{
				auto const p = std::size_t(static_cast<int>(fl->piece_index));
				if (rec.finished[p] == 0) rec.flushed_first[p] = true;
				++rec.flushed[p];
				rec.in_file[p] = piece_in_file(file, fl->piece_index) == expected;
			}
		}
	}
}

// every piece: one piece_flushed_alert, not before its piece_finished_alert,
// with the piece's bytes in the file when it arrived
void check_every_piece(piece_alerts const& rec)
{
	for (int p = 0; p < num_pieces; ++p)
	{
		TEST_EQUAL(rec.finished[std::size_t(p)], 1);
		TEST_EQUAL(rec.flushed[std::size_t(p)], 1);
		TEST_CHECK(!rec.flushed_first[std::size_t(p)]);
		TEST_CHECK(rec.in_file[std::size_t(p)]);
	}
}

// the snapshot of the same thing: flushed_pieces holds exactly the pieces
// written to the files. Also checks that pieces (passed their hash check)
// covers every flushed piece
void check_flushed_snapshot(torrent_handle const& th, int const expect_flushed)
{
	torrent_status const st = th.status(torrent_handle::query_pieces
		| torrent_handle::query_flushed_pieces);
	TEST_EQUAL(st.flushed_pieces.size(), num_pieces);
	TEST_EQUAL(st.flushed_pieces.count(), expect_flushed);
	if (st.flushed_pieces.size() == num_pieces && st.pieces.size() == num_pieces)
	{
		for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
			if (st.flushed_pieces.get_bit(p)) TEST_CHECK(st.pieces.get_bit(p));
	}
	// not filled in unless asked for
	TEST_CHECK(th.status(torrent_handle::query_pieces).flushed_pieces.empty());
	// and filled in without query_pieces as well: the two are independent
	torrent_status const alone = th.status(torrent_handle::query_flushed_pieces);
	TEST_EQUAL(alone.flushed_pieces.size(), num_pieces);
	TEST_EQUAL(alone.flushed_pieces.count(), expect_flushed);
	TEST_CHECK(alone.pieces.empty());
}

} // anonymous namespace

// a backend that writes through (posix_disk_io): every block is in the file
// when its write completes, before the piece is hashed. The two alerts come
// together
TORRENT_TEST(piece_flushed_write_through)
{
	std::string const save_path = complete("piece_flushed_through");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	session_params sp(flushed_settings());
	sp.disk_io_constructor = posix_disk_io_constructor;
	lt::session ses(std::move(sp));
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);
	std::vector<char> const data = piece_data();
	for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
		th.add_piece(p, data.data());

	collect(ses, rec, file, [&] { return rec.all_finished() && rec.all_flushed(); }, 10s);
	check_every_piece(rec);
	check_flushed_snapshot(th, num_pieces);

	// and nothing more
	collect(ses, rec, file, [] { return false; }, 500ms);
	check_every_piece(rec);
	remove_all(save_path, ec);
}

#if defined TORRENT_LINUX
// the default backend, pread_disk_io, hashes the blocks from its cache and
// writes them back afterwards. While the writes are held back, every piece
// passes and none may be reported as written; once they are let through,
// every piece is, and its bytes are in the file by then. The last piece also
// takes the torrent to seeding, which drops the piece picker
TORRENT_TEST(piece_flushed_write_back_cache)
{
	std::string const save_path = complete("piece_flushed_cache");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	set_hold_writes(true);
	std::vector<char> const data = piece_data();
	for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
		th.add_piece(p, data.data());

	collect(ses, rec, file, [&] { return rec.all_finished(); }, 10s);
	TEST_CHECK(rec.all_finished());
	TEST_EQUAL(rec.count(rec.flushed), 0);
	// the writes really are held back
	TEST_CHECK(piece_in_file(file, piece_index_t{0}) != data);
	check_flushed_snapshot(th, 0);
	TEST_EQUAL(th.status(torrent_handle::query_pieces).pieces.count(), num_pieces);

	set_hold_writes(false);
	collect(ses, rec, file, [&] { return rec.all_flushed(); }, 10s);
	check_every_piece(rec);

	collect(ses, rec, file, [] { return false; }, 500ms);
	check_every_piece(rec);
	TEST_CHECK(th.status().is_seeding);
	// a seed that has dropped its piece picker has every piece in the files
	check_flushed_snapshot(th, num_pieces);
	remove_all(save_path, ec);
}

// a disk slower than the pieces come: one flush writes the cached pieces one
// after the other, and each piece is reported as it is written, not when the
// whole flush is done
TORRENT_TEST(piece_flushed_as_each_piece_is_written)
{
	std::string const save_path = complete("piece_flushed_each");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	// every piece passes its hash check while nothing can be written
	set_hold_writes(true);
	std::vector<char> const data = piece_data();
	for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
		th.add_piece(p, data.data());
	collect(ses, rec, file, [&] { return rec.all_finished(); }, 10s);
	TEST_CHECK(rec.all_finished());
	TEST_EQUAL(rec.count(rec.flushed), 0);

	// then a slow disk: each write takes 20 ms or more
	slow_writes = true;
	set_hold_writes(false);
	std::vector<time_point> flushed_at;
	auto const end = clock_type::now() + 10s;
	while (int(flushed_at.size()) < num_pieces && clock_type::now() < end)
	{
		ses.wait_for_alert(100ms);
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
			if (alert_cast<piece_flushed_alert>(a)) flushed_at.push_back(a->timestamp());
	}
	slow_writes = false;
	TEST_EQUAL(int(flushed_at.size()), num_pieces);
	if (!flushed_at.empty())
	{
		auto const spread = total_milliseconds(
			*std::max_element(flushed_at.begin(), flushed_at.end())
			- *std::min_element(flushed_at.begin(), flushed_at.end()));
		std::printf("piece_flushed_alert spread: %d ms over %d pieces\n"
			, int(spread), int(flushed_at.size()));
		// the pieces are written one at a time, 20 ms or more each: the first
		// is reported well before the last
		TEST_CHECK(spread >= 60);
	}
	remove_all(save_path, ec);
}

// pieces downloaded from a peer take the write-completion path of the peer
// connection rather than the one of add_piece()
TORRENT_TEST(piece_flushed_from_peer)
{
	std::string const seed_path = complete("piece_flushed_seed");
	std::string const save_path = complete("piece_flushed_peer");
	error_code ec;
	remove_all(seed_path, ec);
	remove_all(save_path, ec);
	create_directory(seed_path, ec);
	create_directory(save_path, ec);
	write_whole_file(combine_path(seed_path, name));

	lt::session seed(flushed_settings());
	torrent_handle const seed_th = seed.add_torrent(make_params(seed_path));
	for (int i = 0; i < 100 && !seed_th.status().is_seeding; ++i)
		std::this_thread::sleep_for(50ms);
	TEST_CHECK(seed_th.status().is_seeding);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	set_hold_writes(true);
	th.connect_peer(tcp::endpoint(make_address_v4("127.0.0.1"), seed.listen_port()));

	collect(ses, rec, file, [&] { return rec.all_finished(); }, 20s);
	TEST_CHECK(rec.all_finished());
	TEST_EQUAL(rec.count(rec.flushed), 0);
	check_flushed_snapshot(th, 0);

	set_hold_writes(false);
	collect(ses, rec, file, [&] { return rec.all_flushed(); }, 10s);
	check_every_piece(rec);

	collect(ses, rec, file, [] { return false; }, 500ms);
	check_every_piece(rec);
	remove_all(seed_path, ec);
	remove_all(save_path, ec);
}

namespace {

// pieces downloaded from a peer pass their hash check while their writes are
// held; then `stop` pauses or removes the torrent, and the writes are let go.
// Stopping a torrent flushes its cached blocks, so every piece is written and
// reported as it is
void flushed_after_stop(char const* const name_suffix
	, std::function<void(lt::session&, torrent_handle const&)> const& stop)
{
	std::string const seed_path = complete((std::string("piece_flushed_seed_") + name_suffix).c_str());
	std::string const save_path = complete((std::string("piece_flushed_peer_") + name_suffix).c_str());
	error_code ec;
	remove_all(seed_path, ec);
	remove_all(save_path, ec);
	create_directory(seed_path, ec);
	create_directory(save_path, ec);
	write_whole_file(combine_path(seed_path, name));

	lt::session seed(flushed_settings());
	torrent_handle const seed_th = seed.add_torrent(make_params(seed_path));
	for (int i = 0; i < 100 && !seed_th.status().is_seeding; ++i)
		std::this_thread::sleep_for(50ms);
	TEST_CHECK(seed_th.status().is_seeding);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	set_hold_writes(true);
	th.connect_peer(tcp::endpoint(make_address_v4("127.0.0.1"), seed.listen_port()));

	collect(ses, rec, file, [&] { return rec.all_finished(); }, 20s);
	TEST_CHECK(rec.all_finished());
	TEST_EQUAL(rec.count(rec.flushed), 0);

	stop(ses, th);
	set_hold_writes(false);
	collect(ses, rec, file, [&] { return rec.all_flushed(); }, 10s);
	check_every_piece(rec);

	collect(ses, rec, file, [] { return false; }, 500ms);
	check_every_piece(rec);
	remove_all(seed_path, ec);
	remove_all(save_path, ec);
}

} // anonymous namespace

TORRENT_TEST(piece_flushed_from_peer_then_paused)
{
	flushed_after_stop("paused", [](lt::session&, torrent_handle const& th) { th.pause(); });
}

TORRENT_TEST(piece_flushed_from_peer_then_removed)
{
	flushed_after_stop("removed", [](lt::session& ses, torrent_handle const& th) { ses.remove_torrent(th); });
}

namespace {

// asks for resume data and waits for the answer, recording piece alerts on
// the way. Returns false if saving was skipped (save_resume_data_failed_alert)
bool save_resume(lt::session& ses, torrent_handle const& th, piece_alerts& rec
	, resume_data_flags_t const flags, add_torrent_params& out)
{
	th.save_resume_data(flags);
	auto const end = clock_type::now() + 10s;
	while (clock_type::now() < end)
	{
		ses.wait_for_alert(100ms);
		std::vector<alert*> alerts;
		ses.pop_alerts(&alerts);
		for (alert* a : alerts)
		{
			if (auto const* pf = alert_cast<piece_finished_alert>(a))
				++rec.finished[std::size_t(static_cast<int>(pf->piece_index))];
			else if (auto const* fl = alert_cast<piece_flushed_alert>(a))
				++rec.flushed[std::size_t(static_cast<int>(fl->piece_index))];
			else if (auto const* sr = alert_cast<save_resume_data_alert>(a))
			{
				out = sr->params;
				return true;
			}
			else if (alert_cast<save_resume_data_failed_alert>(a))
				return false;
		}
	}
	TEST_ERROR("no answer to save_resume_data()");
	return false;
}

} // anonymous namespace

// resume data lists the pieces whose bytes are in the files. A torrent whose
// every piece passed its hash check already counts as a seed, but with a
// write-back cache the bytes may not be written yet
TORRENT_TEST(resume_data_has_only_written_pieces)
{
	std::string const save_path = complete("piece_flushed_resume_seed");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	set_hold_writes(true);
	std::vector<char> const data = piece_data();
	for (piece_index_t p{0}; p < piece_index_t{num_pieces}; ++p)
		th.add_piece(p, data.data());
	collect(ses, rec, file, [&] { return rec.all_finished(); }, 10s);
	TEST_CHECK(rec.all_finished());
	TEST_EQUAL(rec.count(rec.flushed), 0);

	add_torrent_params held;
	TEST_CHECK(save_resume(ses, th, rec, {}, held));
	TEST_EQUAL(held.have_pieces.count(), 0);

	set_hold_writes(false);
	collect(ses, rec, file, [&] { return rec.all_flushed(); }, 10s);
	TEST_CHECK(rec.all_flushed());

	add_torrent_params written;
	TEST_CHECK(save_resume(ses, th, rec, {}, written));
	TEST_EQUAL(written.have_pieces.count(), num_pieces);
	remove_all(save_path, ec);
}

// resume data saved after a piece passed its hash check but before its bytes
// were written does not have it. Writing it must ask for resume data to be
// saved again, or the piece is only in the next save that happens for another
// reason. Seen best with the last piece, after which no other piece passes
TORRENT_TEST(resume_data_needs_saving_when_the_last_piece_is_written)
{
	std::string const save_path = complete("piece_flushed_resume_last");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);

	lt::session ses(flushed_settings());
	torrent_handle const th = ses.add_torrent(make_params(save_path));
	std::string const file = combine_path(save_path, name);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return th.status().state == torrent_status::downloading; }, 10s);

	piece_index_t const last{num_pieces - 1};
	std::vector<char> const data = piece_data();
	for (piece_index_t p{0}; p < last; ++p)
		th.add_piece(p, data.data());
	collect(ses, rec, file, [&] { return rec.count(rec.flushed) == num_pieces - 1; }, 10s);
	TEST_EQUAL(rec.count(rec.flushed), num_pieces - 1);

	set_hold_writes(true);
	th.add_piece(last, data.data());
	collect(ses, rec, file, [&] { return rec.all_finished(); }, 10s);
	TEST_CHECK(rec.all_finished());

	// saved while the last piece's bytes are held back
	add_torrent_params before;
	TEST_CHECK(save_resume(ses, th, rec, {}, before));
	TEST_EQUAL(before.have_pieces.count(), num_pieces - 1);

	set_hold_writes(false);
	collect(ses, rec, file, [&] { return rec.all_flushed(); }, 10s);
	TEST_CHECK(rec.all_flushed());

	TEST_CHECK(th.need_save_resume_data());
	add_torrent_params after;
	TEST_CHECK(save_resume(ses, th, rec, torrent_handle::only_if_modified, after));
	TEST_EQUAL(after.have_pieces.count(), num_pieces);
	remove_all(save_path, ec);
}
#endif

// pieces taken from resume data are in the file already: each gets its
// piece_finished_alert and its piece_flushed_alert
TORRENT_TEST(piece_flushed_resume_data)
{
	std::string const save_path = complete("piece_flushed_resume");
	error_code ec;
	remove_all(save_path, ec);
	create_directory(save_path, ec);
	std::string const file = combine_path(save_path, name);
	write_whole_file(file);

	lt::session ses(flushed_settings());
	add_torrent_params atp = make_params(save_path);
	atp.have_pieces.resize(num_pieces, true);
	torrent_handle const th = ses.add_torrent(atp);

	piece_alerts rec;
	collect(ses, rec, file, [&] { return rec.all_finished() && rec.all_flushed(); }, 10s);
	check_every_piece(rec);

	collect(ses, rec, file, [] { return false; }, 500ms);
	check_every_piece(rec);
	TEST_CHECK(th.status().is_seeding);
	check_flushed_snapshot(th, num_pieces);
	remove_all(save_path, ec);
}
