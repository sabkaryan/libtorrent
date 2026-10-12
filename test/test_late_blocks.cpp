/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include <algorithm>
#include <set>
#include <vector>

#include "test.hpp"
#include "libtorrent/aux_/late_blocks.hpp"
#include "libtorrent/aux_/peer_connection.hpp"

using namespace lt;
using namespace lt::aux;

namespace {

int const block = default_block_size;
time_point const now = clock_type::now();
piece_index_t const piece{3};

piece_block blk(int const i) { return piece_block(piece, i); }

// what request_late_blocks() and held_blocks() use of a peer_connection
struct peer
{
	static inline constexpr request_flags_t time_critical
		= peer_connection::time_critical;
	static inline constexpr request_flags_t busy = peer_connection::busy;

	explicit peer(int const r) : rate(r) {}

	int rate;
	bool snubbed = false;
	bool can_take = true;
	bool have = true;
	// time-critical requests it takes before its queue is full, -1: no limit
	int room = -1;
	time_point unchoked = now - seconds(60);
	std::vector<pending_block> dq;
	std::vector<pending_block> rq;
	int queued_tc = 0;

	// requests sent, one block each, nothing of them received yet
	void sent(std::vector<piece_block> const& blocks)
	{ for (auto const& b : blocks) dq.emplace_back(b); }
	// filler blocks of another piece ahead in the download queue
	void filler(int const n)
	{ for (int i = 0; i < n; ++i) dq.emplace_back(piece_block(piece_index_t{90}, i)); }

	int outstanding_bytes() const { return int(dq.size()) * block; }
	int download_rate_estimate() const { return rate; }
	bool is_snubbed() const { return snubbed; }
	time_point time_of_last_unchoked() const { return unchoked; }
	bool has_piece(piece_index_t) const { return have; }
	bool can_request_time_critical() const { return can_take && !snubbed && room != 0; }
	int num_queued_time_critical() const { return queued_tc; }
	std::vector<pending_block> const& download_queue() const { return dq; }
	std::vector<pending_block> const& request_queue() const { return rq; }

	bool add_request(piece_block const& b, request_flags_t const flags)
	{
		pending_block pb(b);
		pb.busy = bool(flags & busy);
		if (room > 0) --room;
		if (flags & time_critical) rq.insert(rq.begin() + std::min(queued_tc++, int(rq.size())), pb);
		else rq.push_back(pb);
		return true;
	}

	void cancel_request(piece_block const& b)
	{
		auto const it = std::find_if(rq.begin(), rq.end(), has_block(b));
		if (it == rq.end()) return;
		if (it - rq.begin() < queued_tc) --queued_tc;
		rq.erase(it);
	}

	bool has(piece_block const& b) const
	{
		return std::any_of(dq.begin(), dq.end(), has_block(b))
			|| std::any_of(rq.begin(), rq.end(), has_block(b));
	}
	bool busy_request(piece_block const& b) const
	{
		auto const it = std::find_if(rq.begin(), rq.end(), has_block(b));
		return it != rq.end() && it->busy;
	}
};

// one pass of the time-critical loop over the piece; deadline_ms from now
late_requests pass(std::vector<peer*> const& peers, int const deadline_ms = 0)
{
	std::vector<piece_index_t> const time_critical{piece};
	auto const held = held_blocks(peers, time_critical, block);
	std::set<peer*> with_requests;
	return request_late_blocks(now + milliseconds(deadline_ms), held_blocks_of(held, piece)
		, peers, with_requests, now, block);
}

} // anonymous namespace

// a slow holder (16 KiB/s): its block behind 4 others arrives in 5 s
TORRENT_TEST(late_sent_block_is_copied_to_a_faster_peer)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies, 1);
	TEST_EQUAL(r.moves, 0);
	TEST_CHECK(slow.has(blk(0)));
	TEST_CHECK(fast.busy_request(blk(0)));
}

// the same block, but the deadline is 10 s away
TORRENT_TEST(block_in_time_for_the_deadline_is_left)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	late_requests const r = pass({&slow, &fast}, 10000);
	TEST_EQUAL(r.copies + r.moves, 0);
	TEST_CHECK(!fast.has(blk(0)));
}

// first in the slow holder's queue it arrives in 1 s: not late
TORRENT_TEST(position_in_the_holders_queue_counts)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.sent({blk(0)});
	slow.filler(4);
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// a request not sent yet moves: the holder no longer has it
TORRENT_TEST(late_queued_block_moves_to_a_faster_peer)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	TEST_CHECK(slow.add_request(blk(0), peer::time_critical));
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.moves, 1);
	TEST_EQUAL(r.copies, 0);
	TEST_CHECK(!slow.has(blk(0)));
	TEST_EQUAL(slow.queued_tc, 0);
	TEST_CHECK(fast.has(blk(0)));
	TEST_CHECK(!fast.busy_request(blk(0)));
}

// the other peer is no faster
TORRENT_TEST(no_faster_peer_no_copy)
{
	peer slow(16 * 1024), other(16 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	other.filler(4);
	late_requests const r = pass({&slow, &other});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// already asked of two peers: no third request
TORRENT_TEST(block_with_two_requests_gets_no_third)
{
	peer slow(16 * 1024), slow2(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	slow2.filler(4);
	slow2.sent({blk(0)});
	late_requests const r = pass({&slow, &slow2, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
	TEST_CHECK(!fast.has(blk(0)));
}

// a request of the block timed out (a snubbed peer's), the one live request is
// the extra copy already
TORRENT_TEST(block_with_a_timed_out_request_is_left)
{
	peer gone(16 * 1024), slow(16 * 1024), fast(400 * 1024);
	gone.sent({blk(0)});
	gone.dq.back().timed_out = true;
	slow.filler(4);
	slow.sent({blk(0)});
	late_requests const r = pass({&gone, &slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
	TEST_CHECK(!fast.has(blk(0)));
}

// a snubbed holder is not expected to deliver, however fast it was
TORRENT_TEST(snubbed_holder_is_late)
{
	peer stalled(1000 * 1024), fast(400 * 1024);
	stalled.sent({blk(0)});
	stalled.snubbed = true;
	late_requests const r = pass({&stalled, &fast});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(fast.has(blk(0)));
}

// the holder unchoked us 5 s ago: its rate is not settled
TORRENT_TEST(holder_rate_not_settled)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	slow.unchoked = now - seconds(5);
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// the faster peer unchoked us 5 s ago: its rate is not settled
TORRENT_TEST(candidate_rate_not_settled)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	fast.unchoked = now - seconds(5);
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// the faster peer's own requests come first: 2 MiB queued at 400 KiB/s is 5 s.
// A slower peer with an empty queue delivers in 0.2 s
TORRENT_TEST(candidate_delivers_after_its_queue)
{
	peer slow(4 * 1024), fast(400 * 1024), idle(100 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	fast.filler(128);
	late_requests const r = pass({&slow, &fast, &idle});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(idle.has(blk(0)));
	TEST_CHECK(!fast.has(blk(0)));
}

// queued time-critical requests come before a new one as well
TORRENT_TEST(candidate_delivers_after_its_time_critical_requests)
{
	peer slow(4 * 1024), fast(400 * 1024), idle(100 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	for (int i = 0; i < 128; ++i)
		TEST_CHECK(fast.add_request(piece_block(piece_index_t{90}, i), peer::time_critical));
	late_requests const r = pass({&slow, &fast, &idle});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(idle.has(blk(0)));
}

// a peer that can't take time-critical requests is not asked
TORRENT_TEST(candidate_that_takes_no_time_critical_requests)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	fast.can_take = false;
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// a peer without the piece is not asked
TORRENT_TEST(candidate_without_the_piece)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	fast.have = false;
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies + r.moves, 0);
}

// two late blocks, room for one more request at the faster peer: the later
// of the two gets it
TORRENT_TEST(latest_block_first)
{
	peer slow(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0), blk(1)});
	fast.room = 1;
	late_requests const r = pass({&slow, &fast});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(fast.has(blk(1)));
	TEST_CHECK(!fast.has(blk(0)));
}

// the faster peer still has a request of the block it no longer wants: it is
// not asked again, the next one is
TORRENT_TEST(candidate_with_a_request_not_wanted_any_more)
{
	peer slow(16 * 1024), fast(400 * 1024), idle(100 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	fast.sent({blk(0)});
	fast.dq.back().not_wanted = true;
	late_requests const r = pass({&slow, &fast, &idle});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(idle.has(blk(0)));
	TEST_EQUAL(int(fast.rq.size()), 0);
}

// a request not wanted any more is not a holder: the block has one live
// request, and gets its copy
TORRENT_TEST(request_not_wanted_is_no_holder)
{
	peer slow(16 * 1024), other(16 * 1024), fast(400 * 1024);
	slow.filler(4);
	slow.sent({blk(0)});
	other.sent({blk(0)});
	other.dq.back().not_wanted = true;
	other.can_take = false;
	late_requests const r = pass({&slow, &other, &fast});
	TEST_EQUAL(r.copies, 1);
	TEST_CHECK(fast.has(blk(0)));
}
