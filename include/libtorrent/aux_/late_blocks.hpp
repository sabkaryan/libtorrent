/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_LATE_BLOCKS_HPP_INCLUDED
#define TORRENT_LATE_BLOCKS_HPP_INCLUDED

#include <algorithm>
#include <cstdint>
#include <limits>
#include <set>
#include <tuple>
#include <type_traits>
#include <vector>

#include "libtorrent/aux_/has_block.hpp"
#include "libtorrent/aux_/time.hpp"
#include "libtorrent/piece_block.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/time.hpp"
#include "libtorrent/units.hpp"

// Late blocks of time-critical pieces: a block that one peer is asked for, and
// that this peer is not expected to deliver by the piece's deadline, is asked
// of a peer that would deliver it sooner. Used by
// torrent::request_time_critical_pieces() with peer_connection, and by the
// unit test with a stand-in that has the same members.

namespace libtorrent::aux {

	// the time-critical requests are revisited once a second. A block is late
	// only if it is due more than that later than it needs to be, and another
	// peer is worth asking only if it would deliver it that much sooner
	constexpr std::int64_t late_block_margin_ms = 1000;

	// the download rate is a moving average (1/5 per second) that lags a peer
	// ramping up: rates are only compared once 10 s have passed since the peer
	// unchoked us. Whether it has been sending all that time is not checked
	constexpr seconds rate_settle_time{10};

	// a block of a time-critical piece requested from exactly one peer
	template <typename Peer>
	struct held_block
	{
		piece_block block;
		Peer* holder;
		// how long until the holder delivers it
		std::int64_t arrival_ms;
		// the request is still in the holder's request queue, not sent
		bool queued;
	};

	// what request_late_blocks() did
	struct late_requests
	{
		// sent requests asked of another peer as well (busy requests)
		int copies = 0;
		// requests not sent yet, moved to another peer
		int moves = 0;
	};

	template <typename Peer>
	bool rate_settled(Peer const& p, time_point const now)
	{ return now - p.time_of_last_unchoked() >= rate_settle_time; }

	// how long until p delivers ``bytes`` of what it is asked for. A snubbed
	// peer is not expected to deliver anything
	template <typename Peer>
	std::int64_t arrival_ms(Peer const& p, std::int64_t const bytes)
	{
		if (p.is_snubbed()) return std::numeric_limits<std::int64_t>::max();
		return std::max(bytes, std::int64_t(0)) * 1000 / p.download_rate_estimate();
	}

	// the blocks of the pieces in ``time_critical`` (sorted) that exactly one
	// peer is asked for and no request of which has timed out, by piece, the
	// latest first. A peer answers its requests in order, so a block arrives
	// once every byte asked for before it has
	template <typename Peers, typename Peer = std::remove_pointer_t<typename Peers::value_type>>
	std::vector<held_block<Peer>> held_blocks(Peers const& connections
		, std::vector<piece_index_t> const& time_critical, int const block_size)
	{
		auto const critical = [&](piece_index_t const piece)
		{ return std::binary_search(time_critical.begin(), time_critical.end(), piece); };

		std::vector<held_block<Peer>> held;
		// blocks with a timed out request
		std::vector<piece_block> timed_out;
		for (Peer* p : connections)
		{
			auto const& dq = p->download_queue();
			// the bytes of the first block received so far are not outstanding
			std::int64_t bytes = std::int64_t(p->outstanding_bytes())
				- std::int64_t(dq.size()) * block_size;
			for (auto const& b : dq)
			{
				bytes += block_size;
				if (b.not_wanted || !critical(b.block.piece_index)) continue;
				if (b.timed_out) timed_out.push_back(b.block);
				else held.push_back({b.block, p, arrival_ms(*p, bytes), false});
			}
			bytes = p->outstanding_bytes();
			for (auto const& b : p->request_queue())
			{
				bytes += block_size;
				if (!critical(b.block.piece_index)) continue;
				held.push_back({b.block, p, arrival_ms(*p, bytes), true});
			}
		}

		std::sort(timed_out.begin(), timed_out.end());
		std::sort(held.begin(), held.end()
			, [](held_block<Peer> const& a, held_block<Peer> const& b) { return a.block < b.block; });
		std::vector<held_block<Peer>> single;
		for (auto i = held.begin(); i != held.end();)
		{
			auto const next = std::find_if(i, held.end()
				, [&](held_block<Peer> const& h) { return h.block != i->block; });
			if (next - i == 1 && !std::binary_search(timed_out.begin(), timed_out.end(), i->block))
				single.push_back(*i);
			i = next;
		}
		std::sort(single.begin(), single.end()
			, [](held_block<Peer> const& a, held_block<Peer> const& b)
			{ return std::tie(a.block.piece_index, b.arrival_ms) < std::tie(b.block.piece_index, a.arrival_ms); });
		return single;
	}

	// the blocks of held_blocks() of one piece
	template <typename Peer>
	span<held_block<Peer> const> held_blocks_of(std::vector<held_block<Peer>> const& held
		, piece_index_t const piece)
	{
		auto const first = std::lower_bound(held.begin(), held.end(), piece
			, [](held_block<Peer> const& h, piece_index_t const p) { return h.block.piece_index < p; });
		auto const last = std::upper_bound(first, held.end(), piece
			, [](piece_index_t const p, held_block<Peer> const& h) { return p < h.block.piece_index; });
		return {held.data() + (first - held.begin()), last - first};
	}

	// a block of a time-critical piece is late when its holder is not expected
	// to deliver it by the piece's deadline. If another peer would deliver it
	// sooner, ask that peer as well, or instead of the holder if the request
	// has not been sent yet. Either way, there is at most one extra copy
	template <typename Peers, typename Peer = std::remove_pointer_t<typename Peers::value_type>>
	late_requests request_late_blocks(time_point const deadline
		, span<held_block<Peer> const> const blocks
		, Peers const& peers
		, std::set<Peer*>& peers_with_requests
		, time_point const now, int const block_size)
	{
		std::int64_t const to_deadline = std::max(std::int64_t(0)
			, std::int64_t(total_milliseconds(deadline - now)));
		late_requests ret;
		for (auto const& h : blocks)
		{
			if (h.arrival_ms <= to_deadline + late_block_margin_ms) continue;
			if (!h.holder->is_snubbed() && !rate_settled(*h.holder, now)) continue;

			Peer* best = nullptr;
			std::int64_t best_ms = h.arrival_ms - late_block_margin_ms;
			for (Peer* p : peers)
			{
				if (p == h.holder || !p->has_piece(h.block.piece_index)) continue;
				if (!p->can_request_time_critical() || !rate_settled(*p, now)) continue;
				// a time-critical request goes after the ones queued already,
				// and is sent after every request sent before it
				std::int64_t const ms = arrival_ms(*p, std::int64_t(p->outstanding_bytes())
					+ std::int64_t(p->num_queued_time_critical() + 1) * block_size);
				if (ms >= best_ms) continue;
				// a request it has may be one not wanted any more
				auto const& dq = p->download_queue();
				auto const& rq = p->request_queue();
				if (std::any_of(dq.begin(), dq.end(), has_block(h.block))
					|| std::any_of(rq.begin(), rq.end(), has_block(h.block)))
					continue;
				best = p;
				best_ms = ms;
			}
			if (best == nullptr) continue;

			if (h.queued)
			{
				h.holder->cancel_request(h.block);
				if (!best->add_request(h.block, Peer::time_critical)) continue;
				++ret.moves;
			}
			else
			{
				if (!best->add_request(h.block, Peer::time_critical | Peer::busy)) continue;
				++ret.copies;
			}
			peers_with_requests.insert(best);
		}
		return ret;
	}
}

#endif
