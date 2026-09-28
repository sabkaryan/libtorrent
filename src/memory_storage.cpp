/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/aux_/memory_slab.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp" // for memory_pool_mutex
#include "libtorrent/disk_interface.hpp" // for default_block_size
#include "libtorrent/file_storage.hpp"
#include "libtorrent/assert.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

namespace libtorrent::aux {

namespace {
	std::atomic<int> g_destroyed_pinned{0};
}

	int memory_storages_destroyed_pinned_for_test()
	{
		return g_destroyed_pinned.load(std::memory_order_relaxed);
	}

	memory_storage::memory_storage(file_storage const& fs, bool const v1, bool const v2
		, memory_slab_allocator& alloc, memory_pool_mutex const* const mutex)
		: m_files(fs)
		, m_v1(v1)
		, m_v2(v2)
		, m_alloc(alloc)
		, m_mutex(mutex)
		, m_pieces(piece_index_t{fs.num_pieces()})
		, m_in_file(fs.num_pieces(), false)
		, m_pad_pieces(fs.num_pieces(), false)
		, m_one_shot(fs.num_pieces(), false)
		, m_retire_count(piece_index_t{fs.num_pieces()}, 0)
	{
		for (auto const f : fs.file_range())
		{
			if (!fs.pad_file_at(f) || fs.file_size(f) == 0) continue;
			for (auto const p : file_piece_range_inclusive(fs, f))
			{
				if (!valid(p)) break;
				m_pad_pieces.set_bit(p);
			}
		}
	}

	memory_storage::~memory_storage()
	{
		// the owner keeps the storage alive until every pin on its entries
		// is released: a job may read the blocks of a pinned entry without
		// the pool's mutex. A retired entry leaves m_retired when its last
		// pin goes, so a non-empty list means a pin outlived the storage.
		// Counted too: with asserts off the breach would only leak
		if (pinned()) g_destroyed_pinned.fetch_add(1, std::memory_order_relaxed);
		TORRENT_ASSERT(m_retired.empty());
		// the blocks go back to the allocator, which is not thread safe
		TORRENT_ASSERT(locked());
		for (auto& slot : m_pieces)
		{
			if (!slot.current) continue;
			TORRENT_ASSERT(slot.current->pins == 0);
			// with asserts off, a pinned entry keeps its blocks (they go back
			// with the allocator) rather than being freed under its reader
			if (slot.current->pins == 0) free_blocks(*slot.current);
		}
	}

	bool memory_storage::pinned() const
	{
		if (!m_retired.empty()) return true;
		return std::any_of(m_pieces.begin(), m_pieces.end()
			, [](piece_slot const& slot) { return slot.current && slot.current->pins > 0; });
	}

	bool memory_storage::locked() const
	{
		return m_mutex == nullptr || m_mutex->owned_by_this_thread();
	}

	bool memory_storage::valid(piece_index_t const piece) const
	{
		return piece >= piece_index_t{0} && piece < m_pieces.end_index();
	}

	int memory_storage::blocks_in_piece(piece_index_t const piece) const
	{
		return (m_files.piece_size(piece) + default_block_size - 1) / default_block_size;
	}

	bool memory_storage::is_pad_block(piece_index_t const piece, int const block) const
	{
		if (!valid(piece) || !m_pad_pieces.get_bit(piece)) return false;
		int const piece_size = m_files.piece_size(piece);
		int const offset = block * default_block_size;
		if (block < 0 || offset >= piece_size) return false;
		int const len = std::min(default_block_size, piece_size - offset);
		auto const slices = m_files.map_block(piece, offset, len);
		return !slices.empty() && std::all_of(slices.begin(), slices.end()
			, [this](file_slice const& s) { return m_files.pad_file_at(s.file_index); });
	}

	int memory_storage::required_blocks(piece_index_t const piece) const
	{
		int const n = blocks_in_piece(piece);
		if (!m_pad_pieces.get_bit(piece)) return n;
		int ret = 0;
		for (int b = 0; b < n; ++b)
			if (!is_pad_block(piece, b)) ++ret;
		return ret;
	}

	std::shared_ptr<memory_piece_entry> memory_storage::current(piece_index_t const piece) const
	{
		TORRENT_ASSERT(valid(piece));
		if (!valid(piece)) return {};
		return m_pieces[piece].current;
	}

	std::shared_ptr<memory_piece_entry> memory_storage::start(piece_index_t const piece
		, piece_place const place)
	{
		TORRENT_ASSERT(valid(piece));
		if (!valid(piece)) return {};
		piece_slot& slot = m_pieces[piece];
		++slot.generation;
		if (slot.current) retire_entry(std::move(slot.current));
		auto e = std::make_shared<memory_piece_entry>();
		e->piece = piece;
		e->generation = slot.generation;
		e->place = place;
		e->missing_blocks = required_blocks(piece);
		if (place == piece_place::memory) e->hasher = make_hasher(piece);
		slot.current = e;
		// the one-shot persist is spent once the piece is in the file
		if (place == piece_place::file) m_one_shot.clear_bit(piece);
		return e;
	}

	std::unique_ptr<memory_piece_hasher> memory_storage::make_hasher(piece_index_t const piece) const
	{
		auto h = std::make_unique<memory_piece_hasher>();
		h->piece_size = m_files.piece_size(piece);
		h->piece_size2 = m_v2 ? m_files.piece_size2(piece) : 0;
		h->v1 = m_v1;
		if (m_v2) h->block_hashes.resize(m_files.blocks_in_piece2(piece));
		if (m_pad_pieces.get_bit(piece))
		{
			int const n = blocks_in_piece(piece);
			h->pad_blocks.resize(static_cast<std::size_t>(n), false);
			for (int b = 0; b < n; ++b)
				h->pad_blocks[static_cast<std::size_t>(b)] = is_pad_block(piece, b);
		}
		return h;
	}

	void memory_storage::zero_pad(piece_index_t const piece, int const block, char* const data) const
	{
		if (!m_pad_pieces.get_bit(piece)) return;
		int const piece_size = m_files.piece_size(piece);
		int const offset = block * default_block_size;
		int const len = std::min(default_block_size, piece_size - offset);
		if (len <= 0) return;
		int pos = 0;
		for (file_slice const& s : m_files.map_block(piece, offset, len))
		{
			if (m_files.pad_file_at(s.file_index))
				std::memset(data + pos, 0, static_cast<std::size_t>(s.size));
			pos += static_cast<int>(s.size);
		}
	}

	void memory_storage::retire(piece_index_t const piece)
	{
		TORRENT_ASSERT(valid(piece));
		if (!valid(piece)) return;
		piece_slot& slot = m_pieces[piece];
		if (slot.current) retire_entry(std::move(slot.current));
		slot.current.reset();
		++m_retire_count[piece];
	}

	void memory_storage::retire_all()
	{
		for (auto& slot : m_pieces)
		{
			if (slot.current) retire_entry(std::move(slot.current));
			slot.current.reset();
		}
		for (auto& c : m_retire_count) ++c;
	}

	std::uint32_t memory_storage::retire_count(piece_index_t const piece) const
	{
		TORRENT_ASSERT(valid(piece));
		return valid(piece) ? m_retire_count[piece] : 0;
	}

	// e is no longer current. It moves out of the held bytes, and is freed now
	// if no job holds it, or when its last pin goes
	void memory_storage::retire_entry(std::shared_ptr<memory_piece_entry> e)
	{
		std::int64_t const bytes = std::int64_t(e->num_blocks) * default_block_size;
		if (e->missing_blocks == 0) m_held_complete -= bytes;
		else m_held_partial -= bytes;
		TORRENT_ASSERT(m_held_complete >= 0);
		TORRENT_ASSERT(m_held_partial >= 0);
		if (e->pins == 0)
		{
			free_blocks(*e);
			return;
		}
		m_retired_bytes += bytes;
		m_retired.push_back(std::move(e));
	}

	void memory_storage::unpin(std::shared_ptr<memory_piece_entry> const& e)
	{
		TORRENT_ASSERT(e);
		if (!e) return;
		TORRENT_ASSERT(e->pins > 0);
		if (e->pins > 0) --e->pins;
		if (e->pins > 0 || is_current(*e)) return;
		auto const it = std::find(m_retired.begin(), m_retired.end(), e);
		if (it == m_retired.end()) return;
		memory_piece_entry& retired = **it;
		m_retired_bytes -= std::int64_t(retired.num_blocks) * default_block_size;
		TORRENT_ASSERT(m_retired_bytes >= 0);
		free_blocks(retired);
		m_retired.erase(it);
	}

	bool memory_storage::is_current(memory_piece_entry const& e) const
	{
		if (!valid(e.piece)) return false;
		auto const& c = m_pieces[e.piece].current;
		return c.get() == &e;
	}

	void memory_storage::free_blocks(memory_piece_entry& e)
	{
		// the allocator is not thread safe
		TORRENT_ASSERT(locked());
		std::vector<char*> used;
		used.reserve(static_cast<std::size_t>(e.num_blocks));
		for (char* const b : e.blocks)
			if (b != nullptr) used.push_back(b);
		if (!used.empty()) m_alloc.free(used);
		e.blocks.clear();
		e.blocks.shrink_to_fit();
		e.num_blocks = 0;
	}

	bool memory_storage::write_block(memory_piece_entry& e, int const block
		, span<char const> const data)
	{
		TORRENT_ASSERT(is_current(e));
		if (!is_current(e)) return false;
		int const n = blocks_in_piece(e.piece);
		TORRENT_ASSERT(block >= 0 && block < n);
		TORRENT_ASSERT(data.size() > 0 && data.size() <= default_block_size);
		if (block < 0 || block >= n) return false;
		if (data.size() <= 0 || data.size() > default_block_size) return false;

		if (e.blocks.empty()) e.blocks.resize(n, nullptr);
		char*& slot = e.blocks[block];
		if (slot != nullptr)
		{
			// a block is written once. A hasher may be reading it without
			// the pool's mutex; new bytes for the piece come through start()
			TORRENT_ASSERT_FAIL();
			return false; // written once
		}
		// the allocator is not thread safe
		TORRENT_ASSERT(locked());
		slot = m_alloc.allocate();
		if (slot == nullptr) return false;
		auto const size = static_cast<std::size_t>(data.size());
		std::memcpy(slot, data.data(), size);
		if (size < std::size_t(default_block_size))
			std::memset(slot + size, 0, std::size_t(default_block_size) - size);
		// SHA-1 covers the pad of a v1 piece as zeros, whatever the peer
		// sent there, and a read of pad returns zeros
		zero_pad(e.piece, block, slot);

		++e.num_blocks;
		if (e.missing_blocks > 0 && !is_pad_block(e.piece, block))
		{
			--e.missing_blocks;
			if (e.missing_blocks == 0)
			{
				// the entry moves from partial to complete
				m_held_partial -= std::int64_t(e.num_blocks - 1) * default_block_size;
				m_held_complete += std::int64_t(e.num_blocks) * default_block_size;
				TORRENT_ASSERT(m_held_partial >= 0);
				return true;
			}
		}
		if (e.missing_blocks == 0) m_held_complete += default_block_size;
		else m_held_partial += default_block_size;
		return true;
	}

	char const* memory_storage::block_data(memory_piece_entry const& e, int const block) const
	{
		if (block < 0 || block >= e.blocks.end_index()) return nullptr;
		return e.blocks[block];
	}

	bool memory_storage::hand_to_inner(memory_piece_entry& e, int const block)
	{
		TORRENT_ASSERT(is_current(e));
		TORRENT_ASSERT(e.place == piece_place::file);
		int const n = blocks_in_piece(e.piece);
		if (!is_current(e) || block < 0 || block >= n) return false;
		if (e.inner_blocks.empty()) e.inner_blocks.resize(static_cast<std::size_t>(n), false);
		auto&& handed = e.inner_blocks[static_cast<std::size_t>(block)];
		if (handed || is_pad_block(e.piece, block)) return false;
		handed = true;
		if (e.missing_blocks > 0) --e.missing_blocks;
		return true;
	}

	void memory_storage::inner_block_written(memory_piece_entry& e)
	{
		TORRENT_ASSERT(is_current(e));
		if (!is_current(e)) return;
		++e.inner_written;
		if (e.inner_written == required_blocks(e.piece)) m_in_file.set_bit(e.piece);
	}

	bool memory_storage::in_file(piece_index_t const piece) const
	{
		TORRENT_ASSERT(valid(piece));
		return valid(piece) && m_in_file.get_bit(piece);
	}

	void memory_storage::set_in_file(piece_index_t const piece, bool const value)
	{
		TORRENT_ASSERT(valid(piece));
		if (!valid(piece)) return;
		if (value) m_in_file.set_bit(piece);
		else m_in_file.clear_bit(piece);
	}

	void memory_storage::set_claim_policy(memory_owner_t const owner, memory_policy const policy
		, span<file_index_t const> const files)
	{
		claim& c = m_claims[owner];
		c.has_policy = true;
		c.policy = policy;
		c.covered.clear();
		c.covered.resize(static_cast<int>(m_pieces.end_index()), files.empty());
		if (files.empty()) return;
		for (file_index_t const f : files)
		{
			if (f < file_index_t{0} || f >= m_files.end_file()) continue;
			if (m_files.file_size(f) == 0) continue;
			for (auto const p : file_piece_range_inclusive(m_files, f))
			{
				if (!valid(p)) break;
				c.covered.set_bit(p);
			}
		}
	}

	void memory_storage::set_claim_persist(memory_owner_t const owner
		, span<piece_index_t const> const pieces)
	{
		claim& c = m_claims[owner];
		c.persist.clear();
		if (pieces.empty()) return;
		c.persist.resize(static_cast<int>(m_pieces.end_index()), false);
		for (piece_index_t const p : pieces)
			if (valid(p)) c.persist.set_bit(p);
	}

	void memory_storage::add_one_shot_persist(span<piece_index_t const> const pieces)
	{
		for (piece_index_t const p : pieces)
			if (valid(p)) m_one_shot.set_bit(p);
	}

	void memory_storage::drop_owner(memory_owner_t const owner)
	{
		m_claims.erase(owner);
	}

	void memory_storage::set_torrent_policy(memory_policy const policy)
	{
		m_torrent_policy = policy;
	}

	bool memory_storage::covered_by(piece_index_t const piece, memory_policy const policy) const
	{
		for (auto const& oc : m_claims)
		{
			claim const& c = oc.second;
			if (c.has_policy && c.policy == policy && c.covered.get_bit(piece)) return true;
		}
		return false;
	}

	piece_place memory_storage::decide(piece_index_t const piece, bool const at_limit)
	{
		TORRENT_ASSERT(valid(piece));
		if (!valid(piece)) return piece_place::file;

		// rule 1: persist, one-shot (the piece leaves that set when it starts
		// in the file) or of any owner
		if (m_one_shot.get_bit(piece)) return piece_place::file;
		for (auto const& oc : m_claims)
		{
			claim const& c = oc.second;
			if (!c.persist.empty() && c.persist.get_bit(piece))
				return piece_place::file; // rule 1: persist of an owner
		}
		// rule 2: file wins over memory, whoever asked for it
		if (covered_by(piece, memory_policy::file)) return piece_place::file; // rule 2
		// rules 4 and 5: where the piece goes without the limit. A memory
		// claim covers it, otherwise the torrent policy
		bool const bound_for_memory = covered_by(piece, memory_policy::memory) // rule 4
			|| m_torrent_policy == memory_policy::memory; // rule 5
		piece_place const unlimited = bound_for_memory ? piece_place::memory : piece_place::file;
		// rule 3: the pool is at its limit. Only a piece bound for memory is
		// a spill; one bound for the file would have gone there anyway
		if (at_limit)
		{
			if (unlimited == piece_place::memory) ++m_spilled_pieces; // spill
			return piece_place::file;
		}
		return unlimited;
	}
}
