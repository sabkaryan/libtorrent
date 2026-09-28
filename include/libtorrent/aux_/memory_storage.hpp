/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_STORAGE_HPP_INCLUDED
#define TORRENT_MEMORY_STORAGE_HPP_INCLUDED

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "libtorrent/config.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/units.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/bitfield.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/sha1_hash.hpp"
#include "libtorrent/aux_/vector.hpp"

namespace libtorrent::aux {

	struct memory_slab_allocator;

	// the hash state of one entry, owned by the hasher of the pool
	struct memory_piece_hasher
	{
		hasher ph;
		// the next block to feed to ph
		int cursor = 0;
		// v2 block hashes, one per block
		aux::vector<sha256_hash, int> block_hashes;
	};

	// one generation of one piece. Jobs hold a shared_ptr to their own entry
	// and change the state of the storage only while that entry is current
	struct memory_piece_entry
	{
		piece_index_t piece{0};
		// for logs only. Whether an entry is current is decided by its
		// identity (memory_storage::is_current()), never by this number
		std::uint32_t generation = 0;
		piece_place place = piece_place::none;
		// one 16 kiB block each, nullptr = not received. Empty until the
		// first block is written
		aux::vector<char*, int> blocks;
		// non-null blocks
		int num_blocks = 0;
		// non-pad blocks not received yet. 0 means the piece is complete
		int missing_blocks = 0;
		// an async_hash answered for this entry
		bool hash_returned = false;
		// jobs and client reads holding the entry. A retired entry is freed
		// when this drops to 0
		int pins = 0;
		std::unique_ptr<memory_piece_hasher> hasher;
	};

	// the state of one torrent in the in-memory disk backend: the current
	// entry of every piece, the retired entries still pinned, the "in file"
	// flag of every piece and the claims of the owners. Not thread safe: the
	// pool serialises calls under its mutex.
	struct TORRENT_EXTRA_EXPORT memory_storage
	{
		// fs must outlive the storage, and so must alloc
		memory_storage(file_storage const& fs, bool v1, bool v2, memory_slab_allocator& alloc);
		// frees the blocks of the current entries. The storage must outlive
		// every pin on its entries: the owner (the pool) keeps it alive until
		// the pins drop, since a job may read a pinned entry's blocks without
		// the pool's mutex. Asserts that no entry is pinned; with asserts off
		// a pinned entry's blocks are not freed
		~memory_storage();
		memory_storage(memory_storage const&) = delete;
		memory_storage& operator=(memory_storage const&) = delete;

		// the current entry, nullptr if the piece has none (place none)
		std::shared_ptr<memory_piece_entry> current(piece_index_t piece) const;
		// starts the piece: retires the current entry (freed when unpinned)
		// and makes a new one with generation + 1 and the given place. A
		// piece that starts in the file leaves the one-shot persist set
		std::shared_ptr<memory_piece_entry> start(piece_index_t piece, piece_place place);
		// retires the current entry; the piece has no entry afterwards
		void retire(piece_index_t piece);
		void retire_all();
		// drops one pin; frees a retired entry when its last pin goes
		void unpin(std::shared_ptr<memory_piece_entry> const& e);
		// true if e is the current entry of its piece (by identity)
		bool is_current(memory_piece_entry const& e) const;

		// copies data into block ``block`` of the current entry e, allocating
		// the block (zero-filling the rest of a short block). A block is
		// written once: a hasher may read it without the pool's mutex, so
		// new bytes for a piece come only through start(). Returns false
		// (and asserts) if the block is already present, leaving it
		// unchanged, and false if e is not current, the block is out of
		// range or allocation failed
		bool write_block(memory_piece_entry& e, int block, span<char const> data);
		// the block, nullptr if it was not received (or was freed)
		char const* block_data(memory_piece_entry const& e, int block) const;

		// the "in file" flag, per piece (not per entry)
		bool in_file(piece_index_t piece) const;
		void set_in_file(piece_index_t piece, bool value);

		// claims (each owner changes only its own) and the place for a piece
		// that starts now. An empty list of files covers the whole torrent;
		// an empty list of pieces clears the owner's persist set
		void set_claim_policy(memory_owner_t owner, memory_policy policy
			, span<file_index_t const> files);
		void set_claim_persist(memory_owner_t owner, span<piece_index_t const> pieces);
		void add_one_shot_persist(span<piece_index_t const> pieces);
		void drop_owner(memory_owner_t owner);
		void set_torrent_policy(memory_policy policy);
		// rules 1-5: persist (one-shot or of any owner) -> file; a file
		// claim covers it -> file; at_limit (the pool holds >= its limit)
		// -> file; a memory claim covers it -> memory; otherwise the torrent
		// policy. It changes no claim: a one-shot persist piece stays in its
		// set until start() starts it in the file. A piece that rule 3 sends
		// to the file counts in spilled_pieces() only if rules 4-5 would have
		// sent it to memory
		piece_place decide(piece_index_t piece, bool at_limit);

		// bytes of the blocks of current entries whose every non-pad block
		// is present, and of the other current entries
		std::int64_t held_complete() const { return m_held_complete; }
		std::int64_t held_partial() const { return m_held_partial; }
		// bytes of retired entries that are still pinned
		std::int64_t retired_bytes() const { return m_retired_bytes; }
		// pieces decide() sent to the file because the pool was at its
		// limit, that would have gone to memory otherwise
		int spilled_pieces() const { return m_spilled_pieces; }

		file_storage const& files() const { return m_files; }
		bool v1() const { return m_v1; }
		bool v2() const { return m_v2; }

		// the number of 16 kiB blocks of the piece (v1 piece size, pad
		// included)
		int blocks_in_piece(piece_index_t piece) const;
		// true if the block lies entirely in pad files
		bool is_pad_block(piece_index_t piece, int block) const;

	private:

		struct piece_slot
		{
			std::shared_ptr<memory_piece_entry> current;
			// the generation of the most recent entry of this piece
			std::uint32_t generation = 0;
		};

		struct claim
		{
			bool has_policy = false;
			memory_policy policy = memory_policy::file;
			// the pieces the policy covers
			typed_bitfield<piece_index_t> covered;
			// the persist set, empty when there is none
			typed_bitfield<piece_index_t> persist;
		};

		bool valid(piece_index_t piece) const;
		int required_blocks(piece_index_t piece) const;
		void retire_entry(std::shared_ptr<memory_piece_entry> e);
		void free_blocks(memory_piece_entry& e);
		bool covered_by(piece_index_t piece, memory_policy policy) const;

		file_storage const& m_files;
		bool const m_v1;
		bool const m_v2;
		memory_slab_allocator& m_alloc;

		aux::vector<piece_slot, piece_index_t> m_pieces;
		typed_bitfield<piece_index_t> m_in_file;
		// pieces that overlap a pad file
		typed_bitfield<piece_index_t> m_pad_pieces;
		// retired entries that are still pinned
		std::vector<std::shared_ptr<memory_piece_entry>> m_retired;

		std::map<memory_owner_t, claim> m_claims;
		typed_bitfield<piece_index_t> m_one_shot;
		memory_policy m_torrent_policy = memory_policy::file;

		std::int64_t m_held_complete = 0;
		std::int64_t m_held_partial = 0;
		std::int64_t m_retired_bytes = 0;
		int m_spilled_pieces = 0;
	};
}

#endif
