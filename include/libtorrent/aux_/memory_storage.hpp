/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_STORAGE_HPP_INCLUDED
#define TORRENT_MEMORY_STORAGE_HPP_INCLUDED

#include <cstdint>
#include <functional>
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
	struct memory_pool_mutex;

	// the hash state of one entry held in memory. memory_hasher works on it
	struct memory_piece_hasher
	{
		// SHA-1 of blocks [0, cursor) (v1 only)
		hasher ph;
		// the next block to feed to ph
		int cursor = 0;
		// v2 block hashes, one per block of the v2 piece; all zeros = not
		// computed yet. Guarded by the pool mutex
		aux::vector<sha256_hash, int> block_hashes;

		// the geometry of the piece, taken when the entry starts, so a
		// hashing thread never reads the file_storage (the torrent owns it)
		// the v1 piece size, pad included
		int piece_size = 0;
		// the v2 piece size, 0 without v2
		int piece_size2 = 0;
		bool v1 = false;
		// true for a block entirely in pad files, one per block; empty if
		// the piece has no pad
		std::vector<bool> pad_blocks;

		// scheduling, guarded by the pool mutex. A thread that set busy owns
		// ph and cursor until it clears busy
		bool busy = false;
		// a background hash of the entry is queued and has not started
		bool kick_queued = false;
		// hash jobs that found the entry busy. They are scheduled again when
		// busy clears
		std::vector<std::function<void()>> parked;
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
		// an async_hash answered for this entry with every block it hashed
		// present (a hash with missing blocks, as in a recheck of a partial
		// piece, does not set it)
		bool hash_returned = false;
		// jobs and client reads holding the entry. A retired entry is freed
		// when this drops to 0
		int pins = 0;
		std::unique_ptr<memory_piece_hasher> hasher;
		// place file: the blocks handed to the default backend, one flag per
		// block (empty until the first one), and how many of the non-pad
		// ones it wrote. A piece in the file is complete once every non-pad
		// block was handed over (missing_blocks counts them down)
		std::vector<bool> inner_blocks;
		int inner_written = 0;
		// place transfer: the step that hands the entry's blocks to the
		// default backend was issued (until then the entry waits for its
		// turn and takes writes like an entry in memory)
		bool transfer_started = false;
		// place transfer: the entry had missing blocks when its step was
		// issued. Its blocks go to the default backend with flush_piece and
		// are freed by their write handlers; later blocks go to the default
		// backend, and so does its hash
		bool partial_transfer = false;
		// blocks taken out of the entry while a job pinned it (a hashing
		// thread may still read them). They are freed with its last pin and
		// counted in the retired bytes until then
		std::vector<char*> dropped;
	};

	// the state of one torrent in the in-memory disk backend: the current
	// entry of every piece, the retired entries still pinned, the "in file"
	// flag of every piece and the claims of the owners. Not thread safe: the
	// pool serialises calls under its mutex.
	struct TORRENT_EXTRA_EXPORT memory_storage
	{
		// fs must outlive every call but the destructor and unpin(), and
		// alloc must outlive the storage. With a mutex, every allocation and
		// free asserts that the caller holds it (the pool's)
		memory_storage(file_storage const& fs, bool v1, bool v2, memory_slab_allocator& alloc
			, memory_pool_mutex const* mutex = nullptr);
		// frees the blocks of the current entries. The storage must outlive
		// every pin on its entries: the owner (the pool) keeps it alive until
		// the pins drop, since a job may read a pinned entry's blocks without
		// the pool's mutex. Asserts that no entry is pinned; with asserts off
		// a pinned entry's blocks are not freed. The caller holds the pool's
		// mutex (it is not taken here)
		~memory_storage();
		memory_storage(memory_storage const&) = delete;
		memory_storage& operator=(memory_storage const&) = delete;

		// the current entry, nullptr if the piece has none (place none)
		std::shared_ptr<memory_piece_entry> current(piece_index_t piece) const;
		// starts the piece: retires the current entry (freed when unpinned)
		// and makes a new one with generation + 1 and the given place. A
		// piece that starts in the file leaves the one-shot persist set. An
		// entry that starts in memory gets its hasher
		std::shared_ptr<memory_piece_entry> start(piece_index_t piece, piece_place place);
		// retires the current entry; the piece has no entry afterwards. Both
		// increase the retire count of the piece (of every piece)
		void retire(piece_index_t piece);
		void retire_all();
		// increased by every retire() of the piece and every retire_all().
		// An answer of the default backend for a piece without an entry
		// carries no entry to compare: its issuer remembers this count and
		// the answer applies only if it did not change
		std::uint32_t retire_count(piece_index_t piece) const;
		// drops one pin; frees a retired entry when its last pin goes
		void unpin(std::shared_ptr<memory_piece_entry> const& e);
		// true if e is the current entry of its piece (by identity)
		bool is_current(memory_piece_entry const& e) const;

		// copies data into block ``block`` of the current entry e, allocating
		// the block (zero-filling the rest of a short block and the ranges of
		// the block that lie in pad files). A block is
		// written once: a hasher may read it without the pool's mutex, so
		// new bytes for a piece come only through start(). Returns false
		// (and asserts) if the block is already present, leaving it
		// unchanged, and false if e is not current, the block is out of
		// range or allocation failed
		bool write_block(memory_piece_entry& e, int block, span<char const> data);
		// the block, nullptr if it was not received (or was freed)
		char const* block_data(memory_piece_entry const& e, int block) const;

		// block ``block`` of the current entry e (place file, or a partial
		// transfer) is handed to the default backend. Returns true the first
		// time for a non-pad block: its write, once completed, is passed to
		// inner_block_written().
		// A block whose write fails stays handed over and a retry is not
		// counted: the piece does not become "in file", which only costs a
		// download again after a restart (filter_resume() drops it)
		bool hand_to_inner(memory_piece_entry& e, int block);
		// the default backend wrote a block of the current entry e for which
		// hand_to_inner() returned true. Once every non-pad block of the
		// piece is written, the piece is "in file"
		void inner_block_written(memory_piece_entry& e);
		// the step of a partial transfer hands block ``block``, held in
		// memory, to the default backend. Returns true for a non-pad block,
		// whose completed write is passed to inner_block_written()
		bool hand_held_to_inner(memory_piece_entry& e, int block);
		// whether block ``block`` of e was handed to the default backend
		static bool handed_to_inner(memory_piece_entry const& e, int block);

		// takes a block (every block) out of the current entry e: it is not
		// held any more, and is freed now, or with e's last pin if a job
		// pins e. The caller holds the pool's mutex
		void drop_block(memory_piece_entry& e, int block);
		void drop_blocks(memory_piece_entry& e);
		// the transfer of the complete current entry e to the file ended
		// well: place file, every non-pad block written by the default
		// backend and its hash returned there, the piece is "in file" and
		// leaves the one-shot persist set; the blocks are dropped
		void transfer_done(memory_piece_entry& e);
		// the step of a partial transfer of the current entry e was
		// answered: place file (its later blocks went to the file), the
		// piece leaves the one-shot persist set, the blocks left in memory
		// are dropped. A failed write counts in persist_failures()
		void partial_transfer_done(memory_piece_entry& e, bool failed);
		// the transfer of the current entry e failed: back to place memory,
		// counted in persist_failures()
		void transfer_failed(memory_piece_entry& e);
		// a write of a transfer to the default backend failed
		void add_persist_failure() { ++m_persist_failures; }
		int persist_failures() const { return m_persist_failures; }

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
		memory_policy torrent_policy() const { return m_torrent_policy; }
		// rules 1-5: persist (one-shot or of any owner) -> file; a file
		// claim covers it -> file; at_limit (the pool holds >= its limit)
		// -> file; a memory claim covers it -> memory; otherwise the torrent
		// policy. It changes no claim: a one-shot persist piece stays in its
		// set until start() starts it in the file. A piece that rule 3 sends
		// to the file counts in spilled_pieces() only if rules 4-5 would have
		// sent it to memory
		piece_place decide(piece_index_t piece, bool at_limit);
		// the place rules 1, 2, 4 and 5 give the piece, the limit aside. A
		// piece held in memory whose wanted place is the file is moved there
		piece_place wanted_place(piece_index_t piece) const;

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

		// the torrent was removed: the storage is kept only while jobs pin
		// its (retired) entries
		void mark_removed() { m_removed = true; }
		bool removed() const { return m_removed; }
		// an entry of the storage, current or retired, is pinned
		bool pinned() const;

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
		std::unique_ptr<memory_piece_hasher> make_hasher(piece_index_t piece) const;
		// zeroes the bytes of the block that lie in pad files
		void zero_pad(piece_index_t piece, int block, char* data) const;
		// the caller holds the pool's mutex (if the storage has one)
		bool locked() const;
		void retire_entry(std::shared_ptr<memory_piece_entry> e);
		void free_blocks(memory_piece_entry& e);
		// frees the blocks dropped from e while it was pinned
		void free_dropped(memory_piece_entry& e);
		// adds (sign 1) or removes (sign -1) the bytes of the current entry e
		// to the held bytes, complete or partial by its missing blocks
		void account(memory_piece_entry const& e, int sign);
		bool covered_by(piece_index_t piece, memory_policy policy) const;

		file_storage const& m_files;
		bool const m_v1;
		bool const m_v2;
		memory_slab_allocator& m_alloc;
		memory_pool_mutex const* const m_mutex;

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
		int m_persist_failures = 0;
		aux::vector<std::uint32_t, piece_index_t> m_retire_count;
		bool m_removed = false;
	};

	// test hook: storages destroyed while an entry of theirs was still
	// pinned, since the process started. The owner keeps a storage alive
	// until its pins drop, so this stays 0
	TORRENT_EXTRA_EXPORT int memory_storages_destroyed_pinned_for_test();
}

#endif
