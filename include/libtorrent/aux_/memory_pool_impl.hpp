/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_POOL_IMPL_HPP_INCLUDED
#define TORRENT_MEMORY_POOL_IMPL_HPP_INCLUDED

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "libtorrent/config.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/info_hash.hpp"
#include "libtorrent/storage_defs.hpp" // for storage_index_t
#include "libtorrent/io_context.hpp"
#include "libtorrent/bitfield.hpp"
#include "libtorrent/aux_/memory_slab.hpp"

#if TORRENT_USE_ASSERTS
#include <atomic>
#include <thread>
#endif

namespace libtorrent::aux {

	struct memory_storage;
	struct memory_piece_entry;

	// the mutex of a pool. A std::mutex that, in builds with asserts, also
	// knows which thread holds it, so the code that frees blocks can assert
	// that its caller holds it. Use it with std::lock_guard or
	// std::unique_lock
	struct memory_pool_mutex
	{
		void lock()
		{
			m_mutex.lock();
#if TORRENT_USE_ASSERTS
			m_owner.store(std::this_thread::get_id(), std::memory_order_relaxed);
#endif
		}

		void unlock()
		{
#if TORRENT_USE_ASSERTS
			m_owner.store(std::thread::id{}, std::memory_order_relaxed);
#endif
			m_mutex.unlock();
		}

		// true if the calling thread holds the mutex. Without asserts it is
		// not tracked and this is always true
		bool owned_by_this_thread() const
		{
#if TORRENT_USE_ASSERTS
			return m_owner.load(std::memory_order_relaxed) == std::this_thread::get_id();
#else
			return true;
#endif
		}

	private:
		std::mutex m_mutex;
#if TORRENT_USE_ASSERTS
		std::atomic<std::thread::id> m_owner{};
#endif
	};

	// the state shared by a memory_storage_pool and every memory_disk_io
	// created from it. Every member is guarded by mutex.
	//
	// Lock discipline. The slab allocator is not thread safe, and a
	// memory_storage frees blocks into it when an entry is freed (its last
	// pin goes) and when the storage is destroyed. So:
	//
	// * every drop of a reference that may be the last one to a
	//   memory_storage or to a memory_piece_entry happens with the pool's
	//   mutex held. remove_torrent() drops its storage under the mutex. A
	//   hash job drops its references in its completion, posted to the
	//   network thread, under the mutex; a background hash (a kick, which has
	//   no completion) drops them on the hashing thread under the mutex. A
	//   job holds its references in a memory_entry_ref, whose destructor
	//   takes the mutex if the job never released them (a completion the
	//   io_context destroyed without running it)
	// * ~memory_storage and every block free assert that the caller holds
	//   the mutex; they never take it themselves (no recursive locking)
	// * the default disk backend is never called with the mutex held:
	//   remove_torrent() releases the default backend's storage outside it
	// * handlers of the caller are never called with the mutex held, and
	//   neither is a torrent object released (a retired storage keeps one
	//   alive; memory_entry_ref drops it after the mutex, on the network
	//   thread: see memory_entry_ref)
	//
	// Removed torrents. remove_torrent() retires every entry of the
	// storage. If a job still pins one, the storage is kept in `retired`
	// with the torrent object it was created for (it owns the file_storage
	// the storage refers to) until the last pin drops, and is freed then,
	// under the mutex. Its counters stay in the pool's
	struct memory_pool_impl
	{
		explicit memory_pool_impl(int slab_bytes);

		mutable memory_pool_mutex mutex;

		// the blocks of every storage of the pool
		memory_slab_allocator alloc;

		memory_policy default_policy = memory_policy::file;
		// set_policy(info_hash_t) registrations, made before a torrent is
		// added
		std::vector<std::pair<info_hash_t, memory_policy>> registrations;
		// the number of bytes the pool may hold; 0 until the client sets it
		std::int64_t limit = 0;

		// one storage of a memory_disk_io of the pool
		struct storage_ref
		{
			// the torrent object new_torrent() was given, the one
			// torrent_handle::native_handle() refers to. Compared by owner
			// (owner_before), never by address: a torrent object at the
			// address of a removed one is another torrent
			std::weak_ptr<void> torrent;
			// the info-hashes of the torrent when the storage was created
			info_hash_t info_hashes;
			std::shared_ptr<memory_storage> storage;
			// moves the given entries (place transfer) to the file: posts
			// them to the network thread of the memory_disk_io the storage
			// belongs to, without waiting. Called with the mutex held: while
			// the storage is in `storages`, that memory_disk_io and its
			// io_context exist
			std::function<void(std::vector<std::shared_ptr<memory_piece_entry>>)> transfer;
		};

		// the storages of every memory_disk_io of the pool. A storage is
		// added by new_torrent() and removed by remove_torrent()
		std::vector<storage_ref> storages;

		// a removed storage whose entries are still pinned by jobs
		struct retired_storage
		{
			std::shared_ptr<memory_storage> storage;
			// the torrent object, the owner of the file_storage the storage
			// refers to. Null for a storage created without one
			std::shared_ptr<void> torrent;
			// the network thread's io_context of the session the torrent
			// lived in: the torrent object is released there. It outlives
			// every pin: pins are held only by that session's disk
			// backend, its hasher threads and the default backend's
			// threads (each holds a work guard on it), and by the
			// completions they post to it
			io_context* ios = nullptr;
		};
		std::vector<retired_storage> retired;
		// storages removed while an entry of theirs was pinned, in all
		std::int64_t storages_retired_pinned = 0;
		// the spilled pieces of removed storages
		std::int64_t spilled_of_removed = 0;

		// claims made by torrent_handle for a torrent that has no storage
		// yet (no metadata), applied in order by its new_torrent(). Keyed
		// by the torrent object, compared by owner; expired keys are dropped
		// whenever the list is used
		struct pending_claims
		{
			std::weak_ptr<void> torrent;
			std::vector<std::function<void(memory_storage&)>> ops;
		};
		std::vector<pending_claims> pending;

		// what the pool keeps of a removed storage, for filter_resume(): its
		// info-hashes, its "in file" pieces, the pieces whose entry had place
		// file, and the backend (memory_disk_io) it belonged to. Dropped by
		// forget_record(), by a new storage of the torrent in the same
		// backend (it supersedes it), or with the pool
		struct residue
		{
			info_hash_t info_hashes;
			typed_bitfield<piece_index_t> in_file;
			typed_bitfield<piece_index_t> file_place;
			std::uint64_t backend = 0;
		};
		std::vector<residue> residues;

		// the id the next memory_disk_io of the pool gets. Each backend (one
		// per session) has its own
		std::uint64_t next_backend = 1;

		std::int64_t hash_missing_blocks = 0;

		// the bytes held by the current entries of every storage. The
		// caller holds mutex
		std::int64_t held_bytes() const;
		// the bytes of retired entries, of live and of removed storages.
		// The caller holds mutex
		std::int64_t retired_bytes() const;

		// the storage created for the torrent object, nullptr if it has
		// none. The caller holds mutex
		std::shared_ptr<memory_storage> storage_of(std::shared_ptr<void> const& torrent) const;
		// the policy of a new storage: the first registration matching the
		// torrent's v1 or v2 info-hash, or `best` (storage_params::info_hash)
		// by the registration's v1 or truncated v2; otherwise the default.
		// The caller holds mutex
		memory_policy policy_for(info_hash_t const& ih, sha1_hash const& best) const;
		// queues a claim for a torrent that has no storage yet. The caller
		// holds mutex
		void add_pending(std::weak_ptr<void> torrent, std::function<void(memory_storage&)> op);
		// applies (and drops) the claims queued for the torrent. The caller
		// holds mutex
		void apply_pending(std::shared_ptr<void> const& torrent, memory_storage& s);
		// the last pin of the removed storage s dropped: forgets its record
		// and returns the torrent object it kept and the io_context of its
		// network thread, where the torrent is to be released (after the
		// mutex). The caller holds mutex
		std::pair<std::shared_ptr<void>, io_context*> release_retired(memory_storage const& s);
		// the record of the storage created for the torrent object, nullptr
		// if it has none. The caller holds mutex
		storage_ref const* ref_of(std::shared_ptr<void> const& torrent) const;
		// the claims of the storage changed: every piece held in memory
		// whose wanted place is now the file gets place transfer (and counts
		// in pending_persist_bytes() from now on), and is posted to the
		// network thread to be moved. The caller holds mutex
		void request_transfers(storage_ref const& r);
		// the residue of a removed storage of the backend. The caller holds
		// mutex
		void add_residue(info_hash_t const& ih, memory_storage const& s, std::uint64_t backend);
		// drops the residues of the torrent (its v1 or v2 info-hash
		// matches), of every backend. The caller holds mutex
		void drop_residues(info_hash_t const& ih);
		// drops the residues of the torrent left by this backend only: the
		// ones of another session may still describe its files. The caller
		// holds mutex
		void drop_own_residues(info_hash_t const& ih, std::uint64_t backend);
	};

	// test hooks: compiled in every build, inert unless called. The
	// hooks of a memory_storage_pool
	struct TORRENT_EXTRA_EXPORT memory_pool_test_access
	{
		// the blocks the pool has allocated
		static int blocks_in_use(memory_storage_pool const& pool);
		// removed storages kept for their pins now, and in all
		static int retired_storages(memory_storage_pool const& pool);
		static std::int64_t storages_retired_pinned(memory_storage_pool const& pool);
		// torrents with claims waiting for their storage
		static int pending_claims(memory_storage_pool const& pool);
		// removed storages the pool keeps for filter_resume()
		static int residues(memory_storage_pool const& pool);
	};

	// test hook: the place of the current entry of a piece of a storage of
	// the memory_disk_io ``disk`` (piece_place::none without an entry)
	TORRENT_EXTRA_EXPORT piece_place memory_place_for_test(disk_interface& disk
		, storage_index_t storage, piece_index_t piece);
	// test hooks: the "in file" flag of a piece of a storage of ``disk``
	TORRENT_EXTRA_EXPORT bool memory_in_file_for_test(disk_interface& disk
		, storage_index_t storage, piece_index_t piece);
	TORRENT_EXTRA_EXPORT void memory_set_in_file_for_test(disk_interface& disk
		, storage_index_t storage, piece_index_t piece, bool value);
	// test hook: the blocks the pool of ``disk`` has allocated, of every
	// storage, current or retired
	TORRENT_EXTRA_EXPORT int memory_blocks_in_use_for_test(disk_interface& disk);
	// test hooks for a storage of ``disk`` made without a torrent object
	// (no handle reaches it): the persist set of owner 1 becomes `pieces`,
	// and its pieces held in memory are moved to the file, as by
	// memory_storage_pool::set_persist(); a one-shot persist, as by
	// memory_storage_pool::persist(); the piece is forgotten, as by
	// memory_storage_pool::forget_piece() when libtorrent answers 0
	TORRENT_EXTRA_EXPORT void memory_set_persist_for_test(disk_interface& disk
		, storage_index_t storage, span<piece_index_t const> pieces);
	TORRENT_EXTRA_EXPORT void memory_persist_for_test(disk_interface& disk
		, storage_index_t storage, span<piece_index_t const> pieces);
	TORRENT_EXTRA_EXPORT void memory_forget_for_test(disk_interface& disk
		, storage_index_t storage, piece_index_t piece);
}

#endif
