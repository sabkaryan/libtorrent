/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_POOL_IMPL_HPP_INCLUDED
#define TORRENT_MEMORY_POOL_IMPL_HPP_INCLUDED

#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "libtorrent/config.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/info_hash.hpp"
#include "libtorrent/storage_defs.hpp" // for storage_index_t
#include "libtorrent/aux_/memory_slab.hpp"

#if TORRENT_USE_ASSERTS
#include <atomic>
#include <thread>
#endif

namespace libtorrent::aux {

	struct memory_storage;

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
	// * handlers of the caller are never called with the mutex held
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
			// torrent_handle::native_handle() refers to. Only compared, never
			// dereferenced
			void const* torrent = nullptr;
			std::shared_ptr<memory_storage> storage;
		};

		// the storages of every memory_disk_io of the pool. A storage is
		// added by new_torrent() and removed by remove_torrent()
		std::vector<storage_ref> storages;

		std::int64_t hash_missing_blocks = 0;

		// the bytes held by the current entries of every storage. The
		// caller holds mutex
		std::int64_t held_bytes() const;
	};

	// test hook: the place of the current entry of a piece of a storage of
	// the memory_disk_io ``disk`` (piece_place::none without an entry)
	TORRENT_EXTRA_EXPORT piece_place memory_place_for_test(disk_interface& disk
		, storage_index_t storage, piece_index_t piece);
}

#endif
