/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_HASHER_HPP_INCLUDED
#define TORRENT_MEMORY_HASHER_HPP_INCLUDED

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "libtorrent/config.hpp"
#include "libtorrent/units.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/sha1_hash.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/disk_interface.hpp" // for disk_job_flags_t
#include "libtorrent/io_context.hpp"

namespace libtorrent::aux {

	struct memory_pool_impl;
	struct memory_storage;
	struct memory_piece_entry;

	// the references a job holds on one entry: the pool (for its mutex),
	// the storage (it must outlive the pins on its entries) and the entry,
	// with one pin on it. See the lock discipline in memory_pool_impl.hpp
	struct TORRENT_EXTRA_EXPORT memory_entry_ref
	{
		// takes a pin on e. The caller holds the pool's mutex
		memory_entry_ref(std::shared_ptr<memory_pool_impl> pool
			, std::shared_ptr<memory_storage> storage
			, std::shared_ptr<memory_piece_entry> e);
		// if release() was not called (a completion that was destroyed
		// without running), takes the pool's mutex and releases. It must
		// not run on a thread that holds the pool's mutex.
		//
		// A torrent object kept by a removed storage whose last pin this was
		// is released on its session's network thread: ~torrent frees its
		// peers into the session's allocators, which only that thread may
		// touch. Off that thread (a hashing thread) the release is posted
		// there; the hashing threads keep that io_context running until
		// they exit, so it runs while the session exists. Only an
		// io_context destroyed without being run destroys a completion (or
		// a posted release) unrun; the torrent is then released in place
		~memory_entry_ref();
		memory_entry_ref(memory_entry_ref const&) = delete;
		memory_entry_ref& operator=(memory_entry_ref const&) = delete;

		// drops the pin, the entry and the storage (either may be the last
		// reference). If the storage was removed and this was its last pin,
		// the pool forgets it; the torrent object it kept alive is released
		// when this reference is destroyed, after the mutex, on the network
		// thread (see the destructor). The caller holds the pool's mutex
		void release();
		bool released() const { return !m_entry; }

		// another reference to the same entry, with its own pin. The caller
		// holds the pool's mutex
		std::shared_ptr<memory_entry_ref> another() const;

		memory_pool_impl& pool() const { return *m_pool; }
		memory_storage& storage() const { return *m_storage; }
		memory_piece_entry& entry() const { return *m_entry; }

	private:
		std::shared_ptr<memory_pool_impl> m_pool;
		std::shared_ptr<memory_storage> m_storage;
		std::shared_ptr<memory_piece_entry> m_entry;
		// the torrent object of a removed storage whose last pin this was,
		// and the io_context of its network thread. Released when this
		// reference is destroyed, which is after the pool's mutex
		std::shared_ptr<void> m_keep_alive;
		// outlives every pin, as memory_pool_impl::retired_storage::ios
		// (which it is taken from): this reference is a pin
		io_context* m_keep_alive_ios = nullptr;
	};

	// hashes the pieces held in memory. A block write that may extend the
	// hashed prefix of an entry kicks a background hash of it (SHA-1 in
	// order, SHA-256 of each v2 block as it arrives); async_hash finishes
	// the tail and answers. Hashing reads the blocks of a pinned entry
	// without the pool's mutex: a block is written once and freed only
	// when the entry's last pin goes. Only one thread at a time feeds an
	// entry's SHA-1 state (memory_piece_hasher::busy); a hash job that
	// finds its entry busy is parked on it and scheduled again when the
	// entry is released.
	struct TORRENT_EXTRA_EXPORT memory_hasher
	{
		using hash_handler = std::function<void(piece_index_t, sha1_hash const&, storage_error const&)>;
		using hash2_handler = std::function<void(piece_index_t, sha256_hash const&, storage_error const&)>;

		// threads == 0: hashes inline in hash() and hash2(), on the calling
		// (network) thread, and answers through post, like every answer
		memory_hasher(io_context& ios, int threads);
		// joins the threads
		~memory_hasher();
		memory_hasher(memory_hasher const&) = delete;
		memory_hasher& operator=(memory_hasher const&) = delete;

		void set_threads(int threads);
		// the number of hashing threads asked for. It takes no lock: it is
		// called with the pool's mutex held
		int threads() const;

		// a background hash of the entry of ref (a block was written). With
		// no threads it does nothing: async_hash hashes everything
		void kick(std::shared_ptr<memory_entry_ref> ref);
		// finishes the hash of the entry and answers through post. v2 must
		// stay valid until the handler is called. A missing non-pad block
		// hashes as zeros and counts in memory_pool_impl::hash_missing_blocks
		// (if the entry is still current when the answer is applied)
		void hash(std::shared_ptr<memory_entry_ref> ref, span<sha256_hash> v2
			, disk_job_flags_t flags, hash_handler handler);
		// the SHA-256 of one v2 block, from the one computed as it arrived
		// or hashed now
		void hash2(std::shared_ptr<memory_entry_ref> ref, int offset, hash2_handler handler);

		// jobs not started yet answer operation_aborted; so does every job
		// scheduled from now on. With wait, joins the threads
		void abort(bool wait);

	private:

		enum class job_kind : std::uint8_t { kick, hash, hash2 };

		struct job
		{
			job_kind kind = job_kind::kick;
			std::shared_ptr<memory_entry_ref> ref;
			// hash
			span<sha256_hash> v2;
			disk_job_flags_t flags{};
			hash_handler handler;
			// hash2
			int offset = 0;
			hash2_handler handler2;
		};

		void schedule(job j);
		void run(job& j, bool inline_call);
		void thread_fun(std::shared_ptr<bool> done, executor_work_guard<io_context::executor_type> work);
		void answer_aborted(job& j);
		void join_threads();

		io_context& m_ios;

		// guards the members below
		mutable std::mutex m_mutex;
		std::condition_variable m_cv;
		std::deque<job> m_queue;
		// written under m_mutex; threads() reads it without the mutex
		std::atomic<int> m_max_threads{0};
		int m_running = 0;
		bool m_abort = false;
		struct thread_slot
		{
			std::thread thread;
			// set (under m_mutex) by the thread when it returns
			std::shared_ptr<bool> done;
		};
		std::vector<thread_slot> m_threads;
	};

	// test hooks: compiled in every build, inert unless called. The gate
	// holds a hashing thread before it hashes a block of `piece` (of any
	// storage), and before it answers an async_hash2 of the piece, until it
	// is released. An inline hash (no hashing threads) is never held: it
	// runs on the network thread
	TORRENT_EXTRA_EXPORT void memory_hasher_hold_for_test(piece_index_t piece, bool hold);
	// the number of threads held at the gate now
	TORRENT_EXTRA_EXPORT int memory_hasher_waiting_for_test();
	// blocks hashed by background hashes, and by hash jobs (the tail),
	// since the process started. A job adds its blocks once it has let go
	// of its entry
	TORRENT_EXTRA_EXPORT std::int64_t memory_hasher_background_blocks_for_test();
	TORRENT_EXTRA_EXPORT std::int64_t memory_hasher_tail_blocks_for_test();
	// the thread that computed the answer of the most recent hash job
	TORRENT_EXTRA_EXPORT std::thread::id memory_hasher_answer_thread_for_test();
	// the thread that dropped the most recent torrent object a removed
	// storage kept (a released torrent object is destroyed there, unless
	// someone else still holds it)
	TORRENT_EXTRA_EXPORT std::thread::id memory_retired_torrent_release_thread_for_test();
	// forgets the recorded thread
	TORRENT_EXTRA_EXPORT void memory_retired_torrent_release_reset_for_test();
}

#endif
