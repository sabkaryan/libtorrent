/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"
#include "libtorrent/aux_/memory_hasher.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/assert.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/asio/post.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <algorithm>
#include <atomic>
#include <utility>

namespace libtorrent::aux {

namespace {

	// hashed in place of a pad block, and of a missing block
	char const g_zeros[default_block_size] = {};

	// the test gate and counters
	std::mutex g_gate_mutex;
	std::condition_variable g_gate_cv;
	std::vector<piece_index_t> g_held;
	int g_waiting = 0;
	std::atomic<bool> g_gate_active{false};
	std::atomic<std::int64_t> g_background_blocks{0};
	std::atomic<std::int64_t> g_tail_blocks{0};
	std::mutex g_answer_mutex;
	std::thread::id g_answer_thread;

	// holds a hashing thread while `piece` is held. Never holds the network
	// thread (an inline hash): nothing could release it
	void gate(piece_index_t const piece, bool const inline_call)
	{
		if (!g_gate_active.load(std::memory_order_acquire)) return;
		if (inline_call) return;
		std::unique_lock<std::mutex> l(g_gate_mutex);
		auto const held = [piece] { return std::find(g_held.begin(), g_held.end(), piece) != g_held.end(); };
		if (!held()) return;
		++g_waiting;
		g_gate_cv.wait(l, [&held] { return !held(); });
		--g_waiting;
	}

	int block_len(int const size, int const block)
	{
		return std::min(default_block_size, size - block * default_block_size);
	}

	int num_blocks(memory_piece_hasher const& h)
	{
		return (h.piece_size + default_block_size - 1) / default_block_size;
	}

	bool is_pad(memory_piece_hasher const& h, int const block)
	{
		return !h.pad_blocks.empty() && h.pad_blocks[static_cast<std::size_t>(block)];
	}

	// the background work on an entry: the blocks that extend the SHA-1
	// prefix and the v2 blocks that arrived without a hash. Taken under the
	// pool's mutex by the thread that holds the entry (busy)
	struct pass_work
	{
		// blocks from the cursor on, in order: the data, or g_zeros for a
		// pad block
		std::vector<char const*> v1;
		// v2 blocks without a hash: index and data
		std::vector<std::pair<int, char const*>> v2;
		bool empty() const { return v1.empty() && v2.empty(); }
	};

	pass_work take_work(memory_storage const& s, memory_piece_entry const& e)
	{
		memory_piece_hasher const& h = *e.hasher;
		pass_work w;
		if (h.v1)
		{
			int const n = num_blocks(h);
			for (int b = h.cursor; b < n; ++b)
			{
				char const* const d = s.block_data(e, b);
				if (d != nullptr) w.v1.push_back(d);
				else if (is_pad(h, b)) w.v1.push_back(g_zeros);
				else break;
			}
		}
		for (int b = 0; b < h.block_hashes.end_index(); ++b)
		{
			char const* const d = s.block_data(e, b);
			if (d != nullptr && h.block_hashes[b].is_all_zeros()) w.v2.emplace_back(b, d);
		}
		return w;
	}

	// hashes the work without the pool's mutex. Feeds h.ph (the caller
	// holds the entry) and returns the v2 block hashes
	std::vector<std::pair<int, sha256_hash>> do_work(memory_piece_hasher& h, pass_work const& w)
	{
		int b = h.cursor;
		for (char const* const d : w.v1)
		{
			h.ph.update({d, block_len(h.piece_size, b)});
			++b;
		}
		std::vector<std::pair<int, sha256_hash>> ret;
		ret.reserve(w.v2.size());
		for (auto const& [block, d] : w.v2)
			ret.emplace_back(block, hasher256(span<char const>(d, block_len(h.piece_size2, block))).final());
		return ret;
	}

	// under the pool's mutex
	void commit(memory_piece_hasher& h, int const v1_blocks
		, std::vector<std::pair<int, sha256_hash>> const& v2)
	{
		h.cursor += v1_blocks;
		for (auto const& [block, hash] : v2)
			if (h.block_hashes[block].is_all_zeros()) h.block_hashes[block] = hash;
	}

	void record_answer_thread()
	{
		std::lock_guard<std::mutex> l(g_answer_mutex);
		g_answer_thread = std::this_thread::get_id();
	}
}

	memory_entry_ref::memory_entry_ref(std::shared_ptr<memory_pool_impl> pool
		, std::shared_ptr<memory_storage> storage
		, std::shared_ptr<memory_piece_entry> e)
		: m_pool(std::move(pool))
		, m_storage(std::move(storage))
		, m_entry(std::move(e))
	{
		TORRENT_ASSERT(m_pool->mutex.owned_by_this_thread());
		++m_entry->pins;
	}

	memory_entry_ref::~memory_entry_ref()
	{
		if (!m_entry) return;
		// a completion destroyed without running (the io_context went away).
		// The pin and the references are dropped under the pool's mutex,
		// which this thread must not hold already
		TORRENT_ASSERT(!m_pool->mutex.owned_by_this_thread());
		std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
		release();
	}

	void memory_entry_ref::release()
	{
		TORRENT_ASSERT(m_pool->mutex.owned_by_this_thread());
		if (!m_entry) return;
		m_storage->unpin(m_entry);
		// either may be the last reference: the storage frees blocks when
		// it goes, under the mutex the caller holds
		m_entry.reset();
		m_storage.reset();
	}

	std::shared_ptr<memory_entry_ref> memory_entry_ref::another() const
	{
		TORRENT_ASSERT(m_entry);
		return std::make_shared<memory_entry_ref>(m_pool, m_storage, m_entry);
	}

	memory_hasher::memory_hasher(io_context& ios, int const threads)
		: m_ios(ios)
	{
		set_threads(threads);
	}

	memory_hasher::~memory_hasher()
	{
		abort(true);
	}

	int memory_hasher::threads() const
	{
		std::lock_guard<std::mutex> l(m_mutex);
		return m_max_threads;
	}

	void memory_hasher::set_threads(int const threads)
	{
		std::deque<job> to_inline;
		std::vector<thread_slot> finished;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			if (m_abort) return;
			m_max_threads = std::max(0, threads);
			// threads that returned since the last change
			for (auto it = m_threads.begin(); it != m_threads.end();)
			{
				if (*it->done)
				{
					finished.push_back(std::move(*it));
					it = m_threads.erase(it);
				}
				else ++it;
			}
			while (m_running < m_max_threads)
			{
				auto done = std::make_shared<bool>(false);
				m_threads.push_back({std::thread([this, done] { thread_fun(done); }), done});
				++m_running;
			}
			// the queue has no thread left to run it
			if (m_max_threads == 0) to_inline.swap(m_queue);
			m_cv.notify_all();
		}
		for (auto& t : finished) t.thread.join();
		for (auto& j : to_inline) schedule(std::move(j));
	}

	void memory_hasher::thread_fun(std::shared_ptr<bool> const done)
	{
		for (;;)
		{
			job j;
			{
				std::unique_lock<std::mutex> l(m_mutex);
				m_cv.wait(l, [this] { return m_abort || !m_queue.empty() || m_running > m_max_threads; });
				if (m_running > m_max_threads || m_queue.empty())
				{
					--m_running;
					*done = true;
					return;
				}
				j = std::move(m_queue.front());
				m_queue.pop_front();
			}
			run(j, false);
		}
	}

	void memory_hasher::kick(std::shared_ptr<memory_entry_ref> ref)
	{
		job j;
		j.kind = job_kind::kick;
		j.ref = std::move(ref);
		schedule(std::move(j));
	}

	void memory_hasher::hash(std::shared_ptr<memory_entry_ref> ref, span<sha256_hash> const v2
		, disk_job_flags_t const flags, hash_handler handler)
	{
		// only the hashes this job computes are written to the span
		for (auto& h : v2) h.clear();
		job j;
		j.kind = job_kind::hash;
		j.ref = std::move(ref);
		j.v2 = v2;
		j.flags = flags;
		j.handler = std::move(handler);
		bool inline_hash = false;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			inline_hash = !m_abort && m_max_threads == 0;
		}
		if (inline_hash) run(j, true);
		else schedule(std::move(j));
	}

	void memory_hasher::hash2(std::shared_ptr<memory_entry_ref> ref, int const offset
		, hash2_handler handler)
	{
		job j;
		j.kind = job_kind::hash2;
		j.ref = std::move(ref);
		j.offset = offset;
		j.handler2 = std::move(handler);
		bool inline_hash = false;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			inline_hash = !m_abort && m_max_threads == 0;
		}
		if (inline_hash) run(j, true);
		else schedule(std::move(j));
	}

	// never called with the pool's mutex held
	void memory_hasher::schedule(job j)
	{
		bool aborted = false;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			if (!m_abort && m_max_threads > 0)
			{
				m_queue.push_back(std::move(j));
				m_cv.notify_one();
				return;
			}
			aborted = m_abort;
		}
		if (aborted || j.kind == job_kind::kick)
		{
			// a background hash needs a thread: async_hash does the work
			answer_aborted(j);
			return;
		}
		// no hashing threads (they were removed while the job waited): hash
		// on the network thread
		post(m_ios, [this, j = std::move(j)]() mutable { run(j, true); });
	}

	void memory_hasher::answer_aborted(job& j)
	{
		switch (j.kind)
		{
			case job_kind::kick:
			{
				std::lock_guard<memory_pool_mutex> l(j.ref->pool().mutex);
				memory_piece_entry& e = j.ref->entry();
				if (e.hasher) e.hasher->kick_queued = false;
				j.ref->release();
				return;
			}
			case job_kind::hash:
			{
				piece_index_t const piece = j.ref->entry().piece;
				post(m_ios, [ref = std::move(j.ref), h = std::move(j.handler), piece]
				{
					{
						std::lock_guard<memory_pool_mutex> l(ref->pool().mutex);
						ref->release();
					}
					h(piece, sha1_hash{}, storage_error(boost::asio::error::operation_aborted));
				});
				return;
			}
			case job_kind::hash2:
			{
				piece_index_t const piece = j.ref->entry().piece;
				post(m_ios, [ref = std::move(j.ref), h = std::move(j.handler2), piece]
				{
					{
						std::lock_guard<memory_pool_mutex> l(ref->pool().mutex);
						ref->release();
					}
					h(piece, sha256_hash{}, storage_error(boost::asio::error::operation_aborted));
				});
				return;
			}
		}
	}

	void memory_hasher::run(job& j, bool const inline_call)
	{
		memory_entry_ref& ref = *j.ref;
		memory_pool_impl& pool = ref.pool();
		memory_storage& s = ref.storage();
		memory_piece_entry& e = ref.entry();
		TORRENT_ASSERT(e.hasher);
		memory_piece_hasher& h = *e.hasher;
		piece_index_t const piece = e.piece;

		std::unique_lock<memory_pool_mutex> l(pool.mutex);

		if (j.kind == job_kind::hash2)
		{
			// a block hash needs no hold on the entry: the block is written
			// once, and only the thread holding the entry writes the hashes
			int const block = j.offset / default_block_size;
			sha256_hash result;
			if (block < h.block_hashes.end_index()) result = h.block_hashes[block];
			char const* data = result.is_all_zeros() ? s.block_data(e, block) : nullptr;
			l.unlock();
			int missing = 0;
			if (result.is_all_zeros())
			{
				gate(piece, inline_call);
				int const size = h.piece_size2 > 0 ? h.piece_size2 : h.piece_size;
				if (data == nullptr)
				{
					data = g_zeros;
					missing = 1;
				}
				result = hasher256(span<char const>(data, block_len(size, block))).final();
				g_tail_blocks.fetch_add(1, std::memory_order_relaxed);
			}
			record_answer_thread();
			post(m_ios, [ref = std::move(j.ref), handler = std::move(j.handler2), piece, result, missing]
			{
				{
					std::lock_guard<memory_pool_mutex> ll(ref->pool().mutex);
					if (ref->storage().is_current(ref->entry()))
						ref->pool().hash_missing_blocks += missing;
					ref->release();
				}
				handler(piece, result, storage_error{});
			});
			return;
		}

		if (j.kind == job_kind::kick) h.kick_queued = false;
		if (h.busy)
		{
			if (j.kind == job_kind::kick)
			{
				// the thread holding the entry looks for new blocks before it
				// lets go of it
				j.ref->release();
				return;
			}
			h.parked.emplace_back([this, pj = std::move(j)]() mutable { schedule(std::move(pj)); });
			return;
		}
		h.busy = true;

		// blocks this job hashed, counted once it let go of the entry
		std::int64_t hashed = 0;
		// hash the blocks that extend the prefix, until there are none
		for (;;)
		{
			pass_work const w = take_work(s, e);
			if (w.empty()) break;
			l.unlock();
			gate(piece, inline_call);
			auto const v2 = do_work(h, w);
			hashed += std::int64_t(w.v1.size() + w.v2.size());
			l.lock();
			commit(h, int(w.v1.size()), v2);
		}

		sha1_hash v1_hash;
		int missing_count = 0;
		if (j.kind == job_kind::hash)
		{
			// the tail: blocks after a gap, pad and missing blocks as zeros.
			// It is hashed into a copy: a missing block may still arrive
			bool const need_v1 = bool(j.flags & disk_interface::v1_hash);
			int const n = num_blocks(h);
			int const n2 = h.block_hashes.end_index();
			int const cursor = h.v1 ? h.cursor : 0;
			std::vector<char const*> tail;
			if (need_v1)
			{
				for (int b = cursor; b < n; ++b)
				{
					char const* const d = s.block_data(e, b);
					tail.push_back(d != nullptr ? d : is_pad(h, b) ? g_zeros : nullptr);
				}
			}
			int const v2_blocks = std::min(n2, int(j.v2.size()));
			std::vector<sha256_hash> v2_result(static_cast<std::size_t>(v2_blocks));
			std::vector<char const*> v2_data(static_cast<std::size_t>(v2_blocks), nullptr);
			for (int b = 0; b < v2_blocks; ++b)
			{
				v2_result[std::size_t(b)] = h.block_hashes[b];
				if (v2_result[std::size_t(b)].is_all_zeros()) v2_data[std::size_t(b)] = s.block_data(e, b);
			}
			l.unlock();

			gate(piece, inline_call);
			std::vector<bool> missing(static_cast<std::size_t>(std::max(n, n2)), false);
			if (need_v1)
			{
				hasher t = h.v1 ? h.ph : hasher();
				int b = cursor;
				for (char const* d : tail)
				{
					if (d == nullptr)
					{
						missing[std::size_t(b)] = true;
						d = g_zeros;
					}
					t.update({d, block_len(h.piece_size, b)});
					++b;
				}
				v1_hash = t.final();
				hashed += std::int64_t(tail.size());
			}
			std::vector<std::pair<int, sha256_hash>> computed;
			for (int b = 0; b < v2_blocks; ++b)
			{
				if (!v2_result[std::size_t(b)].is_all_zeros()) continue;
				char const* d = v2_data[std::size_t(b)];
				if (d == nullptr)
				{
					missing[std::size_t(b)] = true;
					d = g_zeros;
				}
				v2_result[std::size_t(b)] = hasher256(span<char const>(d, block_len(h.piece_size2, b))).final();
				if (d != g_zeros) computed.emplace_back(b, v2_result[std::size_t(b)]);
				++hashed;
			}
			missing_count = int(std::count(missing.begin(), missing.end(), true));
			for (int b = 0; b < v2_blocks; ++b) j.v2[b] = v2_result[std::size_t(b)];
			record_answer_thread();

			l.lock();
			commit(h, 0, computed);
		}

		// let go of the entry. Jobs parked on it are scheduled again, and a
		// block that arrived while it was held (its kick was dropped) gets
		// a background hash
		h.busy = false;
		std::vector<std::function<void()>> parked;
		parked.swap(h.parked);
		std::shared_ptr<memory_entry_ref> next;
		if (!h.kick_queued && !take_work(s, e).empty() && threads() > 0)
		{
			h.kick_queued = true;
			next = ref.another();
		}
		if (j.kind == job_kind::kick) j.ref->release();
		l.unlock();
		(j.kind == job_kind::kick ? g_background_blocks : g_tail_blocks)
			.fetch_add(hashed, std::memory_order_relaxed);

		for (auto& p : parked) p();
		if (next) kick(std::move(next));

		if (j.kind != job_kind::hash) return;
		post(m_ios, [ref = std::move(j.ref), handler = std::move(j.handler), piece, v1_hash, missing_count]
		{
			{
				std::lock_guard<memory_pool_mutex> ll(ref->pool().mutex);
				memory_piece_entry& entry = ref->entry();
				if (ref->storage().is_current(entry))
				{
					// the hash of the piece is returned only if no block was
					// missing: a recheck of a partial piece does not make a
					// later write start the piece over
					if (missing_count == 0) entry.hash_returned = true;
					ref->pool().hash_missing_blocks += missing_count;
				}
				ref->release();
			}
			handler(piece, v1_hash, storage_error{});
		});
	}

	void memory_hasher::join_threads()
	{
		std::vector<thread_slot> threads;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			threads.swap(m_threads);
		}
		for (auto& t : threads) t.thread.join();
	}

	void memory_hasher::abort(bool const wait)
	{
		std::deque<job> queue;
		{
			std::lock_guard<std::mutex> l(m_mutex);
			m_abort = true;
			m_max_threads = 0;
			queue.swap(m_queue);
			m_cv.notify_all();
		}
		for (auto& j : queue) answer_aborted(j);
		if (wait) join_threads();
	}

	void memory_hasher_hold_for_test(piece_index_t const piece, bool const hold)
	{
		std::lock_guard<std::mutex> l(g_gate_mutex);
		auto const it = std::find(g_held.begin(), g_held.end(), piece);
		if (hold && it == g_held.end()) g_held.push_back(piece);
		if (!hold && it != g_held.end()) g_held.erase(it);
		g_gate_active.store(!g_held.empty(), std::memory_order_release);
		g_gate_cv.notify_all();
	}

	int memory_hasher_waiting_for_test()
	{
		std::lock_guard<std::mutex> l(g_gate_mutex);
		return g_waiting;
	}

	std::int64_t memory_hasher_background_blocks_for_test()
	{
		return g_background_blocks.load(std::memory_order_relaxed);
	}

	std::int64_t memory_hasher_tail_blocks_for_test()
	{
		return g_tail_blocks.load(std::memory_order_relaxed);
	}

	std::thread::id memory_hasher_answer_thread_for_test()
	{
		std::lock_guard<std::mutex> l(g_answer_mutex);
		return g_answer_thread;
	}
}
