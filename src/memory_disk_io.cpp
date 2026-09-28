/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/session.hpp" // for default_disk_io_constructor
#include "libtorrent/peer_request.hpp"
#include "libtorrent/storage_defs.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/assert.hpp"
#include "libtorrent/disk_buffer_holder.hpp"
#include "libtorrent/error.hpp"
#include "libtorrent/settings_pack.hpp"
#include "libtorrent/aux_/session_settings.hpp"
#include "libtorrent/aux_/memory_hasher.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/aux_/storage_free_list.hpp"
#include "libtorrent/aux_/torrent.hpp" // for info_hash()
#include "libtorrent/aux_/vector.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/asio/post.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace libtorrent {

namespace aux {

namespace {

	// the buffers of async_read answers served from memory
	struct read_buffer_allocator final : buffer_allocator_interface
	{
		void free_disk_buffer(char* const b) override { std::free(b); }
		void free_multiple_buffers(span<char*> const bufs) override
		{
			for (char* const b : bufs) std::free(b);
		}
#if TORRENT_DEBUG_BUFFER_POOL
		void rename_buffer(char*, char const*) override {}
#endif
	};
}

	// a disk_interface that keeps pieces in the memory pool or routes them
	// to the default disk backend, which it owns. A piece's place, decided
	// when the piece starts (its first write), routes its writes, reads and
	// hashes: "memory" is served here, "file" by the default backend, with
	// the index of the storage the default backend created for the torrent
	// and the original arguments.
	//
	// Fences. The eight fence calls of disk_interface are fences here too,
	// as in pread_disk_io, not only in the default backend. A fence is
	// raised on its storage: its part in memory is done at once (a cleared
	// piece, deleted files, lose their entries) and it is handed to the
	// default backend if that has something to do. While it is up, the
	// storage's reads, writes, hashes and fences are parked in posting
	// order. It is lowered when the default backend answered it and our own
	// jobs of the storage issued before it are done (of the piece, for
	// async_clear_piece): async_hash and async_hash2 of pieces in memory,
	// until their handlers ran. The background hash of written blocks is
	// not one: it only reads memory, and generations guard its result. Then
	// its handler is posted and the parked jobs run in posting order, on the
	// network thread, in one pass that no new call can enter; a parked fence
	// is raised again and ends the pass.
	struct memory_disk_io final : disk_interface
	{
		memory_disk_io(io_context& ioc, settings_interface const& sett, counters& cnt
			, std::shared_ptr<memory_storage_pool> pool)
			: m_ios(ioc)
			, m_settings(sett)
			, m_pool(pool->m_impl)
			, m_inner(default_disk_io_constructor(ioc, sett, cnt))
			// hashing_threads threads of its own, in addition to the ones
			// the default backend starts for the same setting
			, m_hasher(ioc, sett.get_int(settings_pack::hashing_threads))
		{}

		~memory_disk_io() override
		{
			// the session removes every torrent first. Should one be left,
			// release it here, while the default backend is still alive
			for (auto const idx : m_torrents.range())
				if (m_torrents[idx]) remove_torrent(idx);
		}

		memory_disk_io(memory_disk_io const&) = delete;
		memory_disk_io& operator=(memory_disk_io const&) = delete;

		// the storage takes the policy of the registration matching the
		// torrent, and the claims made by torrent_handle while the torrent
		// had no storage (no metadata)
		storage_holder new_torrent(storage_params const& params
			, std::shared_ptr<void> const& torrent) override
		{
			auto rec = std::make_shared<storage_record>();
			rec->inner = m_inner->new_torrent(params, torrent);
			rec->own_jobs_of_piece.resize(params.files.num_pieces(), 0);
			// every info-hash of the torrent: storage_params::info_hash is
			// only the best one (the truncated v2 of a hybrid torrent), while
			// a registration may name its v1. The torrent object is the
			// session's aux::torrent (its only caller, on the network
			// thread); tests that drive a disk_interface pass none
			info_hash_t const ih = torrent
				? static_cast<aux::torrent const*>(torrent.get())->info_hash()
				: info_hash_t{};
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				rec->storage = std::make_shared<memory_storage>(params.files
					, params.v1, params.v2, m_pool->alloc, &m_pool->mutex);
				rec->storage->set_torrent_policy(m_pool->policy_for(ih, params.info_hash));
				m_pool->storages.push_back({torrent, ih, rec->storage});
				m_pool->apply_pending(torrent, *rec->storage);
			}
			storage_index_t const idx = m_free_slots.new_index(m_torrents.end_index());
			if (idx == m_torrents.end_index())
				m_torrents.emplace_back(std::move(rec));
			else
				m_torrents[idx] = std::move(rec);
			return {idx, *this};
		}

		// the entries not pinned are freed now, a pinned one when its last
		// pin goes: the storage is then kept by the pool as retired, with
		// the torrent object that owns its file_storage, until the last pin
		// drops (memory_entry_ref::release()). Jobs parked
		// behind a fence of the storage answer operation_aborted once the
		// fence lowers (the fence itself answers as the default backend
		// did); if it never lowers they are destroyed unanswered
		void remove_torrent(storage_index_t const idx) override
		{
			TORRENT_ASSERT(m_torrents[idx]);
			std::shared_ptr<storage_record> const rec = std::move(m_torrents[idx]);
			rec->removed = true;
			// the default backend is never called with the pool's mutex held
			rec->inner.reset();
			// the torrent object, released after the mutex
			std::shared_ptr<void> torrent;
			{
				// the storage frees its blocks into the pool's allocator: the
				// last reference to it is dropped under the mutex. A hash job
				// still holding an entry of it keeps it alive until the job's
				// completion drops it, also under the mutex
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				auto& storages = m_pool->storages;
				auto const it = std::find_if(storages.begin(), storages.end()
					, [&rec](memory_pool_impl::storage_ref const& r) { return r.storage == rec->storage; });
				if (it != storages.end())
				{
					torrent = it->torrent.lock();
					storages.erase(it);
				}
				memory_storage& s = *rec->storage;
				s.retire_all();
				s.mark_removed();
				m_pool->spilled_of_removed += s.spilled_pieces();
				if (s.pinned())
				{
					m_pool->retired.push_back({rec->storage, torrent});
					++m_pool->storages_retired_pinned;
				}
				rec->storage.reset();
			}
			m_free_slots.add(idx);
		}

		void async_read(storage_index_t const storage, peer_request const& r
			, std::function<void(disk_buffer_holder, storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			std::shared_ptr<storage_record> const& rec = m_torrents[storage];
			if (held_back(rec->fence))
			{
				rec->fence.parked.emplace_back([this, w = std::weak_ptr<storage_record>(rec), r
					, h = std::move(handler), flags]() mutable
					{ do_read(w.lock(), r, std::move(h), flags); });
				return;
			}
			do_read(rec, r, std::move(handler), flags);
		}

		// a write parked behind a fence copies the caller's buffer now: it
		// lives only until this returns. The copy is not counted in the held
		// bytes and applies no back-pressure, like the jobs pread_disk_io
		// parks behind a fence
		bool async_write(storage_index_t const storage, peer_request const& r
			, char const* buf, std::shared_ptr<disk_observer> o
			, std::function<void(storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			std::shared_ptr<storage_record> const& rec = m_torrents[storage];
			if (held_back(rec->fence))
			{
				std::vector<char> copy(buf, buf + std::max(0, r.length));
				rec->fence.parked.emplace_back([this, w = std::weak_ptr<storage_record>(rec), r
					, copy = std::move(copy), o = std::move(o), h = std::move(handler), flags]() mutable
					{ do_write(w.lock(), r, copy.data(), std::move(o), std::move(h), flags); });
				return false;
			}
			return do_write(rec, r, buf, std::move(o), std::move(handler), flags);
		}

		void async_hash(storage_index_t const storage, piece_index_t const piece
			, span<sha256_hash> const v2, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha1_hash const&, storage_error const&)> handler) override
		{
			std::shared_ptr<storage_record> const& rec = m_torrents[storage];
			if (held_back(rec->fence))
			{
				rec->fence.parked.emplace_back([this, w = std::weak_ptr<storage_record>(rec), piece
					, v2, flags, h = std::move(handler)]() mutable
					{ do_hash(w.lock(), piece, v2, flags, std::move(h)); });
				return;
			}
			do_hash(rec, piece, v2, flags, std::move(handler));
		}

		void async_hash2(storage_index_t const storage, piece_index_t const piece
			, int const offset, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha256_hash const&, storage_error const&)> handler) override
		{
			std::shared_ptr<storage_record> const& rec = m_torrents[storage];
			if (held_back(rec->fence))
			{
				rec->fence.parked.emplace_back([this, w = std::weak_ptr<storage_record>(rec), piece
					, offset, flags, h = std::move(handler)]() mutable
					{ do_hash2(w.lock(), piece, offset, flags, std::move(h)); });
				return;
			}
			do_hash2(rec, piece, offset, flags, std::move(handler));
		}

		void async_move_storage(storage_index_t const storage, std::string p
			, move_flags_t const flags
			, std::function<void(status_t, std::string const&, storage_error const&)> handler) override
		{
			fence(storage, all_pieces, [this, p = std::move(p), flags, h = std::move(handler)]
				(std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h] { h(disk_status::fatal_disk_error, std::string(), aborted()); });
					return;
				}
				m_inner->async_move_storage(to_inner(*rec), p, flags
					, [this, rec, h](status_t const st, std::string const& path, storage_error const& e)
					{ inner_answered(rec, [h, st, path, e] { h(st, path, e); }); });
			});
		}

		// the handler may be empty
		void async_release_files(storage_index_t const storage
			, std::function<void()> handler) override
		{
			fence(storage, all_pieces, [this, h = std::move(handler)](std::shared_ptr<storage_record> const& rec)
			{
				auto answer = [h] { if (h) h(); };
				if (rec->removed)
				{
					answer_now(*rec, answer);
					return;
				}
				m_inner->async_release_files(to_inner(*rec), [this, rec, answer] { inner_answered(rec, answer); });
			});
		}

		void async_check_files(storage_index_t const storage
			, add_torrent_params const* resume_data
			, aux::vector<std::string, file_index_t> links
			, std::function<void(status_t, storage_error const&)> handler) override
		{
			fence(storage, all_pieces, [this, resume_data, links = std::move(links), h = std::move(handler)]
				(std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h] { h(disk_status::fatal_disk_error, aborted()); });
					return;
				}
				m_inner->async_check_files(to_inner(*rec), resume_data, links
					, [this, rec, h](status_t const st, storage_error const& e)
					{ inner_answered(rec, [h, st, e] { h(st, e); }); });
			});
		}

		// the bytes in memory stay. The handler may be empty
		void async_stop_torrent(storage_index_t const storage
			, std::function<void()> handler) override
		{
			fence(storage, all_pieces, [this, h = std::move(handler)](std::shared_ptr<storage_record> const& rec)
			{
				auto answer = [h] { if (h) h(); };
				if (rec->removed)
				{
					answer_now(*rec, answer);
					return;
				}
				m_inner->async_stop_torrent(to_inner(*rec), [this, rec, answer] { inner_answered(rec, answer); });
			});
		}

		void async_rename_file(storage_index_t const storage
			, file_index_t const index, std::string name
			, std::function<void(std::string const&, file_index_t, storage_error const&)> handler) override
		{
			fence(storage, all_pieces, [this, index, name = std::move(name), h = std::move(handler)]
				(std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h, name, index] { h(name, index, aborted()); });
					return;
				}
				m_inner->async_rename_file(to_inner(*rec), index, name
					, [this, rec, h](std::string const& n, file_index_t const i, storage_error const& e)
					{ inner_answered(rec, [h, n, i, e] { h(n, i, e); }); });
			});
		}

		// every entry is retired and no piece is in the file any more, then
		// the default backend deletes the files
		void async_delete_files(storage_index_t const storage, remove_flags_t const options
			, std::function<void(storage_error const&)> handler) override
		{
			fence(storage, all_pieces, [this, options, h = std::move(handler)]
				(std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h] { h(aborted()); });
					return;
				}
				{
					std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
					memory_storage& s = *rec->storage;
					s.retire_all();
					for (auto const p : s.files().piece_range()) s.set_in_file(p, false);
				}
				m_inner->async_delete_files(to_inner(*rec), options
					, [this, rec, h](storage_error const& e)
					{ inner_answered(rec, [h, e] { h(e); }); });
			});
		}

		void async_set_file_priority(storage_index_t const storage
			, aux::vector<download_priority_t, file_index_t> prio
			, std::function<void(storage_error const&
				, aux::vector<download_priority_t, file_index_t>)> handler) override
		{
			fence(storage, all_pieces, [this, prio = std::move(prio), h = std::move(handler)]
				(std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h, prio] { h(aborted(), prio); });
					return;
				}
				m_inner->async_set_file_priority(to_inner(*rec), prio
					, [this, rec, h](storage_error const& e, aux::vector<download_priority_t, file_index_t> p)
					{ inner_answered(rec, [h, e, p = std::move(p)] { h(e, p); }); });
			});
		}

		// a fence on the piece: the piece loses its entry (the next write
		// starts it again) and is not in the file any more. It goes to the
		// default backend unless the piece was in memory. It waits for our
		// jobs of this piece only
		void async_clear_piece(storage_index_t const storage, piece_index_t const index
			, std::function<void(piece_index_t)> handler) override
		{
			fence(storage, index, [this, index, h = std::move(handler)](std::shared_ptr<storage_record> const& rec)
			{
				if (rec->removed)
				{
					answer_now(*rec, [h, index] { h(index); });
					return;
				}
				piece_place place = piece_place::none;
				{
					std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
					memory_storage& s = *rec->storage;
					if (auto const e = s.current(index)) place = e->place;
					s.retire(index);
					s.set_in_file(index, false);
				}
				if (place == piece_place::memory)
				{
					answer_now(*rec, [h, index] { h(index); });
					return;
				}
				m_inner->async_clear_piece(to_inner(*rec), index
					, [this, rec, h](piece_index_t const p) { inner_answered(rec, [h, p] { h(p); }); });
			});
		}

		void update_stats_counters(counters& c) const override
		{
			m_inner->update_stats_counters(c);
		}

		std::vector<open_file_state> get_status(storage_index_t const storage) const override
		{
			return m_inner->get_status(inner_index(storage));
		}

		// the hash jobs not started answer operation_aborted first
		void abort(bool const wait) override
		{
			m_hasher.abort(wait);
			m_inner->abort(wait);
		}

		void submit_jobs() override
		{
			m_inner->submit_jobs();
		}

		void settings_updated() override
		{
			m_inner->settings_updated();
			m_hasher.set_threads(m_settings.get_int(settings_pack::hashing_threads));
		}

		// test hooks
		piece_place place(storage_index_t const storage, piece_index_t const piece) const
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			auto const e = m_torrents[storage]->storage->current(piece);
			return e ? e->place : piece_place::none;
		}

		bool in_file(storage_index_t const storage, piece_index_t const piece) const
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			return m_torrents[storage]->storage->in_file(piece);
		}

		void set_in_file(storage_index_t const storage, piece_index_t const piece, bool const value)
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			m_torrents[storage]->storage->set_in_file(piece, value);
		}

		int blocks_in_use() const
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			return m_pool->alloc.blocks_in_use();
		}

	private:

		// the fences of one storage. Used on the network thread only
		struct fence_state
		{
			// a fence is up on this storage. A fence that comes while one is
			// up, or while jobs are parked, is parked itself
			bool raised = false;
			// the jobs of the storage that came while a fence was up, in
			// posting order: writes (their buffers already copied), reads,
			// hashes and fences
			std::deque<std::function<void()>> parked;
			// transfers to the file asked for while a fence was up. They
			// start in the pass that lowers the fence, after the parked jobs.
			// Nothing defers one yet
			std::vector<std::shared_ptr<memory_piece_entry>> deferred_transfers;

			// the fence that is up: the piece of an async_clear_piece
			// (all_pieces for the other fences), whether the default backend
			// has yet to answer it, and the call of the caller's handler,
			// posted when it is lowered
			piece_index_t piece{-1};
			bool inner_pending = false;
			std::function<void()> answer;
			// a pass is resuming the parked jobs
			bool resuming = false;
		};

		// one torrent: the storage the default backend created for it, its
		// state in the pool and its fences. Jobs in flight keep it alive
		// (a completion may run after remove_torrent())
		struct storage_record
		{
			storage_holder inner;
			std::shared_ptr<memory_storage> storage;
			fence_state fence;
			// our own jobs of the storage (async_hash and async_hash2 of
			// pieces in memory), from their issue until their handler ran, in
			// all and per piece
			int own_jobs = 0;
			aux::vector<int, piece_index_t> own_jobs_of_piece;
			// remove_torrent() was called: inner and storage are reset
			bool removed = false;
		};

		// the scope of a fence on the whole storage
		static constexpr piece_index_t all_pieces{-1};

		using fence_start = std::function<void(std::shared_ptr<storage_record> const&)>;

		static storage_error aborted()
		{
			return storage_error(boost::asio::error::operation_aborted);
		}

		// true while the jobs of the storage are held back: a fence is up,
		// or the pass that lowered it has not resumed every parked job yet
		static bool held_back(fence_state const& fs)
		{
			return fs.raised || !fs.parked.empty();
		}

		// a fence on `storage` (on one piece, or all_pieces). `start` runs
		// when it is raised: it does its part in memory and either hands it
		// to the default backend through to_inner() and inner_answered(), or
		// sets the answer with answer_now()
		void fence(storage_index_t const storage, piece_index_t const piece, fence_start start)
		{
			std::shared_ptr<storage_record> const& rec = m_torrents[storage];
			if (held_back(rec->fence))
			{
				rec->fence.parked.emplace_back([this, w = std::weak_ptr<storage_record>(rec), piece
					, start = std::move(start)] { raise_fence(w.lock(), piece, start); });
				return;
			}
			raise_fence(rec, piece, start);
		}

		void raise_fence(std::shared_ptr<storage_record> const& rec, piece_index_t const piece
			, fence_start const& start)
		{
			fence_state& fs = rec->fence;
			TORRENT_ASSERT(!fs.raised);
			fs.raised = true;
			fs.piece = piece;
			fs.inner_pending = false;
			fs.answer = nullptr;
			start(rec);
			lower_if_done(rec);
		}

		// the fence that is up is handed to the default backend: the index
		// of the storage there
		storage_index_t to_inner(storage_record& r)
		{
			TORRENT_ASSERT(r.fence.raised);
			r.fence.inner_pending = true;
			return static_cast<storage_index_t>(r.inner);
		}

		// the fence that is up does not go to the default backend
		static void answer_now(storage_record& r, std::function<void()> answer)
		{
			TORRENT_ASSERT(r.fence.raised);
			r.fence.answer = std::move(answer);
		}

		// the default backend answered the fence that is up
		void inner_answered(std::shared_ptr<storage_record> const& rec, std::function<void()> answer)
		{
			fence_state& fs = rec->fence;
			TORRENT_ASSERT(fs.raised && fs.inner_pending);
			fs.answer = std::move(answer);
			fs.inner_pending = false;
			lower_if_done(rec);
		}

		// lowers the fence that is up once the default backend answered it
		// and our jobs it waits for are done: its handler is posted, then
		// the parked jobs resume
		void lower_if_done(std::shared_ptr<storage_record> const& rec)
		{
			fence_state& fs = rec->fence;
			if (!fs.raised || fs.inner_pending) return;
			int const own = fs.piece == all_pieces ? rec->own_jobs : rec->own_jobs_of_piece[fs.piece];
			if (own > 0) return;
			fs.raised = false;
			TORRENT_ASSERT(fs.answer);
			post(m_ios, std::move(fs.answer));
			fs.answer = nullptr;
			resume(rec);
		}

		// runs the parked jobs in posting order, until a parked fence is
		// raised. Calls of the network thread cannot come in between: the
		// jobs post their handlers. A fence lowered inside the pass (one
		// with nothing to wait for) lets the pass go on
		void resume(std::shared_ptr<storage_record> const rec)
		{
			fence_state& fs = rec->fence;
			if (fs.resuming) return;
			fs.resuming = true;
			bool ran = false;
			while (!fs.raised && !fs.parked.empty())
			{
				std::function<void()> job = std::move(fs.parked.front());
				fs.parked.pop_front();
				job();
				ran = true;
			}
			fs.resuming = false;
			TORRENT_ASSERT(fs.deferred_transfers.empty());
			// the jobs the pass handed to the default backend
			if (ran) m_inner->submit_jobs();
		}

		void own_job_started(storage_record& r, piece_index_t const piece)
		{
			++r.own_jobs;
			++r.own_jobs_of_piece[piece];
		}

		void own_job_done(std::shared_ptr<storage_record> const& rec, piece_index_t const piece)
		{
			TORRENT_ASSERT(rec->own_jobs > 0 && rec->own_jobs_of_piece[piece] > 0);
			--rec->own_jobs;
			--rec->own_jobs_of_piece[piece];
			lower_if_done(rec);
		}

		// a piece in memory is copied into a buffer of our own, on the
		// network thread, under the pool's mutex
		void do_read(std::shared_ptr<storage_record> const& rec, peer_request const& r
			, std::function<void(disk_buffer_holder, storage_error const&)> handler
			, disk_job_flags_t const flags)
		{
			if (rec->removed)
			{
				post(m_ios, [h = std::move(handler)] { h(disk_buffer_holder(), aborted()); });
				return;
			}
			bool in_memory = false;
			storage_error error;
			disk_buffer_holder buffer;
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				memory_storage const& s = *rec->storage;
				std::shared_ptr<memory_piece_entry> const e = s.current(r.piece);
				in_memory = e && e->place == piece_place::memory;
				if (in_memory) buffer = read_block(s, *e, r, error);
			}
			if (!in_memory)
			{
				m_inner->async_read(static_cast<storage_index_t>(rec->inner), r, std::move(handler), flags);
				return;
			}
			post(m_ios, [h = std::move(handler), b = std::move(buffer), error]() mutable
				{ h(std::move(b), error); });
		}

		// a piece starts (its place is decided) at its first write, and
		// again at a write into a complete entry whose hash was returned,
		// in memory or in the file (restarts()): the piece was forgotten
		// and is downloaded again (or add_piece() overwrites it). A piece
		// that starts in memory is not "in file" any more. A block of a
		// piece in the file is handed to the default backend, which counts
		// it; once every non-pad block is written there, the piece is "in
		// file". A block in memory is written once. A write of a block that
		// any other entry held in memory already has keeps the stored
		// bytes and succeeds: the torrent fixes a block's bytes, a wrong
		// block fails the piece's hash, and async_clear_piece() starts the
		// piece over. So blocks that arrive again after a recheck, in any
		// order, fill the entry instead of dropping it
		bool do_write(std::shared_ptr<storage_record> const& rec, peer_request const& r
			, char const* buf, std::shared_ptr<disk_observer> o
			, std::function<void(storage_error const&)> handler
			, disk_job_flags_t const flags)
		{
			TORRENT_ASSERT(r.start % default_block_size == 0);
			TORRENT_ASSERT(r.length > 0 && r.length <= default_block_size);
			if (rec->removed)
			{
				post(m_ios, [h = std::move(handler)] { h(aborted()); });
				return false;
			}
			int const block = r.start / default_block_size;
			std::shared_ptr<memory_entry_ref> kick;
			storage_error error;
			bool in_memory = false;
			// place file: the entry, when this is the first hand-over of the
			// block (its write is counted when the default backend answers)
			std::weak_ptr<memory_piece_entry> counted;
			bool count_write = false;
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				memory_storage& s = *rec->storage;
				std::shared_ptr<memory_piece_entry> e = s.current(r.piece);
				if (!e || restarts(*e))
				{
					// the limit is soft: it is checked only here
					bool const at_limit = m_pool->held_bytes() >= m_pool->limit;
					e = s.start(r.piece, s.decide(r.piece, at_limit));
					if (e->place == piece_place::memory) s.set_in_file(r.piece, false);
				}
				in_memory = e->place == piece_place::memory;
				if (e->place == piece_place::file && s.hand_to_inner(*e, block))
				{
					counted = e;
					count_write = true;
				}
				if (in_memory && s.block_data(*e, block) != nullptr)
				{
					// already held: keep the stored bytes
				}
				else if (in_memory)
				{
					if (!s.write_block(*e, block, {buf, r.length}))
					{
						error = storage_error(boost::system::errc::make_error_code(
							boost::system::errc::not_enough_memory), operation_t::alloc_cache_piece);
					}
					else if (!e->hasher->busy && !e->hasher->kick_queued && m_hasher.threads() > 0)
					{
						// the block may extend the hashed prefix. A thread
						// holding the entry (busy) looks for it before it
						// lets go. A background hash is not a job a fence
						// waits for
						e->hasher->kick_queued = true;
						kick = std::make_shared<memory_entry_ref>(m_pool, rec->storage, e);
					}
				}
			}
			if (!in_memory)
			{
				if (count_write)
				{
					handler = [pool = m_pool, rec, counted, h = std::move(handler)](storage_error const& err)
					{
						if (!err) inner_written(*pool, *rec, counted);
						h(err);
					};
				}
				return m_inner->async_write(static_cast<storage_index_t>(rec->inner), r, buf
					, std::move(o)
					, std::move(handler), flags);
			}
			if (kick) m_hasher.kick(std::move(kick));
			post(m_ios, [h = std::move(handler), error] { h(error); });
			return false;
		}

		// the hash of a piece in memory is our own job until its handler ran
		void do_hash(std::shared_ptr<storage_record> const& rec, piece_index_t const piece
			, span<sha256_hash> const v2, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha1_hash const&, storage_error const&)> handler)
		{
			if (rec->removed)
			{
				post(m_ios, [h = std::move(handler), piece] { h(piece, sha1_hash(), aborted()); });
				return;
			}
			if (auto ref = memory_entry(*rec, piece))
			{
				own_job_started(*rec, piece);
				m_hasher.hash(std::move(ref), v2, flags
					, [this, rec, h = std::move(handler)](piece_index_t const p, sha1_hash const& hash
						, storage_error const& e)
					{
						h(p, hash, e);
						own_job_done(rec, p);
					});
				return;
			}
			// the default backend hashes the piece. Its answer is applied
			// (inner_hashed()) only to the entry the piece had now, or, for a
			// piece without an entry, only if the piece was not retired since
			std::weak_ptr<memory_piece_entry> file_entry;
			bool had_entry = false;
			std::uint32_t retire_count = 0;
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				memory_storage const& s = *rec->storage;
				std::shared_ptr<memory_piece_entry> const e = s.current(piece);
				had_entry = bool(e);
				if (e && e->place == piece_place::file) file_entry = e;
				retire_count = s.retire_count(piece);
			}
			m_inner->async_hash(static_cast<storage_index_t>(rec->inner), piece, v2, flags
				, [pool = m_pool, rec, file_entry, had_entry, retire_count, h = std::move(handler)]
				(piece_index_t const p, sha1_hash const& hash, storage_error const& err)
				{
					if (!err) inner_hashed(*pool, *rec, p, file_entry, had_entry, retire_count);
					h(p, hash, err);
				});
		}

		// a write into this entry starts the piece over: every block of it
		// is present (in memory) or was handed to the default backend (in
		// the file), and its hash was returned
		static bool restarts(memory_piece_entry const& e)
		{
			return e.missing_blocks == 0 && e.hash_returned
				&& (e.place == piece_place::memory || e.place == piece_place::file);
		}

		// the default backend wrote a block handed to it for the entry, on
		// the network thread
		static void inner_written(memory_pool_impl& pool, storage_record const& rec
			, std::weak_ptr<memory_piece_entry> const& counted)
		{
			std::lock_guard<memory_pool_mutex> l(pool.mutex);
			if (rec.removed) return;
			// the last reference to the entry may go here, under the mutex
			std::shared_ptr<memory_piece_entry> const e = counted.lock();
			if (e && rec.storage->is_current(*e)) rec.storage->inner_block_written(*e);
		}

		// the default backend answered a hash of the piece without an
		// error, on the network thread. The answer carries no entry: it
		// applies only if the piece was not retired since it was issued
		// (async_clear_piece, async_delete_files, forget_piece) and it has
		// the entry it had then. A piece without an entry is "in file"; a
		// complete entry in the file has its hash returned
		static void inner_hashed(memory_pool_impl& pool, storage_record const& rec
			, piece_index_t const piece, std::weak_ptr<memory_piece_entry> const& file_entry
			, bool const had_entry, std::uint32_t const retire_count)
		{
			std::lock_guard<memory_pool_mutex> l(pool.mutex);
			if (rec.removed) return;
			memory_storage& s = *rec.storage;
			if (s.retire_count(piece) != retire_count) return;
			std::shared_ptr<memory_piece_entry> const current = s.current(piece);
			if (!had_entry)
			{
				if (!current) s.set_in_file(piece, true);
				return;
			}
			std::shared_ptr<memory_piece_entry> const e = file_entry.lock();
			if (e && e == current && e->missing_blocks == 0) e->hash_returned = true;
		}

		void do_hash2(std::shared_ptr<storage_record> const& rec, piece_index_t const piece
			, int const offset, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha256_hash const&, storage_error const&)> handler)
		{
			if (rec->removed)
			{
				post(m_ios, [h = std::move(handler), piece] { h(piece, sha256_hash(), aborted()); });
				return;
			}
			if (auto ref = memory_entry(*rec, piece))
			{
				own_job_started(*rec, piece);
				m_hasher.hash2(std::move(ref), offset
					, [this, rec, h = std::move(handler)](piece_index_t const p, sha256_hash const& hash
						, storage_error const& e)
					{
						h(p, hash, e);
						own_job_done(rec, p);
					});
				return;
			}
			m_inner->async_hash2(static_cast<storage_index_t>(rec->inner), piece, offset, flags, std::move(handler));
		}

		// the index of the torrent's storage in the default backend
		storage_index_t inner_index(storage_index_t const idx) const
		{
			TORRENT_ASSERT(m_torrents[idx]);
			return static_cast<storage_index_t>(m_torrents[idx]->inner);
		}

		// a pinned reference to the piece's current entry if it is held in
		// memory, otherwise nullptr
		std::shared_ptr<memory_entry_ref> memory_entry(storage_record const& rec
			, piece_index_t const piece)
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			std::shared_ptr<memory_piece_entry> e = rec.storage->current(piece);
			if (!e || e->place != piece_place::memory) return {};
			return std::make_shared<memory_entry_ref>(m_pool, rec.storage, std::move(e));
		}

		// copies the bytes of r from the entry. A missing block that is not
		// entirely in pad files is an error (eof, file_read). The caller
		// holds the pool's mutex
		disk_buffer_holder read_block(memory_storage const& s, memory_piece_entry const& e
			, peer_request const& r, storage_error& error)
		{
			int const piece_size = e.hasher->piece_size;
			if (r.length <= 0 || r.start < 0 || r.length > default_block_size
				|| r.start > piece_size - r.length)
			{
				error = storage_error(errors::invalid_request, operation_t::file_read);
				return {};
			}
			char* const buf = static_cast<char*>(std::malloc(std::size_t(default_block_size)));
			if (buf == nullptr)
			{
				error = storage_error(errors::no_memory, operation_t::alloc_cache_piece);
				return {};
			}
			disk_buffer_holder ret(m_read_buffers, buf);
			int pos = 0;
			while (pos < r.length)
			{
				int const offset = r.start + pos;
				int const block = offset / default_block_size;
				int const in_block = offset % default_block_size;
				int const len = std::min(default_block_size - in_block, r.length - pos);
				char const* const data = s.block_data(e, block);
				if (data != nullptr)
					std::memcpy(buf + pos, data + in_block, std::size_t(len));
				else if (s.is_pad_block(e.piece, block))
					std::memset(buf + pos, 0, std::size_t(len));
				else
				{
					error = storage_error(boost::asio::error::eof, operation_t::file_read);
					return {};
				}
				pos += len;
			}
			return ret;
		}

		io_context& m_ios;
		settings_interface const& m_settings;
		std::shared_ptr<memory_pool_impl> const m_pool;
		// declared before m_torrents: a storage_holder in m_torrents calls
		// remove_torrent() on it
		std::unique_ptr<disk_interface> const m_inner;
		aux::vector<std::shared_ptr<storage_record>, storage_index_t> m_torrents;
		// indices into m_torrents of empty slots
		storage_free_list m_free_slots;
		read_buffer_allocator m_read_buffers;
		memory_hasher m_hasher;
	};

namespace {
	memory_disk_io* memory_disk_io_for_test(disk_interface& disk)
	{
		auto* const m = dynamic_cast<memory_disk_io*>(&disk);
		TORRENT_ASSERT(m != nullptr);
		return m;
	}
}

	piece_place memory_place_for_test(disk_interface& disk, storage_index_t const storage
		, piece_index_t const piece)
	{
		auto* const m = memory_disk_io_for_test(disk);
		return m ? m->place(storage, piece) : piece_place::none;
	}

	bool memory_in_file_for_test(disk_interface& disk, storage_index_t const storage
		, piece_index_t const piece)
	{
		auto* const m = memory_disk_io_for_test(disk);
		return m ? m->in_file(storage, piece) : false;
	}

	void memory_set_in_file_for_test(disk_interface& disk, storage_index_t const storage
		, piece_index_t const piece, bool const value)
	{
		if (auto* const m = memory_disk_io_for_test(disk)) m->set_in_file(storage, piece, value);
	}

	int memory_blocks_in_use_for_test(disk_interface& disk)
	{
		auto* const m = memory_disk_io_for_test(disk);
		return m ? m->blocks_in_use() : 0;
	}
}

	disk_io_constructor_type memory_disk_io_constructor(std::shared_ptr<memory_storage_pool> pool)
	{
		TORRENT_ASSERT(pool);
		return [pool = std::move(pool)](io_context& ioc, settings_interface const& sett
			, counters& cnt) -> std::unique_ptr<disk_interface>
		{
			return std::make_unique<aux::memory_disk_io>(ioc, sett, cnt, pool);
		};
	}
}
