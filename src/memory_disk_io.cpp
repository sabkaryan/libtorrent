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
#include "libtorrent/aux_/vector.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/asio/post.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
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
	// and the original arguments. Every other call goes to the default
	// backend.
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

		storage_holder new_torrent(storage_params const& params
			, std::shared_ptr<void> const& torrent) override
		{
			auto rec = std::make_unique<storage_record>();
			rec->inner = m_inner->new_torrent(params, torrent);
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				rec->storage = std::make_shared<memory_storage>(params.files
					, params.v1, params.v2, m_pool->alloc, &m_pool->mutex);
				rec->storage->set_torrent_policy(m_pool->default_policy);
				m_pool->storages.push_back({torrent.get(), rec->storage});
			}
			storage_index_t const idx = m_free_slots.new_index(m_torrents.end_index());
			if (idx == m_torrents.end_index())
				m_torrents.emplace_back(std::move(rec));
			else
				m_torrents[idx] = std::move(rec);
			return {idx, *this};
		}

		void remove_torrent(storage_index_t const idx) override
		{
			TORRENT_ASSERT(m_torrents[idx]);
			std::unique_ptr<storage_record> rec = std::move(m_torrents[idx]);
			// the default backend is never called with the pool's mutex held
			rec->inner.reset();
			{
				// the storage frees its blocks into the pool's allocator: the
				// last reference to it is dropped under the mutex. A hash job
				// still holding an entry of it keeps it alive until the job's
				// completion drops it, also under the mutex
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				auto& storages = m_pool->storages;
				storages.erase(std::remove_if(storages.begin(), storages.end()
					, [&rec](memory_pool_impl::storage_ref const& r) { return r.storage == rec->storage; })
					, storages.end());
				rec->storage.reset();
			}
			m_free_slots.add(idx);
		}

		// a piece in memory is copied into a buffer of our own, on the
		// network thread, under the pool's mutex
		void async_read(storage_index_t const storage, peer_request const& r
			, std::function<void(disk_buffer_holder, storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			bool in_memory = false;
			storage_error error;
			disk_buffer_holder buffer;
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				memory_storage const& s = *m_torrents[storage]->storage;
				std::shared_ptr<memory_piece_entry> const e = s.current(r.piece);
				in_memory = e && e->place == piece_place::memory;
				if (in_memory) buffer = read_block(s, *e, r, error);
			}
			if (!in_memory)
			{
				m_inner->async_read(inner_index(storage), r, std::move(handler), flags);
				return;
			}
			post(m_ios, [h = std::move(handler), b = std::move(buffer), error]() mutable
				{ h(std::move(b), error); });
		}

		// a piece starts (its place is decided) at its first write, and
		// again at a write into a complete entry whose hash was returned:
		// the piece was forgotten and is downloaded again (or add_piece()
		// overwrites it). A block is written once. A write of a block that
		// any other entry held in memory already has keeps the stored
		// bytes and succeeds: the torrent fixes a block's bytes, a wrong
		// block fails the piece's hash, and async_clear_piece() starts the
		// piece over. So blocks that arrive again after a recheck, in any
		// order, fill the entry instead of dropping it
		bool async_write(storage_index_t const storage, peer_request const& r
			, char const* buf, std::shared_ptr<disk_observer> o
			, std::function<void(storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			TORRENT_ASSERT(r.start % default_block_size == 0);
			TORRENT_ASSERT(r.length > 0 && r.length <= default_block_size);
			storage_record const& rec = *m_torrents[storage];
			int const block = r.start / default_block_size;
			std::shared_ptr<memory_entry_ref> kick;
			storage_error error;
			bool in_memory = false;
			{
				std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
				memory_storage& s = *rec.storage;
				std::shared_ptr<memory_piece_entry> e = s.current(r.piece);
				if (!e || (e->place == piece_place::memory && e->missing_blocks == 0 && e->hash_returned))
				{
					bool const at_limit = m_pool->held_bytes() >= m_pool->limit;
					e = s.start(r.piece, s.decide(r.piece, at_limit));
				}
				in_memory = e->place == piece_place::memory;
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
						// lets go
						e->hasher->kick_queued = true;
						kick = std::make_shared<memory_entry_ref>(m_pool, rec.storage, e);
					}
				}
			}
			if (!in_memory)
			{
				return m_inner->async_write(inner_index(storage), r, buf
					, std::move(o)
					, std::move(handler), flags);
			}
			if (kick) m_hasher.kick(std::move(kick));
			post(m_ios, [h = std::move(handler), error] { h(error); });
			return false;
		}

		void async_hash(storage_index_t const storage, piece_index_t const piece
			, span<sha256_hash> const v2, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha1_hash const&, storage_error const&)> handler) override
		{
			if (auto ref = memory_entry(storage, piece))
			{
				m_hasher.hash(std::move(ref), v2, flags, std::move(handler));
				return;
			}
			m_inner->async_hash(inner_index(storage), piece, v2, flags, std::move(handler));
		}

		void async_hash2(storage_index_t const storage, piece_index_t const piece
			, int const offset, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha256_hash const&, storage_error const&)> handler) override
		{
			if (auto ref = memory_entry(storage, piece))
			{
				m_hasher.hash2(std::move(ref), offset, std::move(handler));
				return;
			}
			m_inner->async_hash2(inner_index(storage), piece, offset, flags, std::move(handler));
		}

		void async_move_storage(storage_index_t const storage, std::string p
			, move_flags_t const flags
			, std::function<void(status_t, std::string const&, storage_error const&)> handler) override
		{
			m_inner->async_move_storage(inner_index(storage), std::move(p), flags, std::move(handler));
		}

		void async_release_files(storage_index_t const storage
			, std::function<void()> handler) override
		{
			m_inner->async_release_files(inner_index(storage), std::move(handler));
		}

		void async_check_files(storage_index_t const storage
			, add_torrent_params const* resume_data
			, aux::vector<std::string, file_index_t> links
			, std::function<void(status_t, storage_error const&)> handler) override
		{
			m_inner->async_check_files(inner_index(storage), resume_data, std::move(links)
				, std::move(handler));
		}

		void async_stop_torrent(storage_index_t const storage
			, std::function<void()> handler) override
		{
			m_inner->async_stop_torrent(inner_index(storage), std::move(handler));
		}

		void async_rename_file(storage_index_t const storage
			, file_index_t const index, std::string name
			, std::function<void(std::string const&, file_index_t, storage_error const&)> handler) override
		{
			m_inner->async_rename_file(inner_index(storage), index, std::move(name), std::move(handler));
		}

		void async_delete_files(storage_index_t const storage, remove_flags_t const options
			, std::function<void(storage_error const&)> handler) override
		{
			m_inner->async_delete_files(inner_index(storage), options, std::move(handler));
		}

		void async_set_file_priority(storage_index_t const storage
			, aux::vector<download_priority_t, file_index_t> prio
			, std::function<void(storage_error const&
				, aux::vector<download_priority_t, file_index_t>)> handler) override
		{
			m_inner->async_set_file_priority(inner_index(storage), std::move(prio), std::move(handler));
		}

		void async_clear_piece(storage_index_t const storage, piece_index_t const index
			, std::function<void(piece_index_t)> handler) override
		{
			m_inner->async_clear_piece(inner_index(storage), index, std::move(handler));
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

		// the place of the piece's current entry (test hook)
		piece_place place(storage_index_t const storage, piece_index_t const piece) const
		{
			std::lock_guard<memory_pool_mutex> l(m_pool->mutex);
			auto const e = m_torrents[storage]->storage->current(piece);
			return e ? e->place : piece_place::none;
		}

	private:

		// one torrent: the storage the default backend created for it and
		// its state in the pool
		struct storage_record
		{
			storage_holder inner;
			std::shared_ptr<memory_storage> storage;
		};

		// the index of the torrent's storage in the default backend
		storage_index_t inner_index(storage_index_t const idx) const
		{
			TORRENT_ASSERT(m_torrents[idx]);
			return static_cast<storage_index_t>(m_torrents[idx]->inner);
		}

		// a pinned reference to the piece's current entry if it is held in
		// memory, otherwise nullptr
		std::shared_ptr<memory_entry_ref> memory_entry(storage_index_t const storage
			, piece_index_t const piece)
		{
			storage_record const& rec = *m_torrents[storage];
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
		aux::vector<std::unique_ptr<storage_record>, storage_index_t> m_torrents;
		// indices into m_torrents of empty slots
		storage_free_list m_free_slots;
		read_buffer_allocator m_read_buffers;
		memory_hasher m_hasher;
	};

	piece_place memory_place_for_test(disk_interface& disk, storage_index_t const storage
		, piece_index_t const piece)
	{
		auto* const m = dynamic_cast<memory_disk_io*>(&disk);
		TORRENT_ASSERT(m != nullptr);
		return m ? m->place(storage, piece) : piece_place::none;
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
