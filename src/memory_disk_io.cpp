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
#include "libtorrent/aux_/memory_pool_impl.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/aux_/storage_free_list.hpp"
#include "libtorrent/aux_/vector.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace libtorrent {

namespace aux {

	// a disk_interface that keeps pieces in the memory pool or routes them
	// to the default disk backend, which it owns. Every piece goes to the
	// default backend for now: each call is forwarded to it, with the index
	// of the storage the default backend created for the torrent and the
	// original arguments.
	struct memory_disk_io final : disk_interface
	{
		memory_disk_io(io_context& ioc, settings_interface const& sett, counters& cnt
			, std::shared_ptr<memory_storage_pool> pool)
			: m_pool(pool->m_impl)
			, m_inner(default_disk_io_constructor(ioc, sett, cnt))
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
				std::lock_guard<std::mutex> l(m_pool->mutex);
				rec->storage = std::make_shared<memory_storage>(params.files
					, params.v1, params.v2, m_pool->alloc);
				m_pool->storages.push_back(rec->storage);
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
			rec->inner.reset();
			{
				// the storage frees its blocks into the pool's allocator
				std::lock_guard<std::mutex> l(m_pool->mutex);
				auto& storages = m_pool->storages;
				storages.erase(std::remove(storages.begin(), storages.end(), rec->storage)
					, storages.end());
				rec->storage.reset();
			}
			m_free_slots.add(idx);
		}

		void async_read(storage_index_t const storage, peer_request const& r
			, std::function<void(disk_buffer_holder, storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			m_inner->async_read(inner_index(storage), r, std::move(handler), flags);
		}

		bool async_write(storage_index_t const storage, peer_request const& r
			, char const* buf, std::shared_ptr<disk_observer> o
			, std::function<void(storage_error const&)> handler
			, disk_job_flags_t const flags) override
		{
			return m_inner->async_write(inner_index(storage), r, buf
				, std::move(o)
				, std::move(handler), flags);
		}

		void async_hash(storage_index_t const storage, piece_index_t const piece
			, span<sha256_hash> const v2, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha1_hash const&, storage_error const&)> handler) override
		{
			m_inner->async_hash(inner_index(storage), piece, v2, flags, std::move(handler));
		}

		void async_hash2(storage_index_t const storage, piece_index_t const piece
			, int const offset, disk_job_flags_t const flags
			, std::function<void(piece_index_t, sha256_hash const&, storage_error const&)> handler) override
		{
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

		void abort(bool const wait) override
		{
			m_inner->abort(wait);
		}

		void submit_jobs() override
		{
			m_inner->submit_jobs();
		}

		void settings_updated() override
		{
			m_inner->settings_updated();
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

		std::shared_ptr<memory_pool_impl> const m_pool;
		// declared before m_torrents: a storage_holder in m_torrents calls
		// remove_torrent() on it
		std::unique_ptr<disk_interface> const m_inner;
		aux::vector<std::unique_ptr<storage_record>, storage_index_t> m_torrents;
		// indices into m_torrents of empty slots
		storage_free_list m_free_slots;
	};
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
