/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"
#include "libtorrent/memory_disk_io.hpp"
#include "libtorrent/aux_/memory_pool_impl.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/add_torrent_params.hpp"

#include <algorithm>

namespace libtorrent {

namespace aux {

	memory_pool_impl::memory_pool_impl(int const slab_bytes)
		: alloc(slab_bytes)
	{}
}

	memory_storage_pool::memory_storage_pool()
		: memory_storage_pool(params{})
	{}

	memory_storage_pool::memory_storage_pool(params const& p)
		: m_impl(std::make_shared<aux::memory_pool_impl>(p.slab_bytes))
	{}

	memory_storage_pool::~memory_storage_pool() = default;

	void memory_storage_pool::set_default_policy(memory_policy const policy)
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		m_impl->default_policy = policy;
	}

	void memory_storage_pool::set_policy(info_hash_t const& ih, memory_policy const policy)
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		auto& regs = m_impl->registrations;
		auto const it = std::find_if(regs.begin(), regs.end()
			, [&ih](std::pair<info_hash_t, memory_policy> const& r) { return r.first == ih; });
		if (it != regs.end()) it->second = policy;
		else regs.emplace_back(ih, policy);
	}

	// the calls below name a torrent by its handle. Resolving a handle to a
	// storage of the pool comes with the memory route; until then no torrent
	// has a storage the pool manages, and they answer not_managed (or an
	// empty result)

	void memory_storage_pool::set_policy(torrent_handle const&, memory_owner_t
		, memory_policy, span<file_index_t const>)
	{}

	void memory_storage_pool::set_limit(std::int64_t const bytes)
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		m_impl->limit = bytes;
	}

	void memory_storage_pool::persist(torrent_handle const&, span<piece_index_t const>)
	{}

	void memory_storage_pool::set_persist(torrent_handle const&, memory_owner_t
		, span<piece_index_t const>)
	{}

	void memory_storage_pool::drop_owner(torrent_handle const&, memory_owner_t)
	{}

	std::int64_t memory_storage_pool::pending_persist_bytes(torrent_handle const&) const
	{
		return not_managed;
	}

	int memory_storage_pool::persist_failures(torrent_handle const&) const
	{
		return not_managed;
	}

	memory_forget_result memory_storage_pool::forget_piece(torrent_handle const&, piece_index_t)
	{
		return {not_managed, piece_place::none};
	}

	int memory_storage_pool::read(torrent_handle const&, piece_index_t, int, span<char>) const
	{
		return not_managed;
	}

	memory_pieces memory_storage_pool::in_memory(torrent_handle const&) const
	{
		return {};
	}

	memory_held memory_storage_pool::held_bytes(torrent_handle const&) const
	{
		return {};
	}

	std::int64_t memory_storage_pool::held_bytes() const
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		std::int64_t ret = 0;
		for (auto const& s : m_impl->storages)
			ret += s->held_complete() + s->held_partial();
		return ret;
	}

	std::int64_t memory_storage_pool::retired_bytes() const
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		std::int64_t ret = 0;
		for (auto const& s : m_impl->storages)
			ret += s->retired_bytes();
		return ret;
	}

	std::int64_t memory_storage_pool::spilled_pieces() const
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		std::int64_t ret = 0;
		for (auto const& s : m_impl->storages)
			ret += s->spilled_pieces();
		return ret;
	}

	std::int64_t memory_storage_pool::hash_missing_blocks() const
	{
		std::lock_guard<std::mutex> l(m_impl->mutex);
		return m_impl->hash_missing_blocks;
	}

	void memory_storage_pool::filter_resume(torrent_handle const&, add_torrent_params&) const
	{}

	void memory_storage_pool::filter_resume(add_torrent_params&) const
	{}

	void memory_storage_pool::forget_record(info_hash_t const&)
	{}
}
