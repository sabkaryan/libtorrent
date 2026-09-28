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
#include "libtorrent/aux_/torrent.hpp"
#include "libtorrent/aux_/session_impl.hpp"
#include "libtorrent/aux_/session_call.hpp" // for torrent_wait
#include "libtorrent/torrent_handle.hpp"
#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/disk_interface.hpp" // for default_block_size

#include "libtorrent/aux_/disable_warnings_push.hpp"
#include <boost/asio/dispatch.hpp>
#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include <algorithm>
#include <cstring>
#include <exception>

namespace libtorrent {

namespace aux {

namespace {

	// two references to the same torrent object: compared by owner, never
	// by address
	template <typename Ref>
	bool same_torrent(std::weak_ptr<void> const& a, Ref const& b)
	{
		return !a.owner_before(b) && !b.owner_before(a);
	}

	// drops the claims queued for torrent objects that are gone
	void drop_expired(std::vector<memory_pool_impl::pending_claims>& pending)
	{
		pending.erase(std::remove_if(pending.begin(), pending.end()
			, [](memory_pool_impl::pending_claims const& p) { return p.torrent.expired(); }), pending.end());
	}
}

	memory_pool_impl::memory_pool_impl(int const slab_bytes)
		: alloc(slab_bytes)
	{}

	std::int64_t memory_pool_impl::held_bytes() const
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		std::int64_t ret = 0;
		for (auto const& r : storages)
			ret += r.storage->held_complete() + r.storage->held_partial();
		return ret;
	}

	std::int64_t memory_pool_impl::retired_bytes() const
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		std::int64_t ret = 0;
		for (auto const& r : storages) ret += r.storage->retired_bytes();
		for (auto const& r : retired) ret += r.storage->retired_bytes();
		return ret;
	}

	std::shared_ptr<memory_storage> memory_pool_impl::storage_of(std::shared_ptr<void> const& torrent) const
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		if (!torrent) return {};
		auto const it = std::find_if(storages.begin(), storages.end()
			, [&torrent](storage_ref const& r) { return same_torrent(r.torrent, torrent); });
		return it == storages.end() ? nullptr : it->storage;
	}

	memory_policy memory_pool_impl::policy_for(info_hash_t const& ih, sha1_hash const& best) const
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		auto const matches = [&ih, &best](info_hash_t const& reg)
		{
			if (reg.has_v1() && ih.has_v1() && reg.v1 == ih.v1) return true;
			if (reg.has_v2() && ih.has_v2() && reg.v2 == ih.v2) return true;
			if (reg.has_v1() && reg.v1 == best) return true;
			return reg.has_v2() && reg.get(protocol_version::V2) == best;
		};
		for (auto const& r : registrations)
			if (matches(r.first)) return r.second;
		return default_policy;
	}

	void memory_pool_impl::add_pending(std::weak_ptr<void> torrent
		, std::function<void(memory_storage&)> op)
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		drop_expired(pending);
		auto const it = std::find_if(pending.begin(), pending.end()
			, [&torrent](pending_claims const& p) { return same_torrent(p.torrent, torrent); });
		if (it != pending.end())
		{
			it->ops.push_back(std::move(op));
			return;
		}
		pending.push_back({std::move(torrent), {}});
		pending.back().ops.push_back(std::move(op));
	}

	void memory_pool_impl::apply_pending(std::shared_ptr<void> const& torrent, memory_storage& s)
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		drop_expired(pending);
		if (!torrent) return;
		auto const it = std::find_if(pending.begin(), pending.end()
			, [&torrent](pending_claims const& p) { return same_torrent(p.torrent, torrent); });
		if (it == pending.end()) return;
		for (auto const& op : it->ops) op(s);
		pending.erase(it);
	}

	std::pair<std::shared_ptr<void>, io_context*> memory_pool_impl::release_retired(memory_storage const& s)
	{
		TORRENT_ASSERT(mutex.owned_by_this_thread());
		auto const it = std::find_if(retired.begin(), retired.end()
			, [&s](retired_storage const& r) { return r.storage.get() == &s; });
		if (it == retired.end()) return {};
		std::pair<std::shared_ptr<void>, io_context*> ret(std::move(it->torrent), it->ios);
		retired.erase(it);
		return ret;
	}

	int memory_pool_test_access::blocks_in_use(memory_storage_pool const& pool)
	{
		std::lock_guard<memory_pool_mutex> l(pool.m_impl->mutex);
		return pool.m_impl->alloc.blocks_in_use();
	}

	int memory_pool_test_access::retired_storages(memory_storage_pool const& pool)
	{
		std::lock_guard<memory_pool_mutex> l(pool.m_impl->mutex);
		return int(pool.m_impl->retired.size());
	}

	std::int64_t memory_pool_test_access::storages_retired_pinned(memory_storage_pool const& pool)
	{
		std::lock_guard<memory_pool_mutex> l(pool.m_impl->mutex);
		return pool.m_impl->storages_retired_pinned;
	}

	int memory_pool_test_access::pending_claims(memory_storage_pool const& pool)
	{
		std::lock_guard<memory_pool_mutex> l(pool.m_impl->mutex);
		return int(pool.m_impl->pending.size());
	}
}

namespace {

	// the claim is applied to the torrent's storage now, or queued for it
	// if it has none yet (no metadata). Nothing for an invalid handle
	void claim(aux::memory_pool_impl& impl, torrent_handle const& th
		, std::function<void(aux::memory_storage&)> op)
	{
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		if (!t) return;
		std::lock_guard<aux::memory_pool_mutex> l(impl.mutex);
		if (auto const s = impl.storage_of(t))
		{
			op(*s);
			return;
		}
		impl.add_pending(std::weak_ptr<void>(t), std::move(op));
	}
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
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		m_impl->default_policy = policy;
	}

	void memory_storage_pool::set_policy(info_hash_t const& ih, memory_policy const policy)
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		auto& regs = m_impl->registrations;
		auto const it = std::find_if(regs.begin(), regs.end()
			, [&ih](std::pair<info_hash_t, memory_policy> const& r) { return r.first == ih; });
		if (it != regs.end()) it->second = policy;
		else regs.emplace_back(ih, policy);
	}

	void memory_storage_pool::set_policy(torrent_handle const& th, memory_owner_t const owner
		, memory_policy const policy, span<file_index_t const> const files)
	{
		claim(*m_impl, th, [owner, policy, f = std::vector<file_index_t>(files.begin(), files.end())]
			(aux::memory_storage& s) { s.set_claim_policy(owner, policy, f); });
	}

	void memory_storage_pool::set_limit(std::int64_t const bytes)
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		m_impl->limit = bytes;
	}

	void memory_storage_pool::persist(torrent_handle const& th, span<piece_index_t const> const pieces)
	{
		claim(*m_impl, th, [p = std::vector<piece_index_t>(pieces.begin(), pieces.end())]
			(aux::memory_storage& s) { s.add_one_shot_persist(p); });
	}

	void memory_storage_pool::set_persist(torrent_handle const& th, memory_owner_t const owner
		, span<piece_index_t const> const pieces)
	{
		claim(*m_impl, th, [owner, p = std::vector<piece_index_t>(pieces.begin(), pieces.end())]
			(aux::memory_storage& s) { s.set_claim_persist(owner, p); });
	}

	void memory_storage_pool::drop_owner(torrent_handle const& th, memory_owner_t const owner)
	{
		claim(*m_impl, th, [owner](aux::memory_storage& s) { s.drop_owner(owner); });
	}

	// the moves to the file come with the transfers: nothing is moved yet

	std::int64_t memory_storage_pool::pending_persist_bytes(torrent_handle const&) const
	{
		return not_managed;
	}

	int memory_storage_pool::persist_failures(torrent_handle const&) const
	{
		return not_managed;
	}

	memory_forget_result memory_storage_pool::forget_piece(torrent_handle const& th
		, piece_index_t const piece)
	{
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		if (!t) return {not_managed, piece_place::none};
		{
			std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
			if (!m_impl->storage_of(t)) return {not_managed, piece_place::none};
		}

		// one task on the network thread: nothing of the torrent or of the
		// disk_interface runs between torrent::forget_piece() and the
		// retirement of the entry
		auto& ses = static_cast<aux::session_impl&>(t->session());
		std::shared_ptr<aux::memory_pool_impl> const impl = m_impl;
		memory_forget_result ret{not_managed, piece_place::none};
		bool done = false;
		std::exception_ptr ex;
		dispatch(ses.get_context(), [&ret, &done, &ex, &ses, t, impl, piece]
		{
#ifndef BOOST_NO_EXCEPTIONS
			try {
#endif
				bool const managed = [&] {
					std::lock_guard<aux::memory_pool_mutex> l(impl->mutex);
					return bool(impl->storage_of(t));
				}();
				if (managed)
				{
					// not under the pool's mutex: libtorrent is never called
					// with it held
					int const code = t->forget_piece(piece);
					std::lock_guard<aux::memory_pool_mutex> l(impl->mutex);
					ret = {code, piece_place::none};
					// the storage cannot be removed in between: that runs on
					// this thread too
					std::shared_ptr<aux::memory_storage> const s = impl->storage_of(t);
					if (s && piece >= piece_index_t{0} && piece < s->files().end_piece())
					{
						if (auto const e = s->current(piece)) ret.place = e->place;
						if (code == 0)
						{
							s->retire(piece);
							s->set_in_file(piece, false);
						}
					}
				}
#ifndef BOOST_NO_EXCEPTIONS
			} catch (...) {
				ex = std::current_exception();
			}
#endif
			std::unique_lock<std::mutex> l(ses.mut);
			done = true;
			ses.cond.notify_all();
		});
		aux::torrent_wait(done, ses);
		if (ex) std::rethrow_exception(ex);
		return ret;
	}

	int memory_storage_pool::read(torrent_handle const& th, piece_index_t const piece
		, int const offset, span<char> const buf) const
	{
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		std::shared_ptr<aux::memory_storage> const st = m_impl->storage_of(t);
		if (!st) return not_managed;
		aux::memory_storage const& s = *st;
		if (piece < piece_index_t{0} || piece >= s.files().end_piece()) return not_in_memory;
		auto const e = s.current(piece);
		if (!e || e->place != piece_place::memory) return not_in_memory;
		int const piece_size = e->hasher->piece_size;
		if (offset < 0 || offset >= piece_size) return not_in_memory;
		int const len = static_cast<int>(std::min(buf.size(), std::ptrdiff_t(piece_size - offset)));
		int pos = 0;
		while (pos < len)
		{
			int const block = (offset + pos) / default_block_size;
			int const in_block = (offset + pos) % default_block_size;
			int const n = std::min(default_block_size - in_block, len - pos);
			char const* const data = s.block_data(*e, block);
			if (data != nullptr)
				std::memcpy(buf.data() + pos, data + in_block, std::size_t(n));
			else if (s.is_pad_block(piece, block))
				std::memset(buf.data() + pos, 0, std::size_t(n));
			else
				return not_in_memory;
			pos += n;
		}
		return len;
	}

	memory_pieces memory_storage_pool::in_memory(torrent_handle const& th) const
	{
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		std::shared_ptr<aux::memory_storage> const s = m_impl->storage_of(t);
		if (!s) return {};
		memory_pieces ret;
		int const n = s->files().num_pieces();
		ret.complete.resize(n, false);
		ret.partial.resize(n, false);
		for (auto const p : s->files().piece_range())
		{
			auto const e = s->current(p);
			if (!e || e->place != piece_place::memory) continue;
			if (e->missing_blocks == 0) ret.complete.set_bit(p);
			else ret.partial.set_bit(p);
		}
		return ret;
	}

	memory_held memory_storage_pool::held_bytes(torrent_handle const& th) const
	{
		std::shared_ptr<aux::torrent> const t = th.native_handle();
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		std::shared_ptr<aux::memory_storage> const s = m_impl->storage_of(t);
		if (!s) return {not_managed, not_managed};
		return {s->held_complete(), s->held_partial()};
	}

	std::int64_t memory_storage_pool::held_bytes() const
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		return m_impl->held_bytes();
	}

	std::int64_t memory_storage_pool::retired_bytes() const
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		return m_impl->retired_bytes();
	}

	std::int64_t memory_storage_pool::spilled_pieces() const
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		std::int64_t ret = m_impl->spilled_of_removed;
		for (auto const& r : m_impl->storages)
			ret += r.storage->spilled_pieces();
		return ret;
	}

	std::int64_t memory_storage_pool::hash_missing_blocks() const
	{
		std::lock_guard<aux::memory_pool_mutex> l(m_impl->mutex);
		return m_impl->hash_missing_blocks;
	}

	void memory_storage_pool::filter_resume(torrent_handle const&, add_torrent_params&) const
	{}

	void memory_storage_pool::filter_resume(add_torrent_params&) const
	{}

	void memory_storage_pool::forget_record(info_hash_t const&)
	{}
}
