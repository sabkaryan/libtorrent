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
#include "libtorrent/aux_/memory_slab.hpp"

namespace libtorrent::aux {

	struct memory_storage;

	// the state shared by a memory_storage_pool and every memory_disk_io
	// created from it. Every member is guarded by mutex
	struct memory_pool_impl
	{
		explicit memory_pool_impl(int slab_bytes);

		mutable std::mutex mutex;

		// the blocks of every storage of the pool
		memory_slab_allocator alloc;

		memory_policy default_policy = memory_policy::file;
		// set_policy(info_hash_t) registrations, made before a torrent is
		// added
		std::vector<std::pair<info_hash_t, memory_policy>> registrations;
		// the number of bytes the pool may hold; 0 until the client sets it
		std::int64_t limit = 0;

		// the storages of every memory_disk_io of the pool. A storage is
		// added by new_torrent() and removed by remove_torrent()
		std::vector<std::shared_ptr<memory_storage>> storages;

		std::int64_t hash_missing_blocks = 0;
	};
}

#endif
