/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_SLAB_HPP_INCLUDED
#define TORRENT_MEMORY_SLAB_HPP_INCLUDED

#include <vector>

#include "libtorrent/config.hpp"
#include "libtorrent/span.hpp"

namespace libtorrent::aux {

	// fixed-size 16 kiB blocks carved out of anonymous mappings of slab_bytes.
	// Not thread safe: the owner (the pool) serialises calls under its mutex.
	struct TORRENT_EXTRA_EXPORT memory_slab_allocator
	{
		explicit memory_slab_allocator(int slab_bytes); // multiple of default_block_size, >= default_block_size
		~memory_slab_allocator();
		memory_slab_allocator(memory_slab_allocator const&) = delete;
		memory_slab_allocator& operator=(memory_slab_allocator const&) = delete;

		// nullptr when mmap fails
		char* allocate();
		// returns the block's pages to the system (MADV_DONTNEED); unmaps a slab
		// that becomes empty, except one kept as a spare
		void free(char* block);
		// frees several blocks, coalescing madvise over adjacent ones
		void free(span<char* const> blocks);

		int blocks_in_use() const { return m_blocks_in_use; }
		int mapped_slabs() const { return static_cast<int>(m_slabs.size()); }
		int slab_bytes() const { return m_slab_bytes; }

	private:

		// one anonymous mapping of m_slab_bytes, carved into fixed-size blocks
		struct slab
		{
			char* base = nullptr;
			int used_blocks = 0;
			// free blocks of this slab, as a LIFO stack
			std::vector<char*> free_blocks;
		};

		slab* find_slab(char* block);
		void release_empty_slabs();

		int const m_slab_bytes;
		int const m_blocks_per_slab;
		int m_blocks_in_use = 0;
		std::vector<slab> m_slabs;
	};
}

#endif
