/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_SLAB_HPP_INCLUDED
#define TORRENT_MEMORY_SLAB_HPP_INCLUDED

#include <map>
#include <set>
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

		// test hook: number of madvise/decommit calls the most recent free()
		// call issued, after cutting the coalesced ranges at slab boundaries
		int last_free_decommit_calls() const { return m_last_free_decommit_calls; }

	private:

		// one anonymous mapping of m_slab_bytes, carved into fixed-size blocks
		struct slab
		{
			int used_blocks = 0;
			// free blocks of this slab, as a LIFO stack
			std::vector<char*> free_blocks;
			// used_bitmap[i] is true while block i (0-based, from this
			// slab's base) is currently handed out by allocate(); free()
			// only accepts a block that is aligned to a block boundary here
			// and whose bit is set, rejecting a double free (two calls, or
			// repeated within one span) or a misaligned pointer instead of
			// silently corrupting this slab's bookkeeping
			std::vector<bool> used_bitmap;
		};

		void release_empty_slabs();

		int const m_slab_bytes;
		int const m_blocks_per_slab;
		int m_blocks_in_use = 0;
		int m_last_free_decommit_calls = 0;
		// slabs kept ordered by base address, so a batch of blocks (already
		// sorted by address for madvise coalescing) can be matched to their
		// owning slabs in one merge pass instead of a linear scan per block
		std::map<char*, slab> m_slabs;
		// bases of the slabs in m_slabs that currently have a free block,
		// so allocate() does not have to scan every mapped slab to find one
		std::set<char*> m_slabs_with_free;
	};
}

#endif
