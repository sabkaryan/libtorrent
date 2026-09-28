/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "libtorrent/config.hpp"
#include "libtorrent/aux_/memory_slab.hpp"
#include "libtorrent/disk_interface.hpp" // for default_block_size
#include "libtorrent/assert.hpp"

#include <algorithm>
#include <cstddef>

#ifdef TORRENT_WINDOWS
#include "libtorrent/aux_/windows.hpp"
#else
#include <sys/mman.h>
#endif

namespace libtorrent::aux {

namespace {

	// the slab is committed memory that can be handed out as blocks
	// straight away, so a freshly mapped slab needs no VirtualAlloc(MEM_COMMIT)
	// of its own on Windows; only blocks decommitted by free() do.
	char* map_slab(int const slab_bytes)
	{
#ifdef TORRENT_WINDOWS
		void* const mem = ::VirtualAlloc(nullptr, static_cast<std::size_t>(slab_bytes)
			, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
		return static_cast<char*>(mem);
#else
		void* const mem = ::mmap(nullptr, static_cast<std::size_t>(slab_bytes)
			, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (mem == MAP_FAILED) return nullptr;
		return static_cast<char*>(mem);
#endif
	}

	// true when the slab's address range was actually given back to the
	// system; only then is it safe to stop tracking it
	bool unmap_slab(char* const base, int const slab_bytes)
	{
#ifdef TORRENT_WINDOWS
		TORRENT_UNUSED(slab_bytes);
		return ::VirtualFree(base, 0, MEM_RELEASE) != 0;
#else
		return ::munmap(base, static_cast<std::size_t>(slab_bytes)) == 0;
#endif
	}

	// give the block's pages back to the system; a private anonymous mapping
	// reads zeroes the next time one of them is touched
	void decommit_range(char* const start, std::size_t const bytes)
	{
#ifdef TORRENT_WINDOWS
		::VirtualFree(start, bytes, MEM_DECOMMIT);
#else
		::madvise(start, bytes, MADV_DONTNEED);
#endif
	}

	// a decommitted block needs to be recommitted before it can be handed
	// out again; a no-op on a block that was never decommitted
	void recommit_block(char* const block)
	{
#ifdef TORRENT_WINDOWS
		::VirtualAlloc(block, static_cast<std::size_t>(default_block_size)
			, MEM_COMMIT, PAGE_READWRITE);
#else
		TORRENT_UNUSED(block);
#endif
	}

	// 0-based index of block within a slab starting at base; only valid
	// once the caller has checked that block is aligned to a block
	// boundary relative to base
	std::size_t block_index(char* const base, char* const block)
	{
		return static_cast<std::size_t>(block - base) / static_cast<std::size_t>(default_block_size);
	}

} // anonymous namespace

	memory_slab_allocator::memory_slab_allocator(int const slab_bytes)
		: m_slab_bytes(slab_bytes)
		, m_blocks_per_slab(slab_bytes / default_block_size)
	{
		TORRENT_ASSERT(slab_bytes >= default_block_size);
		TORRENT_ASSERT(slab_bytes % default_block_size == 0);
	}

	memory_slab_allocator::~memory_slab_allocator()
	{
		for (auto const& entry : m_slabs)
			unmap_slab(entry.first, m_slab_bytes);
	}

	char* memory_slab_allocator::allocate()
	{
		if (!m_slabs_with_free.empty())
		{
			char* const base = *m_slabs_with_free.begin();
			slab& s = m_slabs.find(base)->second;
			char* const block = s.free_blocks.back();
			s.free_blocks.pop_back();
			s.used_bitmap[block_index(base, block)] = true;
			++s.used_blocks;
			++m_blocks_in_use;
			if (s.free_blocks.empty()) m_slabs_with_free.erase(base);
			recommit_block(block);
			return block;
		}

		char* const base = map_slab(m_slab_bytes);
		if (base == nullptr) return nullptr;

		slab s;
		s.free_blocks.reserve(static_cast<std::size_t>(m_blocks_per_slab));
		s.used_bitmap.assign(static_cast<std::size_t>(m_blocks_per_slab), false);
		for (int i = m_blocks_per_slab - 1; i >= 0; --i)
			s.free_blocks.push_back(base + std::ptrdiff_t(i) * default_block_size);

		char* const block = s.free_blocks.back();
		s.free_blocks.pop_back();
		s.used_bitmap[block_index(base, block)] = true;
		s.used_blocks = 1;
		bool const has_spare_blocks = !s.free_blocks.empty();
		m_slabs.emplace(base, std::move(s));
		if (has_spare_blocks) m_slabs_with_free.insert(base);
		++m_blocks_in_use;
		return block;
	}

	void memory_slab_allocator::free(char* const block)
	{
		free(span<char* const>(&block, 1));
	}

	void memory_slab_allocator::free(span<char* const> const blocks)
	{
		m_last_free_decommit_calls = 0;
		if (blocks.empty()) return;

		std::vector<char*> sorted(blocks.begin(), blocks.end());
		std::sort(sorted.begin(), sorted.end());

		// walk the address-sorted batch against the slabs (also ordered by
		// base address) in a single merge pass, instead of looking each
		// block's owner up independently: O(batch + mapped slabs) rather
		// than O(batch x mapped slabs). A block is accepted only if it
		// lies inside a currently mapped slab, is aligned to a block
		// boundary there, and its bit is currently set in that slab's
		// used_bitmap. A block of a slab that was already unmapped, or a
		// foreign pointer, fails the first test; a pointer into a live
		// slab that is not on a block boundary fails the second; a double
		// free (two separate calls, or the same pointer repeated within
		// one span) fails the third, because the first occurrence already
		// cleared its bit. All three are caller bugs: asserted in a debug
		// build and, in a release build (where the assert compiles away),
		// simply skipped -- no bookkeeping, no madvise/decommit -- rather
		// than attributed to whichever slab slab_it happens to be
		// pointing at.
		//
		// Accepted, address-adjacent blocks are coalesced into a single
		// madvise/decommit call, but that run is cut whenever the owning
		// slab changes, even when two slabs happen to be address-adjacent
		// (mmap often packs same-size anonymous mappings back to back): a
		// single madvise spanning two separate mappings is fine on Linux,
		// but the Windows equivalent, VirtualFree(MEM_DECOMMIT), only
		// accepts a range inside one VirtualAlloc region and fails across
		// two.
		auto slab_it = m_slabs.begin();
		char* run_start = nullptr;
		char* run_end = nullptr; // one past the last accepted block of the run
		char* run_owner = nullptr; // base of the slab the run belongs to
		for (char* const block : sorted)
		{
			while (slab_it != m_slabs.end() && slab_it->first + m_slab_bytes <= block)
				++slab_it;

			bool accepted = false;
			std::size_t index = 0;
			if (slab_it != m_slabs.end() && block >= slab_it->first)
			{
				auto const offset = block - slab_it->first;
				if (offset % default_block_size == 0)
				{
					index = static_cast<std::size_t>(offset / default_block_size);
					accepted = slab_it->second.used_bitmap[index];
				}
			}

			if (!accepted)
			{
				TORRENT_ASSERT_FAIL();
				continue;
			}

			slab& owner = slab_it->second;
			owner.used_bitmap[index] = false;
			owner.free_blocks.push_back(block);
			--owner.used_blocks;
			--m_blocks_in_use;
			m_slabs_with_free.insert(slab_it->first);

			if (run_start != nullptr && block == run_end && slab_it->first == run_owner)
			{
				run_end = block + default_block_size;
			}
			else
			{
				if (run_start != nullptr)
				{
					decommit_range(run_start, static_cast<std::size_t>(run_end - run_start));
					++m_last_free_decommit_calls;
				}
				run_start = block;
				run_end = block + default_block_size;
				run_owner = slab_it->first;
			}
		}
		if (run_start != nullptr)
		{
			decommit_range(run_start, static_cast<std::size_t>(run_end - run_start));
			++m_last_free_decommit_calls;
		}

		release_empty_slabs();
	}

	void memory_slab_allocator::release_empty_slabs()
	{
		bool have_spare = false;
		for (auto it = m_slabs.begin(); it != m_slabs.end(); )
		{
			slab& s = it->second;
			if (s.used_blocks != 0)
			{
				++it;
				continue;
			}
			if (!have_spare)
			{
				have_spare = true;
				++it;
				continue;
			}
			if (unmap_slab(it->first, m_slab_bytes))
			{
				m_slabs_with_free.erase(it->first);
				it = m_slabs.erase(it);
				continue;
			}
			// could not give the slab back to the system; keep tracking it
			// rather than losing our only handle to it
			++it;
		}
	}
}
