/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "libtorrent/aux_/memory_slab.hpp"
#include "libtorrent/disk_interface.hpp" // default_block_size
#include <algorithm>
#include <cstring>
#include <random>
#include <vector>

using lt::aux::memory_slab_allocator;

TORRENT_TEST(slab_allocate_free)
{
	memory_slab_allocator a(4 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 5; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), 5);
	TEST_EQUAL(a.mapped_slabs(), 2);
	for (char* p : b) TEST_CHECK(p != nullptr);
	std::memset(b[0], 'x', lt::default_block_size);
	a.free(b[0]);
	TEST_EQUAL(a.blocks_in_use(), 4);
}

// a freed block's pages are given back: the next allocation of the same
// block reads zeroes (MADV_DONTNEED on a private anonymous mapping)
TORRENT_TEST(slab_freed_block_reads_zero)
{
	memory_slab_allocator a(lt::default_block_size);
	char* p = a.allocate();
	std::memset(p, 'x', lt::default_block_size);
	a.free(p);
	char* q = a.allocate();
	TEST_CHECK(q == p);
	TEST_CHECK(std::all_of(q, q + lt::default_block_size, [](char c) { return c == 0; }));
}

// an empty slab is unmapped, one spare is kept
TORRENT_TEST(slab_unmaps_empty_slabs)
{
	memory_slab_allocator a(2 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 6; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.mapped_slabs(), 3);
	a.free(b);
	TEST_EQUAL(a.blocks_in_use(), 0);
	TEST_EQUAL(a.mapped_slabs(), 1);
}

// many concurrently mapped slabs: free() must find each block's owner, and
// allocate() must find a slab with a free block, without scanning every
// mapped slab
TORRENT_TEST(slab_many_slabs)
{
	int const n = 2048;
	memory_slab_allocator a(lt::default_block_size); // one block per slab
	std::vector<char*> b;
	b.reserve(static_cast<std::size_t>(n));
	for (int i = 0; i < n; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), n);
	TEST_EQUAL(a.mapped_slabs(), n);
	for (char* p : b) TEST_CHECK(p != nullptr);

	std::mt19937 rng(0xf00d);
	std::shuffle(b.begin(), b.end(), rng);

	a.free(b);
	TEST_EQUAL(a.blocks_in_use(), 0);
	TEST_EQUAL(a.mapped_slabs(), 1);

	std::vector<char*> c;
	c.reserve(static_cast<std::size_t>(n));
	for (int i = 0; i < n; ++i) c.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), n);
	TEST_EQUAL(a.mapped_slabs(), n);
	for (char* p : c) TEST_CHECK(p != nullptr);
}

// a pointer that lies outside every currently mapped slab -- a foreign
// pointer, or a block of a slab that was already unmapped -- is skipped by
// the merge pass in free() rather than corrupting whichever slab slab_it
// happens to be pointing at. (A double free or a misaligned pointer INSIDE
// a still-live slab is a different failure mode, caught by that slab's
// used_bitmap instead -- see the tests below.) In a debug build this is
// caught by TORRENT_ASSERT_FAIL() and aborts, so this test only runs in a
// release-style build with asserts off, where the assert compiles away and
// the skip path itself is what needs checking.
#if !TORRENT_USE_ASSERTS
TORRENT_TEST(slab_free_skips_foreign_pointer)
{
	memory_slab_allocator a(2 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 4; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), 4);
	TEST_EQUAL(a.mapped_slabs(), 2);

	// a block-sized buffer that was never handed out by this allocator
	std::vector<char> foreign(static_cast<std::size_t>(lt::default_block_size));

	std::vector<char*> batch = b;
	batch.push_back(foreign.data());

	a.free(batch);
	TEST_EQUAL(a.blocks_in_use(), 0);
	TEST_EQUAL(a.mapped_slabs(), 1);
}

// a block freed twice through two separate free() calls is rejected the
// second time: its used_bitmap bit was already cleared by the first free,
// so the second is a no-op instead of pushing the same address onto
// free_blocks twice. The next two allocate() calls must hand back two
// different addresses, not the same one to two callers.
TORRENT_TEST(slab_free_rejects_double_free_across_calls)
{
	memory_slab_allocator a(2 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 2; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), 2);

	a.free(b[0]);
	TEST_EQUAL(a.blocks_in_use(), 1);

	a.free(b[0]); // double free: rejected, no further bookkeeping change
	TEST_EQUAL(a.blocks_in_use(), 1);
	TEST_EQUAL(a.mapped_slabs(), 1);

	char* p = a.allocate();
	char* q = a.allocate();
	TEST_CHECK(p != q);
}

// the same double free, but both occurrences are in one batch passed to
// free(span): the first occurrence in address order clears the bit, the
// repeated one right after it is rejected within the same call.
TORRENT_TEST(slab_free_rejects_double_free_within_span)
{
	memory_slab_allocator a(2 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 2; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), 2);

	std::vector<char*> batch;
	batch.push_back(b[0]);
	batch.push_back(b[0]); // same pointer twice in one span
	a.free(batch);
	TEST_EQUAL(a.blocks_in_use(), 1);
	TEST_EQUAL(a.mapped_slabs(), 1);

	char* p = a.allocate();
	char* q = a.allocate();
	TEST_CHECK(p != q);
}

// a pointer inside a live slab but not on a block boundary is rejected,
// not treated as if it were the block it happens to fall inside.
TORRENT_TEST(slab_free_rejects_misaligned_pointer)
{
	memory_slab_allocator a(2 * lt::default_block_size);
	std::vector<char*> b;
	for (int i = 0; i < 2; ++i) b.push_back(a.allocate());
	TEST_EQUAL(a.blocks_in_use(), 2);

	a.free(b[0] + 1); // misaligned: inside the slab, not on a block boundary
	TEST_EQUAL(a.blocks_in_use(), 2);
	TEST_EQUAL(a.mapped_slabs(), 1);

	a.free(b[0]);
	TEST_EQUAL(a.blocks_in_use(), 1);

	char* p = a.allocate();
	char* q = a.allocate();
	TEST_CHECK(p != q);
}
#endif

// two different slabs that happen to be address-adjacent must not be
// coalesced into one madvise/decommit call: mmap often packs same-size
// anonymous mappings back to back, and the coalescing run is cut at the
// slab boundary regardless -- a single madvise spanning two adjacent Linux
// mappings is legal, but the Windows equivalent, VirtualFree(MEM_DECOMMIT),
// only accepts a range inside one VirtualAlloc region and fails across two.
TORRENT_TEST(slab_free_cuts_madvise_at_slab_boundary)
{
	memory_slab_allocator a(lt::default_block_size); // one block per slab
	int const attempts = 64;
	std::vector<char*> blocks;
	blocks.reserve(static_cast<std::size_t>(attempts));
	for (int i = 0; i < attempts; ++i) blocks.push_back(a.allocate());

	std::vector<char*> sorted_blocks = blocks;
	std::sort(sorted_blocks.begin(), sorted_blocks.end());

	char* first = nullptr;
	char* second = nullptr;
	for (std::size_t i = 0; i + 1 < sorted_blocks.size(); ++i)
	{
		if (sorted_blocks[i + 1] == sorted_blocks[i] + lt::default_block_size)
		{
			first = sorted_blocks[i];
			second = sorted_blocks[i + 1];
			break;
		}
	}

	if (first == nullptr)
	{
		std::printf("note: none of %d one-block slabs landed address-adjacent;"
			" skipping the slab-boundary madvise-cut check\n", attempts);
	}
	else
	{
		std::vector<char*> pair;
		pair.push_back(first);
		pair.push_back(second);
		a.free(pair);
		TEST_EQUAL(a.last_free_decommit_calls(), 2);
	}

	std::vector<char*> rest;
	for (char* p : blocks)
		if (p != first && p != second) rest.push_back(p);
	a.free(rest);
	TEST_EQUAL(a.blocks_in_use(), 0);
}
