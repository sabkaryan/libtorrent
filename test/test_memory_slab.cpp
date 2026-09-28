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
