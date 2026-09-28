/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "libtorrent/aux_/memory_storage.hpp"
#include "libtorrent/aux_/memory_slab.hpp"
#include "libtorrent/disk_interface.hpp" // default_block_size
#include "libtorrent/file_storage.hpp"
#include <vector>

using lt::aux::memory_storage;
using lt::aux::memory_slab_allocator;
using lt::piece_place;
using lt::memory_policy;
using lt::piece_index_t;
using lt::file_index_t;

namespace {

int const piece_bytes = 32 * 1024;
int const bs = lt::default_block_size;

// two files of 3 pieces of 32 kiB each
lt::file_storage two_files()
{
	lt::file_storage fs;
	fs.add_file("t/a", 3 * piece_bytes);
	fs.add_file("t/b", 3 * piece_bytes);
	fs.set_piece_length(piece_bytes);
	fs.set_num_pieces(lt::aux::calc_num_pieces(fs));
	return fs;
}

// two files meeting inside piece 2: [64 kiB, 96 kiB) holds the last 16 kiB
// of file 0 and the first 16 kiB of file 1
lt::file_storage straddling_files()
{
	lt::file_storage fs;
	fs.add_file("t/a", 80 * 1024);
	fs.add_file("t/b", 112 * 1024);
	fs.set_piece_length(piece_bytes);
	fs.set_num_pieces(lt::aux::calc_num_pieces(fs));
	return fs;
}

// piece 1 holds the last 16 kiB of file 0 and a 16 kiB pad file
lt::file_storage padded_files()
{
	lt::file_storage fs;
	fs.add_file("t/a", 48 * 1024);
	fs.add_file("t/.pad/16384", 16 * 1024, lt::file_storage::flag_pad_file);
	fs.add_file("t/b", 2 * piece_bytes);
	fs.set_piece_length(piece_bytes);
	fs.set_num_pieces(lt::aux::calc_num_pieces(fs));
	return fs;
}

std::vector<char> block_of(char const c)
{
	return std::vector<char>(std::size_t(bs), c);
}

} // anonymous namespace

TORRENT_TEST(storage_start_new_generation)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(bs);
	memory_storage st(fs, true, false, alloc);
	piece_index_t const p{1};

	TEST_CHECK(st.current(p) == nullptr);
	auto const first = st.start(p, piece_place::memory);
	TEST_EQUAL(first->generation, 1u);
	TEST_CHECK(first->place == piece_place::memory);
	TEST_CHECK(st.write_block(*first, 0, block_of('a')));
	first->pins = 1;

	auto const second = st.start(p, piece_place::memory);
	TEST_EQUAL(second->generation, 2u);
	TEST_EQUAL(first->generation, 1u);
	TEST_CHECK(first.get() != second.get());
	TEST_CHECK(st.current(p).get() == second.get());
	TEST_CHECK(st.is_current(*second));
	TEST_CHECK(!st.is_current(*first));
	TEST_EQUAL(st.retired_bytes(), bs);
	TEST_EQUAL(st.held_partial(), 0);
	TEST_EQUAL(alloc.blocks_in_use(), 1);

	st.unpin(first);
	TEST_EQUAL(first->pins, 0);
	TEST_EQUAL(st.retired_bytes(), 0);
	TEST_EQUAL(first->num_blocks, 0);
	TEST_EQUAL(alloc.blocks_in_use(), 0);
}

TORRENT_TEST(storage_retired_entry_freed_only_when_unpinned)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(4 * bs);
	memory_storage st(fs, true, false, alloc);
	piece_index_t const p{2};

	auto const old = st.start(p, piece_place::memory);
	TEST_CHECK(st.write_block(*old, 0, block_of('o')));
	TEST_CHECK(st.write_block(*old, 1, block_of('o')));
	TEST_EQUAL(st.held_complete(), 2 * bs);
	old->pins = 1;

	auto const fresh = st.start(p, piece_place::memory);
	TEST_EQUAL(st.retired_bytes(), 2 * bs);
	TEST_EQUAL(st.held_complete(), 0);
	TEST_CHECK(st.write_block(*fresh, 0, block_of('n')));
	TEST_EQUAL(st.held_partial(), bs);
	TEST_EQUAL(alloc.blocks_in_use(), 3);

	// a retired entry that is still pinned keeps its blocks
	TEST_CHECK(old->num_blocks == 2);
	TEST_CHECK(st.block_data(*old, 1) != nullptr);
	TEST_EQUAL(st.block_data(*old, 1)[0], 'o');

	st.unpin(old);
	TEST_EQUAL(st.retired_bytes(), 0);
	TEST_EQUAL(old->num_blocks, 0);
	TEST_EQUAL(alloc.blocks_in_use(), 1);
	// the new generation of the same piece is untouched
	TEST_EQUAL(st.held_partial(), bs);
	TEST_EQUAL(st.held_complete(), 0);
	TEST_EQUAL(fresh->num_blocks, 1);
	TEST_CHECK(st.block_data(*fresh, 0) != nullptr);
	if (st.block_data(*fresh, 0) != nullptr)
		TEST_EQUAL(st.block_data(*fresh, 0)[bs - 1], 'n');
	TEST_CHECK(st.block_data(*fresh, 1) == nullptr);

	// an unpinned entry is freed as soon as it is retired
	st.retire(p);
	TEST_CHECK(st.current(p) == nullptr);
	TEST_EQUAL(st.retired_bytes(), 0);
	TEST_EQUAL(st.held_partial(), 0);
	TEST_EQUAL(alloc.blocks_in_use(), 0);
}

// a retired entry held by two jobs is freed exactly when the last pin goes
TORRENT_TEST(storage_retired_entry_freed_at_last_pin)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(4 * bs);
	memory_storage st(fs, true, false, alloc);
	piece_index_t const p{3};

	auto const old = st.start(p, piece_place::memory);
	TEST_CHECK(st.write_block(*old, 0, block_of('o')));
	old->pins = 2;
	st.retire(p);
	TEST_EQUAL(st.retired_bytes(), bs);
	TEST_EQUAL(st.held_partial(), 0);

	st.unpin(old);
	TEST_EQUAL(old->pins, 1);
	TEST_EQUAL(st.retired_bytes(), bs);
	TEST_EQUAL(old->num_blocks, 1);
	TEST_CHECK(st.block_data(*old, 0) != nullptr);
	TEST_EQUAL(alloc.blocks_in_use(), 1);

	st.unpin(old);
	TEST_EQUAL(old->pins, 0);
	TEST_EQUAL(st.retired_bytes(), 0);
	TEST_EQUAL(old->num_blocks, 0);
	TEST_CHECK(st.block_data(*old, 0) == nullptr);
	TEST_EQUAL(alloc.blocks_in_use(), 0);
}

TORRENT_TEST(storage_decide_rules)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(bs);
	memory_storage st(fs, true, false, alloc);
	file_index_t const f0{0};
	file_index_t const f1{1};
	piece_index_t const in_f0{1};
	piece_index_t const in_f1{4};

	// 5: the torrent policy, file by default
	TEST_CHECK(st.decide(in_f0, false) == piece_place::file);
	st.set_torrent_policy(memory_policy::memory);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::memory);
	st.set_torrent_policy(memory_policy::file);

	// 4: a memory claim covers the piece
	st.set_claim_policy(1, memory_policy::memory, {});
	TEST_CHECK(st.decide(in_f0, false) == piece_place::memory);
	TEST_CHECK(st.decide(in_f1, false) == piece_place::memory);

	// 3: at the limit
	TEST_EQUAL(st.spilled_pieces(), 0);
	TEST_CHECK(st.decide(in_f0, true) == piece_place::file);
	TEST_EQUAL(st.spilled_pieces(), 1);

	// 2: a file claim of another owner wins over the memory claim
	file_index_t const only_f1[] = {f1};
	st.set_claim_policy(2, memory_policy::file, only_f1);
	TEST_CHECK(st.decide(in_f1, false) == piece_place::file);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::memory);
	TEST_EQUAL(st.spilled_pieces(), 1);

	// an owner only changes its own claim
	file_index_t const only_f0[] = {f0};
	st.set_claim_policy(1, memory_policy::memory, only_f0);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::memory);
	TEST_CHECK(st.decide(in_f1, false) == piece_place::file);
	st.drop_owner(2);
	TEST_CHECK(st.decide(in_f1, false) == piece_place::file); // torrent policy
	st.set_torrent_policy(memory_policy::memory);
	TEST_CHECK(st.decide(in_f1, false) == piece_place::memory);

	// 1: a persist claim of any owner
	piece_index_t const persist[] = {in_f0};
	st.set_claim_persist(3, persist);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::file);
	st.drop_owner(3);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::memory);
	st.drop_owner(1);
	st.set_torrent_policy(memory_policy::file);
	TEST_CHECK(st.decide(in_f0, false) == piece_place::file);

	// a file claim covers the piece where two files meet, even though a
	// memory claim covers the other file of that piece
	lt::file_storage const fs2 = straddling_files();
	memory_storage st2(fs2, true, false, alloc);
	st2.set_claim_policy(1, memory_policy::memory, only_f1);
	st2.set_claim_policy(2, memory_policy::file, only_f0);
	TEST_CHECK(st2.decide(piece_index_t{2}, false) == piece_place::file);
	TEST_CHECK(st2.decide(piece_index_t{3}, false) == piece_place::memory);
	TEST_CHECK(st2.decide(piece_index_t{1}, false) == piece_place::file);
	st2.drop_owner(2);
	TEST_CHECK(st2.decide(piece_index_t{2}, false) == piece_place::memory);
	TEST_CHECK(st2.decide(piece_index_t{1}, false) == piece_place::file);
}

TORRENT_TEST(storage_one_shot_persist_leaves_on_file_place)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(bs);
	memory_storage st(fs, true, false, alloc);
	st.set_torrent_policy(memory_policy::memory);
	piece_index_t const p{3};
	piece_index_t const q{4};

	piece_index_t const once[] = {p};
	st.add_one_shot_persist(once);
	TEST_CHECK(st.decide(p, false) == piece_place::file);
	// it left the one-shot set
	TEST_CHECK(st.decide(p, false) == piece_place::memory);

	piece_index_t const always[] = {q};
	st.set_claim_persist(7, always);
	TEST_CHECK(st.decide(q, false) == piece_place::file);
	// the set of an owner stays
	TEST_CHECK(st.decide(q, false) == piece_place::file);

	// a piece in both sets leaves the one-shot set only
	piece_index_t const both[] = {q};
	st.add_one_shot_persist(both);
	TEST_CHECK(st.decide(q, false) == piece_place::file);
	st.drop_owner(7);
	TEST_CHECK(st.decide(q, false) == piece_place::memory);
}

// ABA: a job holding an old entry must not see it as current after the
// piece was retired and started again, even if a generation number repeated
TORRENT_TEST(storage_is_current_by_identity)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(bs);
	memory_storage st(fs, true, false, alloc);
	piece_index_t const p{0};

	auto const held = st.start(p, piece_place::memory);
	held->pins = 1;
	TEST_CHECK(st.is_current(*held));
	st.retire(p);
	TEST_CHECK(!st.is_current(*held));
	auto const again = st.start(p, piece_place::memory);
	TEST_CHECK(st.is_current(*again));
	TEST_CHECK(!st.is_current(*held));
	st.unpin(held);
	TEST_CHECK(st.is_current(*again));
}

// the "in file" flag belongs to the piece, not to an entry
TORRENT_TEST(storage_in_file_is_per_piece)
{
	lt::file_storage const fs = two_files();
	memory_slab_allocator alloc(bs);
	memory_storage st(fs, true, false, alloc);
	piece_index_t const p{5};

	TEST_CHECK(!st.in_file(p));
	st.set_in_file(p, true);
	TEST_CHECK(st.in_file(p));
	TEST_CHECK(!st.in_file(piece_index_t{4}));
	st.start(p, piece_place::memory);
	TEST_CHECK(st.in_file(p));
	st.retire(p);
	TEST_CHECK(st.in_file(p));
	st.set_in_file(p, false);
	TEST_CHECK(!st.in_file(p));
}

// held bytes: complete = every non-pad block present
TORRENT_TEST(storage_held_complete_skips_pad_blocks)
{
	lt::file_storage const fs = padded_files();
	memory_slab_allocator alloc(4 * bs);
	memory_storage st(fs, true, false, alloc);

	// piece 1: block 0 is data, block 1 is pad
	auto const e = st.start(piece_index_t{1}, piece_place::memory);
	TEST_CHECK(st.write_block(*e, 0, block_of('d')));
	TEST_EQUAL(st.held_complete(), bs);
	TEST_EQUAL(st.held_partial(), 0);

	// piece 2: two data blocks
	auto const f = st.start(piece_index_t{2}, piece_place::memory);
	TEST_CHECK(st.write_block(*f, 1, block_of('x')));
	TEST_EQUAL(st.held_partial(), bs);
	// writing the same block again does not count it twice
	TEST_CHECK(st.write_block(*f, 1, block_of('y')));
	TEST_EQUAL(st.held_partial(), bs);
	TEST_EQUAL(f->num_blocks, 1);
	TEST_EQUAL(st.block_data(*f, 1)[0], 'y');
	TEST_CHECK(st.write_block(*f, 0, block_of('x')));
	TEST_EQUAL(st.held_partial(), 0);
	TEST_EQUAL(st.held_complete(), 3 * bs);

	st.retire_all();
	TEST_EQUAL(st.held_complete(), 0);
	TEST_EQUAL(st.held_partial(), 0);
	TEST_EQUAL(st.retired_bytes(), 0);
	TEST_EQUAL(alloc.blocks_in_use(), 0);
	TEST_CHECK(st.current(piece_index_t{1}) == nullptr);
	TEST_CHECK(st.current(piece_index_t{2}) == nullptr);
}
