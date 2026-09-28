/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_TEST_MEMORY_GATES_HPP
#define TORRENT_TEST_MEMORY_GATES_HPP

#include "libtorrent/units.hpp"
#include "libtorrent/aux_/memory_hasher.hpp"

// holds the hashing threads of memory_disk_io before they hash a block of
// `piece`, for as long as it lives (or until release()). A hash on the
// network thread (hashing_threads = 0) is never held
struct memory_hasher_gate
{
	explicit memory_hasher_gate(lt::piece_index_t const piece)
		: m_piece(piece)
	{
		lt::aux::memory_hasher_hold_for_test(m_piece, true);
	}

	~memory_hasher_gate() { release(); }

	memory_hasher_gate(memory_hasher_gate const&) = delete;
	memory_hasher_gate& operator=(memory_hasher_gate const&) = delete;

	void release()
	{
		if (m_released) return;
		m_released = true;
		lt::aux::memory_hasher_hold_for_test(m_piece, false);
	}

	// the number of hashing threads held now (by any gate)
	static int waiting() { return lt::aux::memory_hasher_waiting_for_test(); }

private:
	lt::piece_index_t const m_piece;
	bool m_released = false;
};

#endif
