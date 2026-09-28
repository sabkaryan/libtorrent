/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_DISK_IO_HPP_INCLUDED
#define TORRENT_MEMORY_DISK_IO_HPP_INCLUDED

#include <cstdint>
#include <memory>

#include "libtorrent/config.hpp"
#include "libtorrent/fwd.hpp"
#include "libtorrent/units.hpp"
#include "libtorrent/span.hpp"
#include "libtorrent/bitfield.hpp"
#include "libtorrent/session_params.hpp" // for disk_io_constructor_type

namespace libtorrent {

	namespace aux {
		struct memory_disk_io;
		struct memory_pool_impl;
		struct memory_pool_test_access;
	}

	// where the pieces of a torrent (or of the files a claim names) are meant
	// to be stored by the in-memory disk backend
	enum class memory_policy : std::uint8_t
	{
		// pieces are written to files by the regular disk backend
		file,
		// pieces are kept in the memory pool
		memory
	};

	// where the current bytes of one piece live in the in-memory disk
	// backend. It decides where writes, reads and hashing of the piece go.
	enum class piece_place : std::uint8_t
	{
		// the piece has not been started (or it was cleared). The next
		// write starts it
		none,
		// the piece started in the memory pool
		memory,
		// the piece is held in memory and is being moved to the file
		transfer,
		// the piece is stored in the file by the regular disk backend
		file
	};

	// identifies the owner of a claim. The value is picked by the client;
	// each owner only changes its own claims.
	using memory_owner_t = std::uint32_t;

	// the result of memory_storage_pool::forget_piece()
	struct memory_forget_result
	{
		// the return code of torrent_handle::forget_piece(), or
		// memory_storage_pool::not_managed
		int code;
		// where the bytes of the piece were before it was forgotten
		piece_place place;
	};

	// bytes of piece data held in memory by one torrent
	struct memory_held
	{
		// in pieces whose every block is present
		std::int64_t complete = 0;
		// in pieces with missing blocks
		std::int64_t partial = 0;
	};

	// the pieces of one torrent held in memory
	struct memory_pieces
	{
		// pieces whose every block is present
		typed_bitfield<piece_index_t> complete;
		// pieces with missing blocks
		typed_bitfield<piece_index_t> partial;
	};

	// the shared state of the in-memory disk backend and the calls of the
	// client. The pool is created by the client and handed to
	// memory_disk_io_constructor(); one pool may serve several sessions, and
	// one torrent may live in two of them: each torrent object has its own
	// storage. All calls are safe from any thread.
	//
	// A call naming a torrent_handle finds the storage of the handle's
	// torrent object (torrent_handle::native_handle(), without the network
	// thread). If it has none in the pool (removed, not added through a
	// session of this pool, or an invalid handle) the call answers
	// not_managed (or an empty result). A claim (set_policy() by handle,
	// persist(), set_persist(), drop_owner()) for a torrent that has no
	// storage yet, because it has no metadata, waits for it and applies
	// once the storage is created.
	//
	// Only forget_piece() waits for the network thread; every other call
	// works under the pool's mutex and returns at once.
	struct TORRENT_EXPORT memory_storage_pool
	{
		// the construction parameters of the pool
		struct params
		{
			// the size of one slab of memory the pool maps at a time. A
			// multiple of 16 kiB
			int slab_bytes = 1024 * 1024;
		};

		// returned by read() when the requested bytes are not in memory
		static constexpr int not_in_memory = -1;
		// returned by calls naming a torrent that has no storage in the pool
		static constexpr int not_managed = -2;

		// constructs an empty pool. Its limit is 0 (every piece goes to the
		// file) until set_limit() is called
		memory_storage_pool();
		explicit memory_storage_pool(params const& p);

		// hidden
		~memory_storage_pool();

		// the policy of torrents that are not registered with set_policy().
		// Initially memory_policy::file
		void set_default_policy(memory_policy);

		// registers the policy of a torrent before it is added. It matches by
		// the v1 info-hash or by the truncated v2 info-hash
		void set_policy(info_hash_t const&, memory_policy);

		// the claim of one owner on a torrent, or on the given files of it
		// (all files if empty). It replaces the owner's previous claim. It
		// decides the place of pieces that start from now on, and the pieces
		// held in memory that it sends to the file, complete or partial, are
		// moved there (see pending_persist_bytes())
		void set_policy(torrent_handle const&, memory_owner_t, memory_policy, span<file_index_t const> files = {});

		// the number of bytes the pool may hold, for all sessions. It is 0
		// until this is called: every piece goes to the file. The limit is
		// soft: it is checked when a piece starts, and a piece that starts
		// while the pool holds at least the limit goes to the file (see
		// spilled_pieces()). Lowering it drops nothing already held. The
		// bytes of retired entries still in use by disk jobs
		// (retired_bytes()) are not counted against it
		void set_limit(std::int64_t bytes);

		// a one-shot request to store the given pieces in the file: a piece
		// held in memory is moved there, a piece that has not started starts
		// there. A piece leaves the request once it is in the file
		void persist(torrent_handle const&, span<piece_index_t const>);

		// replaces the set of pieces this owner wants stored in the file. The
		// set stays until the owner changes or drops it: a piece of it that is
		// forgotten and downloaded again goes to the file again. Its pieces
		// held in memory are moved to the file; pieces in the file or being
		// moved are left alone
		void set_persist(torrent_handle const&, memory_owner_t, span<piece_index_t const>);

		// drops every claim of the owner on the torrent
		void drop_owner(torrent_handle const&, memory_owner_t);

		// bytes in memory that wait to be moved to the file (not_managed for
		// a torrent without storage). The calls that move pieces
		// (set_policy() by handle, persist(), set_persist()) count them
		// before they return, and the moves run on the network thread
		// without waiting: 0 right after such a call means nothing is to be
		// moved. Pieces that have not arrived are not counted
		std::int64_t pending_persist_bytes(torrent_handle const&) const;

		// the number of moves to the file that failed (a write error, or the
		// hash of the default backend differs from the pool's): the piece
		// stays in memory. not_managed for a torrent without storage
		int persist_failures(torrent_handle const&) const;

		// forgets the piece atomically: one task on the network thread of
		// the handle's session calls torrent_handle::forget_piece() and, if
		// it returns 0, drops the bytes the pool holds for the piece (freed
		// once no disk job holds them) and the piece is not "in file" any
		// more. Returns that code and the place the piece's bytes had.
		//
		// This call waits for the network thread, like the synchronous calls
		// of torrent_handle. It must not be called from the network thread
		// (an alert handler or an extension running there): like the
		// synchronous calls of torrent_handle, it would run
		// torrent::forget_piece() inline, in the middle of what that thread
		// is doing.
		//
		// With place == piece_place::file the caller may release the bytes
		// in the file, with the limitation of torrent_handle::forget_piece():
		// in a torrent with only v2 hashes added from a magnet link, code 0
		// may come before all the piece's blocks are in the file.
		//
		// A plain torrent_handle::forget_piece() is safe for a piece in the
		// file (the piece starts again when it is downloaded again), but
		// leaves the bytes of a piece in memory held until it is written
		// again
		memory_forget_result forget_piece(torrent_handle const&, piece_index_t);

		// copies bytes of a piece held in memory into the buffer. Returns the
		// number of bytes copied, not_in_memory or not_managed
		int read(torrent_handle const&, piece_index_t, int offset, span<char>) const;

		// the pieces of the torrent held in memory
		memory_pieces in_memory(torrent_handle const&) const;

		// the bytes held in memory by one torrent (both not_managed if it has
		// no storage in the pool), and by the whole pool
		memory_held held_bytes(torrent_handle const&) const;
		std::int64_t held_bytes() const;

		// bytes of forgotten pieces that are still in use by disk jobs, of
		// removed torrents too
		std::int64_t retired_bytes() const;

		// pieces that were bound for memory and went to the file because the
		// pool was at its limit, of removed torrents too
		std::int64_t spilled_pieces() const;

		// blocks that were missing when a piece held in memory was hashed
		std::int64_t hash_missing_blocks() const;

		// removes from resume data the pieces that are not stored in the
		// file. Apply it to the result of save_resume_data() before the
		// resume data is written: after a restart the pool is empty, and
		// resume data saved without it names pieces that were only in
		// memory. A piece stays in have_pieces only if it is "in file" (all
		// its blocks were written to the file by the default backend in this
		// process, or the resume data it was added with named it), and in
		// unfinished_pieces only if its current bytes go to the file.
		//
		// The first form uses the storage of the handle's torrent (by
		// info-hash, as the second form, if it has none). The second one,
		// for a torrent already removed, finds its storages by the
		// info-hashes of the resume data, among the live ones and those the
		// pool keeps of removed torrents: a piece stays only if every one of
		// them has it. A torrent the pool does not know is left unchanged
		void filter_resume(torrent_handle const&, add_torrent_params&) const;
		void filter_resume(add_torrent_params&) const;

		// drops what the pool keeps of removed torrents with these
		// info-hashes (the v1 or the v2 one matches) for filter_resume()
		void forget_record(info_hash_t const&);

	private:
		friend struct aux::memory_disk_io;
		friend struct aux::memory_pool_test_access;
		std::shared_ptr<aux::memory_pool_impl> m_impl;
	};

	// returns a disk_io_constructor_type for session_params that creates
	// the in-memory disk backend on top of the default disk backend, with
	// ``pool`` as its shared state. The backend reads the torrent object the
	// session passes to new_torrent() (the session's own torrent); a direct
	// user of disk_interface passes an empty one
	TORRENT_EXPORT disk_io_constructor_type memory_disk_io_constructor(std::shared_ptr<memory_storage_pool>);
}

#endif
