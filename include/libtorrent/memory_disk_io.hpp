/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_MEMORY_DISK_IO_HPP_INCLUDED
#define TORRENT_MEMORY_DISK_IO_HPP_INCLUDED

#include <cstdint>

#include "libtorrent/config.hpp"

namespace libtorrent {

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
}

#endif
