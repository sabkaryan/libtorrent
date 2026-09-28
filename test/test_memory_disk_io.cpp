/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#include "test.hpp"
#include "disk_io_test.hpp" // for memory_file_disk_io()

#include "libtorrent/disk_interface.hpp"
#include "libtorrent/disk_observer.hpp"
#include "libtorrent/session_params.hpp" // for disk_io_constructor_type
#include "libtorrent/settings_pack.hpp" // for default_settings
#include "libtorrent/storage_defs.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/peer_request.hpp"
#include "libtorrent/pread_disk_io.hpp"
#include "libtorrent/sha1_hash.hpp"
#include "libtorrent/aux_/vector.hpp"
#include "libtorrent/aux_/time.hpp"

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace {

struct counting_observer final : lt::disk_observer
{
	void on_disk() override { ++calls; }
	int calls = 0;
};

// the write queue holds one block: the first async_write reaches the limit,
// returns true and keeps the observer; once the block is flushed the backend
// must call on_disk() on it. A backend that loses the observer on the way
// leaves a peer throttled for good
void observer_woken_after_back_pressure(lt::disk_io_constructor_type const& disk_io)
{
	lt::io_context ios;
	lt::counters cnt;
	lt::settings_pack sett = lt::default_settings();
	sett.set_int(lt::settings_pack::max_queued_disk_bytes, lt::default_block_size);
	sett.set_int(lt::settings_pack::aio_threads, 1);
	sett.set_int(lt::settings_pack::hashing_threads, 1);
	std::unique_ptr<lt::disk_interface> disk = disk_io(ios, sett, cnt);

	lt::file_storage fs;
	fs.set_piece_length(lt::default_block_size);
	fs.add_file("back_pressure/file", lt::default_block_size, {});
	fs.set_num_pieces(1);
	lt::aux::vector<lt::download_priority_t, lt::file_index_t> priorities;
	lt::renamed_files rf;
	lt::storage_params const params{fs, rf, "back_pressure_torrent", {}
		, lt::storage_mode_t::storage_mode_sparse, priorities, lt::sha1_hash{}, true, false};
	lt::storage_holder storage = disk->new_torrent(params, std::shared_ptr<void>());

	std::vector<char> const buf(std::size_t(lt::default_block_size), 'x');
	// back_pressure keeps a weak_ptr: the observer lives as long as a peer
	auto const obs = std::make_shared<counting_observer>();
	lt::peer_request req;
	req.piece = lt::piece_index_t{0};
	req.start = 0;
	req.length = lt::default_block_size;
	bool written = false;
	bool const pressure = disk->async_write(storage, req, buf.data(), obs
		, [&written](lt::storage_error const& ec) { TEST_CHECK(!ec); written = true; });
	TEST_CHECK(pressure);

	// the piece is complete: hashing it lets the backend flush it
	bool hashed = false;
	disk->async_hash(storage, lt::piece_index_t{0}, {}, lt::disk_interface::v1_hash
		, [&hashed](lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const& ec)
		{ TEST_CHECK(!ec); hashed = true; });
	disk->submit_jobs();

	auto const start_time = lt::aux::time_now();
	while (!written || !hashed || obs->calls == 0)
	{
		ios.restart();
		if (ios.run_for(std::chrono::milliseconds(5)) == 0)
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		if (lt::aux::time_now() - start_time > lt::seconds(10))
		{
			TEST_ERROR("timeout: on_disk() not called");
			break;
		}
	}
	TEST_CHECK(written);
	TEST_CHECK(hashed);
	TEST_EQUAL(obs->calls, 1);

	disk->abort(true);
}

} // anonymous namespace

// the reference: pread_disk_io itself wakes the observer
TORRENT_TEST(observer_woken_after_back_pressure_pread)
{
	observer_woken_after_back_pressure(lt::pread_disk_io_constructor);
}

// the pass-through hands the original observer to the default backend
TORRENT_TEST(observer_woken_after_back_pressure_memory_file)
{
	observer_woken_after_back_pressure(memory_file_disk_io());
}
