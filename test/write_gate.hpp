/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_TEST_WRITE_GATE_HPP_INCLUDED
#define TORRENT_TEST_WRITE_GATE_HPP_INCLUDED

// Disk writes a test can slow down or hold back. This interposes pwrite() and
// pwritev() for the whole test binary: include it from exactly one source file
// of the binary and link with -ldl. Linux only.

#if defined TORRENT_LINUX
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/uio.h>

namespace {

// a slow disk: while slow_writes is set, every pwrite()/pwritev() made by the
// disk threads takes at least 20 ms. This widens the time between a piece
// passing its hash check and its blocks reaching the disk, which on a fast
// disk is shorter than one round trip through the network thread.
std::atomic<bool> slow_writes{false};
void maybe_slow_down()
{
	if (slow_writes) std::this_thread::sleep_for(std::chrono::milliseconds(20));
}

// a held disk: while hold_writes is set, every pwrite()/pwritev() waits at
// this gate until it is cleared; writes_waiting counts the calls waiting
std::mutex gate_mutex;
std::condition_variable gate_cv;
bool hold_writes = false;
int writes_waiting = 0;
void maybe_hold()
{
	std::unique_lock<std::mutex> l(gate_mutex);
	if (!hold_writes) return;
	++writes_waiting;
	gate_cv.notify_all();
	gate_cv.wait(l, [] { return !hold_writes; });
	--writes_waiting;
}

void set_hold_writes(bool const hold)
{
	{
		std::lock_guard<std::mutex> l(gate_mutex);
		hold_writes = hold;
	}
	gate_cv.notify_all();
}

}

extern "C" ssize_t pwritev(int fd, struct iovec const* iov, int iovcnt, off_t offset)
{
	using fn = ssize_t (*)(int, struct iovec const*, int, off_t);
	static fn const real = reinterpret_cast<fn>(::dlsym(RTLD_NEXT, "pwritev"));
	maybe_slow_down();
	maybe_hold();
	return real(fd, iov, iovcnt, offset);
}

extern "C" ssize_t pwrite(int fd, void const* buf, size_t count, off_t offset)
{
	using fn = ssize_t (*)(int, void const*, size_t, off_t);
	static fn const real = reinterpret_cast<fn>(::dlsym(RTLD_NEXT, "pwrite"));
	maybe_slow_down();
	maybe_hold();
	return real(fd, buf, count, offset);
}
#endif

#endif
