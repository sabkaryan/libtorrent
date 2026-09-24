/*

Copyright (c) 2026, Sergey Abkaryan
All rights reserved.

You may use, distribute and modify this code under the terms of the BSD license,
see LICENSE file.
*/

#ifndef TORRENT_TEST_WRITE_GATE_HPP_INCLUDED
#define TORRENT_TEST_WRITE_GATE_HPP_INCLUDED

// Disk writes a test can slow down or hold back, and disk reads it can hold
// back. This interposes pwrite(), pwritev() and pread() for the whole test
// binary: include it from exactly one source file of the binary and link with
// -ldl. Linux only.

#if defined TORRENT_LINUX
#include <atomic>
#include <chrono>
#include <cstdint>
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
inline void maybe_slow_down()
{
	if (slow_writes) std::this_thread::sleep_for(std::chrono::milliseconds(20));
}

// a held disk: while hold_writes is set, every pwrite()/pwritev() waits at
// this gate until it is cleared; writes_waiting counts the calls waiting
std::mutex gate_mutex;
std::condition_variable gate_cv;
bool hold_writes = false;
int writes_waiting = 0;
inline void maybe_hold()
{
	std::unique_lock<std::mutex> l(gate_mutex);
	if (!hold_writes) return;
	++writes_waiting;
	gate_cv.notify_all();
	gate_cv.wait(l, [] { return !hold_writes; });
	--writes_waiting;
}

inline void set_hold_writes(bool const hold)
{
	{
		std::lock_guard<std::mutex> l(gate_mutex);
		hold_writes = hold;
	}
	gate_cv.notify_all();
}

// held reads: while hold_reads_before is set, a pread() waits before it reads;
// while hold_reads_after is set, it reads and then waits before it returns, so
// the caller gets the bytes of the moment it read. The counters say how many
// calls wait at each gate, held_read_offset where the last held call reads
bool hold_reads_before = false;
bool hold_reads_after = false;
int reads_waiting_before = 0;
int reads_waiting_after = 0;
std::int64_t held_read_offset = -1;
inline void maybe_hold_read(bool const after, std::int64_t const offset)
{
	std::unique_lock<std::mutex> l(gate_mutex);
	bool& hold = after ? hold_reads_after : hold_reads_before;
	int& waiting = after ? reads_waiting_after : reads_waiting_before;
	if (!hold) return;
	held_read_offset = offset;
	++waiting;
	gate_cv.notify_all();
	gate_cv.wait(l, [&hold] { return !hold; });
	--waiting;
}

inline void set_hold_reads(bool const before, bool const after)
{
	{
		std::lock_guard<std::mutex> l(gate_mutex);
		hold_reads_before = before;
		hold_reads_after = after;
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

extern "C" ssize_t pread(int fd, void* buf, size_t count, off_t offset)
{
	using fn = ssize_t (*)(int, void*, size_t, off_t);
	static fn const real = reinterpret_cast<fn>(::dlsym(RTLD_NEXT, "pread"));
	maybe_hold_read(false, offset);
	ssize_t const ret = real(fd, buf, count, offset);
	maybe_hold_read(true, offset);
	return ret;
}
#endif

#endif
