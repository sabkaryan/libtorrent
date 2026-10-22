#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
STORAGE = "src/memory_storage.cpp"
DISK = "src/memory_disk_io.cpp"
HEADER = "include/libtorrent/aux_/memory_storage.hpp"
CASES = {
    "M1-received-instead-of-reserved": (
        DISK,
        "std::int64_t const reserved = m_pool->reserved_bytes();",
        "std::int64_t const reserved = m_pool->held_bytes();",
        "disk_io",
        "partial_piece_admission_keeps_full_slots_and_file_bytes",
    ),
    "M1b-foreign-torrent-received-only": (
        DISK,
        "std::int64_t const reserved = m_pool->reserved_bytes();",
        "std::int64_t const reserved = m_pool->held_bytes();",
        "disk_io",
        "partial_slots_of_another_torrent_block_new_memory_piece",
    ),
    "M2-admit-piece-into-slack": (
        DISK,
        "= s.reservation_size(r.piece) > m_pool->limit - reserved;",
        "= false;",
        "disk_io",
        "partial_piece_admission_keeps_full_slots_and_file_bytes",
    ),
    "M3-storage-counts-received-only": (
        HEADER,
        "m_limit >= 0 && reservation_size(piece) > m_limit - m_reserved_bytes",
        "m_limit >= 0 && m_held_complete + m_held_partial >= m_limit",
        "storage",
        "storage_partial_slot_survives_retirement_until_last_reader",
    ),
    "M4-retirement-releases-pinned-slot": (
        STORAGE,
        "void memory_storage::retire_entry"
        "(std::shared_ptr<memory_piece_entry> e)\n\t{",
        "void memory_storage::retire_entry"
        "(std::shared_ptr<memory_piece_entry> e)\n\t{\n"
        "\t\tm_reserved_bytes -= e->reserved_bytes;\n\t\te->reserved_bytes = 0;",
        "storage",
        "storage_partial_slot_survives_retirement_until_last_reader",
    ),
    "M5-transfer-releases-pinned-slot": (
        STORAGE,
        "if (failed) ++m_persist_failures;\n\t\te.place = piece_place::file;",
        "if (failed) ++m_persist_failures;\n\t\te.place = piece_place::file;\n"
        "\t\tm_reserved_bytes -= e.reserved_bytes;\n\t\te.reserved_bytes = 0;",
        "storage",
        "storage_transfer_slot_waits_for_held_block_release",
    ),
    "M6-ignore-removed-storage": (
        "src/memory_storage_pool.cpp",
        "for (auto const& r : retired)\n\t\t\tret += r.storage->reserved_bytes();",
        "",
        "disk_io",
        "removed_storage_reservation_blocks_other_torrent",
    ),
    "M7-never-release-reservation": (
        STORAGE,
        "m_reserved_bytes -= e.reserved_bytes;",
        "",
        "storage",
        "storage_partial_slot_survives_retirement_until_last_reader",
    ),
    "M8-short-block-not-rounded": (
        HEADER,
        "return std::int64_t(blocks_in_piece(piece)) * default_block_size;",
        "return m_files.piece_size(piece);",
        "disk_io",
        "short_final_block_requires_its_whole_allocated_slot",
    ),
    "M9-strict-piece-spills": (
        STORAGE,
        "if (strict(piece)) return piece_place::none;",
        "",
        "disk_io",
        "memory_only_declines_a_partial_that_cannot_reserve_full_piece",
    ),
    "M10-restart-keeps-reusable-slot": (
        DISK,
        "if (e)\n\t\t\t\t\t\ts.retire(r.piece);",
        "",
        "disk_io",
        "replacing_unpinned_generation_reuses_the_full_slot",
    ),
    "M11-unpinned-transfer-keeps-slot": (
        STORAGE,
        "if (e.place == piece_place::file && e.num_blocks == 0 && e.dropped.empty())\n"
        "\t\t\trelease_reservation(e);",
        "",
        "storage",
        "unpinned_transfer_returns_the_slot",
    ),
    "M11b-unpinned-failed-partial-keeps-slot": (
        STORAGE,
        "if (e.place == piece_place::file && e.num_blocks == 0 && e.dropped.empty())\n"
        "\t\t\trelease_reservation(e);",
        "",
        "storage",
        "unpinned_failed_partial_transfer_returns_the_slot",
    ),
}


def run(args: argparse.Namespace, name: str) -> None:
    relative, old, new, suite, target = CASES[name]
    path = ROOT / relative
    original = path.read_text()
    if original.count(old) != 1:
        raise RuntimeError(f"{name}: replacement count={original.count(old)}")
    try:
        path.write_text(original.replace(old, new))
        with (args.out / f"{name}-build.log").open("w") as output:
            built = subprocess.run(
                [
                    args.cmake,
                    "--build",
                    str(args.build),
                    "--parallel",
                    "2",
                    "--target",
                    f"test_memory_{suite}",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=900,
            )
        if built.returncode:
            raise RuntimeError(f"{name}: compile failed, not a killed mutant")
        test = f"{ROOT}/test/test_memory_{suite}.cpp.{target}"
        result = subprocess.run(
            [str(args.build / f"test/test_memory_{suite}"), "-n", test],
            cwd=args.out,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=90,
        )
        (args.out / f"{name}-test.log").write_text(result.stdout)
        killed = (
            result.returncode == 77
            and f"test-case: {test}" in result.stdout
            and "TEST_ERROR:" in result.stdout
            and "TEST(S) FAILED" in result.stdout
        )
        verdict = "TARGET RED" if killed else "NOT KILLED"
        line = f"{name}: {verdict} exit={result.returncode} target={target}"
        print(line, flush=True)
        with (args.out / "summary.log").open("a") as output:
            output.write(line + "\n")
        if not killed:
            raise RuntimeError(line)
    finally:
        path.write_text(original)
        os.utime(path, None)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--cmake", default="cmake")
    parser.add_argument("names", nargs="*")
    args = parser.parse_args()
    args.build = args.build.resolve()
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    for name in args.names or CASES:
        run(args, name)


if __name__ == "__main__":
    main()
