#!/bin/bash
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BUILD=${1:?usage: run.sh build-directory [cmake-options ...]}
shift
CMAKE=$(command -v "${CMAKE:-cmake}")
"$CMAKE" -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Debug \
  -Dbuild_tests=ON -Dwebtorrent=OFF -DBUILD_SHARED_LIBS=OFF \
  -Ddeveloper-options=ON -Dasserts=on -Dinvariant-checks=full "$@"
"$CMAKE" --build "$BUILD" --parallel 2 --target \
  test_memory_slab test_memory_storage test_memory_disk_io test_memory_session
"$(dirname "$CMAKE")/ctest" --test-dir "$BUILD" --output-on-failure \
  -R '^test_memory_(slab|storage|disk_io|session)$'
