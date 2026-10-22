#!/bin/bash
set -u
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BUILD=${1:?usage: verify.sh build-directory output-directory}
OUT=${2:?usage: verify.sh build-directory output-directory}
CMAKE=$(command -v "${CMAKE:-cmake}")
PYTHON=${PYTHON:-python3}
mkdir -p "$OUT"
"$PYTHON" "$ROOT/tools/memory-admission/mutate.py" --build "$BUILD" \
  --out "$OUT" --cmake "$CMAKE"
MUTATION_EXIT=$?
"$ROOT/tools/memory-admission/run.sh" "$BUILD" > "$OUT/restored-suite.log" 2>&1
RESTORED_EXIT=$?
printf 'mutations=%s restored_suite=%s\n' "$MUTATION_EXIT" "$RESTORED_EXIT"
[ "$MUTATION_EXIT" -eq 0 ] && [ "$RESTORED_EXIT" -eq 0 ] || exit 1
[ "${FULL_GATE:-1}" -eq 1 ] || exit 0
"$CMAKE" --build "$BUILD" --parallel 2 > "$OUT/full-build.log" 2>&1
FULL_BUILD_EXIT=$?
if [ "$FULL_BUILD_EXIT" -ne 0 ]; then
  printf 'full_build=%s\n' "$FULL_BUILD_EXIT"
  exit 1
fi
"$(dirname "$CMAKE")/ctest" --test-dir "$BUILD" --output-on-failure \
  --timeout 600 > "$OUT/full-suite.log" 2>&1
FULL_SUITE_EXIT=$?
printf 'full_build=%s full_suite=%s\n' "$FULL_BUILD_EXIT" "$FULL_SUITE_EXIT"
[ "$FULL_SUITE_EXIT" -eq 0 ]
