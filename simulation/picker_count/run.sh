#!/bin/bash
# run.sh <tree> <out dir> <first seed> <last seed> [ENV=VAL ...]
# builds simulation/test_picker_count of <tree> (as on disk) in docker as the
# checked config (release, asserts=on, invariant-checks=full) and runs every seed
# in [first, last], nproc at a time, with ASLR off (setarch -R; the order of
# peers follows their addresses). Per seed: <out>/s<seed>.out, <out>/s<seed>.err
# (stderr: the dump and, with PC_LOG=1, the actions). Summary: <out>/driver.log.
# env: DEPS (tree with deps/, simulation/libsimulator), BOOST, IMG (image with
#      gcc, b2), CPUS (default 0-5), MUT (sed script), MUTFILE (src/torrent.cpp),
#      VOL (volume for objects, one per variant), B2X (extra b2 args), INV
#      (invariant-checks, full as in the Jamfile), ASSERTS (asserts, on), TN (picker_count or forget_after_busy_duplicate)
set -u
W=$1; O=$2; S0=$3; S1=$4; shift 4
DEPS=${DEPS:?a tree with deps/ and simulation/libsimulator}
T=$(mktemp -d "${TMPDIR:-/tmp}/picker-count.XXXXXX")
mkdir -p "$O"
rsync -a --exclude=.git "$W/" "$T/"
for d in deps/try_signal deps/asio-gnutls deps/libdatachannel simulation/libsimulator; do rm -rf "$T/$d"; rsync -a --exclude=.git "$DEPS/$d/" "$T/$d/"; done
grep -q test_picker_count "$T/simulation/Jamfile" || echo "run test_picker_count.cpp ;" >> "$T/simulation/Jamfile"
if [ -n "${MUT:-}" ]; then
  f="$T/${MUTFILE:-src/torrent.cpp}"; cp "$f" "$f.orig"; sed -i '' "$MUT" "$f"
  cmp -s "$f" "$f.orig" && { echo "MUTATION DID NOT APPLY: $MUT" | tee "$O/driver.log"; rm -rf "$T"; exit 1; }
  diff -u "$f.orig" "$f" > "$O/mutation.diff"; rm "$f.orig"
fi
git -C "$W" log -1 --format='%h %s' > "$O/commit.txt"; git -C "$W" status --short >> "$O/commit.txt"
printf '%s\n' "$@" > "$O/env.txt"
docker run --rm --security-opt seccomp=unconfined --cpuset-cpus=${CPUS:-0-5} -e S0="$S0" -e S1="$S1" -e B2X="${B2X:-}" -e INV="${INV:-full}" -e ASSERTS="${ASSERTS:-on}" -e TN="${TN:-picker_count}" -v "$T":/src:ro -v "$O":/out -v "${VOL:-picker-count}":/w \
  -v "${BOOST:?boost source tree}":/usr/local/opt/boost/include:ro "${IMG:?docker image}" bash -c '
(apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libssl-dev) > /out/apt.log 2>&1 || exit 1
echo "using gcc ;" > /tmp/uc.jam
# copy only what differs and give it a fresh mtime: a file restored after a
# mutation run carries an older mtime than its object, and b2 would keep it
(cd /src && find . -type f) | while read -r f; do cmp -s "/src/$f" "/w/$f" || { mkdir -p "$(dirname "/w/$f")"; cp "/src/$f" "/w/$f"; touch "/w/$f"; }; done
cd /w/simulation
b2 -j$(nproc) --user-config=/tmp/uc.jam toolset=gcc variant=release asserts=${ASSERTS:-on} invariant-checks=${INV:-full} $B2X \
  debug-iterators=off webtorrent=off link=static testing.execute=off test_picker_count > /out/b2.log 2>&1
echo "b2 exit=$? errors=$(grep -c " error:" /out/b2.log)"
B=$(find /w/simulation/bin -name test_picker_count -type f -perm -u+x | xargs ls -t | head -1)
[ -x "$B" ] || { echo "no binary"; exit 1; }
cp "$B" /out/test_picker_count
one() { s=$1; mkdir -p /tmp/r/$s && cd /tmp/r/$s; env PC_SEED=$s $(cat /out/env.txt) timeout 3600 setarch $(uname -m) -R /out/test_picker_count -n test_picker_count.cpp.$TN > /out/s$s.out 2> /out/s$s.err; e=$?
  echo "seed=$s exit=$e disc=$(grep -c "picker count discrepancy" /out/s$s.err) $(grep -a -o "RESULT.*" /out/s$s.out | cut -c1-200)"; }
export -f one
seq $S0 $S1 | xargs -P $(nproc) -I{} bash -c "one {}"' 2>&1 | tee "$O/driver.log"
rm -rf "$T"
