#!/bin/bash
# series-fix.sh: the directed test on this tree (forget_piece() cancels the
# requests peers still wait for, and incoming_piece() discards a cancelled
# block of a piece neither downloading nor wanted) and with a part of the
# repair removed:
#   C1 = forget_piece() does not cancel at all
#   C2 = unsent requests (request queue) are kept
#   C3 = sent requests (download queue) are neither flagged nor cancelled
#   C4 = sent requests are flagged, but no cancel is sent
#   C5 = incoming_piece() writes a cancelled block of such a piece
# Runs with -p0 set the piece to dont_download before forget_piece()
# (PC_P0=1). BASE, a tree without the repair, runs too when given. Needs
# run.sh's env: DEPS, BOOST, IMG.
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
C1='s/p->cancel_piece_requests(index);/(void)p;/'
C2='/void peer_connection::cancel_piece_requests/,/write_cancel(r);/ s/if (i->block.piece_index != piece)$/if (true)/'
C3='/void peer_connection::cancel_piece_requests/,/write_cancel(r);/ s/pb.not_wanted = true;/continue;/'
C4='/void peer_connection::cancel_piece_requests/,/if (is_disconnecting()) return;/ s/write_cancel(r);/(void)r;/'
C5='s/|| (b->not_wanted$/|| (false \&\& b->not_wanted/'
export TN=forget_after_busy_duplicate
one() { # <name> <mutation> <file> [ENV=VAL ...]
  local n=$1 m=$2 f=$3; shift 3
  INV=$inv VOL=picker-count-c-$inv-${n%%-*} MUT="$m" MUTFILE=$f bash $H/run.sh ${TREE:-$R} $O/c-$n-$inv 1 1 "$@" > /dev/null
}
for inv in ${INVS:-off on}; do
  for p in "" p0; do
    e=; [ -n "$p" ] && e=PC_P0=1
    [ -n "${BASE:-}" ] && TREE=$BASE one base${p:+-$p} "" src/torrent.cpp $e
    one fix${p:+-$p} "" src/torrent.cpp $e
    one C1${p:+-$p} "$C1" src/torrent.cpp $e
    one C2${p:+-$p} "$C2" src/peer_connection.cpp $e
    one C3${p:+-$p} "$C3" src/peer_connection.cpp $e
    one C4${p:+-$p} "$C4" src/peer_connection.cpp $e
    one C5${p:+-$p} "$C5" src/peer_connection.cpp $e
  done
done
for d in $O/c-*; do echo "$(basename $d): $(grep -a -h '^b2\|^seed\|DID NOT' $d/driver.log | tr '\n' ' ' | cut -c1-260)"; done
