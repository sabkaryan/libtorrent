#!/bin/bash
# series-fix.sh: the directed test on this tree (forget_piece() drops or
# flags the requests peers still wait for and then cancels them, and
# incoming_piece() discards a not_wanted block of a piece set to
# dont_download that is not downloading) and with a part of the repair
# removed:
#   C1 = forget_piece() drops and flags nothing
#   C2 = unsent requests (request queue) are kept
#   C3 = sent requests (download queue) are neither flagged nor cancelled
#   C4 = sent requests are flagged, but no cancel is sent
#   C5 = incoming_piece() writes such a block
#   C6 = one pass: each peer sends its cancels before the next peer's
#        requests are flagged
#   C7 = dropping an unsent time critical request leaves
#        m_queued_time_critical as it was
# Each variant runs plain, with -p0 (PC_P0=1: the piece is set to
# dont_download before forget_piece()), -two (PC_TWO=1: a second slow peer)
# and -two-p0. BASE, a tree without the repair, runs too when given. INVS:
# invariant-checks values (default full, as in the Jamfile). Needs run.sh's
# env: DEPS, BOOST, IMG.
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
C1='s/auto r = p->drop_piece_requests(index);/std::vector<peer_request> r;/'
C2='/peer_connection::drop_piece_requests/,/return cancels;/ s/if (i->block.piece_index != piece)$/if (true)/'
C3='/peer_connection::drop_piece_requests/,/return cancels;/ s/pb.not_wanted = true;/continue;/'
C4='/void peer_connection::send_cancels/,/if (is_disconnecting()) return;/ s/write_cancel(r);/(void)r;/'
C5='s/|| (b->not_wanted$/|| (false \&\& b->not_wanted/'
C6='s/if (!r.empty()) cancels.emplace_back(p->self(), std::move(r));/if (!r.empty()) p->send_cancels(r);/'
C7='/peer_connection::drop_piece_requests/,/return cancels;/ s/--m_queued_time_critical;/;/'
export TN=forget_after_busy_duplicate
one() { # <variant> <mutation> <file>
  local v=$1 m=$2 f=$3 n e
  for n in "" p0 two two-p0; do
    e=; case $n in p0) e="PC_P0=1";; two) e="PC_TWO=1";; two-p0) e="PC_TWO=1 PC_P0=1";; esac
    INV=$inv VOL=picker-count-c-$inv-$v MUT="$m" MUTFILE=$f bash $H/run.sh ${TREE:-$R} $O/c-$v${n:+-$n}-$inv 1 1 $e > /dev/null
  done
}
for inv in ${INVS:-full}; do
  [ -n "${BASE:-}" ] && TREE=$BASE one base "" src/torrent.cpp
  one fix "" src/torrent.cpp
  one C1 "$C1" src/torrent.cpp
  one C2 "$C2" src/peer_connection.cpp
  one C3 "$C3" src/peer_connection.cpp
  one C4 "$C4" src/peer_connection.cpp
  one C5 "$C5" src/peer_connection.cpp
  one C6 "$C6" src/torrent.cpp
  one C7 "$C7" src/peer_connection.cpp
done
for d in $O/c-*; do echo "$(basename $d): $(grep -a -h '^b2\|^seed\|DID NOT' $d/driver.log | tr '\n' ' ' | cut -c1-260)"; done
