#!/bin/bash
# series-wf.sh: write_fail_after_busy_duplicate on this tree as is (the piece
# restored by on_piece_fail_sync()) and with FIX: on_piece_fail_sync() gives
# the picker back the requests peers still wait for, as on_piece_sync() does.
# Both with invariant checks off (the RESULT line) and on (the assert).
# PC_ERR=<errno> picks the failure (default ENOMEM). Needs run.sh's env:
# DEPS, BOOST, IMG.
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
FIX='/void torrent::on_piece_fail_sync/,/update_gauge/ s|m_picker->restore_piece(piece);$|& for (auto* p : m_connections) { for (auto const\& b : p->download_queue()) { if (b.timed_out \|\| b.not_wanted \|\| b.block.piece_index != piece) continue; m_picker->mark_as_downloading(b.block, p->peer_info_struct(), p->picker_options()); } for (auto const\& b : p->request_queue()) { if (b.block.piece_index != piece) continue; m_picker->mark_as_downloading(b.block, p->peer_info_struct(), p->picker_options()); } }|'
# GUARD: FIX, and while a block of the piece is still being written (its
# failure is still to come) the piece is neither restored nor re-marked
GUARD='/void torrent::on_piece_fail_sync/,/update_gauge/ s|m_picker->restore_piece(piece);$|bool writing = false; for (auto const\& dp : m_picker->get_download_queue()) if (dp.index == piece \&\& dp.writing > 0) writing = true; if (!writing) { m_picker->restore_piece(piece); for (auto* p : m_connections) { for (auto const\& b : p->download_queue()) { if (b.timed_out \|\| b.not_wanted \|\| b.block.piece_index != piece) continue; m_picker->mark_as_downloading(b.block, p->peer_info_struct(), p->picker_options()); } for (auto const\& b : p->request_queue()) { if (b.block.piece_index != piece) continue; m_picker->mark_as_downloading(b.block, p->peer_info_struct(), p->picker_options()); } } }|'
export TN=write_fail_after_busy_duplicate
# VARIANTS: base fix guard (default: base fix); ASSERTS=off runs need INVS=off
for inv in ${INVS:-off on}; do
  for v in ${VARIANTS:-base fix}; do
    case $v in base) M=;; fix) M=$FIX;; guard) M=$GUARD;; esac
    INV=$inv VOL=wf69-$inv-${ASSERTS:-on}-$v MUT="$M" bash $H/run.sh $R $O/wf-$v-$inv-a${ASSERTS:-on}${TAG:-} 1 ${SEEDS:-1} "$@" > /dev/null
  done
done
for d in $O/wf-*; do echo "$(basename $d): $(grep -a -h '^b2\|^seed\|DID NOT' $d/driver.log | tr '\n' ' ' | cut -c1-240)"; done
