#!/bin/bash
# directed test forget_after_busy_duplicate on the pin, and with one link of
# the chain removed: D2 = cancel_request() flags a queued block it no longer
# finds requested in the picker; D4 = forget_piece() refuses (3) while a peer
# still waits for a block of the piece. Each with and without invariant checks.
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
D2='s/if (!t->picker().is_requested(block)) return;/if (!t->picker().is_requested(block) \&\& std::none_of(m_download_queue.begin(), m_download_queue.end(), aux::has_block(block))) return;/'
D4='s/if (m_picker->is_downloading(index)) return 3;/if (m_picker->is_downloading(index)) return 3; for (auto const* pc : *this) for (auto const\& b : pc->download_queue()) if (b.block.piece_index == index \&\& !b.not_wanted \&\& !b.timed_out) return 3;/'
for inv in ${INVS:-off on}; do
  INV=$inv VOL=picker-count-d-$inv-pin TN=forget_after_busy_duplicate bash $H/run.sh $R $O/d-pin-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-d-$inv-D2 TN=forget_after_busy_duplicate MUT="$D2" MUTFILE=src/peer_connection.cpp bash $H/run.sh $R $O/d-D2-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-d-$inv-D4 TN=forget_after_busy_duplicate MUT="$D4" MUTFILE=src/torrent.cpp bash $H/run.sh $R $O/d-D4-$inv 1 1 > /dev/null
done
for d in d-pin-off d-D2-off d-D4-off d-pin-on d-D2-on d-D4-on; do
  echo "$d: $(grep -a '^b2\|^seed' $O/$d/driver.log | tr '\n' ' ') $(grep -a -h -o 'RESULT.*\|TEST_EQUAL[^*]*\|picker count discrepancy[^\n]*' $O/$d/s1.out $O/$d/s1.err | head -3 | tr '\n' ' ' | cut -c1-260)"
done
