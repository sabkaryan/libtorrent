#!/bin/bash
# series-fix.sh: the directed test on this tree (forget_piece() gives the
# picker back the requests peers still wait for) and with half of the repair
# removed: R1 = not for the download queue, R2 = not for the request queue,
# R3 = not for a dont_download piece. Runs with -p0 set the piece to
# dont_download before forget_piece() (PC_P0=1). BASE, a tree without the
# repair, runs too when given.
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
R1='/int torrent::forget_piece/,/update_gauge/ s/for (auto const\& b : p->download_queue())/& if (false)/'
R2='/int torrent::forget_piece/,/update_gauge/ s/for (auto const\& b : p->request_queue())/& if (false)/'
R3='/int torrent::forget_piece/,/update_gauge/ s/for (auto\* p : m_connections)/if (m_picker->piece_priority(index) != dont_download) &/'
export TN=forget_after_busy_duplicate
for inv in ${INVS:-off on}; do
  [ -n "${BASE:-}" ] && INV=$inv VOL=picker-count-f-$inv-base bash $H/run.sh $BASE $O/f-base-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-fix bash $H/run.sh $R $O/f-fix-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-R1 MUT="$R1" bash $H/run.sh $R $O/f-R1-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-R2 MUT="$R2" bash $H/run.sh $R $O/f-R2-$inv 1 1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-R3 MUT="$R3" bash $H/run.sh $R $O/f-R3-$inv 1 1 > /dev/null
  [ -n "${BASE:-}" ] && INV=$inv VOL=picker-count-f-$inv-base bash $H/run.sh $BASE $O/f-base-p0-$inv 1 1 PC_P0=1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-fix bash $H/run.sh $R $O/f-fix-p0-$inv 1 1 PC_P0=1 > /dev/null
  INV=$inv VOL=picker-count-f-$inv-R3 MUT="$R3" bash $H/run.sh $R $O/f-R3-p0-$inv 1 1 PC_P0=1 > /dev/null
done
for d in $O/f-*; do echo "$(basename $d): $(grep -a -h '^b2\|^seed\|DID NOT' $d/driver.log | tr '\n' ' ' | cut -c1-220)"; done
