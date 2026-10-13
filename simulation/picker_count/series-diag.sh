#!/bin/bash
# series-diag.sh: the random stand with one link of the chain removed (D2, D4
# as in series-d.sh), configs a (all actions) and c (no pauses), 24 seeds each
R=$(cd "$(dirname "$0")/../.." && pwd); H=$R/simulation/picker_count; O=${OUT:-$PWD/out}
eval "$(grep -E '^D[24]=' $H/series-d.sh)"
MUT="$D2" MUTFILE=src/peer_connection.cpp VOL=picker-count-D2 bash $H/run.sh $R $O/D2a 1 24 > /dev/null
MUT="$D2" MUTFILE=src/peer_connection.cpp VOL=picker-count-D2 bash $H/run.sh $R $O/D2c 1 24 PC_PAUSE=0 PC_SEED_PAUSE=0 > /dev/null
MUT="$D4" MUTFILE=src/torrent.cpp VOL=picker-count-D4 bash $H/run.sh $R $O/D4a 1 24 > /dev/null
MUT="$D4" MUTFILE=src/torrent.cpp VOL=picker-count-D4 bash $H/run.sh $R $O/D4c 1 24 PC_PAUSE=0 PC_SEED_PAUSE=0 > /dev/null
for d in D2a D2c D4a D4c; do echo "== $d $(tr '\n' ' ' < $O/$d/env.txt) clean=$(grep -c '^seed=[0-9]* exit=0 disc=0' $O/$d/driver.log)"; grep '^seed' $O/$d/driver.log | grep -v 'exit=0 disc=0' | cut -c1-60; done
