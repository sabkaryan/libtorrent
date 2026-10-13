#!/bin/bash
# series-r.sh [prefix]: the random stand, 24 seeds each: all actions (a),
# no forget_piece() (b), no pauses (c). Results in $OUT (default ./out).
X=${1:-r}; R=$(cd "$(dirname "$0")/../.." && pwd); O=${OUT:-$PWD/out}
bash $R/simulation/picker_count/run.sh $R $O/${X}a 1 24 > /dev/null
bash $R/simulation/picker_count/run.sh $R $O/${X}b 1 24 PC_FORGET=0 > /dev/null
bash $R/simulation/picker_count/run.sh $R $O/${X}c 1 24 PC_PAUSE=0 PC_SEED_PAUSE=0 > /dev/null
for d in ${X}a ${X}b ${X}c; do echo "== $d $(tr '\n' ' ' < $O/$d/env.txt) clean=$(grep -c '^seed=[0-9]* exit=0 disc=0' $O/$d/driver.log)"; grep '^seed' $O/$d/driver.log | grep -v 'exit=0 disc=0' | cut -c1-60; done
