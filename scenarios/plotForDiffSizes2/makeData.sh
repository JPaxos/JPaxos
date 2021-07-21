#!/bin/bash

 NEW="2021.06 - WS,BS,CliCnt tuned for each model×request size"
NEWC="2021.06 - WS=9 BS=192k, CliCnt=20*BS/ReqSize+100"
 OLD="2020.12 - code for SRDS"
OLDM="2020.12 - code for SRDS, with 'mitigations=auto' kernel parameter"

echo "=== Select which results should be extracted: ==="

select RES in "$NEW" "$NEWC" "$OLD" "$OLDM"
do
	case "$RES" in
		"$NEW")
			JM=out_test1_20210618_0731
			JP=out_test1_20210616_1538
			echo "=== Getting results for 12.2020 ===";;
		"$NEWC")
			JM=out_test2_20210630_2332
			JP=out_test2_20210629_0954
			echo "=== Getting results for 12.2020 ===";;
		"$OLD")
			JM=out_test1_20201231_2359
			JP=out_test1_20201231_2359
			echo "=== Getting results for 06.2021 ===";;
		"$OLDM")
		   JM=out_test1_20201231_2358
			JP=out_test1_20201231_2358
			echo "=== Getting results for 12.2020 (with mitigations) ===";;
		*)
			echo "Answer not understood. Quitting."
			exit 1;;
	esac
	break;
done

IN="
../../../jpaxos_pmem/scenarios/plotForDiffSizes2/$JM  ram       pPaxosSM@RAM
../../../jpaxos_pmem/scenarios/plotForDiffSizes2/$JM  fakepmem  pPaxosSM@emulp
../../../jpaxos_pmem/scenarios/plotForDiffSizes2/$JM  pmem      pPaxosSM@pmem
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       fullss    Paxos+SS@pmem
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       disk      Paxos+SS@ssd
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       epochss   Paxos+epochs
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       ram       pPaxos@RAM
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       fakepmem  pPaxos@emulp
../../../jpaxos/scenarios/plotForDiffSizes2/$JP       pmem      pPaxos@pmem
"

while read RESULTS MODEL OUTNAME
do
    [ "$RESULTS" ] || continue # <- this skips empty lines
    [ -f "$RESULTS" ] || { echo "$OUTNAME: $RESULTS does not exist" ;  continue ; }
    echo -n "$OUTNAME: "
    if [ "$STDEV" ]
    then
	    NORM=1 AVG2=1 STDEV=1 ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps $RESULTS | awk '{print ($1/2) " " ($2/1024/1024) " " ($3/1024/1024)}' > $OUTNAME && echo 'done' || echo 'fail'
	 else
	    NORM=1 AVG2=1         ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps $RESULTS | awk '{print ($1/2) " " ($2/1024/1024)                   }' > $OUTNAME && echo 'done' || echo 'fail'
    fi
done <<< "$IN"
