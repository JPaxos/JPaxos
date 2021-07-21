#!/bin/bash

  T50M="50MB snap"
 T50MP="50MB snap, reqSize up to 15kB"

echo "=== Select which results should be extracted: ==="

select RES in "$T50M" "$T50MP" "$T100M"
do
	case "$RES" in
		"$T50M")
			JM=out_test3_20210702_2020
			JP=out_test1_20210701_2125
			echo "=== Getting results for 50MB snap ===";;
		"$T50MP")
			JM=out_test3+_20210703
			JP=out_test1_20210701_2125
			echo "=== Getting results for 50MB snap with large reqsize ===";;
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
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       fullss    Paxos+SS@pmem
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       disk      Paxos+SS@ssd
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       epochss   Paxos+epochs
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       ram       pPaxos@RAM
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       fakepmem  pPaxos@emulp
../../../jpaxos/scenarios/plotForDiffSizes3/$JP       pmem      pPaxos@pmem
"

export AVG2=1
#export MAXN=3

while read RESULTS MODEL OUTNAME
do
    [ "$RESULTS" ] || continue # <- this skips empty lines
    [ -f "$RESULTS" ] || { echo "$OUTNAME: $RESULTS does not exist" ;  continue ; }
    echo -n "$OUTNAME: "
    if [ "$STDEV" ]
    then
	    NORM=1 STDEV=1 ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps $RESULTS | awk '{print ($1/2) " " ($2/1024/1024) " " ($3/1024/1024)}' > $OUTNAME && echo 'done' || echo 'fail'
	else
	    NORM=1         ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps $RESULTS | awk '{print ($1/2) " " ($2/1024/1024)                   }' > $OUTNAME && echo 'done' || echo 'fail'
    fi
done <<< "$IN"
