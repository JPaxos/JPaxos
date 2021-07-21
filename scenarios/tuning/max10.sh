#!/bin/bash
TEMP=`mktemp`

head -n1 out_20210602_1138 > $TEMP

for x in ~/jpaxos/scenarios/tuning/out*[0-9]; do
    tail -n+2 $x | sed 's/\([a-z][a-z]*\)/jpaxos@\1/' >> $TEMP
done;

for x in ~/jpaxos_pmem/scenarios/tuning/out*[0-9]; do
    tail -n+2 $x | sed 's/\([a-z][a-z]*\)/jpaxos_pmem@\1/' >> $TEMP
done;

mkdir -p max10

for MODEL in `cut -d' ' -f 5  $TEMP  | sort | uniq | grep -v Model`; do MAXN=10 NORM=1 ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps $TEMP | awk '{print ($1/2) " " ($2/1024/1024) }' > max10/$MODEL; done

rm $TEMP
