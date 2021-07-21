#!/bin/bash
mkdir -p max10
for MODEL in `cut -d' ' -f 5  out_*  | sort | uniq | grep -v Model`
do
	MAXN=10 NORM=1 ./getSelected.pl $MODEL l_rps,f1_rps,f2_rps out_* | awk '{print ($1/2) " " ($2/1024/1024) }' > max10/$MODEL
done
