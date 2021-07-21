#!/bin/bash
for scn in \
   ../jpaxos/scenarios/tenGigBench_art_epochss \
   ../jpaxos/scenarios/tenGigBench_art_ram \
   ../jpaxos/scenarios/tenGigBench_art_pmem \
   ../jpaxos/scenarios/tenGigBench_art_epochss_b \
   ../jpaxos/scenarios/tenGigBench_art_epochss_b2 \
   ../jpaxos/scenarios/tenGigBench_art_epochss_b3 \
   ../jpaxos/scenarios/tenGigBench_art_ram_b \
   ../jpaxos/scenarios/tenGigBench_art_pmem_b \
   ../jpaxos/scenarios/tenGigBench_art_fullss \
   ../jpaxos/scenarios/tenGigBench_art_fullss_b \
   ../jpaxos/scenarios/tenGigBench_art_fullss_b2 \
   ../jpaxos/scenarios/tenGigBench_art_disk \
   ../jpaxos/scenarios/tenGigBench_art_disk_b \
   ../jpaxos/scenarios/tenGigBench_art_disk_b2 \
   ../jpaxos_pmem/scenarios/tenGigBench_art_fakepmem \
   ../jpaxos_pmem/scenarios/tenGigBench_art_pmem \
   ../jpaxos_pmem/scenarios/tenGigBench_art_ram
do
	OUT=$(echo "$scn"/out_2020_*)
	EXTR="tools/repeatedTestStatstics.pl"
	echo -ne "$scn " | sed 's|../||;s|/scenarios/tenGigBench_art_|@|'

	let IDX=4 # this is RPS
	"$EXTR" "$OUT" | awk '/avg-out5/ {printf("%.2f ", (0.0+$'$((IDX))'+$'$((IDX+12))'+$'$((IDX+24))')/3);}'

	let IDX=12 # this is pre_l_ifUp
	"$EXTR" "$OUT" | awk '/avg-out5/ {printf("%.2f\n", $'$((IDX))');}'
done
