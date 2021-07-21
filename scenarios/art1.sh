#!/bin/bash
for scn in \
   scenarios/tenGigBench_art_epochss \
   scenarios/tenGigBench_art_ram \
   scenarios/tenGigBench_art_pmem \
   scenarios/tenGigBench_art_epochss_b \
   scenarios/tenGigBench_art_epochss_b2 \
   scenarios/tenGigBench_art_epochss_b3 \
   scenarios/tenGigBench_art_ram_b \
   scenarios/tenGigBench_art_pmem_b \
   scenarios/tenGigBench_art_fullss \
   scenarios/tenGigBench_art_fullss_b \
   scenarios/tenGigBench_art_fullss_b2 \
   ../jpaxos_pmem/scenarios/tenGigBench_art_ram \
   ../jpaxos_pmem/scenarios/tenGigBench_art_pmem \
   ../jpaxos_pmem/scenarios/tenGigBench_art_fakepmem
do
	OUT="$scn/out_$(date +%Y_%m_%d__%H_%M)"
	HEADER=1 ./tools/sqlGetResultLine.sh > $OUT
	for x in {1..50}
	do
		time ./runTest.sh $scn  && MEASURING_PERIODS="16 26" ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT
	done
done | tee -a art1.out_$(date +%Y_%m_%d__%H_%M)
