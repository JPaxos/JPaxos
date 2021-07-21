#!/bin/bash
for TEST in pmem fullss epochss
do
	mkdir /tmp/res_$TEST || exit 1
done

for xw in `seq -w 1 250`
do
   for TEST in pmem fullss epochss
	do
		echo "################################### $TEST $xw ###################################";
		./runTest.sh scenarios/recoveryTest_$TEST
		mkdir /tmp/res_$TEST/$xw
		mv /tmp/jpaxos_* /tmp/res_$TEST/$xw
		mv jpdb.sqlite3 /tmp/res_$TEST/$xw
	done
done
