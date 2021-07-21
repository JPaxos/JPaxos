#!/bin/bash
export NORM=1
#export AVG=1
export AVG2=1
#export MEDIAN=1
DATE=20201210_2343

getSel(){
  # data is in MByte/s
  ./getSelected.pl l_rps $1 | awk '{print ($1/2) " " ($3/1024/1024)}'
}

cat << EOF | while read BACKEND FILE
Paxos+FullSS  /home/jkonczak/jpaxos/scenarios/tenGigBench_fullss/test3-$DATE.out
Paxos+EpochSS /home/jkonczak/jpaxos/scenarios/tenGigBench_epochss/test3-$DATE.out
P+pmem@RAM    /home/jkonczak/jpaxos/scenarios/tenGigBench_ram/test3-$DATE.out
P+pmem@pmem   /home/jkonczak/jpaxos/scenarios/tenGigBench_pmem/test3-$DATE.out
pmemP@RAM     /home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_ram/test3-$DATE.out
pmemP@pmem    /home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_pmem/test3-$DATE.out
pmemP@emulp   /home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_fakepmem/test3-$DATE.out
EOF
do
	getSel $FILE > $BACKEND
done

