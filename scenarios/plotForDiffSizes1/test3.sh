#!/bin/bash
#SBATCH -w pmem-[2-6],hpc-9 -t 12:0:0 --wait-all-nodes=1

# 3 * 1 * 12 * 16 * (1 * minute) = 9 h + 36 min

if [ "$SLURM_JOB_ID" ]
then
	SCRIPT="$(scontrol show job $SLURM_JOBID | grep 'Command=' | cut -d'=' -f2- )"
else
	SCRIPT="$(readlink -e $0)"
fi

cd ~/jpaxos

DATE="$(date +%Y%m%d_%H%M)"

clientMachines=2
testTimeout="1m"
export MEASURING_PERIODS="12 22"

for i in {1..20}
do
    for reqPerClient in 25
    do

        cat << EOF | while read reqsize totCliCnt
256  2500
512  1600
768  1000
1024  800
1536  600
2048  600
2560  600
3072  400
3584  400
4096  400
5120  400
6144  300
7168  300
8192  300
9216  300
10240 300
11264 300
12288 300
13312 300
14336 300
15360 300
16384 300
17408 300
18432 300
19456 300
20480 300
EOF
        do
            cliCount=$((totCliCnt/clientMachines/reqPerClient))
				cat << EOF | while read BACKEND
/home/jkonczak/jpaxos/scenarios/tenGigBench_epochss
/home/jkonczak/jpaxos/scenarios/tenGigBench_fullss
/home/jkonczak/jpaxos/scenarios/tenGigBench_ram
/home/jkonczak/jpaxos/scenarios/tenGigBench_pmem
/home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_ram
/home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_pmem
/home/jkonczak/jpaxos_pmem/scenarios/tenGigBench_fakepmem

EOF
            do
                printf "%120s\n" "=" | tr ' ' '='
                echo "=== $BACKEND (#$i) ${clientMachines}×${cliCount} client processes, each sends $reqPerClient parallel requests, request size is ${reqsize}B ==="

                SCRIPTDIR=$BACKEND
                OUT=$BACKEND/test3-${DATE}.out

                # create scenario from template and parameters
                sed "
                    s/_ReqPerClient_/$reqPerClient/;
                    s/_ReqSize_/$reqsize/;
                    s/_ClientCount_/$cliCount/;
                    " "$SCRIPTDIR"/scenario_template > "$SCRIPTDIR"/scenario

                rm -f jpdb.sqlite3

                # run the test
                timeout "$testTimeout" ./runTest.sh "$SCRIPTDIR"

                # on the first run, create column headers (sample db is used to get number of replicas and clients)
                if [ ! -e $OUT ]
                then
                    echo -ne "reqsize clicount " > $OUT
                    HEADER=1 ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT
                fi

                # select results and put them to output table
                echo -ne "$reqsize $(($reqPerClient*$cliCount*$clientMachines)) " >> $OUT
                ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT

                # make sure that all dies
                for dest in pmem-{2..6}
                do
                    ssh $dest -- "pkill -KILL java; pkill -KILL hashMapClient" < /dev/null &
                done
                pendingJobs=$(jobs | wc -l)
                for (( ; pendingJobs>=0 ; --pendingJobs )); do wait; done
            done
        done
    done
done
