#!/bin/bash
#SBATCH -w pmem-[2-6] -t 8-0:0:0 --wait-all-nodes=1

# WARNING: this test can run a large number of clients, and clients fail unless
# for R in pmem-{1..6}; do ssh $R -- sysctl net.ipv4.tcp_tw_reuse=1; done

if [ "$SLURM_JOB_ID" ]
then
        SCRIPT="$(scontrol show job $SLURM_JOBID | grep 'Command=' | cut -d'=' -f2- )"
else
        SCRIPT="$(readlink -e $0)"
fi

DATE="$(date +%Y%m%d_%H%M)"
SCRIPTDIR="${SCRIPT%/*}"
OUT="$SCRIPTDIR/out_$DATE"

LOG="$SCRIPTDIR/log_$DATE"

cd ~/jpaxos

ClientMachines=2
ReqPerClient=25
TestTimeout="1m"
export MEASURING_PERIODS="16 26"

runTest(){
    WindowSize=$1
    BatchingSize=$2
    ReqSize=$3
    TotCliCnt=$4
    Model=$5

    CliCount=$((TotCliCnt/ClientMachines/ReqPerClient))

    MyReqPerClient=$ReqPerClient
    
    if ((CliCount>96))
    then
        CliCount=96
        MyReqPerClient=$((TotCliCnt/ClientMachines/CliCount))
    fi
    
    # create scenario from template and parameters
    sed "
        s/_ReqPerClient_/$MyReqPerClient/;
        s/_ReqSize_/$ReqSize/;
        s/_ClientCount_/$CliCount/;
        " "$SCRIPTDIR"/scenario_template > "$SCRIPTDIR"/scenario
    sed "
        s/_WindowSize_/$WindowSize/;
        s/_BatchSize_/$BatchingSize/;
        " "$SCRIPTDIR"/paxos.properties_"$Model" > "$SCRIPTDIR"/paxos.properties
    cp "$SCRIPTDIR"/config_"$Model" "$SCRIPTDIR"/config

    rm -f jpdb.sqlite3

    # run the test
    timeout "$TestTimeout" ./runTest.sh "$SCRIPTDIR"

    # on the first run, create column headers (sample db is used to get number of replicas and clients)
    if [ ! -e $OUT ]
    then
        echo -ne "WindowSize BatchSize ReqSize TotCliCnt Model " > $OUT
        HEADER=1 ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT
    fi

    # select results and put them to output table
    echo -ne "$WindowSize $BatchingSize $ReqSize $(($MyReqPerClient*$CliCount*$ClientMachines)) $Model " >> $OUT
    ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT

    # make sure that all dies
    for dest in pmem-{2..6}
    do
        ssh $dest -- "pkill -KILL java; pkill -KILL hashMapClient" < /dev/null &
    done
    pendingJobs=$(jobs | wc -l)
    for (( ; pendingJobs>=0 ; --pendingJobs )); do wait; done
}


ReqSize_CliCnt="\
256  2500
512  1600
1024  800
2048  600
3072  400
4096  400
5120  400
6144  300
7168  300
8192  300
10240 300
12288 300
14336 300
16384 300
18432 300
20480 300"

for iteration in 1
# 1 iterations
do
    for WindowSize in {4..10}
    # 7 window sizes
    do
        for BatchingSizeKb in `seq 16 16 127` `seq 128 32 320`
        # 16 batching sizes
        do
        BatchingSize=$((BatchingSizeKb*1024+1))
            while read ReqSize TotCliCnt
            # 16 pairs
            do
                # TotCliCnt is modified to be at most 12096 and at least:
                #  - window size of full batches
                #  - number of replicas of full batches (being built)
                #  - 5 instances decided not yet executed
                #  - safety margin of 100
                aux=$(( (WindowSize+3+5)*BatchingSize/ReqSize + 100 ))
                TotCliCnt=$(( aux>TotCliCnt?aux:TotCliCnt ))
                TotCliCnt=$(( TotCliCnt>12096?12096:TotCliCnt ))
                for Model in fullss disk epochss ram fakepmem pmem
                # 6 models
                do
                    echo -e "\033[1;7m($iteration/1)   WindowSize=$WindowSize   BatchingSize=${BatchingSizeKb}kB   ReqSize=$ReqSize   TotCliCnt=$TotCliCnt   Model=$Model\033[0m"
                    runTest $WindowSize $BatchingSize $ReqSize $TotCliCnt $Model < /dev/null
                done
            done <<< ${ReqSize_CliCnt}
        done
    done
done 2>&1 | tee $LOG
