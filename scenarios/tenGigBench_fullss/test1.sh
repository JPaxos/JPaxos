#!/bin/bash
#SBATCH -w cloud-4,pmem-[1,3-6] -t 12:0:0 --wait-all-nodes=1

# 3 * 1 * 12 * 16 * (1 * minute) = 9 h + 36 min

if [[ "$HOSTNAME" =~ pmem ]]
then
	echo "Script should be run not on pmem!"
	exit 1
fi


if [ "$SLURM_JOB_ID" ]
then
	SCRIPT="$(scontrol show job $SLURM_JOBID | grep 'Command=' | cut -d'=' -f2- )"
else
	SCRIPT="$(readlink -e $0)"
fi

SCRIPTDIR="${SCRIPT%/*}"

cd ~/jpaxos

OUT="${SCRIPT%.sh}-$(date +%Y%m%d_%H%M).out"

clientMachines=2
testTimeout="1m"
export MEASURING_PERIODS="12 22"

for i in 1 2 3
do
    # safe values are up to 100
    for reqPerClient in 25
    do
        # 12 entries: 50 100 200 300 400 600 800 1000 1300 1600 2000 2500
        # safe values for cliCount are up to 96
        for cliCount in 1 2 4 6 8 12 16 20 26 32 40 50
        do

				# 16 entries: 256 512 768 1024 1536 2048 2560 3072 3584 4096 5120 6144 7168 8192 9216 10240
            for reqsize in `seq 256 256 1023` `seq 1024 512 4095` `seq 4096 1024 10240`
            do
                printf "%120s\n" "=" | tr ' ' '='
                echo "=== (#$i) ${clientMachines}×${cliCount} client processes, each sends $reqPerClient parallel requests, request size is ${reqsize}B ==="

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
                for dest in pmem-{1,3,4,5,6}
                do
                    ssh $dest -- "pkill -KILL java; pkill -KILL hashMapClient" &
                done
                pendingJobs=$(jobs | wc -l)
                for (( ; pendingJobs>=0 ; --pendingJobs )); do wait; done
            done
        done
    done
done
