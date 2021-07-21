#!/bin/bash
#SBATCH -w pmem-[1-6] -t 12:0:0 --wait-all-nodes=1

if [ "$SLURM_JOB_ID" ]
then
        SCRIPT="$(scontrol show job $SLURM_JOBID | grep 'Command=' | cut -d'=' -f2- )"
else
        SCRIPT="$(readlink -e $0)"
fi

SCRIPTDIR="${SCRIPT%/*}"

cd ~/jpaxos

OUT="${SCRIPT%.sh}-$(date +%Y%m%d_%H%M).out"
testTimeout="1m"
export MEASURING_PERIODS="16 26"

for TRY in {1..5}
do
	for BF in {1..18}
	do
		BS=$((BF*8*1024-1))

		printf "%120s\n" "=" | tr ' ' '='
		echo "=== (#$TRY) batch size is ${BS}B (BF=$BF) ==="

		# create scenario from template and parameters
		cp "$SCRIPTDIR"/paxos.properties_template "$SCRIPTDIR"/paxos.properties
		echo "BatchSize = $BS" >> "$SCRIPTDIR"/paxos.properties

		rm -f jpdb.sqlite3

		timeout "$testTimeout" ./runTest.sh "$SCRIPTDIR"

      # on the first run, create column headers (sample db is used to get number of replicas and clients)
      if [ ! -e $OUT ]
      then
	      echo -ne "bs bf " > $OUT
      	HEADER=1 ./tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT
      fi

      # select results and put them to output table
      echo -ne "$BS $BF " >> $OUT
      tools/sqlGetResultLine.sh jpdb.sqlite3 >> $OUT

      # make sure that all dies
      for dest in pmem-{2..6}
      do
      	ssh $dest -- "pkill -KILL java; pkill -KILL hashMapClient" &
      done
      pendingJobs=$(jobs | wc -l)
      for (( ; pendingJobs>=0 ; --pendingJobs )); do wait; done
	done
done
