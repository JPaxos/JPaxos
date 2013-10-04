#!/bin/bash

SCENARIO=${SCENARIO:-scenarios/1}

echo "Scenario $SCENARIO"

export LOGGING_PROPS="${SCENARIO}/logback.xml"
export PAXOS_PROPS="${SCENARIO}/paxos.properties"
export OTHER_FILES="${SCENARIO}/process.sh ${SCENARIO}/processClient.sh"

rm -rf /tmp/jpaxosTemplate
./prepare.sh /tmp/jpaxosTemplate

MACHINES=( hpc-2 hpc-3 hpc-4 )
CLIENTS=( hpc-5 hpc-6 hpc-7 )

for n in $(seq 0 $(( ${NUM_REPLICAS:-3} - 1 )) )
do
    echo   "cloning /tmp/jpaxosTemplate to ${MACHINES[$n]}:${TARGET:-/tmp}/jpaxos_$n"
    rsync -a --delete /tmp/jpaxosTemplate/ ${MACHINES[$n]}:${TARGET:-/tmp}/jpaxos_$n
done

for CLIENT in "${CLIENTS[@]}"
do
	echo   "cloning /tmp/jpaxosTemplate to ${CLIENT}:${TARGET:-/tmp}/jpaxos_client"
	rsync -a --delete /tmp/jpaxosTemplate/ ${CLIENT}:${TARGET:-/tmp}/jpaxos_client
done


echo "Running benchmark"

java -cp tools/benchmark.jar benchmark.Benchmark ${SCENARIO}/scenario

if [ "$NOCOLLECT" ]
then
        exit
fi

RESULTS="${SCENARIO}/$(date +%Y_%m_%d__%H_%M)"
mkdir "${RESULTS}"

RR=$(mktemp)

#echo "Collecting data to $RESULTS/db.sqlite3"
echo "Collecting data to $RR (afterwards: $RESULTS/db.sqlite3)"

echo "create table correction ( replica_id , diff );" | sqlite3 $RR

for n in $(seq 0 $(( ${NUM_REPLICAS:-3} -1 )) )
do
        DIR="${TARGET:-/tmp}/jpaxos_$n"
        echo "insert into correction values ( $n , $( /home/jkonczak/bin/getTimeDiff.sh ${MACHINES[$n]} ) );" | sqlite3 $RR
        for (( i=0 ; ; ++i ))
        do
                if ssh ${MACHINES[$n]} test -f "$DIR/log.$i"
                then
                        echo "parsing $DIR/log.$i"
                        ssh ${MACHINES[$n]} cat "$DIR/log.$i" | tools/makesql.sh $n $i | sqlite3 $RR
                else
                        break
                fi
        done
done

echo "clean ram disk"
for n in $(seq 0 $(( ${NUM_REPLICAS:-3} -1 )) )
do
	ssh "${MACHINES[$n]}" rm -rf /ramdisk/*
done

echo "Compressing results"

bzip2 --keep --best $RR --stdout > "$RESULTS/db.sqlite3.orig.bz2"

echo "Extracting data"

tools/extractsql/extractsqlHPC.sh $RR "$RESULTS"

echo "Extracting recovery times"

tools/recoveryTimes.sh $RR > "$RESULTS/recoveryTimes"

bzip2 --keep --best $RR --stdout > "$RESULTS/db.sqlite3.done.bz2"

rm $RR

echo "done!"
