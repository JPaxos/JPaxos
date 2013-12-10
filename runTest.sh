#!/bin/bash

SCENARIO=${SCENARIO:-scenarios/simple}

echo "Scenario $SCENARIO"

export LOGGING_PROPS="${SCENARIO}/logback.xml"
export PAXOS_PROPS="${SCENARIO}/paxos.properties"
export OTHER_FILES="${SCENARIO}/process.sh ${SCENARIO}/processClient.sh"

rm -rf /tmp/jpaxosTemplate
./prepare.sh /tmp/jpaxosTemplate

for n in $(seq 0 $(( ${NUM_REPLICAS:-3} -1 )) ) client
do
    echo "cloning /tmp/jpaxos to ${TARGET:-/tmp}/jpaxos_$n"
    mkdir -p ${TARGET:-/tmp}/jpaxos_$n
    rsync -a --delete /tmp/jpaxosTemplate/ ${TARGET:-/tmp}/jpaxos_$n
done

echo "Running benchmark"

java -cp tools/benchmark.jar benchmark.Benchmark ${SCENARIO}/scenario

RESULTS="${SCENARIO}/$(date +%Y_%m_%d__%H_%M)"
mkdir "${RESULTS}"

if [ "$COLLECT" ]
then
        exit
fi

echo "Collecting data to $RESULTS/db.sqlite3"


for n in $(seq 0 $(( ${NUM_REPLICAS:-3} -1 )) )
do
        DIR="${TARGET:-/tmp}/jpaxos_$n"
        for (( i=0 ; ; ++i ))
        do
                if test -f "$DIR/log.$i"
                then
                        echo "parsing $DIR/log.$i"
                        cat "$DIR/log.$i" | tools/makesql.sh $n $i | sqlite3 $RESULTS/db.sqlite3
                else
                        break
                fi
        done
done

echo "done!"
