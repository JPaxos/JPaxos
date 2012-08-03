#!/bin/zsh

SCENARIO=${SCENARIO:-../tests/1}

export LOGGING_PROPS="${SCENARIO}/logging.properties"
export PAXOS_PROPS="${SCENARIO}/paxos.properties"
export OTHER_FILES="${SCENARIO}/process.sh ${SCENARIO}/processClient.sh"

rm -rf /tmp/jpaxos
./prepare.sh /tmp/jpaxos

for n in $(seq 0 $(( ${NUM_REPLICAS:-3} -1 )) ) client
do
	echo "cloning /tmp/jpaxos to ${TARGET:-/tmp}/$n"
	mkdir -p ${TARGET:-/tmp}/$n
	rsync -a --delete /tmp/jpaxos/ ${TARGET:-/tmp}/$n
done

echo "Running benchmark"

java -cp ../Benchmark/dist/benchmark.jar benchmark.Benchmark ${SCENARIO}/scenario