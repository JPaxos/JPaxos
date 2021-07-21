#!/bin/sh
ulimit -c unlimited
echo 0x1b3 > /proc/self/coredump_filter

cd $( dirname $( readlink -f "$0" ) )

PMEM_FILE=`perl -e 'foreach(<>){print($1) if /NvmBaseDir\s*=\s*(.*)/;}' paxos.properties`/jpaxos.$1

if [[ -f "$PMEM_FILE" && -x ./dumper ]]
then
	let dumpi=1
	while [[ -e dump.$dumpi ]]; do let dumpi++; done
	./dumper "$PMEM_FILE" > dump.$dumpi
fi

java -ea -server -Dlogback.configurationFile=logback.xml -Djava.library.path=. \
  -cp lib/slf4j-api-1.7.26.jar:lib/logback-core-1.2.3.jar:lib/logback-classic-1.2.3.jar:jpaxos.jar \
  lsr.paxos.test.DigestService "$@" 2>&1 | ./process.sh
