#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
java -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos.jar lsr.paxos.test.EchoService $* 3>&1 1>&2 2>&3 3>&- | ./process.sh
