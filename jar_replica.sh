#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
java -Dlogback.configurationFile=logback.xml \
  -cp lib/slf4j-api-1.7.5.jar:lib/logback-core-1.0.13.jar:lib/logback-classic-1.0.13.jar:jpaxos.jar \
  lsr.paxos.test.EchoService $* 3>&1 1>&2 2>&3 3>&- | ./process.sh
