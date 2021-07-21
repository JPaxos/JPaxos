#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
java -Dlogback.configurationFile=logback.xml -Djava.library.path=. \
  -cp lib/slf4j-api-1.7.26.jar:lib/logback-core-1.2.3.jar:lib/logback-classic-1.2.3.jar:jpaxos.jar \
   lsr.paxos.test.GenericMultiClient "$@" | ./processClient.sh
