#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )
java -ea -cp jpaxos.jar lsr.paxos.test.MultiClient $* 3>&1 1>&2 2>&3 3>&- | ./processClient.sh
