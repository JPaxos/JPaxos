#!/bin/bash
if (( $# != 1 ))
then
    echo "Usage: $0 <ID>"
    exit 1
fi

ID=$1
CLASSPATH=jpaxos.jar:Bench.jar
echo "Starting replica $ID (`hostname`)"
#-Xrunhprof:heap=all,depth=8
#-Xrunhprof:cpu=times,heap=sites,depth=8
#-Dcom.sun.management.jmxremote 
#/home/desousa/jre1.6.0_21/bin/
# Use the GNU version of time instead of the internal bash command.
# The GNU version allows redirection of output to a file and more detailed statistics.
#/usr/bin/time -v -o replica-${ID}-times.log /proj/PaxosWAN/jre1.6.0_24/bin/java -XX:+HeapDumpOnOutOfMemoryError -Xms850m -Xmx850m -ea -Djava.util.logging.config.file=log-replica.properties -cp ${CLASSPATH} lsr.paxos.benchmark.Server ${ID}
# taskset controls processor affinity. 
#taskset -c 0 /usr/bin/time -v -o replica-${ID}-times.log java -XX:+HeapDumpOnOutOfMemoryError -Xms850m -Xmx850m -ea -Djava.util.logging.config.file=log-replica.properties -cp ${CLASSPATH} lsr.paxos.benchmark.Server ${ID}
# génère des not found
java -XX:+HeapDumpOnOutOfMemoryError -ea -Djava.util.logging.config.file=log-replica.properties -cp ${CLASSPATH} lsr.paxos.test.EchoServer ${ID}
#java -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos.jar lsr.paxos.test.SimplifiedMapServer $*
