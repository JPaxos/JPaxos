#!/bin/bash

if(($# != 3));
then
echo "usage: $0 <request size> <request count> <client count>";
exit 1;
fi

REQUEST_SIZE=$1;
REQUESTS=$2;
CLIENTS=$3;

echo 'benchmarking/topologies/ideal.model.xml';
#echo 'vnrunhost VNODE MODEL java -Djava.net.preferIPv4Stack=true -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.paxos.Replica NUM echo';
#echo 'vnrunhost VNODE MODEL java -Djava.net.preferIPv4Stack=true -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.client.EchoClient '$REQUEST_SIZE'';
echo 'java -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.paxos.Replica NUM echo';
echo 'java -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.client.EchoClient '$REQUEST_SIZE'';
echo 'Start+0 replica create 1 1 NONE';
echo 'Start+0 replica create 2 2 NONE';
echo 'Start+0 replica create 3 3 NONE';

echo 'Start+20 client a create 6 '$CLIENTS' NONE';

echo -e 'Start+1000 client .* send '"$((REQUESTS/CLIENTS))"' 0 End';
echo 'End+0 client .* stop NONE';

echo 'End+1000 replica stop -1 NONE';
