#!/bin/bash

if(($# != 2))
then
echo "usage: $0 <output dir> <replica_count>";
  exit 1;
fi

FIRST=5
INCREMENT=5
LAST=25
REQUESTS=100000
REPLICA_COUNT=$2

OUTPUT_DIR=$1

for CLIENTS in $( seq $FIRST $INCREMENT $LAST )
do
  {
    echo 'benchmarking/topologies/ideal.model.xml'
      echo 'vnrunhost VNODE MODEL java -Djava.net.preferIPv4Stack=true -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.paxos.Replica NUM echo'
      echo 'vnrunhost VNODE MODEL java -Djava.net.preferIPv4Stack=true -ea -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.client.EchoClient 128'

      for REPLICA in $( seq 1 $REPLICA_COUNT )
      do
        echo "Start+0 replica create $REPLICA $REPLICA NONE"
      done

      echo 'Start+20 client a create 10 '$CLIENTS' NONE'

      echo -e 'Start+1000 client .* send '"$((REQUESTS/CLIENTS))"' 0 End'
      echo 'End+0 client .* stop NONE'

      echo 'End+1000 replica stop -1 NONE'

    
  } > $OUTPUT_DIR/${CLIENTS}_clients
done
