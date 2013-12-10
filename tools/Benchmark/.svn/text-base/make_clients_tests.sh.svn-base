#!/bin/bash

if(($# != 1))
then
  echo "usage: $0 <output dir>"
  exit 1
fi

FIRST=1
INCREMENT=1
LAST=100
REQUESTS=100000
OUTPUT_DIR=$1

for CLIENTS in $( seq $FIRST $INCREMENT $LAST )
do
  {
    echo 'benchmarking/topologies/ideal.model.xml'
      echo 'java -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.paxos.Replica NUM'
      echo 'java -Djava.util.logging.config.file=logging_benchmark.properties -cp bin lsr.client.BenchmarkClient'
      echo 'Start+0 replica create 1 1 NONE'
      echo 'Start+0 replica create 2 2 NONE'
      echo 'Start+0 replica create 3 3 NONE'

      echo 'Start+20 client a create 6 '$CLIENTS' NONE'

      echo -e 'Start+1000 client .* send '"$((REQUESTS/CLIENTS))"' 0 End'
      echo 'End+0 client .* stop NONE'

      echo 'End+1000 replica stop -1 NONE'

  } > $OUTPUT_DIR/${CLIENTS}_clients
done
