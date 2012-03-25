#!/bin/bash
if (( $# != 5 ))
then
    echo "Usage: $0 <WindowSize> <BatchSize> <reqSize> <answerSize> <configFile>"
    exit 1
fi

source test_nodes.sh

OUT=$5
rm -rf $OUT
# $OUT = paxos.properties

i=0
for x in $replicas
do
	I=2030
	J=3040
	I=$(( ${I} + ${i} )) 
	J=$(( ${J} + ${i} )) 
	echo "process.${i} = localhost:${I}:${J}">> $OUT	
	i=$((i+1))
done

./makeCommonProperties.sh $*
