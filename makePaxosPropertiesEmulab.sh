#!/bin/bash
if (( $# != 5 ))
then
    echo "Usage: $0 <WindowSize> <BatchSize> <reqSize> <answerSize> <configFile>"
    exit 1
fi

OUT=$5
N=3

rm -rf $OUT
for ((x=0; x<N; x++))
do	
	echo "process.${x} = node$((1+x)):2001:3001" >> $OUT	
done



./makeCommonProperties.sh $*
