#!/bin/bash
##cd $( dirname $( readlink -f "$0" ) )
rm log
while read x
do
	if [[ "$x" =~ "Finished" ]]
	then
		echo $x 1>&2
	fi
	echo $x >> "log"
done

