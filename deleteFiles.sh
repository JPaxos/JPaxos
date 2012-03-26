#!/bin/bash
if (($# != 1))
then	
	echo "Usage: $0 <dir>"
	exit 1
fi

cd $1

FILES='replica-*.stats.log'
for x in `ls -d w_*_b_*_c_*_rs_*`
do
	echo "Entering $x"
	cd $x		
	rm $FILES	
	cd ..
done
