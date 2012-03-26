#!/bin/bash
if (($# != 1))
then
	# ${n} ${dir} ${clientsPerNode} ${testduration}"
	echo "Usage: $0 <dir>"
	exit 1
fi

cd $1

FILES='client-*.stats.log replica-*.stats.log'
for x in `ls -d w_*_b_*_c_*_rs_*`
do
	echo "Entering $x"
	cd $x
	if [ ! -e "clients.stats.txt" ]
	then
		tar zxf perfStats.tgz --wildcards client-*.stats.log
	fi	
	#rm perfStats.tgz
	#rm replicas.stats.txt
	cd ..
done
