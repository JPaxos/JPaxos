#!/bin/bash
if (($# != 1))
then
	# ${n} ${dir} ${clientsPerNode} ${testduration}"
	echo "Usage: $0 <dir>"
	exit 1
fi

cd $1

FILES='clients-*.stats.log'
for x in `ls -d w_*_b_*_rs_*`
do
	echo "Entering $x"	
	cd $x
	rm client-*.stats.log
	# if [ -e perfStats.tgz ] 
	# then
		# echo 'Logs already compressed. Skipping directory.'
	# else
		# rm client-*.stats.log
		# tar zcf perfStats.tgz $FILES
		# rm $FILES
	# fi
	cd ..
done
