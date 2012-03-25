#!/bin/bash
# Restarts the nodes on the cluster
if (($# < 1))
then
	# ${n} "
	echo "Usage: $0 <#nodes>"
	exit 1
fi

# $# gives the number of arguments.
# $@ expands the command line arguments, except the script name
for x in "$@"
do
	echo "  restarting down lsec${x}"
	ssh root@lsec${x} "shutdown -r now"	
done