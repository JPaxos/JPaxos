#!/bin/bash
# Increases the file limits in all nodes
if (($# != 1))
then
	# ${n} "
	echo "Usage: $0 <#nodes>"
	exit 1
fi

N=$1

# Copy to a directory that is shared among all nodes.
scp 'limits.conf' nfsantos@node1.paxos.paxoswan.emulab.net:~

for ((x=1; x<=N; x++))
do
	node="node${x}.paxos.paxoswan.emulab.net"
	echo "Copying file to $node"	
	ssh nfsantos@${node} "sudo cp ~/limits.conf /etc/security"
done
