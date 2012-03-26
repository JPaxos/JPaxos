#!/bin/bash
# Copies to the cluster the files required to run Paxos. 
if (($# != 1))
then
	# ${n} "
	echo "Usage: $0 <#nodes>"
	exit 1
fi

nnodes=$1
FILES='Bench.jar jpaxos.jar startEmulabRun.sh startReplica.sh startClient.sh log-client.properties log-replica.properties distributeEmulab.sh'

# The /user/nfsantos directory is shared among all nodes
node="node1.paxos.paxoswan.emulab.net"
echo "Copying to ${node}"
ssh nfsantos@${node} "mkdir /tmp/paxos; rm -rf /tmp/paxos/*"
scp $FILES nfsantos@${node}:/tmp/paxos
ssh nfsantos@${node} "cd /tmp/paxos; chmod ug+rw *; chmod a+x *.sh; ./distributeEmulab.sh $nnodes"
