#!/bin/bash
# Runs on the main replica

if (( $# != 1 ))
then
    echo "Usage: $0 <numberReplicas>"
    exit 1
fi

N=$1

for ((x=2; x<=N; x++))
do
	node="node${x}.paxos.paxoswan.emulab.net"
	echo "Copying files to $node"	
	ssh ${node} "mkdir /tmp/paxos; rm -rf /tmp/paxos/*"
	scp -r /tmp/paxos/* ${node}:/tmp/paxos
done