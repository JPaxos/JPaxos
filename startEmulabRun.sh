#!/bin/bash
if (($# != 4))
then
	# ${n} ${clientNodes} ${clientsPerNode} ${testduration}"
	echo "Usage: $0 <nreplicas> <clientNodes> <clientsPerNode> <duration>"
	exit 1
fi

EXPERIMENT="paxos"

n=$1
clientNodes=$2
clientsPerNode=$3
duration=$4

echo "Launching replicas"
for ((x=1; x<=n; x++))
do	
	node="node${x}.${EXPERIMENT}.paxoswan.emulab.net"	
	ssh nfsantos@${node} "cd /tmp/paxos; killall java; sleep 2; rm -rf *.log; rm -rf *.log.gz; ./startReplica.sh $((x-1))" &
done

# Wait for replicas to start and stabilize
sleep 4

echo "Launching clients"
# Executed in parallel for each node
startNode () {	
	node="node$1.${EXPERIMENT}.paxoswan.emulab.net"	
	ssh nfsantos@${node} "killall java; cd /tmp/paxos; rm -rf *.log.tgz *.log"
	ssh nfsantos@${node} \
		"cd /tmp/paxos; \
		./startClient.sh $clientsPerNode" &
	# Opening ssh connections too quickly may lead to the destination host refusing connections.
	# Starting clients too quickly can also cause problems loading 
	# shared libraries, the permissions are denied. (NFS problems?)
	sleep 0.4
}

for ((x=n+1; x<=n+clientNodes; x++))
do
	startNode $x &
done


echo `date +%T` " Waiting for ${duration} seconds"
sleep ${duration}


echo "Test ended. Killing clients"
for ((x=0; x<clientNodes; x++))
do	
	node="node$((n+x+1)).${EXPERIMENT}.paxoswan.emulab.net"
	echo "  ${node}"
	ssh nfsantos@${node} "killall java; \
		sleep 0.5; \
		cd /tmp/paxos; \
		mv client-0.log client-$x.log; \
		tar zcf client-${x}.log.tgz *.log"
done

echo "Killing replicas"
for ((x=0; x<n; x++))
do	
	node="node$((x+1)).${EXPERIMENT}.paxoswan.emulab.net"
	echo "  $node"
	ssh nfsantos@${node} "killall java; \
		sleep 0.5; \
		cd /tmp/paxos; \
		mv replica-0.log replica-${x}.log; \
		tar zcf replica-${x}.log.tgz *.log"
done
echo "Done"