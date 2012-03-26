#!/bin/bash
if (($# != 2))
then
	# ${clientsPerNode} ${testduration}"
	echo "Usage: $0 <clientsPerNode> <duration>"
	exit 1
fi

source test_nodes.sh

clientsPerNode=$1
duration=$2

echo "Launching replicas"
i=0
for x in $replicas
do	
	echo "  Replica $i - lsec$x"
	ssh lisanguyenquangdo@localhost "cd paxos${x}; sleep 1; rm -rf *.log.*; rm -rf *.log; ./startReplica.sh ${i}" &
	i=$((i+1))
done

# Wait for replicas to start and stabilize
echo "Waiting 2 seconds for replicas to start"
sleep 2

echo "Launching clients"
for x in $clients
do	
	echo "  lsec${x}"
	ssh lisanguyenquangdo@localhost "cd paxos${x}; rm -rf *.log.tgz *.log; ./startClient.sh $clientsPerNode" &
done

echo `date +%T` " Waiting for ${duration} seconds"
sleep ${duration}


echo "Test ended. Killing clients"
for x in $clients
do	
	echo "  lsec${x}"
	ssh lisanguyenquangdo@localhost "sleep 0.5; \
		cd paxos${x}; \
		mv client-0.log client-lsec${x}.log; \
		tar zcf client-${x}.log.tgz client-*.log; \
		scp client-${x}.log.tgz lisanguyenquangdo@localhost:~/paxos1"
done

echo "Killing replicas"
i=0
for x in $replicas
do	
	echo "  lsec${x}"
	ssh lisanguyenquangdo@localhost "sleep 0.5; \
		cd paxos${x}; \
		mv replica-0.log replica-${i}.log; \
		tar zcf replica-${i}.log.tgz replica-*.log; \
		scp *.tgz lisanguyenquangdo@localhost:~/paxos1"
	i=$((i+1))
done
# Not needed anymore that JPaxos is giving the correct replica id to the log files
# mv replica-0.log replica-${i}.log;

killall java

echo "Done"