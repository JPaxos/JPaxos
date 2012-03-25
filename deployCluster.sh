#!/bin/bash
# Copies to the cluster the files required to run Paxos. 

source test_nodes.sh

echo "Replicas: $replicas"
echo "Clients: $clients"


# Number of nodes reserved for replicas
nodes=10

#cp jpaxos_experiments.jar jpaxos.jar
#cp Bench_experiments.jar Bench.jar
#FILES='Bench.jar jpaxos.jar startReplica.sh startClient.sh log-client.properties log-replica.properties paxos.properties'
FILES='Bench.jar jpaxos.jar startClient.sh startReplica.sh log-client.properties log-replica.properties paxos.properties'

# Distribute the configuration to all machines
#for ((x=1; x<=nodes; x++))
#do
	#echo "Copying to lsec${x}"
	#ssh desousa@lsec${x} "rm -rf paxos; mkdir paxos"
	#scp $FILES desousa@lsec${x}:~/paxos
	#ssh desousa@lsec${x} "cd paxos; chmod ug+rw *; chmod a+x *.sh"
#done

echo 'Copying files to lsec1'
ssh lisanguyenquangdo@localhost -D 2012 "rm -rf paxos1; mkdir paxos1"
scp $FILES lisanguyenquangdo@localhost:~/paxos1
ssh lisanguyenquangdo@localhost "cd paxos1; chmod ug+rw *; chmod a+x *.sh"
# Distribute the configuration to all machines
for x in $all
do
	echo "Copying to ${x}"
	#I=2012
	#I=$(( ${I} + ${x} )) 
	#-D ${I}
	ssh lisanguyenquangdo@localhost "rm -rf paxos${x}; mkdir paxos${x}; scp -r localhost:~/paxos1/* paxos${x}/" &
done

wait
