#!/bin/bash

# nodes used as clients and as replicas. Variables: $replicas, $clients, $all
source test_nodes.sh
echo "Replicas: $replicas"
echo "Clients: $clients"

# Window size
WSZ="1"
#NCLIENTS="1000"
#NCLIENTS="100 500 1000"
#NCLIENTS="900"

# Batch size (in bytes)
#REQ_SIZE=8192
#BSZ="8300 17000 35000 70000 140000 270000 540000"
#BSZ="140000"

# BSZ="194 338 626 1202 2354 4658 9266 18482 36914 73778"
# REQ_SIZE=128
# BSZ=1202

#NCLIENTS="600"
#BSZ="1340 2380 4460 8620 16940 33580 66860 133420 266540 532780"
#REQ_SIZE=1024

REQ_SIZE="128"
BSZ=1024001

n=3
NCLIENTS=50
clientNodes=1
# In seconds
testDuration=60


echo "Preparing to run on cluster"
# Local directory to store the results for this model
testdir="results-cluster"
mkdir $testdir

# Write the configuration
config="$testdir/conf.txt"
echo "n=$n" > $config
echo "NCLIENTS=$NCLIENTS" >> $config
echo "WSZ=$WSZ" >> $config
echo "BSZ=$BSZ" >> $config
echo "clientNodes=$clientNodes" >> $config
echo "testDuration=$testDuration" >> $config
echo "REQ_SIZE=$REQ_SIZE" >> $config

# Make sure there are no processes still alive from previous runs

killall java > /dev/null 2>&1 &		

#for ncli in ${NCLIENTS}
#do
answerSize=8
ncli=${NCLIENTS}
for rsz in ${REQ_SIZE}
do
	for wsz in ${WSZ}
	do
		for bsz in ${BSZ}
		do				
			testName="w_${wsz}_b_${bsz}_c_${ncli}_rs_${rsz}"
			clientsPerNode=$((${ncli}/${clientNodes}))			
			
			# Force batching of 2 requests.
			# bsz=$(($rsz*2+$rsz/2))
			echo "********************************************************"
			echo " WndSize=${wsz}, BatchSize=${bsz}, Clients=$((${clientNodes}*${clientsPerNode})) (${clientNodes}*$clientsPerNode), ReqSz=${rsz}, AnsSz=${answerSize}"
			echo "********************************************************"
		
			# testName="w_${wsz}_b_${bsz}_c_${ncli}_reqSz_${cReqSize}"
			# clientsPerNode=$((${ncli}/${clientNodes}))
			# echo "********************************************************"
			# echo " WndSize=${wsz}, BatchSize=${bsz}, Clients=$((${clientNodes}*${clientsPerNode})) (${clientNodes}*$clientsPerNode)"
			# echo "********************************************************"

			echo "Generating configuration: paxos.properties"
			#./makePaxosPropertiesCluster.sh ${wsz} ${bsz} ${rsz} ${answerSize} paxos.properties
			./makePaxosPropertiesCluster.sh ${wsz} ${bsz} ${rsz} ${answerSize} paxos.properties

			FILES="paxos.properties"
			chmod ug+rw $FILES
			
			# Copy to lsec1. 
			echo "Copying configuration files to cluster"				
			# Distribute the configuration to all machines
			for x in $all
			do
				echo "  Copying to lsec${x}"
				ssh lisanguyenquangdo@localhost "cd paxos${x};"
				scp $FILES localhost:~/paxos${x} &
			done
			wait
			
			echo "Starting experiment"
			ssh lisanguyenquangdo@localhost "cd paxos1; rm -rf *.log.gz *.log.tgz;"
			./startClusterRun.sh ${clientsPerNode} ${testDuration}

			echo "Retrieving logs"
			rm -rf ${testdir}/${testName}
			mkdir ${testdir}/${testName}
			# Save the configuration for reference
			cp paxos.properties ${testdir}/${testName}
			cd ${testdir}/${testName}
			scp lisanguyenquangdo@localhost:~/paxos1/*.log.tgz .
			
			# Unpack the client logs
			echo 'Unpacking logs'
			for x in `ls *.log.tgz`
			do			
				tar xf $x
				rm $x
			done
			cd ../..
		done
	done
done

echo "Done"



