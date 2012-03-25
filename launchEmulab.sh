#!/bin/bash

# Window size
WSZ="30 40 50"
#WSZ="1 2"
#NCLIENTS="6 30 99 501 900" 
NCLIENTS="1200"


# Batch size (in bytes)
#BSZ="1024 2048 4096 8192 16384 32768 65536"
#REQ_SIZE="1024 2048 4096 8192 16384 32768"

# For testing increasing w
#BSZ="194 338 626 1202 2354 4658 9266 18482 36914 73778"
#BSZ="18482 36914 73778"
#REQ_SIZE=128

# 532780
#BSZ="1340 2380 4460 8620 16940 33580 66860 133420 266540"
BSZ="33580"
REQ_SIZE=1024

#BSZ="8300 17000 35000 70000 140000 270000"
#REQ_SIZE=8192

EXPERIMENT="paxos"

n=3
clientNodes=3
# In seconds
testDuration=180


echo "Preparing to run on emulab"
# Local directory to store the results for this model
testdir="results-emulab"
mkdir $testdir

# Copy the network topology to the results dir
cp basic.ns $testdir

# Write the configuration
config="$testdir/conf.txt"
echo "n=$n" > $config
echo "NCLIENTS=$NCLIENTS" >> $config
echo "WSZ=$WSZ" >> $config
echo "BSZ=$BSZ" >> $config
echo "clientNodes=$clientNodes" >> $config
echo "testDuration=$testDuration" >> $config
echo "REQ_SIZE=$REQ_SIZE" >> $config

for ((x=1; x<=n+clientNodes; x++))
do
	ssh nfsantos@node${x}.paxos.paxoswan.emulab.net "killall java > /dev/null 2>&1" &
done

#for ncli in ${NCLIENTS}
ncli=${NCLIENTS}
for rsz in ${REQ_SIZE}
do
	for wsz in ${WSZ}
	do
		for bsz in ${BSZ}
		do
			testName="w_${wsz}_b_${bsz}_c_${ncli}_rs_${rsz}"
			clientsPerNode=$((${ncli}/${clientNodes}))
			answerSize=8
			
			echo "********************************************************"
			echo " WndSize=${wsz}, BatchSize=${bsz}, Clients=$((${clientNodes}*${clientsPerNode})) (${clientNodes}*$clientsPerNode), ReqSz=${rsz}, AnsSz=${answerSize}"
			echo "********************************************************"
			
			echo "Generating configuration: paxos.properties"
			#./makePaxosPropertiesEmulab.sh ${wsz} ${bsz} ${cReqSize} paxos.properties
			./makePaxosPropertiesEmulab.sh ${wsz} ${bsz} ${rsz} ${answerSize} paxos.properties

			FILES="paxos.properties"
			chmod ug+rw $FILES
					
			echo "Copying configuration files to emulab"
			# Distribute the configuration to all machines
			for ((x=1; x<=n+clientNodes; x++))
			do
				node="node${x}.paxos.paxoswan.emulab.net"
				echo "  Copying to ${node}"			
				scp $FILES nfsantos@${node}:/tmp/paxos &
			done
			wait

			echo "Starting experiment"
			node="node1.paxos.paxoswan.emulab.net"
			ssh nfsantos@${node} "cd /tmp/paxos; ./startEmulabRun.sh ${n} ${clientNodes} ${clientsPerNode} ${testDuration}"

			echo "Retrieving logs"
			rm -rf ${testdir}/${testName}
			mkdir ${testdir}/${testName}
			# Save the configuration for reference
			cp paxos.properties ${testdir}/${testName}
			cd ${testdir}/${testName}
			for ((x=1; x<=n+clientNodes; x++))
			do
				node="node${x}.${EXPERIMENT}.paxoswan.emulab.net"
				echo "  ${node}"
				scp nfsantos@${node}:/tmp/paxos/*.tgz . &
			done
			wait

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