#!/bin/bash
testdir="results-cluster"
			testName="w_${wsz}_b_${bsz}_c_${ncli}_rs_${rsz}"
source test_nodes.sh
echo "Replicas: $replicas"
echo "Clients: $clients"

for x in $clients
do	
	echo "  lsec${x}"
	ssh lisanguyenquangdo@localhost "sleep 0.5; \
		cd paxos${x}; \
		mv client-0.log client-lsec${x}.log; \
		tar zcf client-${x}.log.tgz client-*.log; \
		scp client-${x}.log.tgz lisanguyenquangdo@localhost:~/paxos1"
	break
done

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

echo "Done"



