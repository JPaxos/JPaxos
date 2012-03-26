XXX - is used to stand for either emulab or cluster. 

Main scripts ************ 
- launchXXX.sh 
- deployXXX.sh 

Deploying 
********* 
Scripts: deployXXX.sh, distributeEmulab.sh 

Using the script deployXXX.sh, copy the binary files and the auxiliary 
scripts to all the experimental nodes. This needs to 
be done only when the scripts or the .jar files change. If only the 
experimental parameters are changed, there's no need to redeploy. 

In the case of emulab, the files are first copied to one of the remote 
nodes and then from this node to all the other nodes, using the script 
distributeEmulab.sh. This is faster than copying directly to each node. 
In the case of the cluster the files are copied directly to each node. 



Launching 
**********
Scripts: launchXXX.sh, makePaxosPropertiesXXX.sh, startXXXrun.sh

The scripts start a series of experiments either on the cluster or on 
emulab. There are three parameters that can be defined at 
the top of the scripts: 
	- WSZ - window size. Bound on the number of parallel instances. 
	- NCLIENTS - number of clients that will connect to the replicas 
	- BSZ - Batch size. Bound on the maximum batch size. In bytes. 

These properties consist of lists of values. The script will run an 
experiment for each possible triplet of (WSZ, NCLIENTS, BSZ). Therefore, 
the number of experiments is WSZ*NCLIENTS*BSZ. 

The following additional parameters control the experimental 
environment: 
- n - number of replicas 
- clientNodes - number of nodes used to run clients. The clients will be 
evenly distributed among these nodes until reaching the NCLIENTS parameter. 
- testDuration - how long to run each test. 

The script will then do the following: 
- create a local directory to store the results, where the output of each 
test will be put in a separate subdirectory. 
- Connect to the experiment nodes and kill any process that 
might have been left from previous experiments. 
- For each test (triplet of the experimental parameters) 
	- create the paxos.properties file (using the script makePaxosPropertiesXXX.sh) 
	- copy the configuration files to the nodes start the experiment by running 
	the script startXXXRun.sh on the head of the remote nodes. 
	- wait for the experiment to terminate copy the output of the test. 

The script startXXXRun.sh starts the experiments on the cluster/emulab. This script 
	is executed by one of the experimental nodes. The script starts the processes 
	on each node, waits for the specified time, kills the processes, retrieves the 
	output to the main node, and then returns. The main script, launchXXX.sh, will 
	then continue and copy the results from the main node. 


*****************	
Analyzing results 
*****************
I'm using Matlab scripts to analyse the results. 

The analysis is done in two passes: 1) parsing and summarizing the 
results, 2) generating the plots. 

The first step is time consuming but needs to be done only once, while 
the second is fast and can be done as many times as need to generate 
different plots. 

Summarizing 
***********
Scripts: summarizeAll.m, summarizeClients.m, summarizeReplicas.m, filterData.m, 

- Clients: For each subdirectory on the given directory, it parses the 
files "client-*.stats.log", removes the first x% of the run using the 
script filterData.m, and generates several statistics from the raw data 
and saving them on the file client.stats.txt 
- Replicas: Similar as above, but uses the files replica-*stats.log and 
replicas.stats.txt, as input and output respectively. 

Generating plots 
****************
Scripts: genAllGraphs.m, genGraphsXXX.m, loadGraphsSettings.m, loadReplicaSummary.m, 
loadClientSummary.m 

These are tailored for the type of plots to be produced. Must be adapted 
on a case by case basis. 

