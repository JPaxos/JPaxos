#!/bin/bash
for x in `seq 30 10 1000`
do
	rm -rf scenarios/temp
	cp -r scenarios/nocrash scenarios/temp
	sed "s/200/$x/g" scenarios/nocrash/scenario >scenarios/temp/scenario
	NOCOLLECT=true SCENARIO=scenarios/temp ./runTestHpc.sh &>/dev/null
	rm -rf scenarios/temp
	echo "$((3*$x)) $(ssh hpc2 "cd /tmp/jpaxos_0 && grep 'RPS:' log.0 | awk 'NR>5{sum+=\$2;count++};END{printf(\"%f\",sum/count);}'")" | tee -a testCliPerf.out
done
