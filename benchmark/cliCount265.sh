#!/bin/bash
#SBATCH --wait-all-nodes=1 --nodelist="hpc-[2-7]" --time 01:15:00

[[ `hostname` == hpc-2 ]] || exit 0

out="`pwd`/${SLURM_JOB_NAME%.sh}.out"
echo "# `date`" > $out

cd ..

for x in `seq 1000 300 4000 | awk '{for (i=0;i<10;++i) print;}' | tac`
do
	rm -rf scenarios/temp
	cp -r scenarios/nocrash scenarios/temp

	cat scenarios/nocrash/scenario |\
		sed "s/REQSIZE/256/g" |\
		sed "s/CLINO/$x/g" |\
		cat > scenarios/temp/scenario

	cat scenarios/nocrash/paxos.properties |\
		sed "s/WINSIZE/10/g" |\
		sed "s/NETWORK/NIO/g" |\
		cat > scenarios/temp/paxos.properties

	NOCOLLECT=true SCENARIO=scenarios/temp ./runTestHpc.sh &>/ramdisk/jpaxos_benchmark # &>> ${out}.debug

	rm -rf /ramdisk/jpaxos_benchmark

	rm -rf scenarios/temp

	echo "$((x*3)) $(ssh hpc-2 "cd /tmp/jpaxos_0 && grep 'RPS:' log.0 |\
	     awk 'NR>50{sum+=\$3;count++};END{printf(\"%f\",sum/count);}'")" |\
	     cat >> $out
	     #tee -a $out
done
