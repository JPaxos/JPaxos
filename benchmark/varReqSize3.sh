#!/bin/bash
#SBATCH --wait-all-nodes=1 --nodelist="hpc-[2-7]" --time 01:00:00

[[ `hostname` == hpc-2 ]] || exit 0

out="`pwd`/${SLURM_JOB_NAME%.sh}.out"
echo "# `date`" > $out

cd ..

for x in `seq 16 16 512 | awk '{for (i=0;i<3;++i) print;}' | shuf`
do
	rm -rf scenarios/temp
	cp -r scenarios/nocrash scenarios/temp

	cat scenarios/nocrash/scenario |\
		sed "s/REQSIZE/$x/g" |\
		sed "s/CLINO/333/g" |\
		cat > scenarios/temp/scenario

	cat scenarios/nocrash/paxos.properties |\
		sed "s/WINSIZE/10/g" |\
		sed "s/NETWORK/NIO/g" |\
		cat > scenarios/temp/paxos.properties

	NOCOLLECT=true SCENARIO=scenarios/temp ./runTestHpc.sh &>/dev/null # &>> ${out}.debug

	rm -rf scenarios/temp

	echo "$x $(ssh hpc-2 "cd /tmp/jpaxos_0 && grep 'RPS:' log.0 |\
	     awk 'NR>50{sum+=\$3;count++};END{printf(\"%f\",sum/count);}'")" |\
	     cat >> $out
	     #tee -a $out
done
