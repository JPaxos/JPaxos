#!/bin/bash
#SBATCH --wait-all-nodes=1 --nodelist="hpc-[2-7]" --time 12:00:00

[[ `hostname` == hpc-2 ]] || exit 0

scenario=${SLURM_JOB_NAME%%_*}

out="`pwd`/${SLURM_JOB_NAME%.sh}.out"

mkdir "$out"
echo "`date`" > "$out"/date

cd ../..

for i in {1..400}
do
for model in FullSS ViewSS EpochSS
do
	rm -rf scenarios/temp
	cp -r scenarios/$scenario scenarios/temp

	cat scenarios/$scenario/scenario |\
		sed "s/REQSIZE/1024/g" |\
		sed "s/CLINO/700/g" |\
		cat > scenarios/temp/scenario

	cat scenarios/$scenario/paxos.properties |\
		sed "s/CRASH_MODEL/${model}/g" |\
		sed "s/WINSIZE/3/g" |\
		sed "s/NETWORK/TCP/g" |\
		cat > scenarios/temp/paxos.properties

	NOCOLLECT=true SCENARIO=scenarios/temp ./runTestHpc.sh &>/ramdisk/jpaxos_benchmark # &>> ${out}.debug

	rm -rf /ramdisk/jpaxos_benchmark

	rm -rf scenarios/temp

	mkdir -p "$out"/$model/$i
	pushd "$out"/$model/$i &> /dev/null

	for r in 0 1 2
	do
		machine="hpc-$((r+2))"
		scp ${machine}:/tmp/jpaxos_${r}/log.* .
		for l in log.*
		do
			mv $l $r.${l#*.}
		done
	done

	popd &> /dev/null

done
done
