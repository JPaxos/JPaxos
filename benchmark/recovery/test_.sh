#!/bin/bash
#SBATCH --wait-all-nodes=1 --nodelist="hpc-[2-7]" --time 00:02:00

[[ `hostname` == hpc-2 ]] || exit 0

scenario=${SLURM_JOB_NAME%%_*}

out="`pwd`/${SLURM_JOB_NAME%.sh}.out"

rm -rf "$out"

mkdir "$out"
echo "`date`" > "$out"/date

cd ../..

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
		sed "s/WINSIZE/10/g" |\
		sed "s/NETWORK/TCP/g" |\
		cat > scenarios/temp/paxos.properties

	NOCOLLECT=true SCENARIO=scenarios/temp ./runTestHpc.sh &>> ${out}/runTestHpc.debug

	rm -rf /ramdisk/jpaxos_benchmark

	rm -rf scenarios/temp

	mkdir -p "$out"/$model
	pushd "$out"/$model &> /dev/null

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
