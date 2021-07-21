#!/bin/zsh

export LC_NUMERIC=C

PUTSIZE=$((16*1024))
AVGSIZE=$(((PUTSIZE+9)/2))

echo "bs avg_bs fill avg_puts rpsMb rps dps"
paste -d ' '	<(
						# RPS
						cut "$@" -d ' ' -f 1,5,17,29  | sort -n | awk '{if(last==$1){count+=3; sum+=$2+$3+$4;} else {if(sum)printf("%d %f\n", last, sum/count); count=3; sum=$2+$3+$4; last=$1;}} END {if(sum)printf("%d %f\n", last, sum/count);}'
				   ) <(
				   	# DPS
						cut "$@" -d ' ' -f 1,3,15,27  | sort -n | awk '{if(last==$1){count+=3; sum+=$2+$3+$4;} else {if(sum)printf("%f\n", sum/count); count=3; sum=$2+$3+$4; last=$1;}} END {if(sum)printf("%f\n", sum/count);}'
					) |\
	while read BS RPS DPS
	do
		RpsNormMb=$((RPS*AVGSIZE/1024/1024.0))
		AvgBatchSize=$((RPS*AVGSIZE/DPS/1.0))
		AvgPutCnt=$((AvgBatchSize/PUTSIZE/1.0))
		BatchFillPercent=$((AvgBatchSize/BS*100.0))
		printf "%d %d %.1f%% %.2f " $BS $AvgBatchSize $BatchFillPercent $AvgPutCnt
		printf "%.2f %d %d\n" $RpsNormMb $RPS $DPS
	done

