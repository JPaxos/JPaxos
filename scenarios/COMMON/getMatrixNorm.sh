#!/bin/bash
SCRIPTDIR=$(realpath $0)
SCRIPTDIR=${SCRIPTDIR%/*}

${SCRIPTDIR}/getMatrix.pl "$@" |\
awk '
NR==1 {
			for(i=2;i<=NF;++i)
				size[i]=1.0*$i;
			print;
			next
		};
{
	printf "%s", $1;
	for(i=2;i<=NF;++i)
		printf " %f", size[i]*$i;
	printf "\n"
}
'
