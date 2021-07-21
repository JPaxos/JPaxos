#!/bin/bash

# call the ordinary postprocess script
DIR=`readlink -e "$0"`
"${DIR%/*}"/postprocess_default.sh

LOGPATH=`grep -i '^[[:space:]]*LogPath' "$SCENARIO/paxos.properties"  | sed 's/^[^=]*=[[:space:]]*//'`

rm -v /tmp/jpaxos_?/jpaxosLogs/?/decisions.log.?
# fetch & check decision.log
awk '$2~/replica/ && $3~/create/ {print $4 " " $5}' "$SCENARIO/scenario"  | sort | uniq |\
while read HOST ID
do
	if [[ "$HOST" == "$(hostname)" ]]; then continue; fi
	echo "Fetching decisions logs from $HOST"
	mkdir -p /tmp/jpaxos_${ID}/jpaxosLogs/${ID}/
	scp $HOST:"${LOGPATH:-/tmp/jpaxos_${ID}/jpaxosLogs}"/${ID}/decisions.log.'?' /tmp/jpaxos_${ID}/jpaxosLogs/${ID}/
done

echo -e "checking decisions.log.X"
let i=0
lastcntchange=0
lastcnt=0
# line below is in proces substitution, so that vars changed inside loop are visible
#head -qn-1 /tmp/jpaxos_?/jpaxosLogs/?/decisions.log.? | sort -n | uniq -c |\
        while read cnt seqNo hash
        do
            if (( lastcnt == 0 ))
            then
            	lastcnt=$cnt
            fi

        		# gap
        		if ((seqNo > i))
        		then
        			echo "$lastcnt [$lastcntchange-$((i-1))]"
        			echo -e "0 [$i-$((seqNo-1))] \033[01;31mGAP!\033[00m"
        			lastcntchange=$seqNo
					lastcnt=$cnt
        			i=$seqNo
        		fi

				# mismatch
            if ((seqNo != i))
            then
                echo -e "\n\033[01;05;41mERROR AT SEQNO $seqNo\033[00m\n"
                exit 1
            fi

				# change in replica count
        		if (( lastcnt != cnt ))
        		then
        			echo "$lastcnt [$lastcntchange-$((seqNo-1))]"
        			lastcnt=$cnt
        			lastcntchange=$seqNo
        		fi

        let i++
        done  < <(head -qn-1 /tmp/jpaxos_?/jpaxosLogs/?/decisions.log.? | sort -n | uniq -c)
echo "$lastcnt [$lastcntchange-$((i-1))]"
echo -e "decisions.log.X consistent"
