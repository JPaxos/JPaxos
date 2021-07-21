#!/bin/bash

# if this is Pmem, then clean up old files first
CRASHMODEL=$(perl -e 'for(<>){ print $1 if /^\s*CrashModel\s*=\s*(.*)\s*(#.*)?$/;}' "$SCENARIO/paxos.properties")
if [ "Pmem" == "$CRASHMODEL" ]
then
    NVMBASEDIR=$(perl -e 'for(<>){ print $1 if /^\s*NvmBaseDir\s*=\s*(.*)\s*(#.*)?$/;}' "$SCENARIO/paxos.properties")

    LOCKFILE=$(mktemp)
    for HOST in $(awk '$2~/replica/ && $3~/create/ {print $4}' "$SCENARIO/scenario" | sort | uniq)
    do
        echo "cleaning old pmem files in $NVMBASEDIR at $HOST"
        flock -s $LOCKFILE ssh $HOST -- rm -vrf "$NVMBASEDIR"/'jpaxos.*' "$NVMBASEDIR"/'service.*' < /dev/null &
    done
    flock $LOCKFILE true
    echo "cleaning done"
    rm $LOCKFILE
fi

# if this is FullSS, remove old log files
LOGPATH=$(perl -e 'for(<>){ print $1 if /^\s*logpath\s*=\s*(\/.*)\s*$/i ;}' "$SCENARIO/paxos.properties")
if [[ "$LOGPATH" && "FullSS" == "$CRASHMODEL" ]]
then
   LOCKFILE=$(mktemp)
   awk '$2~/replica/ && $3~/create/ {print $4 " " $5}' "$SCENARIO/scenario" | sort | uniq | \
   while read HOST ID
	do
	    echo "cleaning old LogPath in $LOGPATH/$ID at $HOST"
	    flock -s $LOCKFILE ssh $HOST -- rm -vrf "$LOGPATH/$ID" < /dev/null &
	done
	flock $LOCKFILE true
	echo "cleaning done"
	rm $LOCKFILE
fi

LOCKFILE=$(mktemp)

# start system monitoring tools
(
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "replica" && $3 ~ /create/ {print $4 " " $5}'
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "client"  && $4 ~ /create/ {print $5 " client"}'
) | sort | uniq |\
while read LOCATION ID
do
    [[ $LOCATION =~ hpc ]] && \
    flock -s $LOCKFILE ssh $LOCATION -- "sudo /opt/jkonczak/netConfig.sh /tmp/jpaxos_$ID/netConfigStop 100 &> /tmp/jpaxos_$ID/systemStats2.out & disown" < /dev/null &
    [[ $LOCATION =~ pmem ]] && \
    flock -s $LOCKFILE ssh $LOCATION -- "cd /tmp/jpaxos_$ID/ && ( ./systemStats emlx0 100 &> systemStats.out & disown )" < /dev/null &
    [[ $LOCATION =~ cloud ]] && \
    flock -s $LOCKFILE ssh $LOCATION -- "cd /tmp/jpaxos_$ID/ && ( ./systemStats eth4 100 &> systemStats.out & disown )" < /dev/null &
done

flock $LOCKFILE true
rm $LOCKFILE
