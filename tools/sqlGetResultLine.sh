#!/bin/bash

### Usage:
# export MEASURING_PERIODS=...
# HEADER=1 ./sqlg.sh jpdb.sqlite3
# ./sqlg.sh jpdb1.sqlite3
# ./sqlg.sh jpdb2.sqlite3
# ./sqlg.sh jpdb...

######################################
###  configuration  ##################
######################################

# each line of times has format:
#FROM TO PREFIX
# PREFIX can be mpty

if [ -z "$MEASURING_PERIODS" ]
then
MEASURING_PERIODS="
11 21 pre_
29 39 mid_
48 58 post_
"
fi

######################################

DB="${1:-jpdb.sqlite3}"

REPLICA_COUNT=$(sqlite3 "$DB" "select count(distinct id) from start;")
CLIENTS=$(sqlite3 "$DB" "select distinct id from cliCpu;")

######################################

if [ $HEADER ]
then
    while read FROM TO NAME 
    do
        if [ -z "$TO" ]; then continue; fi
        for id in l $(seq 1 $((REPLICA_COUNT-1)) | awk '{print "f"$0}')
        do
            echo -n \
            "$NAME${id}_dps ${id}_devDps"\
            "$NAME${id}_rps ${id}_devRps"\
            "$NAME${id}_maxCpu ${id}_devMaxCpu"\
            "$NAME${id}_avgCpu ${id}_devAvgCpu"\
            "$NAME${id}_ifDown ${id}_ifDevDown"\
            "$NAME${id}_ifUp ${id}_ifDevUp"\
            ""
        done
        for id in $(echo $CLIENTS)
        do
            echo -n \
            "$NAME${id}_maxCpu ${id}_devMaxCpu"\
            "$NAME${id}_avgCpu ${id}_devAvgCpu"\
            "$NAME${id}_ifDown ${id}_ifDevDown"\
            "$NAME${id}_ifUp ${id}_ifDevUp"\
            ""
        done
    done <<< $MEASURING_PERIODS | sed 's/ $//'
    echo
    exit
fi

######################################

function leaderAt() {
    sqlite3 "$DB" "select id from viewchangesucceeded where time < $1 order by view desc limit 1;"
}

function loadStdDev() {
echo ".output /dev/null
SELECT load_extension('./tools/libsqlitefunctions');
.output stdout
"
}

function selectReplica() {
    WHERE="$1"
    (
        loadStdDev
        echo "
            select ifnull(avg(dps),0.0), stdev(dps) from dps where $WHERE;
            select ifnull(avg(rps),0.0), stdev(rps) from rps where $WHERE;
            select avg(max), stdev(max) from cpu where $WHERE;
            select avg(avg), stdev(avg) from cpu where $WHERE;
            select avg(down*8/1000/1000), stdev(down*8/1000/1000) from net where $WHERE;
            select avg(  up*8/1000/1000), stdev(  up*8/1000/1000) from net where $WHERE;
        "
    ) | sqlite3 "$DB"
}

function selectClient() {
    WHERE="$1"
    (
        loadStdDev
        echo "
            select avg(max), stdev(max) from cliCpu where $WHERE;
            select avg(avg), stdev(avg) from cliCpu where $WHERE;
            select avg(down*8/1000/1000), stdev(down*8/1000/1000) from cliNet where $WHERE;
            select avg(  up*8/1000/1000), stdev(  up*8/1000/1000) from cliNet where $WHERE;
        "
    ) | sqlite3 "$DB"
}

######################################

while read FROM TO NAME 
do
    if [ -z "$TO" ]; then continue; fi
    TIMESPEC="time+0 >= $FROM and time+0 <= $TO"
    LEADER=$(leaderAt $FROM)
    
    selectReplica "$TIMESPEC and id=$LEADER"
    
    for (( id = 0 ; id < REPLICA_COUNT ; ++id ))
    do
        if (( LEADER == id)); then continue; fi
        selectReplica "$TIMESPEC and id=$id"
    done
    
    for id in $(echo $CLIENTS)
    do
        selectClient "$TIMESPEC and id=\"$id\""
    done
    
done <<< $MEASURING_PERIODS | tr -s '|\n' ' ' | sed 's/ $//'
echo
