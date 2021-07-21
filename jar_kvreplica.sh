#!/bin/sh
cd $( dirname $( readlink -f "$0" ) )

# add "/dev/null coredumps" to FILES
if [ -f coredumps ]
then
    ulimit -c unlimited
    echo 0x1b3 > /proc/self/coredump_filter
fi

OPTS=
if [ "$(awk '/^MemTotal: .* kB$/ {printf "%d", $2/1024/1024}' /proc/meminfo)" -le 32 ]
then
    OPTS="$OPTS -Xms1G -Xmx8G"
else
    OPTS="$OPTS -Xms8G -Xmx8G"
fi
OPTS="$OPTS -server"
#OPTS="$OPTS -ea"
OPTS="$OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
OPTS="$OPTS -cp lib/slf4j-api-1.7.26.jar:lib/logback-core-1.2.3.jar:lib/logback-classic-1.2.3.jar:jpaxos.jar"
OPTS="$OPTS -Dlogback.configurationFile=logback.xml"
OPTS="$OPTS -Djava.library.path=."
#OPTS="$OPTS -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
#OPTS="$OPTS -Dcom.sun.management.jmxremote.rmi.port=3334 -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.local.only=false"

if [ -f profile ]
then
    crashes=$(ls log.* 2>/dev/null | wc -l)
    OPTS="$OPTS -XX:+PreserveFramePointer"
    exec 3< profile
    while read line <&3
    do
        # detect & ignore comments
        perl -e 'exit !( "'"$line"'" =~ /^\s*[#\$]/)' && continue
        # parse line
        read id run start duration name opts <<< "$line"
        # continue if id is not ok
        [[ "$id" == "$1" || "$id" == "-1" ]] || continue
        # continue if run is not ok
        [[ "$run" == "$crashes" ]] || continue
        # all fine, spawn background thread that waits and calls perf
        {
            echo "waking in $start seconds for $name" >> profile.log
            sleep $start
            pid=$(pgrep -f lsr.paxos.test)
            [[ "$pid" && -d "/proc/$pid" ]] || ( echo "missing $pid for $name" >> profile.log  exit 1 )
            echo "going to profile $pid for $duration seconds under name $name" >> profile.log
            export PERF_RECORD_SECONDS=$duration
            export PERF_MAP_OPTIONS="unfold"
            export FLAMEGRAPH_DIR=/home/jkonczak/bin/FlameGraph/
            export PERF_JAVA_TMP=`pwd`
            export PERF_DATA_FILE="$PERF_JAVA_TMP/perf-$name.data"
            export PERF_FLAME_OUTPUT="$PERF_JAVA_TMP/flamegraph-$name.svg"
            perf-java-flames $pid --call-graph dwarf,512 -F 99 $opts &>> profile.log
            echo "done with $name" >> profile.log
        }&
        disown
    done
fi


PMEM_FILE=`perl -e 'foreach(<>){print($1) if /^\s*NvmBaseDir\s*=\s*(.*)/;}' paxos.properties`/jpaxos.$1

if [[ -f "$PMEM_FILE" && -x ./dumper ]]
then
   let dumpi=1
   while [[ -e dump.$dumpi ]]; do let dumpi++; done
   ./dumper "$PMEM_FILE" > dump.$dumpi
fi

java $OPTS lsr.paxos.test.HashMapService "$@" 2>&1 | ./process.sh
