#!/bin/bash
# makes last piped whiles behave better
shopt -s lastpipe

die() {
    echo -e "\033[00;31m""$@""\033[00m"
    exit 1
}

if [[ $# != 1 ]]
then
    echo "Usage: $0 <path/to/scenario>"
    exit 1
fi

export SCENARIO="$1"

echo "=== Scenario $SCENARIO ==="

. "$SCENARIO/config" || die "failed to configure the scenario!"

if [[ -z "$BUILD" || -z "$FILES" ]]
then
    die "env vars missing"
fi

##############################################################################
### build

# bash cannot exteded regex
if ( echo "$BUILD" | perl -e 'exit!( <> =~ /^(y|yes)$/i )' )
then
    echo "== building =="
    ant clean jar || die "ant failed"
    pushd natives/build
    make || die "natives make failed"
    popd
    echo "built fine"
else
    echo "== skipping build =="
fi

##############################################################################
### prepare template

echo "== preparing template =="

instdir="/tmp/jpaxosTemplate"
rm -rf "${instdir}"

mkdir "${instdir}" || die "Cannot create directory "${instdir}""

FILES="
jpaxos.jar
logback.xml
paxos.properties
lib/slf4j-api-1.7.26.jar
lib/logback-core-1.2.3.jar
lib/logback-classic-1.2.3.jar
$FILES"

echo "$FILES" | while read FROM TO
do
    if ( echo "$FROM" | perl -e 'exit!( <> =~ /^\s*(#|$)/ )'); then continue; fi

    TO="${instdir}/${TO:-$FROM}"
    if [[ -e "$SCENARIO/$FROM" ]]
    then
        FROM="$SCENARIO/$FROM"
    fi
    install -v -D -m $(stat -c%a "${FROM}") "${FROM}" "$TO" || die "Failed to copy $FROM to $TO"
done

##############################################################################
### copy template everywhere

LOCK1=$(mktemp)
#LOCK2=$(mktemp)

echo "== deploying template =="

(
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "replica" && $3 ~ /create/ {print $4 " " $5}'
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "client"  && $4 ~ /create/ {print $5 " client"}'
) | sort | uniq |\
while read LOCATION ID
do
    if ( echo "$LOCATION" | perl -e 'exit!( <> =~ /^[×xX.·-–—]*$/ )')
    then
        # local
        echo      "cloning ${instdir} to ${TARGET:-/tmp}/jpaxos_$ID"
        rsync -a --delete --force "${instdir}/" "${TARGET:-/tmp}/jpaxos_$ID" || die "failed"
    else
        # remote
        echo      "cloning ${instdir} to $LOCATION:${TARGET:-/tmp}/jpaxos_$ID"
        flock -s $LOCK1 bash -c "rsync -a --delete --force \"${instdir}/\" \"$LOCATION:${TARGET:-/tmp}/jpaxos_$ID\" || kill $$" &
        #flock -s $LOCK2 ssh $LOCATION -- "ntpdate -qp 8 pool.ntp.org > ${TARGET:-/tmp}/jpaxos_$ID/ntpdate.out" &
    fi
done

#if [ "$WAIT_NTPDATE" ]
#then
#    echo "waiting for ntpdate(s) to complete..."
#    flock $LOCK2 true
#fi

echo "waiting for deploy(s) to complete..."
flock $LOCK1 true
echo "deploy(s) completed"

rm -f $LOCK1 $LOCK2

if [ -e "${SCENARIO}/preprocess.sh" ]
then
    # this shall intentionally die if postprocess.sh is not executable
    echo "== running ${SCENARIO}/preprocess.sh =="
    "${SCENARIO}/preprocess.sh" || die "Preprocessing failed"
elif [ -e "${instdir}/preprocess.sh" ]
then
	echo "== running ${instdir}/preprocess.sh =="
	"${instdir}/preprocess.sh" || die "Preprocessing failed"
fi


##############################################################################
### run the benchmark

echo "== running benchmark =="

java -cp tools/benchmark.jar benchmark.Benchmark "${SCENARIO}/scenario"

echo -e "\033[00m"

##############################################################################
### postprocess

if [ -e "${SCENARIO}/postprocess.sh" ]
then
    # this shall intentionally die if postprocess.sh is not executable
    echo "== running ${SCENARIO}/postprocess.sh =="
    "${SCENARIO}/postprocess.sh" || die "Postprocessing failed"
elif [ -e "${instdir}/postprocess.sh" ]
then
	echo "== running ${instdir}/postprocess.sh =="
	"${instdir}/postprocess.sh" || die "Postprocessing failed"
fi

echo "== done =="
