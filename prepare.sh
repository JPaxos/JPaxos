#!/bin/bash
if (( $# != 1 && $# != 2 ))
then
	echo 'No args supplied!'
	echo "usage: $0 <instdir> [<clean&build as Y/n>]"
	exit 1
fi

instdir="$1"
build="${2:-n}"
lib="lib/slf4j-api-1.7.5.jar lib/logback-core-1.0.13.jar lib/logback-classic-1.0.13.jar"

fail() {
	echo "$@"
	exit 1
}

echoR(){
	echo -e "\033[31m""$@""\033[00m"
}

if [[  "${build}" = 'Y' || "${build}" = 'y' ]]
then
	echoR "clean"
	ant clean || fail "clean failed"
	echoR "build"
	ant jar || fail "build failed"
else
	echoR "Skipping build"
fi

echoR "Copying to ${instdir}"

(
mkdir -p "${instdir}" && mkdir "${instdir}"/lib &&
cp ${LOGGING_PROPS:-logging.properties} "${instdir}"/logging.properties &&
cp ${PAXOS_PROPS:-paxos.properties} jpaxos.jar `echo ${OTHER_FILES}` "${instdir}"/ &&
cp ${lib} "${instdir}"/lib &&
install jar_mClient.sh "${instdir}"/mClient.sh &&
install jar_replica.sh "${instdir}"/replica.sh
) || fail "install failed"

echoR "done"
