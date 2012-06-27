#!/bin/zsh
if (( $# != 1 && $# != 2 ))
then
	echo 'No args supplied!'
	echo "usage: $0 <instdir> [<clean&build as Y/n>]"
	exit 1
fi

instdir="$1"
build="${2:-n}"

fail() {
	echo "$@"
	exit 1
}

echoR(){
	echo -e "\033[31m""$@""\033[00m"
}

if [[  "${build}" =~ '^[yY]$' ]]
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
mkdir -p "${instdir}" &&
cp logging.properties "${instdir}"/logging.properties &&
cp paxos.properties jpaxos.jar "${instdir}"/ &&
install jar_mClient.sh "${instdir}"/mClient.sh &&
install jar_replica.sh "${instdir}"/replica.sh
) || fail "install failed"

echoR "done"
