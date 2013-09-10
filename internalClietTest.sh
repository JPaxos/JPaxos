#!/bin/bash
source $([[ $0 =~ "/" ]] && echo ${0%/*}/common.sh || echo common.sh) 
java ${OPTS} lsr.paxos.test.InternalClientTest "${1}" "${2:-100}" "${3:-10}" "${4:-1024}"
