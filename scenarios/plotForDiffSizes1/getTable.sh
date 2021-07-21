#!/bin/sh
L=`echo Paxos+{EpochSS,FullSS} P+pmem@{RAM,pmem} pmemP@{RAM,emulp,pmem}`
echo "avgReqSize $L"
paste $L | sed 's/\t[^ ]*//g'
