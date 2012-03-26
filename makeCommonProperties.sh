#!/bin/bash
if (( $# != 5 ))
then
    echo "Usage: $0 <WindowSize> <BatchSize> <reqSize> <answerSize> <configFile>"
    exit 1
fi

WINDOW_SIZE=$1
BATCH_SIZE=$2
CLIENT_REQUEST_SIZE=$3
SERVICE_ANSWER_SIZE=$4
OUT=$5
MAX_UDP=65507
MAX_BATCH_DELAY=50

echo "CrashModel=CrashStop" >> $OUT
echo "WindowSize=${WINDOW_SIZE}" >> $OUT
echo "BatchSize=${BATCH_SIZE}" >> $OUT
echo "Network=TCP" >> $OUT
echo "MaxBatchDelay=${MAX_BATCH_DELAY}" >> $OUT
echo "MaxUDPPacketSize=${MAX_UDP}" >> $OUT
echo "BusyThreshold=20240" >> $OUT
echo "ClientRequestSize=${CLIENT_REQUEST_SIZE}" >> $OUT
echo "ServiceAnswerSize=${SERVICE_ANSWER_SIZE}" >> $OUT
echo "MayShareSnapshots=true" >> $OUT
echo "ClientIDGenerator=Simple" >> $OUT
echo "BenchmarkRun=true" >> $OUT
echo "FDSuspectTimeout=500" >> $OUT
echo "FDSendTimeout=200" >> $OUT