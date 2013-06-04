#!/bin/bash

scriptdir=`readlink -f $0`
scriptdir=${scriptdir%/*}

db=$1
outdir=$2

echo "Prarsing: $db  To: $outdir"

echo fixtime2
time sqlite3 $db < ${scriptdir}/fixtime2.sql

echo tables
time sqlite3 $db < ${scriptdir}/createTables.sql

echo extracts
time LD_PRELOAD='/home/jkonczak/jpaxos/tools/libsqlitefunctions.so' sqlite3 -csv -header $db < ${scriptdir}/time_requests.sql | tail -n+3 > $outdir/timeRequests
time LD_PRELOAD='/home/jkonczak/jpaxos/tools/libsqlitefunctions.so' sqlite3 -csv -header $db < ${scriptdir}/time_requests2.sql | tail -n+3 > $outdir/timeRequests2
time sqlite3 -csv -header $db < ${scriptdir}/time_sm_phases.sql > $outdir/time_sm_phases
time sqlite3 -csv -header $db < ${scriptdir}/time_sm_phases2.sql > $outdir/time_sm_phases2
  
