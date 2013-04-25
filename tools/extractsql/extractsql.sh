#!/bin/bash

scriptdir=`readlink -f $0`
scriptdir=${scriptdir%/*}

extract()
{
  file=$1
  
  echo "=== $file ==="
  
  pushd $file
  
  if [ -f db.sqlite3.bz2 ]
  then
    rm db.sqlite3
    bunzip2 -k db.sqlite3.bz2
  fi
  
  echo fixtime2
  time sqlite3 db.sqlite3 < ${scriptdir}/fixtime2.sql
  
  echo tables
  time sqlite3 db.sqlite3 < ${scriptdir}/createTables.sql
  
  echo extracts
  time sqlite3 -csv -header db.sqlite3 < ${scriptdir}/time_requests.sql | tail -n+3 > timeRequests
  time sqlite3 -csv -header db.sqlite3 < ${scriptdir}/time_requests2.sql | tail -n+3 > timeRequests2
  time sqlite3 -csv -header db.sqlite3 < ${scriptdir}/time_sm_phases.sql > time_sm_phases
  time sqlite3 -csv -header db.sqlite3 < ${scriptdir}/time_sm_phases2.sql > time_sm_phases2
  
  popd
  
}


while [ "$1" ] 
do
  extract $1
  shift
done
  