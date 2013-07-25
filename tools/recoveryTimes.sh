#!/bin/bash

database="$1"

db(){
  printcoma=0
  sqlite3 "$database" <<< "$1" | tr '|' ',' |\
  while read line
  do
    (( printcoma )) && echo -n ', '
    echo -n "($line)"
    printcoma=1
  done
}


echo 'create table if not exists tcp ( who, time );'
echo 'create table if not exists recoveryanswer ( who, time );'
echo 'create table if not exists catchupquery ( seq, time );'
echo 'create table if not exists catchupsnapshot ( time, size );'
echo 'create table if not exists catchuprespinst ( seq, time, size, instancesCount );'
echo 'create table if not exists recoveryfinished ( time );'



echo -n "insert into tcp values "
db "select who, min(time) - (select max(time) from recoverystarted) from tcpconnected where run_no=1 group by who;"
echo ";"

echo -n "insert into recoveryanswer values "
db "select sender, time - (select max(time) from recoverystarted) from recoveryanswer where run_no=1;"
echo ";"

echo -n "insert into catchupquery values "
db "select (select count(*) from catchupquery q where q.time <= r.time and q.time > (select time from recoverystarted where run_no=1) ), time - (select time from recoverystarted where run_no=1) t from catchupquery r where t>0 order by time;"
echo ";"

echo -n "insert into catchupsnapshot values "
db "select time - (select time from recoverystarted where run_no=1) t, size from catchupsnapshot where t>0;"
echo ";"

echo -n "insert into catchuprespinst values "
#db "select time - (select time from recoverystarted where run_no=1) t, size, instancesCount from catchuprespinst where t>0;"
db "select (select count(*) from catchupquery q where q.time <= r.time and q.time > (select time from recoverystarted where run_no=1) ) seq, avg(time - (select time from recoverystarted where run_no=1)), sum(size), sum(instancesCount) from catchuprespinst r where time>(select time from recoverystarted where run_no=1) group by seq;"
echo ";"

echo -n "insert into recoveryfinished values "
db "select time - (select time from recoverystarted where run_no=1) from recoveryfinished where run_no=1;"
echo ";"
