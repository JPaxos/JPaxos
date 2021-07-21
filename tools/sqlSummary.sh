#!/bin/bash

# ~/TEMP/sqlg.sh | sqlite3 jpdb.sqlite3 | tr -s '|\n' ' ' | sed 's/ $/\n/'
# HEADER=1 ~/TEMP/sqlg.sh | tr -s '|\n' ' ' | sed 's/ $/\n/'

REPLICAS=$(sqlite3 ${1:-jpdb.sqlite3} 'select max(id) from rps';)

# expected format: TIMES='11 21 29 39 48 58'
TIMES=($TIMES)

color(){
sed 's/\('$1'\)/\o033['${2:-'1;36'}'m\1\o033[0m/'
}

(
	echo -e 'RPS:' $(seq 0 $REPLICAS) '\033[1;30mAVG\033[0m'

	name[0]=' pre'
	name[1]=' mid'
	name[2]='post'

	cond[0]="time >= ${TIMES[0]:-11} and time <= ${TIMES[1]:-21}"
	cond[1]="time >= ${TIMES[2]:-29} and time <= ${TIMES[3]:-39}"
	cond[2]="time >= ${TIMES[4]:-48} and time <= ${TIMES[5]:-58}"

	for w in {0..2}
	do
		echo -n ${name[$w]}
		for id in $(seq 0 $REPLICAS)
		do
			echo -n " "$(sqlite3 ${1:-jpdb.sqlite3} <<< "select ifnull(cast(avg(rps) as INT),'—') from rps where ${cond[$w]} and id=$id;")
		done
		echo -en " \\033[1;30m"$(sqlite3 ${1:-jpdb.sqlite3} <<< "select ifnull(cast(avg(rps) as INT),'—') from rps where ${cond[$w]};")'\033[0m'
		echo
	done
) | column -t -R $(seq 2 $((2+$REPLICAS)) | tr '\n' ',') | color 'RPS:'

echo
sqlite3 -header -separator ' ' jpdb.sqlite3 \
   <<< "select time as 'Leaders:    from [s]',id as 'replica', view from viewchangesucceeded order by time asc;" \
   | column -t -R 1,2,3 | color 'Leaders:'

echo
sqlite3 -header -separator ' ' jpdb.sqlite3 \
   <<< "select id as 'Cach-ups:  replica', run, time as 'start', (select time from catchupend where catchupend.id = catchupstart.id and catchupend.run = catchupstart.run and catchupend.time > catchupstart.time order by time asc limit 1 ) as 'end' from catchupstart order by time asc;" \
   | column -t -R 1,2,3,4 | color 'Cach-ups:'

