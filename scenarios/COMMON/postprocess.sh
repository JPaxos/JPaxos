#!/bin/bash

#egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "replica" && $3 ~ /create/ {print $4 " " $5}' | sort | uniq |\

(
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "replica" && $3 ~ /create/ {print $4 " " $5}'
    egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "client"  && $4 ~ /create/ {print $5 " client"}'
) | sort | uniq |\
while read LOCATION ID
do
    ssh $LOCATION -- "pkill '^systemStats\$'" < /dev/null
    [[ $LOCATION =~ hpc ]] && \
    ssh $LOCATION -- "echo > /tmp/jpaxos_$ID/netConfigStop" < /dev/null
done

echo "creating jpdb.sqlite3"
rm -rf jpdb.sqlite3
tools/resultsToSql.sh | sqlite3 jpdb.sqlite3

echo -e "\nerrs:"
awk '$2~/replica/ && $3~/create/ {print $4 " " $5}' "$SCENARIO/scenario"  | sort | uniq |\
while read HOST ID
do
	ssh $HOST "egrep -i '^[^[:print:]]*(java\.|assert|terminate|exception)' /tmp/jpaxos_${ID}/log.* | sort | uniq" </dev/null
done
echo

tools/sqlSummary.sh
