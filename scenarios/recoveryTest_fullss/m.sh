# produce the results

mkdir results

for CRASH in lc
do
    cat << EOF | while read COLUMN TABLE NAME
rps rps rps
dps  dps dps
max  cpu maxCpu
up*8 net ifUp
EOF
    do
        for PROC in 0 1 2
        do
            echo "$CRASH - $NAME$PROC"
            for DB in $CRASH/*.sqlite3
            do
                sqlite3 $DB <<< "select time, $COLUMN from $TABLE where id=$PROC and time+0>0;";
            done | tr '|' '\t' | ~/jpaxos/tools/movingWindow.pl 0.05 > results/${CRASH}_${NAME}${PROC}_0.05
       done
   done
done
gawk -i inplace '$1-last > 2 {print ""};{last=$1; print}' results/*

