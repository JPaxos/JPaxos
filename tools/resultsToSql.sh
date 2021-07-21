#!/bin/bash

die() {
    echo -e "\033[00;31m""$@""\033[00m"
    exit 1
}


# fetching files

# to prevent fetching files, add to $SCENARIO/config
#export NOFETCH=1
if [ -z "$NOFETCH" ]
then

rm -rf /tmp/jpaxos_*/log.*
rm -rf /tmp/jpaxos_client/systemStats.out_*

LOCK=$(mktemp)
trap "rm -f $LOCK" EXIT
trap "echo -e '\033[00;31mFetching failed\033[00m'; exit 1" USR1

awk '$2~/replica/ && $3~/create/ {print $4 " " $5}' "$SCENARIO/scenario" | sort | uniq |\
while read HOST ID
do
        if [[ "$HOST" == "$(hostname)" ]]; then
                echo -e "\033[00;31mThis host is a part of the scenario, this means that its logs have been wiped (!)\033[00m"
                continue;
        fi
        echo "Started fetching results from $HOST in background..." 1>&2
        mkdir -p /tmp/jpaxos_${ID}/
        flock -s $LOCK bash -c "scp $HOST:/tmp/jpaxos_${ID}/log.'*' /tmp/jpaxos_${ID}/ || kill -USR1 $$ " &
        flock -s $LOCK bash -c "scp $HOST:/tmp/jpaxos_${ID}/systemStats'*'.out /tmp/jpaxos_${ID}/ || kill -USR1 $$ " &
done

mkdir -p /tmp/jpaxos_client/
egrep -v '^[[:space:]]*#' "$SCENARIO/scenario" | awk '$2 == "client" && $4 ~ /create/ {print $3 " " $5 (NF==7?" " $6:" 1")}' | sort | uniq |\
while read NAME HOST CLICOUNT
do
    if [[ "$CLICOUNT" -eq 1 ]]
    then
        echo "Started fetching client ${NAME} net/cpu stats from $HOST in background..." 1>&2
    else
        echo "Started fetching client ${NAME}{0..$((CLICOUNT-1))} net/cpu stats from $HOST in background..." 1>&2
    fi
    flock -s $LOCK bash -c "scp $HOST:/tmp/jpaxos_client/systemStats.out /tmp/jpaxos_client/systemStats.out_${NAME} || kill -USR1 $$ " &
done

fi

flock $LOCK true
rm -f $LOCK
trap EXIT
echo "Fetching completed" 1>&2

STARTTIME=$(head -qn1 /tmp/jpaxos_?/log.* | cut -f1 -d ' ' | sort -n | head -n1)


echo "begin transaction;"

# making database
echo "
create table up                  (id, run, time, realTime);
create table start               (id, run, time, realTime);
create table prepSent            (id, run, time, realTime, view);
create table prepRcvd            (id, run, time, realTime, view);
create table prepOKSent          (id, run, time, realTime, view);
create table prepOKRcvd          (id, run, time, realTime, view, sender);
create table viewChanged         (id, run, time, realTime, view);
create table viewchangeSucceeded (id, run, time, realTime, view);
create table instanceProp        (id, run, time, realTime, inst, reqcnt);
create table instanceDec         (id, run, time, realTime, inst);
create table instanceExec        (id, run, time, realTime, inst);
create table requestExec         (id, run, time, realTime, seqno);
create table recoveryStart       (id, run, time, realTime);
create table recSent             (id, run, time, realTime, msgid);
create table recRcvd             (id, run, time, realTime, msgid, sender);
create table recAckSent          (id, run, time, realTime, msgid, rcpt);
create table recAckRcvd          (id, run, time, realTime, msgid, sender);
create table recoveryEnd         (id, run, time, realTime);
create table recCatchupStart     (id, run, time, realTime);
create table recCatchupEnd       (id, run, time, realTime);
create table catchupStart        (id, run, time, realTime);
create table catchQuerySent      (id, run, time, realTime, rcpt);
create table catchQueryRcvd      (id, run, time, realTime, sender);
create table catchSnapSent       (id, run, time, realTime, rcpt, size);
create table catchSnapRcvd       (id, run, time, realTime, sender);
create table catchSnapUnpacked   (id, run, time, realTime);
create table catchSnapApplied    (id, run, time, realTime);
create table catchRespSent       (id, run, time, realTime, rcpt);
create table catchRespRcvd       (id, run, time, realTime, sender);
create table catchupEnd          (id, run, time, realTime);
create table rps                 (id, run, time, realTime, rps);
create table dps                 (id, run, time, realTime, dps);
create table net                 (id, time, realTime, up, down);
create table cpu                 (id, time, realTime, max, sum, avg);
create table netIptbl            (id, time, realTime, tDown, tUp, rDown, rUp, cDown, cUp);
create table cliNet              (id, time, realTime, up, down);
create table cliCpu              (id, time, realTime, max, sum, avg);
"


for file in /tmp/jpaxos_?/log.*
do

RUN=$(sed 's:^/tmp/jpaxos_.*/log\.\(.*\)$:\1:'<<<$file)
REPLICA=$(sed 's:^/tmp/jpaxos_\(.*\)/log\..*$:\1:'<<<$file)

gawk "
/UP/        {print \"insert into up                  values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/START/     {print \"insert into start               values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/P1A S/     {print \"insert into prepSent            values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/P1A R/     {print \"insert into prepRcvd            values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/VIEW/      {print \"insert into viewChanged         values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/P1B S/     {print \"insert into prepOKSent          values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/P1B R/     {print \"insert into prepOKRcvd          values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \", \" \$5 \");\"; next};
/PREP/      {print \"insert into viewchangeSucceeded values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/IP/        {print \"insert into instanceProp        values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \", \" \$4 \");\"; next};
\$2==\"ID\" {print \"insert into instanceDec         values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/IX/        {print \"insert into instanceExec        values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/XX/        {print \"insert into requestExec         values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/REC B/     {print \"insert into recoveryStart       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/R1 S/      {print \"insert into recSent             values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/R1 R/      {print \"insert into recRcvd             values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \", \" \$5 \");\"; next};
/R2 S/      {print \"insert into recAckSent          values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \", \" \$5 \");\"; next};
/R2 R/      {print \"insert into recAckRcvd          values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \", \" \$5 \");\"; next};
/REC E/     {print \"insert into recoveryEnd         values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CRB/       {print \"insert into recCatchupStart     values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CRE/       {print \"insert into recCatchupEnd       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CB/        {print \"insert into catchupStart        values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CQ S/      {print \"insert into catchQuerySent      values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/CQ R/      {print \"insert into catchQueryRcvd      values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/CS S/      {print \"insert into catchSnapSent       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \", \" \$5 \");\"; next};
/CS R/      {print \"insert into catchSnapRcvd       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/CS U/      {print \"insert into catchSnapUnpacked   values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CS X/      {print \"insert into catchSnapApplied    values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/CR S/      {print \"insert into catchRespSent       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/CR R/      {print \"insert into catchRespRcvd       values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$4 \");\";            next};
/CE/        {print \"insert into catchupEnd          values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \");\";                       next};
/RPS/       {print \"insert into rps                 values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
/DPS/       {print \"insert into dps                 values ($REPLICA, $RUN, \" (\$1 - $STARTTIME) \", \" \$1 \", \" \$3 \");\";            next};
{print \"Unrecognized: \" \$0 > \"/dev/stderr\"};
" $file || die "parsing log failed"

done

for file in /tmp/jpaxos_?/systemStats.out
do
REPLICA=$(sed 's:^/tmp/jpaxos_\(.*\)/systemStats.out$:\1:'<<<$file)
perl -e '
  use List::Util qw(max sum);
  for(<>){
    my @a = split " ";
    my ($t, $u, $d) = splice @a, 0, 3;
    print "insert into net values ('$REPLICA', \"".($t-'$STARTTIME')."\", \"$t\", \"$u\", \"$d\");\n";
    print "insert into cpu values ('$REPLICA', \"".($t-'$STARTTIME')."\", \"$t\", \"". max(@a) . "\", \"" . sum(@a) . "\", \"" . (sum(@a)/@a) . "\");\n";
  }
' $file
done

for file in /tmp/jpaxos_client/systemStats.out_*
do
NAME=${file#/tmp/jpaxos_client/systemStats.out_}
perl -e '
  use List::Util qw(max sum);
  for(<>){
    my @a = split " ";
    my ($t, $u, $d) = splice @a, 0, 3;
    print "insert into cliNet values (\"'$NAME'\", \"".($t-'$STARTTIME')."\", \"$t\", \"$u\", \"$d\");\n";
    print "insert into cliCpu values (\"'$NAME'\", \"".($t-'$STARTTIME')."\", \"$t\", \"". max(@a) . "\", \"" . sum(@a) . "\", \"" . (sum(@a)/@a) . "\");\n";
  }
' $file
done

for file in $(readlink -e /tmp/jpaxos_?/systemStats2.out)
do
REPLICA=$(sed 's:^/tmp/jpaxos_\(.*\)/systemStats2.out$:\1:'<<<$file)
awk "{
print \"insert into netIptbl values ($REPLICA, \" (\$1 - $STARTTIME) \", \" \$1 \", '\" \$2 \"', '\" \$3 \"', '\" \$4 \"', '\" \$5 \"', '\" \$6 \"', '\" \$7 \"');\";
}" $file
done

echo "commit transaction;"
