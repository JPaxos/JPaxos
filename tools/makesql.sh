
REP_ID=$1
RUN_NO=$2

echo 'create table if not exists suspecting       ( replica_id, run_no, time, suspected_id, view);'
echo 'create table if not exists prepairing       ( replica_id, run_no, time, view);'
echo 'create table if not exists prepareok        ( replica_id, run_no, time, sender, view, instancesCount, size);'
echo 'create table if not exists CatchUpRespInst  ( replica_id, run_no, time, sender, view, instancesCount, size);'
echo 'create table if not exists CatchUpRespSnap  ( replica_id, run_no, time, sender, view);'
echo 'create table if not exists CatchUpSnapshot  ( replica_id, run_no, time, sender, view, size);'
echo 'create table if not exists RecoveryAnswer   ( replica_id, run_no, time, sender);'
echo 'create table if not exists ClientRequest    ( replica_id, run_no, time, clientId, reqId);'
echo 'create table if not exists Proposing        ( replica_id, run_no, time, instanceId);'
echo 'create table if not exists Decided          ( replica_id, run_no, time, instanceId);'
echo 'create table if not exists InstReq          ( instanceId , clientId, reqId);'
echo 'create table if not exists StartExecution   ( replica_id, run_no, time, clientId, reqId);'
echo 'create table if not exists EndExecution     ( replica_id, run_no, time, clientId, reqId, reportedTime);'
echo 'create table if not exists ClientReply      ( replica_id, run_no, time, clientId, reqId);'
echo 'create table if not exists SnapshotMade     ( replica_id, run_no, time, instanceId, estSize);'
echo 'create table if not exists RecoveryStarted  ( replica_id, run_no, time);'
echo 'create table if not exists RecoveryFinished ( replica_id, run_no, time);'
echo 'create table if not exists TcpConnected     ( replica_id, run_no, time, who);'
echo 'create table if not exists CatchUpQuery     ( replica_id, run_no, time, target, ifSnapshot);'

echo "begin transaction;"

sed 's/id=/ /g' - | tr -s '\[\]\(\):=,;' ' ' | awk '
/Suspecting [0-9]+ on view/ {printf("insert into suspecting values (%s,%s,%s,%s,%s);\n", "'$REP_ID'","'$RUN_NO'",$1,$3,$6); next;}
/Preparing view/ {printf("insert into prepairing values (%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$4); next;}
/Received [0-9]+ PrepareOK/ {printf("insert into prepareok values (%s,%s,%s,%s,%s,%s,%s);\n;","'$REP_ID'","'$RUN_NO'",$1,$3,$6,(NF-9)/6,$NF); next;}
/Received [0-9]+ CatchUpResponse v/ {printf("insert into CatchUpRespInst values (%s,%s,%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3,$6,int((NF-10)/6),$NF); next;}
/Received [0-9]+ CatchUpResponse -/ {printf("insert into CatchUpRespSnap values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3,$10); next;}
/Received [0-9]+ CatchUpSnapshot/   {printf("insert into CatchUpSnapshot values (%s,%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3,$6,$NF); next;}
/Received [0-9]+ RecoveryAnswer/    {printf("insert into RecoveryAnswer values (%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3); next;}
/Received client request/ {printf("insert into ClientRequest values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$5,$6); next;}
/Proposing/ {printf("insert into Proposing values (%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3); next;}
/Decided [0-9]+/  {printf("insert into Decided values (%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3); next;}
/Passing request/ {printf("insert into StartExecution values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$9,$10); next;}
/execution took/ {printf("insert into EndExecution values (%s,%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$3,$4,$7); next;}
/Scheduling sending reply/  {printf("insert into ClientReply values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$5,$6); next;}
/Snapshot received from state machine/ {printf("insert into SnapshotMade values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$8,$14); next;}
/Recovery phase started/ {printf("insert into RecoveryStarted values (%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1); next;}
/Recovery phase finished/ {printf("insert into RecoveryFinished values (%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1); next;}
/Tcp connected/ {printf("insert into TcpConnected values (%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,$4); next;}
/Sent CatchUpQuery.*snapshot/ {printf("insert into CatchUpQuery values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,substr($NF,2),"1"); next;}
/Sent CatchUpQuery.*instances/ {printf("insert into CatchUpQuery values (%s,%s,%s,%s,%s);\n","'$REP_ID'","'$RUN_NO'",$1,substr($NF,2),"0"); next;}
/Executing instance/ {printf("insert into InstReq values (%s,%s,%s);\n",$4,$5,$6); for(i=7;i<NF;i+=2) printf("insert into InstReq values (%s, %s, %s);\n", $4, $i, $(i+1)); next;}
'

echo "commit;"


# 7 -> nic
# 10 -> jedno
# 13 -> dwa

# echo 'create table client (replica run_no clientId instanceId receivedTS proposingTS decidedTS executedTS replyTS)'

# echo 'create table suspecting (replica, run_no, time, whom, view)'
# echo 'create table prepairing (replica, run_no, time, view)'
# echo 'create table prepareOKrecv (replica, run_no, time, from, view, values, size)'
# sed "
# s/\([0-9]*\).*Suspecting \([0-9]*\) on view \([0-9]*\)/insert into suspecting values ($REPLICA_ID, $RUN_NO, \1, \2, \3)/
# s/\([0-9]*\).*Preparing view: \([0-9]*\)/insert into preparing values ($REPLICA_ID, $RUN_NO, \1, \2)/
# s/\([0-9]*\).*Received [\([0-9]*\)] PrepareOK(v:\([0-9]*\), values: [\(.*\)]) size: \([0-9]*\)/ insert into prepareOKrecv ($REPLICA_ID, $RUN_NO, \1, \2, \3, \"\4\", \5 )/
# 
# 
# " -



# Received [X] PrepareOK ...
# View prepared X
# Advancing to view X, Leader=Y
#
# Received client request:
# Proposing: ...
# Decided X, Log Size Y
# <Service> Executed request no.X
# Sending reply to client ...
#
# Snapshot received from state machine
#
# Recovery phase started                <- replika UP
# Tcp connected X                       <- połączenie UP
# 
# Received [X] RecoveryAnswer           <- 
# Recovery phase finished               <- replika operational
# 
# Sent CatchUpQuery ( ... to [pX]
# Received [X] CatchUpResponse - only snapshot available
# Received [X] CatchUpResponse (
# 
# Sent CatchUpQuery for snapshot ...  to [pX]
# Received [X] CatchUpSnapshot
# 
# 
# 
# For every client request:
# replica run_no clientId instanceId receivedTS proposingTS decidedTS executedTS replyTS
# 
# For every replica:
# replica run_no start event info time
# event: 
