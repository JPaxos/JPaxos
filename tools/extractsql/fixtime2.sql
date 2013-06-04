begin transaction;

update     CatchUpQuery set time = time - ( select diff from correction c where c.replica_id = CatchUpQuery.replica_id);
update    ClientRequest set time = time - ( select diff from correction c where c.replica_id = ClientRequest.replica_id);
update RecoveryFinished set time = time - ( select diff from correction c where c.replica_id = RecoveryFinished.replica_id);
update  CatchUpRespInst set time = time - ( select diff from correction c where c.replica_id = CatchUpRespInst.replica_id);
update          Decided set time = time - ( select diff from correction c where c.replica_id = Decided.replica_id);
update  RecoveryStarted set time = time - ( select diff from correction c where c.replica_id = RecoveryStarted.replica_id);
update       prepairing set time = time - ( select diff from correction c where c.replica_id = prepairing.replica_id);
update  CatchUpRespSnap set time = time - ( select diff from correction c where c.replica_id = CatchUpRespSnap.replica_id);
update     EndExecution set time = time - ( select diff from correction c where c.replica_id = EndExecution.replica_id);
update     SnapshotMade set time = time - ( select diff from correction c where c.replica_id = SnapshotMade.replica_id);
update        prepareok set time = time - ( select diff from correction c where c.replica_id = prepareok.replica_id);
update  CatchUpSnapshot set time = time - ( select diff from correction c where c.replica_id = CatchUpSnapshot.replica_id);
update        Proposing set time = time - ( select diff from correction c where c.replica_id = Proposing.replica_id);
update   StartExecution set time = time - ( select diff from correction c where c.replica_id = StartExecution.replica_id);
update       suspecting set time = time - ( select diff from correction c where c.replica_id = suspecting.replica_id);
update      ClientReply set time = time - ( select diff from correction c where c.replica_id = ClientReply.replica_id);
update   RecoveryAnswer set time = time - ( select diff from correction c where c.replica_id = RecoveryAnswer.replica_id);
update     TcpConnected set time = time - ( select diff from correction c where c.replica_id = TcpConnected.replica_id);

drop table correction;

update     CatchUpQuery set time = time - ( select min(time) from recoverystarted );
update    ClientRequest set time = time - ( select min(time) from recoverystarted );
update RecoveryFinished set time = time - ( select min(time) from recoverystarted );
update  CatchUpRespInst set time = time - ( select min(time) from recoverystarted );
update          Decided set time = time - ( select min(time) from recoverystarted );
update       prepairing set time = time - ( select min(time) from recoverystarted );
update  CatchUpRespSnap set time = time - ( select min(time) from recoverystarted );
update     EndExecution set time = time - ( select min(time) from recoverystarted );
update     SnapshotMade set time = time - ( select min(time) from recoverystarted );
update        prepareok set time = time - ( select min(time) from recoverystarted );
update  CatchUpSnapshot set time = time - ( select min(time) from recoverystarted );
update        Proposing set time = time - ( select min(time) from recoverystarted );
update   StartExecution set time = time - ( select min(time) from recoverystarted );
update       suspecting set time = time - ( select min(time) from recoverystarted );
update      ClientReply set time = time - ( select min(time) from recoverystarted );
update   RecoveryAnswer set time = time - ( select min(time) from recoverystarted );
update     TcpConnected set time = time - ( select min(time) from recoverystarted );
update  RecoveryStarted set time = time - ( select min(time) from recoverystarted );

commit;
