begin transaction;

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
