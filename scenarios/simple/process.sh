#!/bin/bash
##cd $( dirname $( readlink -f "$0" ) )

num=0;
while [[ -f "log.$num" ]]
do
	let num++
done

# while read x
# do
# 	echo $x >> "log.$num"
# done
# exit

tee "full.$num" |\
awk '
/Suspecting [0-9]+ on view/                                                          {print; next;}
/Preparing view/                                                                     {print; next;}
/View prepared/                                                                      {print; next;}
/Proposing:/                                                                         {print; next;}
/Decided/                                                                            {print; next;}
/Received client request/                                                            {print; next;}
/Scheduling sending reply/                                                           {print; next;}
/Snapshot received from state machine/                                               {print; next;}
/Passing request/                                                                    {print; next;}
/execution took/                                                                     {print; next;}
/Executing instance/                                                                 {print; next;}
/Received \[[0-9]+\] (PrepareOK|CatchUpResponse|CatchUpSnapshot|RecoveryAnswer)/     {print; next;}
/Recovery phase started/                                                             {print; next;}
/Tcp connected/                                                                      {print; next;}
/Recovery phase finished/                                                            {print; next;}
/Sent CatchUpQuery/                                                                  {print; next;}
' > "log.$num"

# Suspecting X on view Y
# Preparing view X
#
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
# replica run_no start  
# 
