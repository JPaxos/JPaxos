#!/bin/bash

ssh nfsantos@users.emulab.net "/usr/testbed/bin/loghole -e PaxosWAN/Paxos sync;"
mkdir emulab-logs
scp nfsantos@users.emulab.net:/proj/PaxosWAN/exp/Paxos/logs/tbdelay0/local/logs/* emulab-logs
scp nfsantos@users.emulab.net:/proj/PaxosWAN/exp/Paxos/logs/tbdelay1/local/logs/* emulab-logs
