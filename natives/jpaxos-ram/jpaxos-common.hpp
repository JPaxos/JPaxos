#ifndef JPAXOS_COMMON_H
#define JPAXOS_COMMON_H


#include <unistd.h>
#include <cstdio>

#include <jni.h>

class PaxosStorage;
class ReplicaStorage;
class ConsensusLog;
class ServiceProxyStorage;

extern ConsensusLog * consensusLog;
extern PaxosStorage * paxosStorage;
extern ReplicaStorage * replicaStorage;
extern ServiceProxyStorage * serviceProxyStorage;

const unsigned char & numReplicas();
const unsigned char & majority();
const unsigned char & localId();

#include "paxosstorage.h"
#include "replicastorage.h" 
#include "consensuslog.h"
#include "serviceproxystorage.h"

#endif // JPAXOS_COMMON_H
