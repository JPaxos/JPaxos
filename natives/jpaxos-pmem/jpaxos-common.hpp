#ifndef JPAXOS_COMMON_H
#define JPAXOS_COMMON_H

#undef DEBUG_LASTREPLYFORCLIENT
#undef DEBUG_TX

#if defined(DEBUG_LASTREPLYFORCLIENT) || defined(DEBUG_TX)
    #include <stdlib.h>
    #include <time.h>
    #include <sys/syscall.h>
    #define gettid() syscall(SYS_gettid)
    extern FILE * debugLogFile;
#endif

#include <unistd.h>
#include <cstdio>

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include <jni.h>

#include "../common/hashmap_persistent_synchronized.hpp"
#include "../common/hashset_persistent.hpp"
#include "../common/linkedqueue_persistent.hpp"

namespace pm = pmem::obj;

struct root;
class PaxosStorage;
class ReplicaStorage;
class ConsensusLog;
class ServiceProxyStorage;

extern pm::pool<root> * pop;

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

extern "C" {
    void dumpJpaxosPmem(char * pmemFile, FILE * outFile);
}

#endif // JPAXOS_COMMON_H
