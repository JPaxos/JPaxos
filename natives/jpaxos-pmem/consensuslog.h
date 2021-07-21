#ifndef CONSENSUSLOG_H
#define CONSENSUSLOG_H

#include "jpaxos-common.hpp"
#include "consensusinstance.h"

#include <vector>

class ConsensusInstance;

class ConsensusLog
{
    // TODO: what performs better: 
    //  - hashmap_persistent<jint, ConsensusInstance>
    //  - hashmap_persistent<jint, persistent_ptr<ConsensusInstance>>
    hashmap_persistent_synchronized<jint, ConsensusInstance> instances = hashmap_persistent_synchronized<jint, ConsensusInstance>(*pop, 2048);
    
    pm::p<jint> nextId = 0;
    pm::p<jint> lowestAvaialbale = 0;
    
    linkedqueue_persistent<jint> decidedBelow;
    
public:
    ConsensusLog(){}
    
    jint append();
    void appendUpTo(jint id);
    ConsensusInstance & getInstanceRw(jint id);
    const ConsensusInstance & getInstanceRo(jint id);
    const ConsensusInstance * getInstanceIfExists(jint id);
    void truncateBelow(jint id);
    jintArray clearUndecidedBelow(JNIEnv * env, jint id);
    
    jint getNextId() const {return nextId;}
    jint getLowestAvailable() const {return lowestAvaialbale;}
    
    unsigned instanceCount() const {return instances.count();}
    jintArray instanceIds(JNIEnv * env) const;
    jlong byteSizeBetween(jint fromIncl, jint toExcl) const;
    
    void assertInvariants() const;
    
    void dump(FILE* out) const;
};

#endif // CONSENSUSLOG_H
