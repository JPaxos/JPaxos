#ifndef CONSENSUSLOG_H
#define CONSENSUSLOG_H

#include "consensusinstance.h"

#include <unordered_map>
#include <list>
#include <shared_mutex>

class ConsensusInstance;

class ConsensusLog
{
    std::unordered_map<jint, ConsensusInstance> instances = std::unordered_map<jint, ConsensusInstance>(2048);
    mutable std::shared_mutex instancesMutex;
    
    jint nextId = 0;
    jint lowestAvaialbale = 0;
    
    std::list<jint> decidedBelow;
    
public:
    ConsensusLog(){}
    
    jint append();
    void appendUpTo(jint id);
    ConsensusInstance & getInstanceRw(jint id);
    const ConsensusInstance & getInstanceRo(jint id);
    const ConsensusInstance * getInstanceIfExists(jint id) const;
    void truncateBelow(jint id);
    jintArray clearUndecidedBelow(JNIEnv * env, jint id);
    
    jint getNextId() const {return nextId;}
    jint getLowestAvailable() const {return lowestAvaialbale;}
    
    unsigned instanceCount() const {return instances.size();}
    jintArray instanceIds(JNIEnv * env) const;
    jlong byteSizeBetween(jint fromIncl, jint toExcl) const;
    
    void assertInvariants() const;
    
    void dump(FILE* out) const;
};

#endif // CONSENSUSLOG_H
