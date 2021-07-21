#ifndef PAXOSSTORAGE_H
#define PAXOSSTORAGE_H

#include "jpaxos-common.hpp"
#include "headers/lsr_paxos_core_Proposer.h"


class PaxosStorage
{
    jint view = 0;
    jint firstUncommited = 0;
    
    jbyte* lastSnapshot = nullptr;
    jint lastSnapshotSize = 0;
    jint lastSnapshotNextId = -1;
    
    jint runUniqueId = 0;
    
    jbyte proposerState = lsr_paxos_core_Proposer_ENUM_PROPOSERSTATE_INACTIVE;
    
public:
    PaxosStorage(){}
    
    jint getFirstUncommited(){return firstUncommited;}
    jint updateFirstUncommited();

    jint getView(){return view;}
    void setView(jint newView){view = newView;}
    
    // returns nextInstanceId or -1 if no snapshot exists
    void setLastSnapshot(JNIEnv* env, jint nextInstanceId, jobject directBB, jint size);
    jbyteArray getLastSnapshot(JNIEnv * env);
    jint getLastSnapshotNextId() {return lastSnapshotNextId;}
    
    jint getRunUniqueId() {return runUniqueId;}
    void incRunUniqueId() {runUniqueId++;}
    
    jbyte getProposerState() {return proposerState;}
    void setProposerState(jbyte state) {proposerState = state;}
    
    void dump(FILE* out) const;
};

#endif // PAXOSSTORAGE_H
