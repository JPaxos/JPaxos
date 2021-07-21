#include "paxosstorage.h"


jint PaxosStorage::updateFirstUncommited(){
    pmem::obj::transaction::automatic tx(*pop);
    firstUncommited = std::max(lastSnapshotNextId, firstUncommited);
    while(true){
        const ConsensusInstance * ci = consensusLog->getInstanceIfExists(firstUncommited);
        if(!ci || ci->getState() != DECIDED )
            break;
        firstUncommited++;
    }
    return firstUncommited;
}

void PaxosStorage::setLastSnapshot(JNIEnv* env, jint nextInstanceId, jobject directBB, jint size){
    jbyte* snap = (jbyte*) env->GetDirectBufferAddress(directBB);
    pmem::obj::transaction::automatic tx(*pop);
    if(lastSnapshot)
        delete_persistent<jbyte[]>(lastSnapshot, lastSnapshotSize);
    
    lastSnapshotSize = size;
    lastSnapshot = make_persistent<jbyte[]>(size);
    memcpy(lastSnapshot.get(), snap, size);
    
    lastSnapshotNextId = nextInstanceId;
}


jbyteArray PaxosStorage::getLastSnapshot(JNIEnv* env){
    jbyteArray jba = env->NewByteArray(lastSnapshotSize);
    env->SetByteArrayRegion(jba, 0, lastSnapshotSize, lastSnapshot.get());
    return jba;
}

void PaxosStorage::dump(FILE* out) const {
    fprintf(out, "View: %-10d             RunUniqueId: %d\n"
                 "FirstUncommited: %-10d  proposerState: %s\n",  view.get_ro(), runUniqueId.get_ro(), firstUncommited.get_ro(),
                 proposerState == lsr_paxos_core_Proposer_ENUM_PROPOSERSTATE_INACTIVE  ? "INACTIVE" :
                 proposerState == lsr_paxos_core_Proposer_ENUM_PROPOSERSTATE_PREPARING ? "PREPARING" :
                 proposerState == lsr_paxos_core_Proposer_ENUM_PROPOSERSTATE_PREPARED  ? "PREPARED" : "UNKNOWN"
           );
    fprintf(out, "Last snapshot for %d (of size %d)\n", lastSnapshotNextId.get_ro(), lastSnapshotSize.get_ro());
}

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentStorage_getFirstUncommitted_1 (JNIEnv *, jclass){
    return paxosStorage->getFirstUncommited();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentStorage_updateFirstUncommitted_1 (JNIEnv *, jclass){
    return paxosStorage->updateFirstUncommited();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentStorage_getLastSnapshotNextId_1 (JNIEnv *, jclass){
    return paxosStorage->getLastSnapshotNextId();
}

JNIEXPORT jbyteArray JNICALL Java_lsr_paxos_storage_PersistentStorage_getLastSnapshot_1 (JNIEnv * env, jclass){
    return paxosStorage->getLastSnapshot(env);
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentStorage_setLastSnapshot (JNIEnv * env, jclass, jint nextId, jobject directBB, jint size){
    return paxosStorage->setLastSnapshot(env, nextId, directBB, size);
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentStorage_getView_1 (JNIEnv *, jclass){
    return paxosStorage->getView();
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentStorage_setView_1 (JNIEnv *, jclass, jint newView){
    paxosStorage->setView(newView);
}

JNIEXPORT jlong JNICALL Java_lsr_paxos_storage_PersistentStorage_getRunUniqueId (JNIEnv *, jobject){
    return paxosStorage->getRunUniqueId();
}

JNIEXPORT jbyte JNICALL Java_lsr_paxos_storage_PersistentStorage_getProposerState_1 (JNIEnv *, jobject){
    return paxosStorage->getProposerState();
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentStorage_setProposerState (JNIEnv *, jobject, jbyte state){
    paxosStorage->setProposerState(state);
}


#ifdef __cplusplus
}
#endif
