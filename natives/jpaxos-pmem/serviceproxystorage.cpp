#include "serviceproxystorage.h"

void ServiceProxyStorage::dump(FILE* out) const {
    fprintf(out, "Next seqNo: %d    seqNo at last snapshot: %d\nInstance to starting seqNo map:", nextSeqNo.get_ro(), lastSnapshotNextSeqNo.get_ro());
    for(const auto& e : startingSeqNo)
        fprintf(out, " %d-%d", e.first, e.second);
    fprintf(out, "\n");
}


#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_incNextSeqNo (JNIEnv *, jobject) {
    serviceProxyStorage->incNextSeqNo();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_getNextSeqNo (JNIEnv *, jobject) {
    return serviceProxyStorage->getNextSeqNo();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_setNextSeqNo (JNIEnv *, jobject, jint nsn) {
    serviceProxyStorage->setNextSeqNo(nsn);
}

JNIEXPORT jint JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_getLastSnapshotNextSeqNo (JNIEnv *, jobject) {
    return serviceProxyStorage->getLastSnapshotNextSeqNo();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_setLastSnapshotNextSeqNo (JNIEnv *, jobject, jint lsnsn) {
    serviceProxyStorage->setLastSnapshotNextSeqNo(lsnsn);
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_addStartingSeqenceNo (JNIEnv *, jobject, jint inst, jint sn){
    serviceProxyStorage->appendStartingSeqenceNo(inst, sn);
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_truncateStartingSeqNo (JNIEnv *, jobject, jint sn){
    serviceProxyStorage->truncateStartingSeqNo(sn);
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_clearStartingSeqNo (JNIEnv *, jobject){
    serviceProxyStorage->clearStartingSeqNo();
}

JNIEXPORT jintArray JNICALL Java_lsr_paxos_replica_storage_PersistentServiceProxyStorage_getFrontStartingSeqNo_1 (JNIEnv * env, jobject){
    return serviceProxyStorage->getFrontStartingSeqNo(env);
}
 
  
#ifdef __cplusplus
}
#endif
