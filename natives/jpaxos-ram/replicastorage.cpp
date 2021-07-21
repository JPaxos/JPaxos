#include "replicastorage.h"

namespace jniGlue {
    //jclass lsr_common_Reply;
    jmethodID lsr_common_Reply__constructor;
    jclass lsr_common_Reply(JNIEnv * env) {
        return env->FindClass("lsr/common/Reply");
    }
    
    void prepareReplicaStorageGlue(JNIEnv * env){
        //lsr_common_Reply = env->FindClass("lsr/common/Reply");
        lsr_common_Reply__constructor = env->GetMethodID(lsr_common_Reply(env), "<init>", "(JI[B)V");
    }
    
    jobject reply_to_reply(JNIEnv * env, const ClientReply& replyC, jclass lsr_common_Reply){
        jbyteArray valueJ = env->NewByteArray(replyC.valueLength);
        env->SetByteArrayRegion(valueJ, 0, replyC.valueLength, replyC.value);
        return env->NewObject(lsr_common_Reply, lsr_common_Reply__constructor, replyC.clientId, replyC.seqNo, valueJ);
    }
}

jobject ReplicaStorage::getLastReplyForClient(jlong clientId, JNIEnv * env) const {
    std::shared_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
    auto reply = lastReplyForClient_live.find(clientId);
    if (reply == lastReplyForClient_live.end())
        return nullptr;
    return jniGlue::reply_to_reply(env, reply->second, jniGlue::lsr_common_Reply(env));
}

#ifdef __cplusplus
extern "C" {
#endif
    
JNIEXPORT jint JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_getExecuteUB_1 (JNIEnv *, jclass){
    return replicaStorage->getExcuteUB();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_setExecuteUB_1 (JNIEnv *, jclass, jint executeUB){
    return replicaStorage->setExcuteUB(executeUB);
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_incrementExecuteUB_1 (JNIEnv *, jclass){
    return replicaStorage->incrementExcuteUB();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_addDecidedWaitingExecution (JNIEnv *, jclass, jint instanceId){
    replicaStorage->addDecidedWaitingExecution(instanceId);
}

JNIEXPORT jboolean JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_isDecidedWaitingExecution (JNIEnv *, jclass, jint instanceId){
    return replicaStorage->isDecidedWaitingExecution(instanceId);
}
    
JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_releaseDecidedWaitingExecution (JNIEnv *, jobject, jint instanceId){
    replicaStorage->releaseDecidedWaitingExecution(instanceId);
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_releaseDecidedWaitingExecutionUpTo (JNIEnv *, jobject, jint instanceId){
    replicaStorage->releaseDecidedWaitingExecutionUpTo(instanceId);
}

JNIEXPORT jint JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_decidedWaitingExecutionCount (JNIEnv *, jobject){
    return replicaStorage->decidedWaitingExecutionCount();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_setLastReplyForClient (JNIEnv * env, jclass, jint instanceId, jlong clientId, jint clientSeqNo, jbyteArray valueJ){
    size_t valueLength = (size_t) env->GetArrayLength(valueJ);
    signed char * valueC = new signed char[valueLength+1];
    env->GetByteArrayRegion(valueJ, 0, valueLength, valueC);
    replicaStorage->setLastReplyForClient(instanceId, clientId, clientSeqNo, valueC, valueLength);
}

JNIEXPORT jint JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_getLastReplySeqNoForClient_1 (JNIEnv *, jclass, jlong clientId){
    return replicaStorage->getLastReplySeqNoForClient(clientId);
}

JNIEXPORT jobject JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_getLastReplyForClient_1 (JNIEnv * env, jclass, jlong clientId){
    return replicaStorage->getLastReplyForClient(clientId, env);
}

JNIEXPORT jobjectArray JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_getRepliesInInstance_1 (JNIEnv * env, jclass, jint instanceId){
    auto * repliesC = replicaStorage->getRepliesInInstance(instanceId);
    if(!repliesC)
        return nullptr;
    jclass lsr_common_Reply = jniGlue::lsr_common_Reply(env);
    jobjectArray repliesJ = env->NewObjectArray(repliesC->size(), lsr_common_Reply, nullptr);
    auto iter = repliesC->begin();
    jsize num = 0;
    while(iter != repliesC->end()) {
        env->SetObjectArrayElement(repliesJ, num, jniGlue::reply_to_reply(env, *iter, lsr_common_Reply));
        ++num;
        ++iter;
    };
    return repliesJ;
}

JNIEXPORT jobjectArray JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_getLastRepliesUptoInstance_1 (JNIEnv * env, jclass, jint instanceId){
    const auto& repliesC = replicaStorage->getRepliesMapUpToInstance(instanceId);
    jclass lsr_common_Reply = jniGlue::lsr_common_Reply(env);
    auto count = repliesC.size();
    jobjectArray repliesJ = env->NewObjectArray(count, lsr_common_Reply, nullptr);
    jsize num = 0;
    for(const auto& pair : repliesC){
        const auto& reply = pair.second;
        assert(([&]{
            if(reply.referenceCounterGet() > 0)
                return true;
            fprintf(stdout, "Problematic pair: %ld:%d (reference count is %hhd)\n", reply.clientId, reply.seqNo, reply.referenceCounterGet());
            fflush(stdout);
            return false;
        })());
        assert(([&]{
            if(num < (jint)count)
                return true;
            fprintf(stdout, "Count %lu\n", repliesC.size());
            fflush(stdout);
            return false;
        })());
        env->SetObjectArrayElement(repliesJ, num++, jniGlue::reply_to_reply(env, reply, lsr_common_Reply));
    }
    return repliesJ;
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_dropLastReplyForClient (JNIEnv *, jclass){
    replicaStorage->dropAllLastRepliesForClients();
}

JNIEXPORT void JNICALL Java_lsr_paxos_replica_storage_PersistentReplicaStorage_dropRepliesInInstance (JNIEnv *, jclass){
    replicaStorage->dropAllRepliesInInstances();
}

#ifdef __cplusplus
}
#endif
