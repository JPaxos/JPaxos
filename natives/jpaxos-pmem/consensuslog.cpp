#include "consensuslog.h"

#include <map>

#ifndef NDEBUG
    #define ASSERT_INVARIANTS assertInvariants();
#else
    #define ASSERT_INVARIANTS
#endif

void ConsensusLog::assertInvariants() const {
    for(int i = lowestAvaialbale; i < nextId; ++i)
        assert(instances.get_if_exists(i));
}

ConsensusInstance & ConsensusLog::getInstanceRw(jint id) {
    return *instances.get_if_exists(id);
}

const ConsensusInstance & ConsensusLog::getInstanceRo(jint id) {
    return *instances.get_if_exists(id);
}

const ConsensusInstance * ConsensusLog::getInstanceIfExists(jint id)
{
    const auto iid = instances.get_if_exists(id);
    if(!iid)
        return nullptr;
    return iid;
}


jint ConsensusLog::append(){
    pmem::obj::transaction::automatic tx(*pop);
    instances.get(*pop, nextId.get_ro(), nextId.get_ro());
    nextId++;
    ASSERT_INVARIANTS
    return nextId-1;
}

void ConsensusLog::appendUpTo(jint id){
    pmem::obj::transaction::automatic tx(*pop);
    for(; nextId <= id; ++nextId)
        instances.get(*pop, nextId.get_ro(), nextId.get_ro());
    ASSERT_INVARIANTS
}

jintArray ConsensusLog::instanceIds(JNIEnv * env) const {
    jintArray ja = env->NewIntArray(instanceCount());
    jint* jaa = env->GetIntArrayElements(ja, nullptr);
    unsigned idx = 0;
    auto lock = instances.lockShared();
    for(const auto & pair : instances)
        jaa[idx++] = pair.first;
    lock.unlock();
    env->ReleaseIntArrayElements(ja, jaa, 0);
    return ja;
}

jlong ConsensusLog::byteSizeBetween(jint fromIncl, jint toExcl) const{
    if(fromIncl < lowestAvaialbale)
        fromIncl = lowestAvaialbale;
    if(toExcl > nextId)
        toExcl = nextId;
    
    jlong size = 0;
    for(; fromIncl < toExcl; ++fromIncl){
        const auto inst = instances.get_if_exists(fromIncl);
        if(inst){
            size += inst->byteSize();
        }
    }
    return size;
}

void ConsensusLog::truncateBelow(jint id){
    pmem::obj::transaction::automatic tx(*pop);
    while(decidedBelow.count() && decidedBelow.front() < id){
        int debe = decidedBelow.pop_front();
        instances.get_if_exists(debe)->freeMemory();
        instances.erase(*pop, debe);
    }
    while(lowestAvaialbale < id){
        auto inst = instances.get_if_exists(lowestAvaialbale);
        if(inst){ 
            inst->freeMemory();
            instances.erase(*pop, lowestAvaialbale);
        }
        lowestAvaialbale++;
    }
    ASSERT_INVARIANTS
}

jintArray ConsensusLog::clearUndecidedBelow(JNIEnv * env, jint id){
    std::vector<jint> erased(id-lowestAvaialbale);
    pmem::obj::transaction::automatic tx(*pop);
    while(lowestAvaialbale < id){
        auto inst = instances.get_if_exists(lowestAvaialbale);
        if(inst && inst->getState() != DECIDED){
            inst->freeMemory();
            instances.erase(*pop, lowestAvaialbale);
            erased.push_back(lowestAvaialbale);
        } else
            decidedBelow.push_back(lowestAvaialbale);
        lowestAvaialbale++;
    }
    nextId = std::max(lowestAvaialbale, nextId);
    pmem::obj::transaction::commit();
    jintArray ja = env->NewIntArray(erased.size());
    env->SetIntArrayRegion(ja, 0, erased.size(), erased.data());
    ASSERT_INVARIANTS
    return ja;
}

void ConsensusLog::dump(FILE* out) const {
    fprintf(out, "Consensus log contains ids [%d-%d)\n", lowestAvaialbale.get_ro(), nextId.get_ro());

    auto lock = instances.lockShared();
    
    std::vector<jint> ids;
    ids.reserve(instances.count());
    
    for(auto p : instances)
        ids.push_back(p.first);
    
    std::sort(ids.begin(), ids.end());
    
    for(auto id : ids)
        instances.get_if_exists(id)->dump(out);
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jintArray JNICALL Java_lsr_paxos_storage_PersistentLog_getExistingInstances (JNIEnv * env, jclass){
    return consensusLog->instanceIds(env);
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentLog_getNextId_1 (JNIEnv *, jclass){
    return consensusLog->getNextId();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentLog_getLowestAvailableId (JNIEnv *, jobject){
    return consensusLog->getLowestAvailable();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentLog_append_1 (JNIEnv *, jclass){
    return consensusLog->append();
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentLog_appendUpTo (JNIEnv *, jclass, jint id){
     consensusLog->appendUpTo(id);
}

JNIEXPORT jlong JNICALL Java_lsr_paxos_storage_PersistentLog_byteSizeBetween (JNIEnv *, jobject, jint fromIncl, jint toExcl){
    return consensusLog->byteSizeBetween(fromIncl, toExcl);
}
  
JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentLog_truncateBelow_1 (JNIEnv *, jclass, jint id){
    return consensusLog->truncateBelow(id);
}

JNIEXPORT jintArray JNICALL Java_lsr_paxos_storage_PersistentLog_clearUndecidedBelow_1 (JNIEnv * env, jclass, jint id){
    return consensusLog->clearUndecidedBelow(env, id);
}

#ifdef __cplusplus
} // extern "C"
#endif
