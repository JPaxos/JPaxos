#include "jpaxos-common.hpp"
#include "consensuslog.h"

#include <vector>

ConsensusInstance & ConsensusLog::getInstanceRw(jint id) {
    auto l = std::shared_lock(instancesMutex);
    return instances[id];
}

const ConsensusInstance & ConsensusLog::getInstanceRo(jint id) {
    auto l = std::shared_lock(instancesMutex);
    return instances[id];
}

const ConsensusInstance * ConsensusLog::getInstanceIfExists(jint id) const {
    auto l = std::shared_lock(instancesMutex);
    auto iid = instances.find(id);
    if(iid==instances.end())
        return nullptr;
    return &(iid->second);
}


jint ConsensusLog::append(){
    auto l = std::unique_lock(instancesMutex);
    instances.emplace(nextId, nextId);
    nextId++;
    return nextId-1;
}

void ConsensusLog::appendUpTo(jint id){
    auto l = std::unique_lock(instancesMutex);
    for(; nextId <= id; ++nextId)
        instances.emplace(nextId, nextId);
}

jintArray ConsensusLog::instanceIds(JNIEnv * env) const {
    jintArray ja = env->NewIntArray(instanceCount());
    jint* jaa = env->GetIntArrayElements(ja, nullptr);
    unsigned idx = 0;
    {
        auto l = std::unique_lock(instancesMutex);
        for(const auto & pair : instances)
            jaa[idx++] = pair.first;
    }
    env->ReleaseIntArrayElements(ja, jaa, 0);
    return ja;
}

jlong ConsensusLog::byteSizeBetween(jint fromIncl, jint toExcl) const {
    if(fromIncl < lowestAvaialbale)
        fromIncl = lowestAvaialbale;
    if(toExcl > nextId)
        toExcl = nextId;
    
    auto l = std::shared_lock(instancesMutex);
    jlong size = 0;
    for(; fromIncl < toExcl; ++fromIncl){
        auto inst = getInstanceIfExists(fromIncl);
        if(inst){
            size += inst->byteSize();
        }
    }
    return size;
}

void ConsensusLog::truncateBelow(jint id){
    auto l = std::unique_lock(instancesMutex);
    while(decidedBelow.size() && decidedBelow.front() < id){
        int debe = decidedBelow.front();
        decidedBelow.pop_front();
        instances.at(debe).freeMemory();
        instances.erase(debe);
    }
    while(lowestAvaialbale < id){
        auto inst = instances.find(lowestAvaialbale);
        if(inst!=instances.end()){ 
            inst->second.freeMemory();
            instances.erase(inst);
        }
        lowestAvaialbale++;
    }
}

jintArray ConsensusLog::clearUndecidedBelow(JNIEnv * env, jint id){
    std::vector<jint> erased(id-lowestAvaialbale);
    {
         auto l = std::unique_lock(instancesMutex);
        while(lowestAvaialbale < id){
            auto inst = instances.find(lowestAvaialbale);
            if(inst!=instances.end() && inst->second.getState() != DECIDED){
                inst->second.freeMemory();
                instances.erase(inst);
                erased.push_back(lowestAvaialbale);
            } else
                decidedBelow.push_back(lowestAvaialbale);
            lowestAvaialbale++;
        }
    }
    nextId = std::max(lowestAvaialbale, nextId);
    jintArray ja = env->NewIntArray(erased.size());
    env->SetIntArrayRegion(ja, 0, erased.size(), erased.data());
    
    return ja;
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
