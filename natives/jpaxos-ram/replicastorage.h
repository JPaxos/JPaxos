#ifndef REPLICASTORAGE_H
#define REPLICASTORAGE_H

#include "jpaxos-common.hpp"

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <shared_mutex>
#include <cassert>

struct ClientReply {
    jlong clientId {0};
    jint seqNo {-1};
    signed char * value {nullptr};
    size_t valueLength {0};
    
    char referenceCounterGet() const {return value==nullptr ? 0 : value[valueLength];}
    void referenceCounterSet(char cnt)   {(value[valueLength]) = cnt;}
    void referenceCounterInc(char by = 1){(value[valueLength]) += by;}
    void referenceCounterDec(){
        if(!value)
            return;
        if(!--(value[valueLength])) {
            delete [] value;
            value=nullptr;
        }
    }
};

class ReplicaStorage
{
    jint executeUB {0};
    std::unordered_set<jint> decidedWaitingExecution = std::unordered_set<jint>(128);
    
    // this is checked by Replica when a new client request arrives
    std::unordered_map<jlong, ClientReply> lastReplyForClient_live                   = std::unordered_map<jlong, ClientReply>(2<<14 /* 16384 */);
    // this was given upon previous snapshot creation
    std::unordered_map<jlong, ClientReply> lastReplyForClient_snapshot               = std::unordered_map<jlong, ClientReply>(2<<14 /* 16384 */);
    // this is the next instance not included in lastReplyForClient_snapshot
    jint lastReplyForClient_snapshotLastInstance {0};
    // this is roughly the difference between the two maps above
    std::unordered_map<jint, std::list<ClientReply>> repliesInInstance  = std::unordered_map<jint, std::list<ClientReply>>(2<<8  /*   256 */);
    
    mutable std::shared_mutex lastReplyForClient_live_mutex;
public:
    ReplicaStorage(){}
    
    jint getExcuteUB() const {return executeUB;}
    void setExcuteUB(jint _executeUB) {executeUB=_executeUB;}
    void incrementExcuteUB(){++executeUB;}
    
    void addDecidedWaitingExecution(jint instanceId) {decidedWaitingExecution.insert(instanceId);}
    jboolean isDecidedWaitingExecution(jint instanceId) const {return (decidedWaitingExecution.find(instanceId) != decidedWaitingExecution.end()) ? JNI_TRUE : JNI_FALSE;}
    void releaseDecidedWaitingExecution(jint instanceId) {decidedWaitingExecution.erase(instanceId);}
    void releaseDecidedWaitingExecutionUpTo(jint instanceId) {
        std::list<jint> instancesToRemove;
        for(auto iid: decidedWaitingExecution){
            if(iid < instanceId)
                instancesToRemove.push_back(iid);
        }
        for(auto iid: instancesToRemove)
            decidedWaitingExecution.erase(iid);
    }
    size_t decidedWaitingExecutionCount() const {return decidedWaitingExecution.size();}
    
    /// called from within a transaction
    /// WARNING: value MUST be 1 byte longer than valueLength (reference count is stored there)
    void setLastReplyForClient(jint instanceId, jlong clientId, jint clientSeqNo, signed char * value, size_t valueLength){
        std::unique_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
        ClientReply & reply = lastReplyForClient_live[clientId];
        reply.referenceCounterDec();
        
        reply.clientId    = clientId;
        reply.seqNo       = clientSeqNo;
        reply.value       = value;
        reply.valueLength = valueLength;

        reply.referenceCounterSet(2);

        lock.unlock();
        
        auto & repliesInCurrentInstance = repliesInInstance[instanceId];
        repliesInCurrentInstance.push_back(reply);
    }
    
    jint getLastReplySeqNoForClient(jlong clientId) const {
        std::shared_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
        auto reply = lastReplyForClient_live.find(clientId);
        if(reply != lastReplyForClient_live.end())
            return reply->second.seqNo;
        return -1;
    }
    
    jobject getLastReplyForClient(jlong clientId, JNIEnv * env) const;
    
    const std::list<ClientReply> * getRepliesInInstance(jint instanceId) const {
        auto replies = repliesInInstance.find(instanceId);
        return (replies!=repliesInInstance.end()) ? &replies->second : nullptr;
    }
    
    const std::unordered_map<jlong, ClientReply>& getRepliesMapUpToInstance(jint instanceId) {
        // go instance by instance, from lastReplyForClient_snapshotLastInstance up to instanceId
        // (warning: lastReplyForClient_snapshotLastInstance updates here)
        for(auto & inst = lastReplyForClient_snapshotLastInstance; inst < instanceId; ++inst){
            // for every instance, get replies sent for the requests
            auto replies = repliesInInstance.find(inst);
            if(replies == repliesInInstance.end()) continue;
            // and add each reply to the lastReplyForClient_snapshot map
            for(const ClientReply& newReply : replies->second){
                ClientReply & oldReply = lastReplyForClient_snapshot[newReply.clientId];
                oldReply.referenceCounterDec();
                oldReply = newReply;
                assert(oldReply.referenceCounterGet()>0);
                // do not invoke oldReply.referenceCounterInc() - repliesInInstance are dropped just after the loop
            }
            repliesInInstance.erase(inst);
        }
        return lastReplyForClient_snapshot;
    }
    
    void dropAllLastRepliesForClients(){
        std::unique_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
        for(auto p: lastReplyForClient_live)
            p.second.referenceCounterDec();
        lastReplyForClient_live.clear();
        lock.unlock();
        
        for(auto p: lastReplyForClient_snapshot)
            p.second.referenceCounterDec();
        lastReplyForClient_snapshot.clear();
    }

    void dropAllRepliesInInstances(){
        for(auto p : repliesInInstance){
            auto & replies = p.second;
            for(auto & reply : replies){
                    reply.referenceCounterDec();
            }
            replies.clear();
        }
        repliesInInstance.clear();
    }
    
};

namespace jniGlue {
    // TODO: classes seem to be not cachable?
    // extern jclass lsr_common_Reply;
    jclass lsr_common_Reply();
    extern jmethodID lsr_common_Reply__constructor;
    void prepareReplicaStorageGlue(JNIEnv * env);
}

#endif // REPLICASTORAGE_H
