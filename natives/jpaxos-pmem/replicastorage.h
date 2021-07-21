#ifndef REPLICASTORAGE_H
#define REPLICASTORAGE_H

#include "jpaxos-common.hpp"

#include <libpmemobj++/persistent_ptr.hpp>
#include <list>

#include <libpmemobj.h>

using namespace pmem::obj;

struct ClientReply {
    jlong clientId {0};
    jint seqNo {-1};
    persistent_ptr<signed char[]> value {nullptr};
    size_t valueLength {0};
    
    //void advertiseWriteRefCtr(){pmem::obj::transaction::snapshot(value.get()+valueLength, 1);}
    void advertiseWriteRefCtr(){pmemobj_tx_add_range(value.raw(), valueLength, 1);}
    
    char referenceCounterGet() const {return value==nullptr ? 0 : value.get()[valueLength];}
    void referenceCounterSet(char cnt)   {advertiseWriteRefCtr(); (value.get()[valueLength]) = cnt;}
    void referenceCounterInc(char by = 1){advertiseWriteRefCtr(); (value.get()[valueLength]) += by;}
    void referenceCounterDec(){
        if(!value)
            return;
        advertiseWriteRefCtr();
        if(!--(value.get()[valueLength])) {
            delete_persistent<signed  char[]>(value, valueLength+1);
            value=nullptr;
        }
    }
};

class ReplicaStorage
{
    pmem::obj::p<jint> executeUB {0};
    hashset_persistent<jint> decidedWaitingExecution = hashset_persistent<jint>(*pop, 128);
    
    // this is checked by Replica when a new client request arrives
    hashmap_persistent<jlong, ClientReply> lastReplyForClient_live                   = hashmap_persistent<jlong, ClientReply>(*pop, 2<<14 /* 16384 */);
    // this was given upon previous snapshot creation
    hashmap_persistent<jlong, ClientReply> lastReplyForClient_snapshot               = hashmap_persistent<jlong, ClientReply>(*pop, 2<<14 /* 16384 */);
    // this is the next instance not included in lastReplyForClient_snapshot
    pmem::obj::p<jint> lastReplyForClient_snapshotLastInstance {0};
    // this is roughly the difference between the two maps above
    hashmap_persistent<jint, linkedqueue_persistent<ClientReply>> repliesInInstance  = hashmap_persistent<jint, linkedqueue_persistent<ClientReply>>(*pop, 2<<8  /*   256 */);
    
    mutable std::shared_mutex lastReplyForClient_live_mutex;
public:
    ReplicaStorage(){}
    
    jint getExcuteUB() const {return executeUB;}
    void setExcuteUB(jint _executeUB) {executeUB=_executeUB;}
    void incrementExcuteUB(){++executeUB;}
    
    void addDecidedWaitingExecution(jint instanceId) {decidedWaitingExecution.add(*pop, instanceId);}
    jboolean isDecidedWaitingExecution(jint instanceId) const {return decidedWaitingExecution.contains(instanceId) ? JNI_TRUE : JNI_FALSE;}
    void releaseDecidedWaitingExecution(jint instanceId) {decidedWaitingExecution.erase(*pop, instanceId);}
    void releaseDecidedWaitingExecutionUpTo(jint instanceId) {
        std::list<jint> instancesToRemove;
        for(auto iid: decidedWaitingExecution){
            if(iid < instanceId)
                instancesToRemove.push_back(iid);
        }
        pmem::obj::transaction::run(*pop,[&]{
            for(auto iid: instancesToRemove)
                decidedWaitingExecution.erase(*pop, iid);
        });
    }
    size_t decidedWaitingExecutionCount() const {return decidedWaitingExecution.count();}
    
    /// called from within a transaction
    /// WARNING: value MUST be 1 byte longer than valueLength (reference count is stored there)
    void setLastReplyForClient(jint instanceId, jlong clientId, jint clientSeqNo, persistent_ptr<signed char[]> value, size_t valueLength){
        std::unique_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
        ClientReply & reply = lastReplyForClient_live.get(*pop, clientId).get_rw();
        reply.referenceCounterDec();
        
        reply.clientId    = clientId;
        reply.seqNo       = clientSeqNo;
        reply.value       = value;
        reply.valueLength = valueLength;

        reply.referenceCounterSet(2);

        lock.unlock();
        
        auto & repliesInCurrentInstance = repliesInInstance.get(*pop, instanceId).get_rw();
        repliesInCurrentInstance.push_back(*pop, reply);
        
        #ifdef DEBUG_LASTREPLYFORCLIENT
            fprintf(debugLogFile, "Adding reply: %ld:%d\n", clientId, clientSeqNo);
        #endif
    }
    
    jint getLastReplySeqNoForClient(jlong clientId) const {
        std::shared_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
        auto reply = lastReplyForClient_live.get_if_exists(clientId);
        return reply ? reply->get_ro().seqNo : -1;
    }
    
    jobject getLastReplyForClient(jlong clientId, JNIEnv * env) const;
    
    const linkedqueue_persistent<ClientReply> * getRepliesInInstance(jint instanceId) const {
        auto replies = repliesInInstance.get_if_exists(instanceId);
        return replies ? &replies->get_ro() : nullptr;
    }
    
    const hashmap_persistent<jlong, ClientReply>& getRepliesMapUpToInstance(jint instanceId) {
        pmem::obj::transaction::run(*pop,[&]{
            // go instance by instance, from lastReplyForClient_snapshotLastInstance up to instanceId
            // (warning: lastReplyForClient_snapshotLastInstance updates here)
            for(auto & inst = lastReplyForClient_snapshotLastInstance.get_rw(); inst < instanceId; ++inst){
                // for every instance, get replies sent for the requests
                auto replies = repliesInInstance.get_if_exists(inst);
                if(!replies) continue;
                // and add each reply to the lastReplyForClient_snapshot map
                for(const ClientReply& newReply : replies->get_ro()){
                    ClientReply & oldReply = lastReplyForClient_snapshot.get(*pop, newReply.clientId).get_rw();
                    oldReply.referenceCounterDec();
                    oldReply = newReply;
                    assert(oldReply.referenceCounterGet()>0);
                    // do not invoke oldReply.referenceCounterInc() - repliesInInstance are dropped just after the loop
                }
                repliesInInstance.get(*pop, inst).get_rw().clear(*pop);
                repliesInInstance.erase(*pop, inst);
            }
        });
        return lastReplyForClient_snapshot;
    }
    
    void dropAllLastRepliesForClients(){
        pmem::obj::transaction::run(*pop,[&]{
            std::unique_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
            for(auto p: lastReplyForClient_live)
                p.second.get_rw().referenceCounterDec();
            lastReplyForClient_live.clear(*pop);
            lock.unlock();
            
            for(auto p: lastReplyForClient_snapshot)
                p.second.get_rw().referenceCounterDec();
            lastReplyForClient_snapshot.clear(*pop);
        });
    }

    void dropAllRepliesInInstances(){
        pmem::obj::transaction::run(*pop,[&]{
            for(auto p : repliesInInstance){
                auto & replies = p.second.get_rw();
                for(auto & reply : replies){
                        reply.get_rw().referenceCounterDec();
                }
                replies.clear(*pop);
            }
            repliesInInstance.clear(*pop);
        });
    }
    #ifdef DEBUG_LASTREPLYFORCLIENT
        void dumpLastClientReply(){
            fprintf(debugLogFile, "Dumping client respose IDs:\n");
            std::shared_lock<std::shared_mutex> lock(lastReplyForClient_live_mutex);
            for(auto pair : lastReplyForClient_live)
                fprintf(debugLogFile, "%ld:%d\n", pair.first, pair.second.get_ro().seqNo);
            lock.unlock();
            fprintf(debugLogFile, "Client respose IDs dumped\n");
        }
    #endif
    
    void dump(FILE* out) const;
};

namespace jniGlue {
    // TODO: classes seem to be not cachable?
    // extern jclass lsr_common_Reply;
    jclass lsr_common_Reply();
    extern jmethodID lsr_common_Reply__constructor;
    void prepareReplicaStorageGlue(JNIEnv * env);
}

#endif // REPLICASTORAGE_H
