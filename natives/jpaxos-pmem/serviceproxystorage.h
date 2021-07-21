#ifndef SERVICEPROXYSTORAGE_H
#define SERVICEPROXYSTORAGE_H

#include "jpaxos-common.hpp"

class ServiceProxyStorage
{
    pm::p<jint> nextSeqNo {0};
    pm::p<jint> lastSnapshotNextSeqNo {0};
    linkedqueue_persistent<std::pair<jint, jint>> startingSeqNo;
    
public:
    ServiceProxyStorage() {startingSeqNo.push_back(0,0);}
    
    jint getNextSeqNo(){return nextSeqNo;}
    void setNextSeqNo(jint nextSeqNo) {pmem::obj::transaction::run(*pop,[&]{ this->nextSeqNo = nextSeqNo;});}
    void incNextSeqNo() {pmem::obj::transaction::run(*pop,[&]{nextSeqNo++;});}

    jint getLastSnapshotNextSeqNo(){return lastSnapshotNextSeqNo;}
    void setLastSnapshotNextSeqNo(jint lsnsn) {pmem::obj::transaction::run(*pop,[&]{ lastSnapshotNextSeqNo = lsnsn;});}
    
    void appendStartingSeqenceNo(jint instance, jint sequenceNo) {startingSeqNo.push_back(instance, sequenceNo);}
    void truncateStartingSeqNo(int lowestSeqNo) {
        pmem::obj::transaction::automatic tx(*pop);
        while(true){
            if(startingSeqNo.count() <= 1)
                return;
            
            auto it = startingSeqNo.begin();
            ++it;
            const auto & nextSn = it->second;
            
            if(nextSn > lowestSeqNo)
                return;
            
            startingSeqNo.pop_front();
        }
    }
    void clearStartingSeqNo() {startingSeqNo.clear();}
    jintArray getFrontStartingSeqNo(JNIEnv * env) {
        jintArray jia = env->NewIntArray(2);
        jint* ia = env->GetIntArrayElements(jia, nullptr);
        const auto & front =  startingSeqNo.begin();
        ia[0] = front->first;
        ia[1] = front->second;
        env->ReleaseIntArrayElements(jia, ia, 0);
        return jia;
    }
    
    void dump(FILE* out) const;
};

#endif // SERVICEPROXYSTORAGE_H
