#ifndef SERVICEPROXYSTORAGE_H
#define SERVICEPROXYSTORAGE_H

#include "jpaxos-common.hpp"

class ServiceProxyStorage
{
    jint nextSeqNo {0};
    jint lastSnapshotNextSeqNo {0};
    std::list<std::pair<jint, jint>> startingSeqNo;
    
public:
    ServiceProxyStorage() {startingSeqNo.emplace_back(0,0);}
    
    jint getNextSeqNo(){return nextSeqNo;}
    void setNextSeqNo(jint nextSeqNo) {this->nextSeqNo = nextSeqNo;}
    void incNextSeqNo() {nextSeqNo++;}

    jint getLastSnapshotNextSeqNo(){return lastSnapshotNextSeqNo;}
    void setLastSnapshotNextSeqNo(jint lsnsn) {lastSnapshotNextSeqNo = lsnsn;}
    
    void appendStartingSeqenceNo(jint instance, jint sequenceNo) {startingSeqNo.emplace_back(instance, sequenceNo);}
    void truncateStartingSeqNo(int lowestSeqNo) {
        while(true){
            if(startingSeqNo.size() <= 1)
                return;
            
            auto it = startingSeqNo.begin();
            ++it;
            auto & nextSn = it->second;
            
            if(nextSn > lowestSeqNo)
                return;
            
            startingSeqNo.pop_front();
        }
    }
    void clearStartingSeqNo() {startingSeqNo.clear();}
    jintArray getFrontStartingSeqNo(JNIEnv * env) {
        jintArray jia = env->NewIntArray(2);
        jint* ia = env->GetIntArrayElements(jia, nullptr);
        auto & front =  startingSeqNo.front();
        ia[0] = front.first;
        ia[1] = front.second;
        env->ReleaseIntArrayElements(jia, ia, 0);
        return jia;
    }
};

#endif // SERVICEPROXYSTORAGE_H
