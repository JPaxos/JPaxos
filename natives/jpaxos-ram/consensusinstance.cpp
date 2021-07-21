#include "consensusinstance.h"

#include "jpaxos-common.hpp"

bool ConsensusInstance::updateStateFromPropose(JNIEnv* env, jint proposeSender, jint view, jbyteArray newValue){
    if(view < lastSeenView){
        assert (state == DECIDED);
        return false;
    }
    
    auto update = [&]{
        lastSeenView  = view;
        lastVotedView = view;

        assert(valueLength==0 && value==nullptr);
        
        valueLength = env->GetArrayLength(newValue);
        
        // code below does: value = newValue;
        jbyte* ba = env->GetByteArrayElements(newValue, nullptr);
        value = new jbyte[valueLength];
        memcpy(value, ba, valueLength);
        env->ReleaseByteArrayElements(newValue, ba, JNI_ABORT);
        
        state = KNOWN;
        
        accepts |= (1 << proposeSender) | (1 << localId());
    };
                
    switch(state){
        case UNKNOWN:
            update();
            break;
        case KNOWN:
            if(view > lastSeenView){
                accepts = 0;
                deleteValueUnchecked();
                update();
            }
            break;
        case RESET:
            if(view > lastSeenView){
                accepts = 0;
            }
            deleteValueUnchecked();
            update();
            break;
        case DECIDED:
            break;
        default:
            env->ThrowNew(env->FindClass("java/lang/RuntimeException"), "Unknown instance state?!");
    }
    
    return isReadyToBeDecieded();
}

bool ConsensusInstance::updateStateFromAccept(jint view, jint acceptSender){
    switch(state){
        case KNOWN:
            /* ~ ~ fall through ~ ~ */
        case RESET:
            if(view > lastSeenView){
                lastSeenView = view;
                accepts = 0;
                state = RESET;
            }
            break;
        case UNKNOWN:
            lastSeenView = view;
            break;
        case DECIDED:
            /* ~ ~ fall through ~ ~ */
        default:
            throw "bad / unknown instance state";
    }
    accepts |= (1<<acceptSender);
    return isReadyToBeDecieded();
}

void ConsensusInstance::updateStateFromDecision(JNIEnv* env, jint view, jbyteArray newValue){
    lastSeenView  = view;
    lastVotedView = view;
    
    valueLength = env->GetArrayLength(newValue);
    
    // code below does: value = newValue;
    jbyte* ba = env->GetByteArrayElements(newValue, nullptr);
    value = new jbyte[valueLength];
    memcpy(value, ba, valueLength);
    env->ReleaseByteArrayElements(newValue, ba, JNI_ABORT);
    
    state = KNOWN;
}

#ifdef __cplusplus
extern "C" {
#endif
    
JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_getLastSeenView (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).getLastSeenView();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_getLastVotedView (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).getLastVotedView();
}

JNIEXPORT jbyteArray JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_getValue (JNIEnv * env, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).getValue(env);
}

JNIEXPORT jbyte JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_getState (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).getState();
}

JNIEXPORT jboolean JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_isMajority (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).isMajority();
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_setDecided (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRw(id).setDecided();
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_writeAsLastVoted__ILjava_nio_ByteBuffer_2I (JNIEnv * env, jclass, jint id, jobject bb, jint pos){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    const ConsensusInstance & instance = consensusLog->getInstanceRo(id);
    assert(instance.byteSize() <= (size_t) env->GetDirectBufferCapacity(bb));
    instance.writeTo(static_cast<jbyte*>(env->GetDirectBufferAddress(bb))+pos);
    return instance.byteSize();
}

JNIEXPORT jbyteArray JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_writeAsLastVoted__I (JNIEnv * env, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    const ConsensusInstance & instance = consensusLog->getInstanceRo(id);
    auto len = instance.byteSize();
    jbyteArray jba = env->NewByteArray(len);
    jbyte ba[len];
    instance.writeTo(ba);
    env->SetByteArrayRegion(jba, 0, len, ba);
    return jba;
}

JNIEXPORT jint JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_byteSize (JNIEnv *, jclass, jint id){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRo(id).byteSize();
}

JNIEXPORT jboolean JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_updateStateFromPropose (JNIEnv * env, jclass, jint id, jint proposeSender, jint view, jbyteArray newValue){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRw(id).updateStateFromPropose(env, proposeSender, view, newValue);
}

JNIEXPORT jboolean JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_updateStateFromAccept (JNIEnv *, jclass, jint id, jint view, jint acceptSender){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    return consensusLog->getInstanceRw(id).updateStateFromAccept(view, acceptSender);
}

JNIEXPORT void JNICALL Java_lsr_paxos_storage_PersistentConsensusInstance_updateStateFromDecision (JNIEnv * env, jclass, jint id, jint view, jbyteArray newValue){
    assert(consensusLog->getInstanceIfExists(id)!=nullptr);
    consensusLog->getInstanceRw(id).updateStateFromDecision(env, view, newValue);
} 

#ifdef __cplusplus
}
#endif
