#include <unistd.h>

#include "jpaxos-common.hpp"
#include "replicastorage.h"
#include "paxosstorage.h"

#include "headers/lsr_paxos_NATIVE_PersistentMemory.h"

#ifndef NDEBUG
#include <signal.h>
void (*original_sigabrt)(int);
void on_SIGABRT(int){
    // java handles abrt by aborting self, and handles sevg by dumping usefull info and calling abort
    // and assert calls abrt
    // so we catch first abrt and change it to segv, then resore abrt handler.
    // This does not work well, but at least good enough to see what went wrong.
    signal(SIGABRT, original_sigabrt);
    raise(SIGSEGV);
}

#endif

struct root {
    unsigned char numReplicas;
    ConsensusLog consensusLog;
    PaxosStorage paxosStorage;
    ReplicaStorage replicaStorage;
    ServiceProxyStorage serviceProxyStorage;
};

// These three variables are extern'ed for convenience 
pm::pool<root> *pop;
PaxosStorage *paxosStorage;
ReplicaStorage *replicaStorage;
ConsensusLog *consensusLog;
ServiceProxyStorage *serviceProxyStorage;

thread_local pm::transaction::automatic * currentTransaction = nullptr;
thread_local unsigned currentTransactionDepth = 0;

#if defined(DEBUG_LASTREPLYFORCLIENT) || defined(DEBUG_TX)
    FILE * debugLogFile;
#endif
    
unsigned char numReplicas_;
unsigned char majority_;
unsigned char localId_;
const unsigned char & numReplicas(){return numReplicas_;}
const unsigned char & majority(){return majority_;}
const unsigned char & localId(){return localId_;}

inline void createRootDataItem(jint numReplicas);

__attribute__((constructor))
void init() {
}

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *, void *){
    return JNI_VERSION_1_8;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *, void *){
}

JNIEXPORT void JNICALL Java_lsr_paxos_NATIVE_PersistentMemory_init (JNIEnv * jnienv, jclass, jstring jPmemFile, jlong jPmemFileSize, jint numReplicas, jint localId){
    #ifndef NDEBUG
    original_sigabrt = signal(SIGABRT, on_SIGABRT);
    #endif
    if(numReplicas > 32)
        jnienv->ThrowNew(jnienv->FindClass("java/lang/IllegalArgumentException"), "JPaxos PM natives use 32-bit integer for acceptor bitset - you have too many replicas!");
    
    // unwrap java.lang.String
    auto len = jnienv->GetStringUTFLength(jPmemFile);
    char pmemFile[len+1];
    strncpy(pmemFile, jnienv->GetStringUTFChars(jPmemFile, nullptr), len);
    pmemFile[len]=0;
    
    // create/open pool
    pop = new pm::pool<root>();
    if(access(pmemFile,R_OK|W_OK)){
        *pop = pm::pool<root>::create(pmemFile, std::string(), jPmemFileSize);
        ::numReplicas_ = numReplicas;
        pm::transaction::run(*pop, [&]{
            createRootDataItem(numReplicas);
        });
    } else {
        *pop = pm::pool<root>::open(pmemFile, std::string());
    }
  
    paxosStorage = &(pop->root()->paxosStorage);
    replicaStorage = &(pop->root()->replicaStorage);
    consensusLog = &(pop->root()->consensusLog);
    serviceProxyStorage = &(pop->root()->serviceProxyStorage);
    
    ::numReplicas_ = pop->root()->numReplicas;
    if(::numReplicas_ != numReplicas){
        jnienv->ThrowNew(jnienv->FindClass("java/lang/IllegalArgumentException"), "Number of replicas passed to PeristentMemory.init() does not match number of replicas read from an existing pmem file!");
    }
    ::majority_ = (numReplicas+1)/2;
    ::localId_ = localId;
    
    paxosStorage->incRunUniqueId();
    
    jniGlue::prepareReplicaStorageGlue(jnienv);
    #if defined(DEBUG_LASTREPLYFORCLIENT) || defined(DEBUG_TX)
        char name[255];
        sprintf(name, "/tmp/jpaxos_debug_%d_%ld_XXXXXX", localId, time(0));
        mktemp(name);
        debugLogFile = fopen(name, "w");
        
        #ifdef DEBUG_LASTREPLYFORCLIENT
        replicaStorage->dumpLastClientReply();
        #endif
    #endif

}

JNIEXPORT void JNICALL Java_lsr_paxos_NATIVE_PersistentMemory_startThreadLocalTx (JNIEnv *, jclass){
    if(!currentTransaction)
        currentTransaction = new pm::transaction::automatic(*pop);
    currentTransactionDepth++;
    #ifdef DEBUG_TX
        fprintf(debugLogFile, "TX++ %u @ %lx\n", currentTransactionDepth, gettid());
    #endif
}

JNIEXPORT void JNICALL Java_lsr_paxos_NATIVE_PersistentMemory_commitThreadLocalTx (JNIEnv * env, jclass){
    if(!currentTransaction)
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"), "Committing a non-existent transaction");
    currentTransactionDepth--;
    if(!currentTransactionDepth){
        delete currentTransaction;
        currentTransaction = nullptr;
    }
    #ifdef DEBUG_TX
        fprintf(debugLogFile, "TX-- %u @ %lx\n", currentTransactionDepth, gettid());
    #endif
}

void dumpJpaxosPmem(char * pmemFile, FILE * outFile){
    pop = new pm::pool<root>();
    if(access(pmemFile, R_OK)){
        printf("File \"%s\" unavailable\n", pmemFile);
        return;
    }
    *pop = pm::pool<root>::open(pmemFile, std::string());
    paxosStorage = &(pop->root()->paxosStorage);
    replicaStorage = &(pop->root()->replicaStorage);
    consensusLog = &(pop->root()->consensusLog);
    serviceProxyStorage = &(pop->root()->serviceProxyStorage);
    
    ::numReplicas_ = pop->root()->numReplicas;
    ::majority_ = (::numReplicas_+1)/2;
    
    fprintf(outFile, "File \"%s\" (%d replicas)\n", pmemFile, numReplicas_);
    paxosStorage->dump(outFile);
    consensusLog->dump(outFile);
    replicaStorage->dump(outFile);
    serviceProxyStorage->dump(outFile);
}

#ifdef __cplusplus
}
#endif

inline void createRootDataItem(jint numReplicas){
    // Lines below invoke constructor on given address.
    // PMDK zeroes root on init, so otherwise the objects would be invalid
    new (&(pop->root()->consensusLog)) ConsensusLog();
    new (&(pop->root()->paxosStorage)) PaxosStorage();
    new (&(pop->root()->replicaStorage)) ReplicaStorage();
    new (&(pop->root()->serviceProxyStorage)) ServiceProxyStorage();
    pop->root()->numReplicas = numReplicas;
}

