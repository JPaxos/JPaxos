#include "jni.h"
#include "hashmap_persistent.hpp"
#include "hashset_persistent.hpp"
#include "linkedqueue_persistent.hpp"
class ClientReply;
class ConsensusInstance;

template<> std::hash<jint>     hashmap_persistent<jint, jint>::_hash    = std::hash<jint>();
template<> std::equal_to<jint> hashmap_persistent<jint, jint>::_compare = std::equal_to<jint>();

template<> std::hash<jint>     hashset_persistent<jint>::_hash    = std::hash<jint>();
template<> std::equal_to<jint> hashset_persistent<jint>::_compare = std::equal_to<jint>();

template<> std::hash<jint>     hashmap_persistent<jint, linkedqueue_persistent<ClientReply>, std::hash<jint>, std::equal_to<jint> >::_hash    = std::hash<jint>();
template<> std::equal_to<jint> hashmap_persistent<jint, linkedqueue_persistent<ClientReply>, std::hash<jint>, std::equal_to<jint> >::_compare = std::equal_to<jint>();

template<> std::hash<jlong>     hashmap_persistent<jlong, ClientReply, std::hash<jlong>, std::equal_to<jlong> >::_hash    = std::hash<jlong>();
template<> std::equal_to<jlong> hashmap_persistent<jlong, ClientReply, std::hash<jlong>, std::equal_to<jlong> >::_compare = std::equal_to<jlong>();

template<> std::hash<jint>      hashmap_persistent<jint, ConsensusInstance>::_hash    = std::hash<jint>();
template<> std::equal_to<jint>  hashmap_persistent<jint, ConsensusInstance>::_compare = std::equal_to<jint>();
