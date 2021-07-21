#ifndef HASHMAP_PERSISTENT_SYNCHRONIZED_H
#define HASHMAP_PERSISTENT_SYNCHRONIZED_H

#include "hashmap_persistent.hpp"

#include <mutex>
#include <shared_mutex>

/**
 * WARNING: superclass intentionally does not have virtual methods
 * 
 * WARNING: iterating over / transactions require manual lockUnique / lockShared
 **/
template <typename K, typename V, class Hash = std::hash<K>, class Compare = std::equal_to<K>>
class hashmap_persistent_synchronized : public hashmap_persistent<K,V,Hash,Compare> {

    mutable std::shared_mutex mutex;

public:
    hashmap_persistent_synchronized(pmem::obj::pool_base & pop, size_t bucketCount = 128)
        : hashmap_persistent<K,V,Hash,Compare> (pop, bucketCount) {}

    hashmap_persistent_synchronized & operator=(const hashmap_persistent_synchronized &) = delete;

    template<typename... ConstructorArgs>
    pmem::obj::p<V>& get (pmem::obj::pool_base & pop, const K & k, ConstructorArgs... constructorArgs) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        return hashmap_persistent<K,V,Hash,Compare>::get(pop, k, constructorArgs...);
    }

    pmem::obj::p<V> * get_if_exists (const K & k) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return hashmap_persistent<K,V,Hash,Compare>::get_if_exists(k);
    }

    const pmem::obj::p<V> * get_if_exists (const K & k) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return hashmap_persistent<K,V,Hash,Compare>::get_if_exists(k);
    }

    bool erase(pmem::obj::pool_base & pop, const K & k) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        return hashmap_persistent<K,V,Hash,Compare>::erase(pop, k);
    }

    void clear(pmem::obj::pool_base & pop) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        hashmap_persistent<K,V,Hash,Compare>::clear(pop);
    }

    std::unique_lock<std::shared_mutex> lockUnique(){
        return std::unique_lock<std::shared_mutex>(mutex);
    }

    std::shared_lock<std::shared_mutex> lockShared() const {
        return std::shared_lock<std::shared_mutex>(mutex);
    }

};

#endif // HASHMAP_PERSISTENT_SYNCHRONIZED_H



