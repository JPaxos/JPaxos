#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>

template <typename T>
class linkedqueue_persistent {
    struct element {
        element(const T& v) : value(v){}
        
        pmem::obj::p<T> value;
        pmem::obj::persistent_ptr<element> next {nullptr};
    };
    
    pmem::obj::persistent_ptr<element> head {nullptr};
    pmem::obj::persistent_ptr<element> tail {nullptr};
    pmem::obj::p<size_t> elementCount {0};
    
    struct iterator_base {
        iterator_base(const pmem::obj::persistent_ptr<element> & start) : current(start){}
        
        pmem::obj::persistent_ptr<element> current;
        
        iterator_base &         operator ++() {current = current->next; return *this;}
        bool                    operator !=(const iterator_base & o) const {return o.current!=current;}
    };
    struct const_iterator : public iterator_base {
        const_iterator(const pmem::obj::persistent_ptr<element> & start) : iterator_base(start){}
        const pmem::obj::p<T> * operator ->() const {return &this->current->value;}
        const pmem::obj::p<T> & operator * () const {return this->current->value;}
    };
    struct iterator : public const_iterator {
        iterator(const pmem::obj::persistent_ptr<element> & start) : const_iterator(start){}
        pmem::obj::p<T> * operator ->() {return &this->current->value;}
        pmem::obj::p<T> & operator * () {return this->current->value;}
    };
    
    
public:
    linkedqueue_persistent() = default;
    void push_back (pmem::obj::pool_base & pop, const T & v) {
        pmem::obj::transaction::run(pop, [&]{
            if(head == nullptr){
                head = pmem::obj::make_persistent<element>(v);
                tail = head;
            } else {
                tail->next = pmem::obj::make_persistent<element>(v);
                tail = tail->next;
            }
            elementCount.get_rw()++;
        });
    }
    void clear(pmem::obj::pool_base & pop) {
        pmem::obj::transaction::run(pop, [&]{
            pmem::obj::persistent_ptr<element> next, curr = head;
            while(curr) {
                next = curr->next;
                pmem::obj::delete_persistent<element>(curr);
                curr = next;
            }
            head = nullptr;
            tail = nullptr;
            elementCount.get_rw()=0;
        });
    }
    
    T pop_front(pmem::obj::pool_base & pop){
        T e;
        pmem::obj::transaction::run(pop, [&]{
            if(head==nullptr)
                throw "Removing from an empty list";
            
            if(head==tail){
                // 1 element only 
                e = head->value.get_ro();
                pmem::obj::delete_persistent<element>(head);
                head = nullptr;
                tail = nullptr;
                return;
            }
            
            // more elements
            e = head->value.get_ro();
            auto next = head->next;
            pmem::obj::delete_persistent<element>(head);
            head = next;
            return;
        });
        elementCount.get_rw()--;
        return e;
    }
    
    const T& front(){
        if(head==nullptr)
            throw "Reading head of an empty list";
        return head->value.get_ro();
    }
    
    size_t count() const {return elementCount.get_ro();}
    
    iterator begin() {return iterator(head);}
    const_iterator begin() const {return iterator(head);}
    
    iterator end() {return iterator(nullptr);}
    const_iterator end() const {return iterator(nullptr);}
};
