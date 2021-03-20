#ifndef MY_RECLAIMER_BUFFER_QUEUE_H
#define MY_RECLAIMER_BUFFER_QUEUE_H

#include <cstdint>
#include "reclaimer_def.h"


struct Listhead{
    Listhead * prev,* next;
};


#define POINTER void **


#define GET_QUEUE_NODE(p) \
            (Listhead *)((POINTER)p-2)

#define GET_MEM(listnode) \
            (void *)(listnode+1)

#define GET_MALLOC_RETURN(len) \
            (void *)((void **)malloc(len) + 2)
template <typename T>
class BufferQueue{
public:
    BufferQueue();

    T * remove(uint64_t len,bool & malloc_flag);

    void add(void * ptr,uint64_t len);

    T * pop(); //used in freebag

    inline void set_unif_size(uint64_t usize);
    inline uint64_t get_size();
    inline uint64_t get_unif_size();

private:


    uint64_t unif_size; //used in multi_level_queue to identify uniform memory
                        //unif_size does not contain list pointer
    uint64_t size;  //number of block memory the buffer has
    Listhead listhead; //queue head
    PAD;
};

template <typename T>
BufferQueue<T>::BufferQueue():unif_size(0),size(0){
    listhead.prev = & listhead;
    listhead.next = & listhead;
}

template <typename T>
inline void BufferQueue<T>::set_unif_size(uint64_t usize) {
    this->unif_size = usize;
}

template <typename T>
inline uint64_t BufferQueue<T>::get_size() {
    return this->size;
}

template <typename T>
inline uint64_t BufferQueue<T>::get_unif_size() {
    return this->unif_size;
}

//this method only be used in multi_level_queue to allocate a specific size of memory
template <typename T>
T * BufferQueue<T>::remove(uint64_t len,bool & malloc_flag) {
    assert(unif_size !=0 && len <= MAX_UNIF_SIZE);

    void *p;

    if(size > 0 ){
        malloc_flag = false;
        Listhead *head=& listhead;
        Listhead *node;
        node=head->prev;
        head->prev->prev->next=head;
        head->prev=head->prev->prev;
        size --;
        node->prev= nullptr;
        node->next= nullptr;
        p = GET_MEM(node);
    }else{
        tw_info.num_new_item_malloc++;
        malloc_flag = true;
        p=malloc(unif_size + 2 * sizeof(POINTER)); //buffer size + two pointer spaces used to construct lists
        ((Listhead *)p)->prev= nullptr;
        ((Listhead *)p)->next= nullptr;
        p =  (POINTER)p+2;
    }
    return (T*)p;
}

//only used when freeing freebag
template<typename T>
T * BufferQueue<T>::pop() {
    assert(size > 0 );
    Listhead *head=& listhead;
    Listhead *node;
    node=head->prev;
    head->prev->prev->next=head;
    head->prev=head->prev->prev;
    size --;
    node->prev= nullptr;
    node->next= nullptr;
    return (T *)GET_MEM(node);
}

//both used in multi_level_queue and limbo bag
template<typename T>
void BufferQueue<T>::add(void *ptr, uint64_t len) {
    assert(len <= unif_size || unif_size==0);


    Listhead * head= & listhead;
    Listhead * node= GET_QUEUE_NODE(ptr);
    head->next->prev=node;
    node->next=head->next;
    head->next=node;
    node->prev=head;
    size++;
}


#endif //MY_RECLAIMER_BUFFER_QUEUE_H
