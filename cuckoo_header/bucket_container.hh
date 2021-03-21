#ifndef CZL_CUCKOO_BUCKET_CONTAINER_HH
#define CZL_CUCKOO_BUCKET_CONTAINER_HH

#include <array>
#include <atomic>
#include <mutex>
#include <vector>

#include "cuckoohash_config.hh"
#include "cuckoohash_config.hh"
#include "item.h"
#include "reclaimer_debra.h"


namespace libcuckoo {

    using size_type = size_t;
    using partial_t = uint8_t;
    using Reclaimer = Reclaimer_debra;

    thread_local int cuckoo_tid;
    thread_local bool thread_initialized;

    static const uint64_t empty_entry = 0ll;

    static const int partial_offset = 56;
    static const uint64_t partial_mask = 0xffull << partial_offset;
    static const uint64_t ptr_mask = 0xffffffffffffull; //lower 48bit
    static const uint64_t kick_lock_mask = 1ull << (partial_offset - 1);


    inline static Item *extract_ptr(uint64_t entry) {
        return (Item *) (entry & ptr_mask);
    }

    inline static partial_t extract_partial(uint64_t entry) {
        return static_cast<partial_t>((entry & partial_mask) >> partial_offset);
    }

    inline uint64_t merge_par_ptr(partial_t partial, uint64_t ptr) {
        return (((uint64_t) partial << partial_offset) | ptr);
    }

    template<size_type SLOT_PER_BUCKET>
    class Bucket_container {

    public:
        friend class BC;
        Bucket_container(size_type hp, int cuckoo_thread_num) : rc(cuckoo_thread_num){
            set_hashpower(hp);
            buckets_ = new Bucket[get_size()]();
        };

        ~Bucket_container() { destory_bc(); }

        void destory_bc() {
            bool still_have_item = false;
            for (size_type i = 0; i < get_size(); ++i) {
                for (size_type j = 0; j < SLOT_PER_BUCKET; j++) {
                    if (buckets_[i].values_[j].load(std::memory_order_relaxed) != empty_entry) {
                        still_have_item = true;
                        break;
                    }
                }
            }
            ASSERT(!still_have_item, "bucket still have item!");
            delete[]buckets_;
        }

        class Bucket {
        public:
            friend class bucket_container;

            Bucket() {
                for (size_type i = 0; i < SLOT_PER_BUCKET; i++)
                    values_[i].store((uint64_t) nullptr);
            };

            ~Bucket() = default;


            bool empty() {
                //ASSERT(!table_anaz_mtx.try_lock(), "Bucket.empty need to be locked before call!");
                for (size_type i = 0; i < SLOT_PER_BUCKET; i++) {
                    if (values_[i].load() != (uint64_t) nullptr)
                        return false;
                }
                return true;
            };

            uint64_t get_entry(size_type i) {
                return values_[i].load(std::memory_order_relaxed);
            };
            std::array<std::atomic<uint64_t>, SLOT_PER_BUCKET> values_;
        };

        //??
        Bucket &operator[](size_type i) { return buckets_[i]; }

        const Bucket &operator[](size_type i) const { return buckets_[i]; }


        void init_thread(int tid) {
            cuckoo_tid = tid;
            rc.initThread(tid);
        }

        void swap(Bucket_container &bc);

        inline size_type get_hashpower() const { return hashpower_.load(std::memory_order_relaxed); }

        inline void set_hashpower(size_type hp) { hashpower_.store(hp); }

        inline size_type get_size() const { return size_type(1) << get_hashpower(); }

        inline size_type get_slot_num() const { return 4 * get_size(); }



        void startOp(){rc.startOp(cuckoo_tid);}
        void endOp(){rc.endOp(cuckoo_tid);}
        void deallocate(Item * ptr){rc.deallocate(cuckoo_tid,ptr);}
        Item *allocate_item(char *key, size_t key_len, char *value, size_t value_len) {
            //Item * p = (Item * )malloc(key_len + value_len + 2 * sizeof(uint32_t));
            Item *p = (Item *) rc.allocate(cuckoo_tid, ITEM_LEN_ALLOC(key_len, value_len));
            ASSERT(p != nullptr, "malloc failure");
            p->key_len = key_len;
            p->value_len = value_len;
            memcpy(ITEM_KEY(p), key, key_len);
            memcpy(ITEM_VALUE(p), value, value_len);
            return p;
        }



        inline size_type read_from_slot(Bucket &b, size_type slot) {
            return b.values_[slot].load(std::memory_order_relaxed);
        }

        inline size_type read_from_bucket_slot(size_type ind, size_type slot) {
            return buckets_[ind].values_[slot].load(std::memory_order_relaxed);
        }

        inline std::atomic<size_t> &get_atomic_entry(size_type ind, size_type slot) {
            return buckets_[ind].values_[slot];
        }



        //do insert: just input old_entry = empty_entry;
        //do erase: just input update_entry = empty_entry;
        //this function does not deallocate item
        bool try_update_entry(size_type ind, size_type slot, uint64_t old_entry, uint64_t update_entry) {
            Bucket &b = buckets_[ind];
            uint64_t old = b.values_[slot].load(std::memory_order_relaxed);
            if (old != old_entry) return false;
            if (b.values_[slot].compare_exchange_strong(old, update_entry)) {
                return true;
            } else {
                return false;
            }
        }


        //Count the number of items only if strong is true
        uint64_t get_item_num(bool strong) {
            if (strong || tmp_kv_num_store_ == 0) {
                table_anaz_mtx.lock();
                uint64_t count = 0;
                for (size_t i = 0; i < get_size(); i++) {
                    Bucket &b = buckets_[i];
                    for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
                        if (b.values_[j].load(std::memory_order_relaxed) != empty_entry) {
                            count++;
                        }
                    }
                }
                tmp_kv_num_store_ = count;
                table_anaz_mtx.unlock();
                return count;
            } else {
                return tmp_kv_num_store_;
            }

        }

        void get_key_position_info(std::vector<double> &kpv) {
            table_anaz_mtx.lock();
            ASSERT(kpv.size() == SLOT_PER_BUCKET, "key_position_info length error");
            std::vector<uint64_t> count_vtr(SLOT_PER_BUCKET);
            for (size_type i = 0; i < get_size(); i++) {
                Bucket &b = buckets_[i];
                for (int j = 0; j < SLOT_PER_BUCKET; j++) {
                    if (b.values_[j].load(std::memory_order_relaxed) != empty_entry) {
                        count_vtr[j]++;
                    }
                }
            }

            for (size_type i = 0; i < SLOT_PER_BUCKET; i++) {
                kpv[i] = count_vtr[i] * 1.0 / get_item_num(false);
            }
            table_anaz_mtx.unlock();
        }



    private:
        std::atomic<size_type> hashpower_;

        Bucket *buckets_;

        Reclaimer rc;

        std::mutex table_anaz_mtx;

        size_type tmp_kv_num_store_;

    };


}


#endif //CZL_CUCKOO_BUCKET_CONTAINER_HH
