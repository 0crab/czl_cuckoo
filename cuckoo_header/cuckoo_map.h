#ifndef CZL_CUCKOO_CUCKOO_MAP_H
#define CZL_CUCKOO_CUCKOO_MAP_H

#include <cstring>
#include <memory>

#include "bucket_container.hh"
#include "hash_func.h"


thread_local size_t kick_num_l,
        depth0_l, // ready to kick, then find empty slot
        kick_lock_failure_data_check_l,
        kick_lock_failure_haza_check_l,
        kick_lock_failure_other_lock_l,
        kick_lock_failure_haza_check_after_l,
        kick_lock_failure_data_check_after_l,
        key_duplicated_after_kick_l;
thread_local size_t kick_path_length_log_l[6];



namespace libcuckoo {

    struct str_equal_to {
        bool operator()(const char *first, size_t first_len, const char *second, size_t second_len) {
            if (first_len != second_len) return false;
            return !std::memcmp(first, second, first_len);
        }
    };

    struct str_hash {
        std::size_t operator()(const char *str, size_t n) const noexcept {
            return MurmurHash64A((void *) str, n);
        }
    };

    template<class KeyEqual = str_equal_to ,
            class Hash = str_hash,
            std::size_t SLOT_PER_BUCKET = DEFAULT_SLOT_PER_BUCKET>
    class Cuckoohash_map{
    private:

        using BC = Bucket_container<SLOT_PER_BUCKET>;
        using Bucket = typename BC::Bucket;

        class hashpower_changed {};
        class need_rehash {};

        enum cuckoo_status {
            ok,
            failure,
            failure_key_not_found,
            failure_key_duplicated,
            failure_table_full,
            failure_under_expansion,
        };

        struct hash_value {
            size_type hash;
            partial_t partial;
        };

        struct table_position {
            size_type index;
            size_type slot;
            cuckoo_status status;
        };

        static partial_t partial_key(const size_type hash) {
            const uint64_t hash_64bit = hash;
            const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                         static_cast<uint32_t>(hash_64bit >> 32));
            const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                         static_cast<uint16_t>(hash_32bit >> 16));
            const uint8_t hash_8bit = (static_cast<uint8_t>(hash_16bit) ^
                                       static_cast<uint8_t>(hash_16bit >> 8));
            return hash_8bit;
        }

        static inline size_type index_hash(const size_type hp, const size_type hv) {
            return hv & hashmask(hp);
        }

        static inline size_type alt_index(const size_type hp, const partial_t partial,
                                          const size_type index) {
            // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
            // hash constant from 64-bit MurmurHash2
            const size_type nonzero_tag = static_cast<size_type>(partial) + 1;
            return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
        }

        hash_value hashed_key(const char *key, size_type key_len) const {
            const size_type hash = Hash()(key, key_len);
            return {hash, partial_key(hash)};
        }

        class TwoBuckets {
        public:
            TwoBuckets() {}

            TwoBuckets(size_type i1_, size_type i2_)
                    : i1(i1_), i2(i2_) {}

            size_type i1, i2;

        };

        class KickHazardKeyManager{
        public:

            static const int ALIGN_RATIO = 128 / sizeof (size_type);

            KickHazardKeyManager(int thread_num) : running_thread_num(thread_num){
                manager = new std::atomic<size_type>[running_thread_num * ALIGN_RATIO]();
                for(int i = 0; i < running_thread_num * ALIGN_RATIO;i++)
                    manager[i].store(0ul,std::memory_order_relaxed);
            }


            bool inquiry_is_registerd(size_type hash){
                for(int i = 0; i < running_thread_num; i++){
                    if(i == cuckoo_tid) continue;
                    size_type store_record =  manager[i * ALIGN_RATIO].load(std::memory_order_seq_cst);
                    if(is_handled(store_record) && equal_hash(store_record,hash)){
                        return true;
                    }
                }
                return false;
            }

            std::atomic<size_type> * register_hash(size_type hash) {
                manager[cuckoo_tid * ALIGN_RATIO].store(con_store_record(hash),std::memory_order_seq_cst);
                return &manager[cuckoo_tid * ALIGN_RATIO];
            }

            //this function will not be called because the unique_ptr is used to log out automatically
            void cancellate_hash(){
                manager[cuckoo_tid * ALIGN_RATIO].store(0ul,std::memory_order_seq_cst);
            }


            bool empty(){
                for(int i  = 0 ; i < running_thread_num ; i++){
                    size_type store_record = manager[i * ALIGN_RATIO].load(std::memory_order_seq_cst);
                    if(is_handled(store_record)) return false;
                }
                return true;
            }

            inline size_type con_store_record(size_type hash){return hash | 1ul;}
            inline bool is_handled(size_type store_record){return store_record & handle_mask;}
            inline bool equal_hash(size_type store_record,size_type hash){
                ASSERT(is_handled(store_record),"compare record not be handled");
                return (store_record & hash_mask ) == ( hash & hash_mask);
            }

            int running_thread_num;

            std::atomic<size_type>  * manager;

            size_type hash_mask = ~0x1ul;
            size_type handle_mask = 0x1ul;

        };

        struct ParRegisterDeleter {
            void operator()(std::atomic<size_type> *l) const { l->store(0ul); }
        };

        using ParRegisterManager = std::unique_ptr<std::atomic<size_type>, ParRegisterDeleter>;

        class HashRegisterManager{
            std::atomic<size_type> * holder;
        };


        class EpochManager{
            friend class Cuckoohash_map<>;

            EpochManager(BC & bc):bc_(bc) {
                bc_.startOp();
            }
            ~EpochManager(){
                bc_.endOp();
            }
            BC & bc_;
        };


        inline bool is_kick_locked(uint64_t entry) const { return kick_lock_mask & entry ;}
        inline bool try_kick_lock_entry(std::atomic<uint64_t> & atomic_par_ptr){ASSERT(false,"not implement yet")}
        inline void kick_unlock_entry(std::atomic<uint64_t> & atomic_par_ptr){ASSERT(false,"not implement yet")}
        bool kick_lock(size_type bucket_index, size_type slot){ASSERT(false,"not implement yet")}
        void kick_unlock(size_type bucket_index, size_type slot){ASSERT(false,"not implement yet")}

        TwoBuckets get_two_buckets(const hash_value &hv) const {
            const size_type hp = hashpower();
            const size_type i1 = index_hash(hp, hv.hash);
            const size_type i2 = alt_index(hp, hv.partial, i1);
            return TwoBuckets(i1, i2);
        }

        int try_read_from_bucket( Bucket &b, const partial_t partial,
                                  const char *key, size_type key_len) const {

            for (int i = 0; i < SLOT_PER_BUCKET; ++i) {
                //block when kick
                size_type entry;
                do{
                    entry = bc.read_from_slot(b,i);
                }
                while(is_kick_locked(entry));

                partial_t read_partial = extract_partial(entry);
                Item * read_ptr = extract_ptr(entry);
                if (read_ptr ==  nullptr ||
                    (partial != read_partial)) {
                    continue;
                } else if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                    return i;
                }
            }
            return -1;
        }

        table_position cuckoo_find(const char *key, size_type key_len, const partial_t partial,
                                   const size_type i1, const size_type i2) const {
            int slot = try_read_from_bucket(bc[i1], partial, key, key_len);
            if (slot != -1) {
                return table_position{i1, static_cast<size_type>(slot), ok};
            }
            slot = try_read_from_bucket(bc[i2], partial, key, key_len);
            if (slot != -1) {
                return table_position{i2, static_cast<size_type>(slot), ok};
            }
            return table_position{0, 0, failure_key_not_found};
        }

        //false : key_duplicated. the slot is the position of deplicated key
        //true : have empty slot.the slot is the position of empty slot. -1 -> no empty slot
        bool try_find_insert_bucket( Bucket &b, int &slot,
                                     const partial_t partial, const char *key, size_type key_len) const {
            // Silence a warning from MSVC about partial being unused if is_simple.
            //(void)partial;
            slot = -1;
            for (int i = 0; i < SLOT_PER_BUCKET; ++i) {
                //block when kick
                size_type entry;
                do{
                    entry = bc.read_from_slot(b,i);
                }
                while(is_kick_locked(entry));

                partial_t read_partial = extract_partial(entry);
                Item * read_ptr = extract_ptr(entry);

                if (read_ptr != nullptr) {
                    if (partial != read_partial) {
                        continue;
                    }
                    if (str_equal_to()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                        slot = i;
                        return false;
                    }
                } else {
                    slot = slot == -1 ? i : slot;
                }
            }
            return true;
        }

        cuckoo_status run_cuckoo(TwoBuckets &b, size_type &insert_bucket,
                                 size_type &insert_slot) {
            ASSERT(false,"not implement yet")
        }
        table_position cuckoo_insert(const hash_value hv, TwoBuckets &b, char *key, size_type key_len) {
            int res1, res2;
            Bucket &b1 = bc[b.i1];
            if (!try_find_insert_bucket(b1, res1, hv.partial, key, key_len)) {
                return table_position{b.i1, static_cast<size_type>(res1),
                                      failure_key_duplicated};
            }
            Bucket &b2 = bc[b.i2];
            if (!try_find_insert_bucket(b2, res2, hv.partial, key, key_len)) {
                return table_position{b.i2, static_cast<size_type>(res2),
                                      failure_key_duplicated};
            }
            if (res1 != -1) {
                return table_position{b.i1, static_cast<size_type>(res1), ok};
            }
            if (res2 != -1) {
                return table_position{b.i2, static_cast<size_type>(res2), ok};
            }

            //We are unlucky, so let's perform cuckoo hashing.
            size_type insert_bucket = 0;
            size_type insert_slot = 0;
            cuckoo_status st = run_cuckoo(b, insert_bucket, insert_slot);
            kick_num_l ++;

            if (st == failure_under_expansion) {
                // The run_cuckoo operation operated on an old version of the table,
                // so we have to try again. We signal to the calling insert method
                // to try again by returning failure_under_expansion.
                return table_position{0, 0, failure_under_expansion};
            } else if (st == ok) {

                // Since we unlocked the buckets during run_cuckoo, another insert
                // could have inserted the same key into either b.i1 or
                // b.i2, so we check for that before doing the insert.
                table_position pos = cuckoo_find(key,key_len, hv.partial, b.i1, b.i2);
                if (pos.status == ok) {
                    pos.status = failure_key_duplicated;
                    return pos;
                }
                return table_position{insert_bucket, insert_slot, ok};
            }
            ASSERT(st == failure,"st type error");
            return table_position{0, 0, failure_table_full};

        }

        table_position cuckoo_insert_loop(hash_value hv, TwoBuckets &b, char *key, size_type key_len) {
            table_position pos;
            while (true) {
                const size_type hp = hashpower();
                pos = cuckoo_insert(hv, b, key, key_len);
                switch (pos.status) {
                    case ok:
                    case failure_key_duplicated:
                        return pos;
                    case failure_table_full:
                        throw need_rehash();

                        ASSERT(false,"table full,need rehash");
                        // Expand the table and try again, re-grabbing the locks
                        //cuckoo_fast_double<TABLE_MODE, automatic_resize>(hp);
                        //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                        break;
                    case failure_under_expansion:
                        assert(false);
                        // The table was under expansion while we were cuckooing. Re-grab the
                        // locks and try again.
                        //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                        break;
                    default:
                        assert(false);
                }
            }
        }


        std::atomic<size_type> * block_when_rehashing(const hash_value hv ){
            std::atomic<size_type> * tmp_handle;

            while(true){

                while( rehash_flag.load(std::memory_order_seq_cst) ){pthread_yield();}

                tmp_handle = kickHazardKeyManager.register_hash(hv.hash);

                if(!rehash_flag.load(std::memory_order_seq_cst)) break;

                tmp_handle->store(0ul);

            }
            return tmp_handle;
        }

        inline void wait_for_other_thread_finish(){
            while(!kickHazardKeyManager.empty()){pthread_yield();}
        }

        //TODO
        inline bool check_insert_unique(table_position pos,TwoBuckets b,hash_value hv,Item * item){
            ;
        }

    public:
        Cuckoohash_map(size_type hp = DEFAULT_HASHPOWER,int thread_num=0):
                    cuckoo_thread_num(thread_num),
                    rehash_flag(false),
                    kickHazardKeyManager(thread_num),
                    bc(hp,thread_num){

        }

        void init_thread(int tid){ bc.init_thread(tid); }

        inline size_type hashpower() const { return bc.get_hashpower(); }

        static inline size_type hashmask(const size_type hp) { return hashsize(hp) - 1; }

        static inline size_type hashsize(const size_type hp) { return size_type(1) << hp; }

        inline size_type bucket_num(){return bc.get_size();}

        inline size_type slot_num(){return bc.get_slot_num();}

        size_type get_item_num(){bc.get_item_num(true);}

        void get_key_position_info(std::vector<double> & kpv){bc.get_key_position_info(kpv);}



        //true hit , false miss
        bool find(char *key, size_t key_len){
            const hash_value hv = hashed_key(key, key_len);

            ParRegisterManager pm(block_when_rehashing(hv));

            TwoBuckets b = get_two_buckets(hv);

            table_position pos;
            {
                EpochManager epochManager(bc);
                pos = cuckoo_find(key, key_len, hv.partial, b.i1, b.i2);
            }


            if (pos.status == ok) {
                EpochManager epochManager(bc);
                //do some thing
                uint64_t entry = bc.read_from_bucket_slot(pos.index,pos.slot);
                Item * ptr = extract_ptr(entry);
                bool a = str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),key,key_len);
                ASSERT(a,"key error");

                return true;
            }
            return false;
        }

        //true insert , false key failure_key_duplicated
        bool insert(char *key, size_t key_len, char *value, size_t value_len){
            while(true){
                //Item *item = allocate_item(key, key_len, value, value_len);
                Item * item = bc.allocate_item(key,key_len,value,value_len);
                const hash_value hv = hashed_key(key, key_len);

                ParRegisterManager pm(block_when_rehashing(hv));

                TwoBuckets b = get_two_buckets(hv);
                table_position pos;
                size_type old_hashpower = hashpower();

                try {

                    EpochManager epochManager(bc);
                    pos = cuckoo_insert_loop(hv, b, key, key_len);

                }catch (need_rehash){

                    pm.get()->store(0ul);

                    bool old_flag = false;
                    if(rehash_flag.compare_exchange_strong(old_flag,true)){

                        if(old_hashpower != hashpower()){
                            //ABA,other thread has finished rehash.release rehash_flag and redo
                            rehash_flag.store(false);
                            continue;
                        }else{
                            wait_for_other_thread_finish();

                            //TODO do not annotation
                            //migrate_to_new();

                            rehash_flag.store(false);

                            continue;
                        }

                    }else{
                        continue;
                    }

                }

                if (pos.status == ok) {
                    if (bc.try_update_entry(pos.index, pos.slot,empty_entry,merge_par_ptr(hv.partial, (uint64_t) item))) {
                        return true;
                        //return check_insert_unique(pos,b,hv,item);
                    }
                } else {
                    //key_duplicated
                    //buckets_.deallocator->read(cuckoo_thread_id);
                    bc.deallocate(item);
                    return false;
                }

            }

        }

        //true insert , false update
        bool insert_or_assign(char *key, size_t key_len, char *value, size_t value_len){;}

        //true erase success, false miss
        bool erase(char *key, size_t key_len){;}


    private:
        std::atomic<bool> rehash_flag;

        mutable BC bc;

        KickHazardKeyManager  kickHazardKeyManager;

        int cuckoo_thread_num;

    };

}


#endif //CZL_CUCKOO_CUCKOO_MAP_H
