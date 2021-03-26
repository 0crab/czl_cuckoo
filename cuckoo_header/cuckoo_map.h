#ifndef CZL_CUCKOO_CUCKOO_MAP_H
#define CZL_CUCKOO_CUCKOO_MAP_H

#include <cstring>
#include <memory>

#include "bucket_container.hh"
#include "hash_func.h"


thread_local size_t kick_num_l,
        depth0_l, // ready to kick, then find empty slot
        kick_lock_failure_other_lock_l,
        kick_lock_failure_data_check_l,
        kick_lock_failure_cas_failure_l,
        kick_lock_failure_data_check_after_l,
        key_duplicated_after_kick_l,
        kick_send_redo_signal_l;
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
            //uint64_t entry;
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
            static const int ALIGN_RATIO_BOOL = 128 / sizeof (bool);

            KickHazardKeyManager(int thread_num) : running_thread_num(thread_num){
                manager = new std::atomic<size_type>[running_thread_num * ALIGN_RATIO]();
                redo_flags = new std::atomic<bool>[running_thread_num * ALIGN_RATIO_BOOL];
                for(int i = 0; i < running_thread_num * ALIGN_RATIO;i++){
                    manager[i].store(0ul,std::memory_order_relaxed);
                }
                for(int i = 0; i < running_thread_num * ALIGN_RATIO_BOOL; i++ ){
                    redo_flags[i].store(false,std::memory_order_relaxed);
                }

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

            bool inquiry_need_redo(){
                return redo_flags[cuckoo_tid * ALIGN_RATIO_BOOL].load(std::memory_order_seq_cst);
            }

            void clean_redo_flag(){
                redo_flags[cuckoo_tid * ALIGN_RATIO_BOOL].store(false,std::memory_order_seq_cst);
            }

            void set_redo_if_hazard(size_type hash){
                for(int i = 0; i < running_thread_num; i++){
                    if(i == cuckoo_tid) continue;
                    size_type store_record =  manager[i * ALIGN_RATIO].load(std::memory_order_seq_cst);
                    if(is_handled(store_record) && equal_hash(store_record,hash)){
                        redo_flags[i * ALIGN_RATIO_BOOL].store(true,std::memory_order_seq_cst);
                        cout<<"thread "<<i<<" redo find"<<endl;
                    }
                }
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

            std::atomic<bool> * redo_flags;

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

        // CuckooRecord holds one position in a cuckoo path. Since cuckoopath
        // elements only define a sequence of alternate hashings for different hash
        // values, we only need to keep track of the hash values being moved, rather
        // than the keys themselves.
        typedef struct {
            size_type bucket;
            size_type slot;
            hash_value hv;
        } CuckooRecord;

        // The maximum number of items in a cuckoo BFS path. It determines the
        // maximum number of slots we search when cuckooing.
        static constexpr uint8_t MAX_BFS_PATH_LEN = 5;

        // An array of CuckooRecords
        using CuckooRecords = std::array<CuckooRecord, MAX_BFS_PATH_LEN>;

        // A constexpr version of pow that we can use for various compile-time
        // constants and checks.
        static constexpr size_type const_pow(size_type a, size_type b) {
            return (b == 0) ? 1 : a * const_pow(a, b - 1);
        }

        // b_slot holds the information for a BFS path through the table.
        struct b_slot {
            // The bucket of the last item in the path.
            size_type bucket;
            // a compressed representation of the slots for each of the buckets in
            // the path. pathcode is sort of like a base-slot_per_bucket number, and
            // we need to hold at most MAX_BFS_PATH_LEN slots. Thus we need the
            // maximum pathcode to be at least slot_per_bucket()^(MAX_BFS_PATH_LEN).
            uint16_t pathcode;
            //TODO directly comment , maybe existing problem
//            static_assert(const_pow(slot_per_bucket(), MAX_BFS_PATH_LEN) <
//                          std::numeric_limits<decltype(pathcode)>::max(),
//                          "pathcode may not be large enough to encode a cuckoo "
//                          "path");
            // The 0-indexed position in the cuckoo path this slot occupies. It must
            // be less than MAX_BFS_PATH_LEN, and also able to hold negative values.
            int8_t depth;
            static_assert(MAX_BFS_PATH_LEN - 1 <=
                          std::numeric_limits<decltype(depth)>::max(),
                          "The depth type must able to hold a value of"
                          " MAX_BFS_PATH_LEN - 1");
            static_assert(-1 >= std::numeric_limits<decltype(depth)>::min(),
                          "The depth type must be able to hold a value of -1");
            b_slot() {}
            b_slot(const size_type b, const uint16_t p, const decltype(depth) d)
                    : bucket(b), pathcode(p), depth(d) {
                assert(d < MAX_BFS_PATH_LEN);
            }
        };

        // b_queue is the queue used to store b_slots for BFS cuckoo hashing.
        class b_queue {
        public:
            b_queue() noexcept : first_(0), last_(0) {}

            void enqueue(b_slot x) {
                assert(!full());
                slots_[last_++] = x;
            }

            b_slot dequeue() {
                assert(!empty());
                assert(first_ < last_);
                b_slot &x = slots_[first_++];
                return x;
            }

            bool empty() const { return first_ == last_; }

            bool full() const { return last_ == MAX_CUCKOO_COUNT; }

        private:
            // The size of the BFS queue. It holds just enough elements to fulfill a
            // MAX_BFS_PATH_LEN search for two starting buckets, with no circular
            // wrapping-around. For one bucket, this is the geometric sum
            // sum_{k=0}^{MAX_BFS_PATH_LEN-1} slot_per_bucket()^k
            // = (1 - slot_per_bucket()^MAX_BFS_PATH_LEN) / (1 - slot_per_bucket())
            //
            // Note that if slot_per_bucket() == 1, then this simply equals
            // MAX_BFS_PATH_LEN.

            //TODO directly comment , maybe existing problem
//            static_assert(slot_per_bucket() > 0,
//                          "SLOT_PER_BUCKET must be greater than 0.");
//            static constexpr size_type MAX_CUCKOO_COUNT =
//                    2 * ((slot_per_bucket() == 1)
//                         ? MAX_BFS_PATH_LEN
//                         : (const_pow(slot_per_bucket(), MAX_BFS_PATH_LEN) - 1) /
//                           (slot_per_bucket() - 1));
            static const int  MAX_CUCKOO_COUNT = 682;
            // An array of b_slots. Since we allocate just enough space to complete a
            // full search, we should never exceed the end of the array.
            b_slot slots_[MAX_CUCKOO_COUNT];
            // The index of the head of the queue in the array
            size_type first_;
            // One past the index of the last_ item of the queue in the array.
            size_type last_;
        };


        inline bool is_kick_locked(uint64_t entry) const { return kick_lock_mask & entry ;}

        bool kick_lock(size_type bucket_index, size_type slot){

            while(true){

                uint64_t entry = bc.read_from_bucket_slot(bucket_index,slot);
                Item * ptr = extract_ptr(entry);

                if(is_kick_locked(entry)){
                    kick_lock_failure_other_lock_l++;
                    continue;
                }

                if(ptr == nullptr){
                    kick_lock_failure_data_check_l++;
                    return false;
                }

                if(!bc.try_update_entry(bucket_index,slot,entry,entry | kick_lock_mask)){
                    kick_lock_failure_cas_failure_l++;
                    continue;
                }

                return true;
            }

        }
        void kick_unlock(size_type bucket_index, size_type slot){
            uint64_t entry = bc.read_from_bucket_slot(bucket_index,slot);
            ASSERT(is_kick_locked(entry),"try kick unlock an unlocked par_ptr ");
            bool res = bc.try_update_entry(bucket_index,slot,entry, entry & ~kick_lock_mask);
            ASSERT(res,"unlock failure")
        }

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
                size_type entry = bc.read_from_slot(b,i);

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
                size_type entry = bc.read_from_slot(b,i);

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


        // slot_search searches for a cuckoo path using breadth-first search. It
        // starts with the i1 and i2 buckets, and, until it finds a bucket with an
        // empty slot, adds each slot of the bucket in the b_slot. If the queue runs
        // out of space, it fails.
        //
        // throws hashpower_changed if it changed during the search
        b_slot slot_search(const size_type hp, const size_type i1,
                           const size_type i2) {
            b_queue q;
            // The initial pathcode informs cuckoopath_search which bucket the path
            // starts on
            q.enqueue(b_slot(i1, 0, 0));
            q.enqueue(b_slot(i2, 1, 0));
            while (!q.empty()) {
                b_slot x = q.dequeue();
                Bucket &b = bc[x.bucket];
                // Picks a (sort-of) random slot to start from
                size_type starting_slot = x.pathcode % SLOT_PER_BUCKET;
                for (size_type i = 0; i < SLOT_PER_BUCKET; ++i) {
                    uint16_t slot = (starting_slot + i) % SLOT_PER_BUCKET;

                    //block when kick locked
                    uint64_t entry = bc.read_from_slot(b,slot);

                    partial_t kick_partial = extract_partial(entry);

                    if (entry == empty_entry) {
                        // We can terminate the search here
                        x.pathcode = x.pathcode * SLOT_PER_BUCKET + slot;
                        return x;
                    }

                    // If x has less than the maximum number of path components,
                    // have come from if we kicked out the item at this slot.
                    // create a new b_slot item, that represents the bucket we would
                    //const partial_t partial = b.partial(slot);
                    if (x.depth < MAX_BFS_PATH_LEN - 1) {
                        assert(!q.full());
                        b_slot y(alt_index(hp, kick_partial, x.bucket),
                                 x.pathcode * SLOT_PER_BUCKET + slot, x.depth + 1);
                        q.enqueue(y);
                    }
                }
            }
            // We didn't find a short-enough cuckoo path, so the search terminated.
            // Return a failure value.
            return b_slot(0, 0, -1);
        }

        int cuckoopath_search(const size_type hp, CuckooRecords &cuckoo_path,
                              const size_type i1, const size_type i2) {
            b_slot x = slot_search(hp, i1, i2);
            if (x.depth == -1) {
                return -1;
            }
            // Fill in the cuckoo path slots from the end to the beginning.
            for (int i = x.depth; i >= 0; i--) {
                cuckoo_path[i].slot = x.pathcode % SLOT_PER_BUCKET;
                x.pathcode /= SLOT_PER_BUCKET;
            }
            // Fill in the cuckoo_path buckets and keys from the beginning to the
            // end, using the final pathcode to figure out which bucket the path
            // starts on. Since data could have been modified between slot_search
            // and the computation of the cuckoo path, this could be an invalid
            // cuckoo_path.
            CuckooRecord &first = cuckoo_path[0];
            if (x.pathcode == 0) {
                first.bucket = i1;
            } else {
                assert(x.pathcode == 1);
                first.bucket = i2;
            }
            {

                Bucket &b = bc[first.bucket];

                //block when kick locked
                uint64_t entry = bc.read_from_slot(b,first.slot);

                Item * ptr = extract_ptr(entry);

                if (entry == empty_entry) {
                    // We can terminate here
                    return 0;
                }
                first.hv = hashed_key(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr));
            }
            for (int i = 1; i <= x.depth; ++i) {
                CuckooRecord &curr = cuckoo_path[i];
                const CuckooRecord &prev = cuckoo_path[i - 1];
                assert(prev.bucket == index_hash(hp, prev.hv.hash) ||
                       prev.bucket ==
                       alt_index(hp, prev.hv.partial, index_hash(hp, prev.hv.hash)));
                // We get the bucket that this slot is on by computing the alternate
                // index of the previous bucket
                curr.bucket = alt_index(hp, prev.hv.partial, prev.bucket);
                //const auto lock_manager = lock_one(hp, curr.bucket, TABLE_MODE());
                Bucket &b = bc[curr.bucket];

                uint64_t entry = bc.read_from_slot(b,curr.slot);

                Item * ptr = extract_ptr(entry);

                if (entry == empty_entry) {
                    // We can terminate here
                    return i;
                }
                curr.hv = hashed_key(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr));
            }
            return x.depth;
        }

        bool cuckoopath_move(const size_type hp, CuckooRecords &cuckoo_path,
                             size_type depth, TwoBuckets &b) {
            if (depth == 0) {
                // There is a chance that depth == 0, when try_add_to_bucket sees
                // both buckets as full and cuckoopath_search finds one empty. In
                // this case, we lock both buckets. If the slot that
                // cuckoopath_search found empty isn't empty anymore, we unlock them
                // and return false. Otherwise, the bucket is empty and insertable,
                // so we hold the locks and return true.
                depth0_l++;
                const size_type bucket_i = cuckoo_path[0].bucket;
                assert(bucket_i == b.i1 || bucket_i == b.i2);
                //b = lock_two(hp, b.i1, b.i2, TABLE_MODE());

                //block when kick locked
                Bucket & b = bc[bucket_i];

                size_type entry = bc.read_from_slot(b,cuckoo_path[0].slot);

                if (entry == empty_entry ){
                    return true;
                } else {
                    return false;
                }
            }

            while (depth > 0) {
                CuckooRecord &from = cuckoo_path[depth - 1];
                CuckooRecord &to = cuckoo_path[depth];
                const size_type fb = from.bucket;
                const size_type tb = to.bucket;
                const size_type fs = from.slot;
                const size_type ts = to.slot;

                //LockManager extra_manager;

                if(!kick_lock(fb,fs))
                    return false;

                uint64_t mv_entry = bc.read_from_bucket_slot(fb,fs);
                ASSERT(is_kick_locked(mv_entry)," kick lock false");
                ASSERT(extract_ptr(mv_entry) != nullptr,"mv empty entry")

                Item * mv_ptr = extract_ptr(mv_entry);
                size_type mv_hash = hashed_key(ITEM_KEY(mv_ptr),ITEM_KEY_LEN(mv_ptr)).hash;
                if( mv_hash != from.hv.hash){
                    kick_unlock(fb,fs);
                    kick_lock_failure_data_check_after_l ++;
                    return false;
                }

                if(!bc.try_update_entry(tb,ts,empty_entry,mv_entry & ~kick_lock_mask)){
                    kick_unlock(fb,fs);
                    kick_lock_failure_data_check_after_l ++;
                    return false;
                }

                kickHazardKeyManager.set_redo_if_hazard(mv_hash);

                bool res = bc.try_update_entry(fb,fs,mv_entry,empty_entry);
                ASSERT(res,"kick rm from entry fail")

                depth--;
            }

            return true;
        }

        cuckoo_status run_cuckoo(TwoBuckets &b, size_type &insert_bucket,
                                 size_type &insert_slot) {
            size_type hp = hashpower();
            CuckooRecords cuckoo_path;
            bool done = false;
            try {
                //LOOP CONTROL
                size_t loop_count = 0;
                while (!done) {
                    loop_count ++;

                    ASSERT(loop_count < 1000000,"MAYBE DEAD LOOP");
                    const int depth =
                            cuckoopath_search(hp, cuckoo_path, b.i1, b.i2);

                    kick_path_length_log_l[depth]++;
                    if (depth < 0) {
                        break;
                    }

                    //show_cuckoo_path(cuckoo_path,depth);

                    if (cuckoopath_move(hp, cuckoo_path, depth, b)) {
                        insert_bucket = cuckoo_path[0].bucket;
                        insert_slot = cuckoo_path[0].slot;

                        assert(insert_bucket == b.i1 || insert_bucket == b.i2);

                        done = true;
                        break;
                    }
                }
            } catch (hashpower_changed &) {
                // The hashpower changed while we were trying to cuckoo, which means
                // we want to retry. b.i1 and b.i2 should not be locked
                // in this case.
                return failure_under_expansion;
            }
            return done ? ok : failure;
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
                    key_duplicated_after_kick_l++;
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
                        ASSERT(false,"table full,need rehash");
                        throw need_rehash();


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

        inline bool check_ptr(Item * ptr, char *key, size_type key_len) {
            if (ptr == nullptr) return false;
            return str_equal_to()(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr), key, key_len);
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

        size_type get_item_num(){ return bc.get_item_num(true);}

        void get_key_position_info(std::vector<double> & kpv){bc.get_key_position_info(kpv);}



        //true hit , false miss
        bool find(char *key, size_t key_len){
            const hash_value hv = hashed_key(key, key_len);

            ParRegisterManager pm(block_when_rehashing(hv));

            TwoBuckets b = get_two_buckets(hv);


            table_position pos;
            do{
                EpochManager epochManager(bc);
                pos = cuckoo_find(key, key_len, hv.partial, b.i1, b.i2);
            }while(pos.status == failure_key_not_found && kickHazardKeyManager.inquiry_need_redo());
            kickHazardKeyManager.clean_redo_flag();

            if (pos.status == ok) {
                EpochManager epochManager(bc);
                //do some thing
                uint64_t entry = bc.read_from_bucket_slot(pos.index,pos.slot);
                Item * ptr = extract_ptr(entry);
                if(ptr != nullptr)
                    return str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),key,key_len);
                //ASSERT(a,"key error");

                return false;
            }
            return false;
        }

        //true insert , false key failure_key_duplicated
        bool insert(char *key, size_t key_len, char *value, size_t value_len){

            Item * item = bc.allocate_item(key,key_len,value,value_len);
            const hash_value hv = hashed_key(key, key_len);

            while(true){

                ParRegisterManager pm(block_when_rehashing(hv));

                TwoBuckets b = get_two_buckets(hv);
                size_type old_hashpower = hashpower();

                table_position pos;
                try {
                    do{
                        EpochManager epochManager(bc);
                        pos = cuckoo_insert_loop(hv, b, key, key_len);
                    }while(pos.status == failure_key_not_found && kickHazardKeyManager.inquiry_need_redo());
                    kickHazardKeyManager.clean_redo_flag();
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

        //TODO insert or assign also need rehash interface
        //true insert , false update
        bool insert_or_assign(char *key, size_t key_len, char *value, size_t value_len){
            //Item *item = allocate_item(key, key_len, value, value_len);
            Item * item = bc.allocate_item(key,key_len,value,value_len);
            const hash_value hv = hashed_key(key, key_len);

            while (true) {
                //protect from kick
                ParRegisterManager pm(block_when_rehashing(hv));

                TwoBuckets b = get_two_buckets(hv);

                table_position pos;
                do{
                    EpochManager epochManager(bc);
                    pos = cuckoo_insert_loop(hv, b, key, key_len);
                }while(pos.status == failure_key_not_found && kickHazardKeyManager.inquiry_need_redo());
                kickHazardKeyManager.clean_redo_flag();

                if (pos.status == ok) {
                    if (bc.try_update_entry(pos.index, pos.slot,empty_entry,merge_par_ptr(hv.partial, (uint64_t) item))) {
                        return true;
                    }
                } else {
                    uint64_t old_entry = bc.read_from_bucket_slot(pos.index,pos.slot);
                    if(is_kick_locked(old_entry)) continue;
                    Item * old_ptr = extract_ptr(old_entry);

                    //We only confirm to update target key.
                    //Regardless of whether the original key has been replaced at the position indicated by pos
                    //try_update_entry function will deallocate the old entry if CAS success.
                    if (check_ptr(old_ptr, key, key_len)) {
                        if (bc.try_update_entry(pos.index, pos.slot,old_entry,merge_par_ptr(hv.partial, (uint64_t) item))) {
                            bc.deallocate(old_ptr);
                            return false;
                        }
                    }
                }
            }
        }

        //true erase success, false miss
        bool erase(char *key, size_t key_len){
            const hash_value hv = hashed_key(key, key_len);

            while (true) {

                //protect from kick
                ParRegisterManager pm(block_when_rehashing(hv));

                TwoBuckets b = get_two_buckets(hv);

                table_position pos;
                do{
                    EpochManager epochManager(bc);
                    pos = cuckoo_find(key, key_len, hv.partial, b.i1, b.i2);
                }while(pos.status == failure_key_not_found && kickHazardKeyManager.inquiry_need_redo());
                kickHazardKeyManager.clean_redo_flag();

                if (pos.status == ok) {
                    uint64_t old_entry = bc.read_from_bucket_slot(pos.index,pos.slot);
                    if(is_kick_locked(old_entry)) continue;
                    Item * erase_ptr = extract_ptr(old_entry);
                    if (check_ptr(erase_ptr, key, key_len)) {
                        if (bc.try_update_entry(pos.index, pos.slot, old_entry,empty_entry )) {
                            return true;
                        }
                    }
                } else {
                    //return false only when key not find
                    return false;
                }
            }
        }


    private:
        std::atomic<bool> rehash_flag;

        mutable BC bc;

        KickHazardKeyManager  kickHazardKeyManager;

        int cuckoo_thread_num;

    };

}


#endif //CZL_CUCKOO_CUCKOO_MAP_H
