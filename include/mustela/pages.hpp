#ifndef pages_hpp
#define pages_hpp

#include <string>
#include <cstring>

namespace mustela {
    template<class T>
    void unpack_uint_be(const unsigned char * buf, unsigned si, T & val){
        T result = 0;
        for(unsigned i = 0; i != si; ++i ) {
            result <<= 8;
            result |= buf[i];
        }
        val = result;
    }
    template<class T>
    void unpack_uint_be(const char * buf, unsigned si, T & val){
        return unpack_uint_be((const unsigned char *)buf, si, val);
    }
    template<class T>
    void pack_uint_be(unsigned char * buf, unsigned si, T val){
        for(unsigned i = si; i-- > 0; ) {
            buf[i] = static_cast<unsigned char>(val);
            val >>= 8;
        }
    }
    template<class T>
    void pack_uint_be(char * buf, unsigned si, T val){
        return pack_uint_be((unsigned char *)buf, si, val);
    }
    unsigned char get_compact_size_sqlite4(uint64_t val);
    unsigned char read_u64_sqlite4(uint64_t & val, const void * ptr);
    unsigned char write_u64_sqlite4(uint64_t val, void * ptr);
    class Exception {
    public:
        explicit Exception(const std::string & what)
        {}
    };
    inline void ass(bool expr, std::string what){
        if( !expr )
            throw Exception(what);
    }
    struct MVal {
        char * data;
        size_t size;
        MVal(char * data, size_t size):data(data), size(size)
        {}
        char * end()const{ return data + size; }
    };
    struct Val {
        const char * data;
        size_t size;
        
        explicit Val():data(nullptr), size(0)
        {}
        explicit Val(const char * data, size_t size):data(data), size(size)
        {}
        explicit Val(const char * data):data(data), size(strlen(data))
        {}
        explicit Val(const std::string & str):data(str.data()), size(str.size())
        {}
        Val(const MVal & mval):data(mval.data), size(mval.size) // allow conversion
        {}
        const char * end()const{ return data + size; }

        std::string to_string()const{
            return std::string((char *)data, size);
        }
        int compare(const Val & other)const;
        bool operator==(const Val & other)const{
            return compare(other) == 0;
        }
        bool operator!=(const Val & other)const{
            return !operator==(other);
        }
        bool operator<(const Val & other)const{
            return compare(other) < 0;
        }
        size_t encoded_size()const;
    };

    typedef uint64_t Tid;
    typedef uint64_t Pid;
    typedef uint16_t PageOffset;
    
    constexpr uint32_t OUR_VERSION = 1;

    constexpr uint64_t MAX_MAPPING_SIZE = 1ULL << 34; // 16GB for now
    constexpr uint64_t META_MAGIC = 0x58616c657473754d; // MustelaX in binary form
    constexpr uint64_t META_MAGIC_ALTENDIAN = 0x4d757374656c6158;
    constexpr int NODE_PID_SIZE = 4; // 4 bytes to store page index will result in ~4 billion pages limit, or 16TB max for 4KB pyges
    // fixed pid size allows simple logic when replacing page in node index

#pragma pack(push, 1)
    struct MetaPage { // We use uint64_t here independent of Page and PageOffset to make meta pages readable across platforms
        uint64_t pid;
        uint64_t tid;
        uint64_t magic;
        uint32_t version;
        uint32_t page_size;
        uint64_t page_count; // excess pages in file are all free
        uint64_t free_root_page;
        uint64_t main_root_page;
        uint64_t main_height; // 0 when root is leaf
        // uint64_t tid2; // protect against write shredding (if tid does not match tid2, use lowest of two as an effective tid)

        bool check(uint32_t system_page_size, uint64_t file_size)const;
    };
    struct DataPage {
        Pid pid; /// for consistency debugging. Never written except in get_free_page
        Tid tid; /// transaction which did write the page

        PageOffset lower_bound_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key, bool * found)const;
        PageOffset upper_bound_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key)const;
//        PageOffset find_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key)const;
        MVal get_item_key(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, PageOffset item);
        Val get_item_key(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, PageOffset item)const{
            return const_cast<DataPage *>(this)->get_item_key(page_size, item_offsets, item_count, item);
        }
        size_t item_size(uint32_t page_size, bool is_leaf, const PageOffset * item_offsets, PageOffset item_count, PageOffset item)const;
        void remove_simple(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset to_remove_item);
        MVal insert_at(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset insert_index, Val key, size_t kv_size);
    };
    struct NodePage : public DataPage {
        //uint64_t total_item_count; // with child pages
        PageOffset item_count;
        PageOffset items_size; // bytes keys+values + their sizes occupy. for branch pages instead of svalue we store pagenum
        PageOffset free_end_offset; // we can have a bit of gaps, will shift when free middle space not enough to store new item
        PageOffset item_offsets[1];
        // each NodePage has NODE_PID_SIZE bytes at the end, storing the last link to child, which has no associated key
        enum { NODE_HEADER_SIZE = sizeof(Pid)+sizeof(Tid)+3*sizeof(PageOffset) };
        // Branch page // TODO insert total_item_count
        // header [io0, io1, io2] free_middle [skey2 page_be2, gap, skey0 page_be0, gap, skey1 page_be1] page_last

        void init_dirty(uint32_t page_size, Tid tid);
        void clear(uint32_t page_size){
            init_dirty(page_size, this->tid);
        }
        PageOffset lower_bound_item(uint32_t page_size, Val key, bool * found = nullptr)const{
            return DataPage::lower_bound_item(page_size, item_offsets, item_count, key, found);
        }
        PageOffset upper_bound_item(uint32_t page_size, Val key)const{
            return DataPage::upper_bound_item(page_size, item_offsets, item_count, key);
        }
//        PageOffset find_item(uint32_t page_size, Val key)const{
//            return DataPage::find_item(page_size, item_offsets, item_count, key);
//        }
        MVal get_item_key(uint32_t page_size, PageOffset item){
            return DataPage::get_item_key(page_size, item_offsets, item_count, item);
        }
        Val get_item_key(uint32_t page_size, PageOffset item)const{
            return DataPage::get_item_key(page_size, item_offsets, item_count, item);
        }
        Val get_item_kv(uint32_t page_size, PageOffset item, Pid & val)const;
        
        // to get special first value without key, set item to -1
        Pid get_item_value(uint32_t page_size, PageOffset item)const;
        void set_item_value(uint32_t page_size, PageOffset item, Pid value);

        size_t item_size(uint32_t page_size, PageOffset item)const{
            return DataPage::item_size(page_size, false, item_offsets, item_count, item);
        }
        PageOffset kv_size(uint32_t page_size, Val key, Pid value)const;
        bool enough_size_for_elements(uint32_t page_size, PageOffset count, size_t total_kvsize)const{
            size_t free_size = page_size - NODE_HEADER_SIZE - NODE_PID_SIZE;
            return free_size >= count * sizeof(PageOffset) + total_kvsize;
        }
        
        bool has_enough_free_space(uint32_t page_size, size_t required_size)const{
            size_t free_size = page_size - NODE_HEADER_SIZE - NODE_PID_SIZE - items_size - item_count * sizeof(PageOffset);
            return free_size >= required_size;
        }

        void remove_simple(uint32_t page_size, PageOffset to_remove_item){
            DataPage::remove_simple(page_size, false, item_offsets, item_count, items_size, free_end_offset, to_remove_item);
            if( item_count == 0)
                free_end_offset = page_size - NODE_PID_SIZE; // compact on last delete :)
        }
        void compact(uint32_t page_size, size_t kv_size2);
        void insert_at(uint32_t page_size, PageOffset insert_index, Val key, Pid value){
            ass(insert_index <= item_count, "Cannot insert at this index");
            size_t kv_size2 = kv_size(page_size, key, value);
            compact(page_size, kv_size2);
            ass(NODE_HEADER_SIZE + sizeof(PageOffset)*(item_count + 1) + kv_size2 <= free_end_offset, "No space to insert in node");
            MVal new_key = DataPage::insert_at(page_size, false, item_offsets, item_count, items_size, free_end_offset, insert_index, key, kv_size2);
            pack_uint_be((unsigned char *)new_key.end(), NODE_PID_SIZE, value);
        }
        PageOffset half_size(uint32_t page_size)const{
            return (page_size - NODE_HEADER_SIZE - NODE_PID_SIZE)/2;
        }
        PageOffset data_size()const{
            return items_size + item_count * sizeof(PageOffset);
        }
        // -------------------- Old below



        void copy_from(uint32_t page_size, bool is_leaf, const DataPage * src, PageOffset item); // we should have space
        void compact(uint32_t page_size, bool is_leaf, size_t required_size);

        size_t leaf_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Val value);
        size_t node_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Pid value);
        
    };
    struct LeafPage : public DataPage {
        PageOffset item_count;
        PageOffset items_size; // bytes keys+values + their sizes occupy. for branch pages instead of svalue we store pagenum
        PageOffset free_end_offset; // we can have a bit of gaps, will shift when free middle space not enough to store new item
        PageOffset item_offsets[1];
        enum { LEAF_HEADER_SIZE = sizeof(Pid)+sizeof(Tid)+3*sizeof(PageOffset) };
        // Leaf page
        // header [io0, io1, io2] free_middle [skey2 svalue2, gap, skey0 svalue0, gap, skey1 svalue1]
        
        void init_dirty(uint32_t page_size, Tid tid);
        void clear(uint32_t page_size){
            init_dirty(page_size, this->tid);
        }
        PageOffset lower_bound_item(uint32_t page_size, Val key, bool * found = nullptr)const{
            return DataPage::lower_bound_item(page_size, item_offsets, item_count, key, found);
        }
//        PageOffset find_item(uint32_t page_size, Val key)const{
//            return DataPage::find_item(page_size, item_offsets, item_count, key);
//        }
        MVal get_item_key(uint32_t page_size, PageOffset item){
            return DataPage::get_item_key(page_size, item_offsets, item_count, item);
        }
        Val get_item_key(uint32_t page_size, PageOffset item)const{
            return DataPage::get_item_key(page_size, item_offsets, item_count, item);
        }
        Val get_item_kv(uint32_t page_size, PageOffset item, Val & val)const;
        size_t item_size(uint32_t page_size, PageOffset item){
            return DataPage::item_size(page_size, true, item_offsets, item_count, item);
        }
        void remove_simple(uint32_t page_size, PageOffset to_remove_item){
            DataPage::remove_simple(page_size, true, item_offsets, item_count, items_size, free_end_offset, to_remove_item);
            if( item_count == 0)
                free_end_offset = page_size; // compact on last delete :)
        }
        void compact(uint32_t page_size, size_t kv_size2);
        void insert_at(uint32_t page_size, PageOffset insert_index, Val key, Val value){
            ass(insert_index <= item_count, "Cannot insert at this index");
            size_t kv_size2 = kv_size(page_size, key, value);
            compact(page_size, kv_size2);
            ass(LEAF_HEADER_SIZE + sizeof(PageOffset)*(item_count + 1) + kv_size2 <= free_end_offset, "No space to insert in leaf");
            MVal new_key = DataPage::insert_at(page_size, true, item_offsets, item_count, items_size, free_end_offset, insert_index, key, kv_size2);
            auto valuesizesize = write_u64_sqlite4(value.size, new_key.end());
            memmove(new_key.end() + valuesizesize, value.data, value.size);
        }
        PageOffset kv_size(uint32_t page_size, Val key, Val value)const;
        bool enough_size_for_elements(uint32_t page_size, PageOffset count, size_t total_kvsize)const{
            size_t free_size = page_size - LEAF_HEADER_SIZE;
            return free_size >= count * sizeof(PageOffset) + total_kvsize;
        }
        bool has_enough_free_space(uint32_t page_size, size_t required_size){
            size_t free_size = page_size - LEAF_HEADER_SIZE - items_size - item_count * sizeof(PageOffset);
            return free_size >= required_size;
        }
        PageOffset half_size(uint32_t page_size)const{
            return (page_size - LEAF_HEADER_SIZE)/2;
        }
        PageOffset data_size()const{
            return items_size + item_count * sizeof(PageOffset);
        }
        // -------------------- Old below

        size_t item_size(uint32_t page_size, bool is_leaf, PageOffset item);
        
        void copy_from(uint32_t page_size, bool is_leaf, const DataPage * src, PageOffset item); // we should have space
        void compact(uint32_t page_size, bool is_leaf, size_t required_size);
        
        size_t leaf_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Val value);
        size_t node_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Pid value);
        
    };
#pragma pack(pop)

    void test_data_pages();
}

#endif /* pages_hpp */
