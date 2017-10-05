#include "pages.hpp"
#include <map>
#include <iostream>

namespace mustela {
    unsigned char get_compact_size_sqlite4(uint64_t val){
        if (val <= 240)
            return 1;
        if (val <= 2287)
            return 2;
        if (val <= 67823)
            return 3;
        if (val <= 16777215)
            return 4;
        if (val <= 4294967295)
            return 5;
        if (val <= 1099511627775)
            return 6;
        if (val <= 281474976710655)
            return 7;
        if (val <= 72057594037927935)
            return 8;
        return 9;
    }
    unsigned char read_u64_sqlite4(uint64_t & val, const void * vptr){
        const unsigned char * ptr = (const unsigned char * )vptr;
        unsigned char a0 = *ptr;
        if (a0 <= 240) {
            val = a0;
            return 1;
        }
        if(a0 <= 248) {
            unsigned char a1 = *(ptr + 1);
            val = 240 + 256 * (a0 - 241) + a1;
            return 2;
        }
        if(a0 == 249) {
            const unsigned char * buf = ptr + 1;
            val = 2288 + 256 * buf[0] + buf[1];
            return 3;
        }
        const unsigned char * buf = ptr + 1;
        int bytes = 3 + a0 - 250;
        //if( bytes > 4 )
        //    throw std::runtime_error("read_u32_sqlite4 value does not fit");
        unpack_uint_be<uint64_t>(buf, bytes, val);
        return 1 + bytes;
    }
    unsigned char write_u64_sqlite4(uint64_t val, void * vptr){
        unsigned char * ptr = (unsigned char *)vptr;
        if (val <= 240) {
            *ptr = static_cast<unsigned char>(val);
            return 1;
        }
        if (val <= 2287) {
            *ptr = (val - 240)/256 + 241;
            *(ptr + 1) = static_cast<unsigned char>(val - 240);
            return 2;
        }
        if (val <= 67823) {
            *ptr = 249;
            *(ptr + 1) = (val - 2288)/256;
            *(ptr + 2) = static_cast<unsigned char>(val - 2288);
            return 3;
        }
        if (val <= 16777215) {
            *ptr = 250;
            pack_uint_be<uint32_t>(ptr + 1, 3, static_cast<uint32_t>(val));
            return 4;
        }
        if (val <= 4294967295) {
            *ptr = 251;
            pack_uint_be<uint64_t>(ptr + 1, 4, val);
            return 5;
        }
        if (val <= 1099511627775) {
            *ptr = 252;
            pack_uint_be<uint64_t>(ptr + 1, 5, val);
            return 6;
        }
        if (val <= 281474976710655) {
            *ptr = 253;
            pack_uint_be<uint64_t>(ptr + 1, 6, val);
            return 7;
        }
        if (val <= 72057594037927935) {
            *ptr = 254;
            pack_uint_be<uint64_t>(ptr + 1, 7, val);
            return 8;
        }
        *ptr = 255;
        pack_uint_be<uint64_t>(ptr + 1, 8, val);
        return 9;
    }
    int Val::compare(const Val & other)const{
        size_t min_size = std::min(size, other.size);
        int cmp = memcmp(data, other.data, min_size);
        if( cmp != 0 )
            return cmp;
        return int(size) - int(other.size);
    }
    size_t Val::encoded_size()const{
        return get_compact_size_sqlite4(size) + size;
    }
    
    bool MetaPage::check(uint32_t system_page_size, uint64_t file_size)const{
        return magic == META_MAGIC && version == OUR_VERSION && page_size == system_page_size && page_count * page_size <= file_size && main_root_page < page_count && free_root_page < page_count;
    }

/*    void DataPage::init_dirty(uint32_t page_size, Pid new_pid, Tid new_tid){
        memset(this, 0, page_size);
        pid = new_pid;
        tid = new_tid;
    }
    void DataPage::clear(uint32_t page_size){
        Pid save_pid = this->pid;
        Pid save_tid = this->tid;
        init_dirty(page_size, save_pid, save_tid);
    }*/
    MVal DataPage::get_item_key(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, PageOffset item){
        ass(item < item_count, "get_item_key item too large");
        char * raw_this = (char *)this;
        PageOffset item_offset = item_offsets[item];
        uint64_t keysize;
        auto keysizesize = read_u64_sqlite4(keysize, raw_this + item_offset);
        ass(item_offset + keysizesize + keysize <= page_size, "get_item_key key spills over page");
        return MVal(raw_this + item_offset + keysizesize, keysize);
    }
    PageOffset DataPage::lower_bound_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key, bool * found)const{
        PageOffset first = 0;
        PageOffset count = item_count;
        while (count > 0) {
            PageOffset step = count / 2;
            PageOffset it = first + step;
            Val itkey = get_item_key(page_size, item_offsets, item_count, it);
            if (itkey < key) {
                first = it + 1;
                count -= step + 1;
            }
            else
                count = step;
        }
        if( found ){
            if( first == item_count)
                *found = false;
            else
                *found = Val(get_item_key(page_size, item_offsets, item_count, first)) == key;
        }
        return first;
    }
    PageOffset DataPage::upper_bound_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key)const{
        PageOffset first = 0;
        PageOffset count = item_count;
        while (count > 0) {
            PageOffset step = count / 2;
            PageOffset it = first + step;
            Val itkey = get_item_key(page_size, item_offsets, item_count, it);
            if (!(key < itkey)) {
                first = it + 1;
                count -= step + 1;
            } else
                count = step;
        }
        return first;
        
    }
//    PageOffset DataPage::find_item(uint32_t page_size, const PageOffset * item_offsets, PageOffset item_count, Val key)const{
//        PageOffset off = lower_bound_item(page_size, item_offsets, item_count, key);
//        return off;
//    }
    size_t DataPage::item_size(uint32_t page_size, bool is_leaf, const PageOffset * item_offsets, PageOffset item_count, PageOffset item)const{
        ass(item < item_count, "item_size item too large");
        const char * raw_this = (const char *)this;
        PageOffset item_offset = item_offsets[item];
        uint64_t keysize;
        auto keysizesize = read_u64_sqlite4(keysize, raw_this + item_offset);
        size_t result;
        if( is_leaf ){
            uint64_t valuesize;
            auto valuesizesize = read_u64_sqlite4(valuesize, raw_this + item_offset + keysizesize + keysize);
            result = keysizesize + keysize + valuesizesize + valuesize;
        }else{
            //uint64_t child_page_id;
            //auto valuesizesize = read_u64_sqlite4(child_page_id, raw_this + item_offset + keysizesize + keysize);
            result = keysizesize + keysize + NODE_PID_SIZE;
        }
        ass(item_offset + result <= page_size, "item_size item spills over page");
        return result;
    }
    void DataPage::remove_simple(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset to_remove_item){
        char * raw_this = (char *)this;
        auto rem_size = item_size(page_size, is_leaf, item_offsets, item_count, to_remove_item);
        memset(raw_this + item_offsets[to_remove_item], 0, rem_size); // clear unused part
        if( item_offsets[to_remove_item] == free_end_offset )
            free_end_offset += rem_size; // Luck, removed item is after free middle space
        for(PageOffset pos = to_remove_item; pos != item_count - 1; ++pos)
            item_offsets[pos] = item_offsets[pos+1];
        items_size -= rem_size;
        item_count -= 1;
        item_offsets[item_count] = 0; // clear unused part
    }
    MVal DataPage::insert_at(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset insert_index, Val key, size_t kv_size){
//        compact(page_size, is_leaf, kv_size + sizeof(PageOffset)); // for item_offsets array element
        char * raw_this = (char *)this;
        //PageOffset insert_index = lower_bound_item(page_size, key);
        for(PageOffset pos = item_count; pos-- > insert_index;)
            item_offsets[pos + 1] = item_offsets[pos];
        auto insert_offset = free_end_offset - kv_size;
        item_offsets[insert_index] = insert_offset;
        free_end_offset -= kv_size;
        items_size += kv_size;
        item_count += 1;
        auto keysizesize = write_u64_sqlite4(key.size, raw_this + insert_offset);
        memmove(raw_this + insert_offset + keysizesize, key.data, key.size);
        return MVal(raw_this + insert_offset + keysizesize, key.size);
    }

    void NodePage::init_dirty(uint32_t page_size, Tid new_tid){
        char * raw_this = (char *)this;
        memset(raw_this + sizeof(DataPage), 0, page_size - sizeof(DataPage));
        tid = new_tid;
        free_end_offset = page_size - NODE_PID_SIZE;
    }
    void NodePage::compact(uint32_t page_size, size_t kv_size2){
        if(NODE_HEADER_SIZE + sizeof(PageOffset)*(item_count + 1) + kv_size2 <= free_end_offset)
            return;
        char buf[page_size];
        memmove(buf, this, page_size);
        NodePage * my_copy = (NodePage *)buf;
        clear(page_size);
        set_item_value(page_size, -1, my_copy->get_item_value(page_size, -1));
        for(PageOffset i = 0; i != my_copy->item_count; ++i){
            Pid val;
            Val key = my_copy->get_item_kv(page_size, i, val);
            insert_at(page_size, i, key, val);
        }
    }

    PageOffset NodePage::kv_size(uint32_t page_size, Val key, Pid value)const{
        size_t kv_size = get_compact_size_sqlite4(key.size) + key.size + NODE_PID_SIZE;
        if( kv_size <= page_size - NODE_HEADER_SIZE - 1 * sizeof(PageOffset) )
            return kv_size;
        throw std::runtime_error("Item does not fit in node");
    }
    Pid NodePage::get_item_value(uint32_t page_size, PageOffset item)const{
        Pid value;
        if( item == PageOffset(-1) ){
            const char * raw_this = (const char *)this;
            unpack_uint_be(raw_this + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
            return value;
        }
        Val result = get_item_key(page_size, item);
        unpack_uint_be(result.end(), NODE_PID_SIZE, value);
        return value;
    }
    Val NodePage::get_item_kv(uint32_t page_size, PageOffset item, Pid & value)const{
        Val result = get_item_key(page_size, item);
        unpack_uint_be(result.end(), NODE_PID_SIZE, value);
        return result;
    }

    void NodePage::set_item_value(uint32_t page_size, PageOffset item, Pid value){
        if( item == PageOffset(-1) ){
            char * raw_this = (char *)this;
            pack_uint_be(raw_this + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
            return;
        }
        MVal result = get_item_key(page_size, item);
        pack_uint_be(result.end(), NODE_PID_SIZE, value);
    }

    void LeafPage::compact(uint32_t page_size, size_t kv_size2){
        if(LEAF_HEADER_SIZE + sizeof(PageOffset)*(item_count + 1) + kv_size2 <= free_end_offset)
            return;
        char buf[page_size];
        memmove(buf, this, page_size);
        LeafPage * my_copy = (LeafPage *)buf;
        clear(page_size);
        for(PageOffset i = 0; i != my_copy->item_count; ++i){
            Val val;
            Val key = my_copy->get_item_kv(page_size, i, val);
            insert_at(page_size, i, key, val);
        }
    }
    void LeafPage::init_dirty(uint32_t page_size, Tid new_tid){
        char * raw_this = (char *)this;
        memset(raw_this + sizeof(DataPage), 0, page_size - sizeof(DataPage));
        tid = new_tid;
        free_end_offset = page_size;
    }
    PageOffset LeafPage::kv_size(uint32_t page_size, Val key, Val value)const{
        size_t kv_size = get_compact_size_sqlite4(key.size) + key.size + get_compact_size_sqlite4(value.size) + value.size;
        if( kv_size <= page_size - LEAF_HEADER_SIZE - 1 * sizeof(PageOffset) )
            return kv_size;
        throw std::runtime_error("Item does not fit in leaf");
    }
    Val LeafPage::get_item_kv(uint32_t page_size, PageOffset item, Val & val)const{
        Val result = get_item_key(page_size, item);
        uint64_t valuesize;
        auto valuesizesize = read_u64_sqlite4(valuesize, result.end());
        val = Val(result.end() + valuesizesize, valuesize);
        return result;
    }
    // -------------------- Old below

/*    Val DataPage::node_get_item_kv(uint32_t page_size, PageOffset item, Pid & val){
        MVal result = get_item_key(page_size, item);
        //uint64_t valuesize;
        //auto valuesizesize = read_u64_sqlite4(valuesize, result.end());
        //val = valuesize;
        unpack_uint_be(result.end(), NODE_PID_SIZE, val);
        return result;
    }
    
    size_t DataPage::leaf_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Val value){
        size_t kv_size = leaf_kv_size(page_size, key, value);
        if( to_remove_item == item_count )
            return kv_size + sizeof(PageOffset); // witthout deleting old item we need to store kv plus store offset
        size_t old_size = item_size(page_size, true, to_remove_item);
        if( old_size >= kv_size )
            return 0;
        return kv_size - old_size;
    }
    size_t DataPage::node_required_size_to_insert(uint32_t page_size, PageOffset to_remove_item, Val key, Pid value){
        size_t kv_size = node_kv_size(page_size, key, value);
        if( to_remove_item == item_count )
            return kv_size + sizeof(PageOffset); // witthout deleting old item we need to store kv plus store offset
        size_t old_size = item_size(page_size, true, to_remove_item);
        if( old_size >= kv_size )
            return 0;
        return kv_size - old_size;
    }
    void DataPage::copy_from(uint32_t page_size, bool is_leaf, const DataPage * src, PageOffset item){
        char * raw_this = (char *)this;
        size_t size_in_leaf = src->item_size(page_size, is_leaf, item);
        auto insert_offset = free_end_offset - size_in_leaf;
        PageOffset free_begin_offset = LEAF_HEADER_SIZE + (item_count + 1)*sizeof(PageOffset);
        ass(free_end_offset - free_begin_offset >= size_in_leaf, "copy_from not enough space to insert");
        memmove(raw_this + insert_offset, src + src->item_offsets[item], size_in_leaf);
        free_end_offset -= size_in_leaf;
        item_offsets[item_count] = insert_offset;
        item_count += 1;
        items_size += size_in_leaf;
    }

    void DataPage::compact(uint32_t page_size, bool is_leaf, size_t required_size){
        char * raw_this = (char *)this;
        PageOffset free_begin_offset = LEAF_HEADER_SIZE + item_count*sizeof(PageOffset);
        if( free_end_offset - free_begin_offset >= required_size)
            return;
        ass(free_begin_offset + items_size + required_size <= page_size, "compact required_size does not fit");
        char raw_other[page_size];
        memmove(raw_other, raw_this, page_size);
        DataPage * other = (DataPage *)raw_other;
        ass(item_count == other->item_count, "compact different count");
        ass(items_size == other->items_size, "compact different items_size");
        free_end_offset = page_size;
        for(int i = 0; i != item_count; ++i){
            size_t size_in_leaf = other->item_size(page_size, is_leaf, i);
            auto insert_offset = free_end_offset - size_in_leaf;
            ass(free_end_offset - free_begin_offset >= size_in_leaf, "compact not enough space to insert");
            memmove(raw_this + insert_offset, raw_other + other->item_offsets[i], size_in_leaf);
            free_end_offset -= size_in_leaf;
            item_offsets[i] = insert_offset;
        }
        memset(raw_this + free_begin_offset, 0, free_end_offset - free_begin_offset);
    }*/

    void test_node_page(){
        const uint32_t page_size = 128;
        NodePage * pa = (NodePage *)malloc(page_size);
        pa->init_dirty(page_size, 10);
        std::map<std::string, Pid> mirror;
        pa->set_item_value(page_size, -1, 123456);
        for(int i = 0; i != 1000; ++i){
            std::string key = "key" + std::to_string(rand() % 100);
            Pid val = rand() % 100000;
            bool same_key;
            PageOffset existing_item = pa->lower_bound_item(page_size, Val(key), &same_key);
            bool remove_existing = rand() % 2;
            if( same_key ){
                if( !remove_existing )
                    continue;
                pa->remove_simple(page_size, existing_item);
                mirror.erase(key);
            }
            size_t new_kvsize = pa->kv_size(page_size, Val(key), val);
            bool add_new = rand() % 2;
            if( add_new && pa->has_enough_free_space(page_size, sizeof(PageOffset) + new_kvsize) ){
                pa->insert_at(page_size, existing_item, Val(key), val);
                mirror[key] = val;
            }
        }
        std::cout << "Special value=" << pa->get_item_value(page_size, -1) << std::endl;
        std::cout << "Mirror" << std::endl;
        for(auto && ma : mirror)
            std::cout << ma.first << ":" << ma.second << std::endl;
        std::cout << "Page" << std::endl;
        for(int i = 0; i != pa->item_count; ++i){
            Pid value;
            Val key = pa->get_item_kv(page_size, i, value);
            std::cout << key.to_string() << ":" << value << std::endl;
        }
    }
    void test_data_pages(){
        test_node_page();
        const uint32_t page_size = 256;
        LeafPage * pa = (LeafPage *)malloc(page_size);
        pa->init_dirty(page_size, 10);
        std::map<std::string, std::string> mirror;
        for(int i = 0; i != 1000; ++i){
            std::string key = "key" + std::to_string(rand() % 100);
            std::string val = "value" + std::to_string(rand() % 100000);
            bool same_key;
            PageOffset existing_item = pa->lower_bound_item(page_size, Val(key), &same_key);
            bool remove_existing = rand() % 2;
            if( same_key ){
                if( !remove_existing )
                    continue;
                pa->remove_simple(page_size, existing_item);
                mirror.erase(key);
            }
            size_t new_kvsize = pa->kv_size(page_size, Val(key), Val(val));
            bool add_new = rand() % 2;
            if( add_new && pa->has_enough_free_space(page_size, sizeof(PageOffset) + new_kvsize) ){
                pa->insert_at(page_size, existing_item, Val(key), Val(val));
                mirror[key] = val;
            }
        }
        std::cout << "Mirror" << std::endl;
        for(auto && ma : mirror)
            std::cout << ma.first << ":" << ma.second << std::endl;
        std::cout << "Page" << std::endl;
        for(int i = 0; i != pa->item_count; ++i){
            Val value;
            Val key = pa->get_item_kv(page_size, i, value);
            std::cout << key.to_string() << ":" << value.to_string() << std::endl;
        }
    }

}
