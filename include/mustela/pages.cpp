#include "pages.hpp"
#include <map>
#include <iostream>

namespace mustela {
 
    bool MetaPage::check(uint32_t system_page_size, uint64_t file_size)const{
        return magic == META_MAGIC && version == OUR_VERSION && page_size == system_page_size && page_count * page_size <= file_size && main_root_page < page_count && free_root_page < page_count;
    }

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
    size_t DataPage::get_item_size(uint32_t page_size, bool is_leaf, const PageOffset * item_offsets, PageOffset item_count, PageOffset item)const{
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
            result = keysizesize + keysize + NODE_PID_SIZE;
        }
        ass(item_offset + result <= page_size, "item_size item spills over page");
        return result + sizeof(PageOffset);
    }

    void DataPage::remove_simple(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset to_remove_item){
        char * raw_this = (char *)this;
        auto rem_size = get_item_size(page_size, is_leaf, item_offsets, item_count, to_remove_item);
        auto kv_size = rem_size - sizeof(PageOffset);
        memset(raw_this + item_offsets[to_remove_item], 0, kv_size); // clear unused part
        if( item_offsets[to_remove_item] == free_end_offset )
            free_end_offset += kv_size; // Luck, removed item is after free middle space
        for(PageOffset pos = to_remove_item; pos != item_count - 1; ++pos)
            item_offsets[pos] = item_offsets[pos+1];
        items_size -= rem_size;
        item_count -= 1;
        item_offsets[item_count] = 0; // clear unused part
    }
    MVal DataPage::insert_at(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset insert_index, Val key, size_t item_size){
        char * raw_this = (char *)this;
        auto kv_size = item_size - sizeof(PageOffset);
        for(PageOffset pos = item_count; pos-- > insert_index;)
            item_offsets[pos + 1] = item_offsets[pos];
        auto insert_offset = free_end_offset - kv_size;
        item_offsets[insert_index] = insert_offset;
        free_end_offset -= kv_size;
        items_size += item_size;
        item_count += 1;
        auto keysizesize = write_u64_sqlite4(key.size, raw_this + insert_offset);
        memmove(raw_this + insert_offset + keysizesize, key.data, key.size);
        return MVal(raw_this + insert_offset + keysizesize, key.size);
    }

    void NodePtr::init_dirty(Tid new_tid){
        char * raw_page = (char *)page;
        memset(raw_page + sizeof(DataPage), 0, page_size - sizeof(DataPage));
        mpage()->tid = new_tid;
        mpage()->free_end_offset = page_size - NODE_PID_SIZE;
    }
    void NodePtr::compact(size_t item_size){
        if(NODE_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset)
            return;
        char buf[page_size]; // TODO - remove copy from stack
        memmove(buf, page, page_size);
        CNodePtr my_copy(page_size, (NodePage *)buf);
        clear();
        set_value(-1, my_copy.get_value(-1));
        append_range(my_copy, 0, my_copy.size());
    }

    PageOffset CNodePtr::get_item_size(Val key, Pid value)const{
        size_t item_size = sizeof(PageOffset) + get_compact_size_sqlite4(key.size) + key.size + NODE_PID_SIZE;
        if( item_size <= capacity() )
            return item_size;
        throw std::runtime_error("Item does not fit in node");
    }
    Pid CNodePtr::get_value(PageOffset item)const{
        Pid value;
        if( item == PageOffset(-1) ){
            const char * raw_page = (const char *)page;
            unpack_uint_be(raw_page + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
            return value;
        }
        Val result = get_key(item);
        unpack_uint_be(result.end(), NODE_PID_SIZE, value);
        return value;
    }
    ValPid CNodePtr::get_kv(PageOffset item)const{
        ValPid result(get_key(item), 0);
        unpack_uint_be(result.key.end(), NODE_PID_SIZE, result.pid);
        return result;
    }

    void NodePtr::set_value(PageOffset item, Pid value){
        if( item == PageOffset(-1) ){
            char * raw_page = (char *)mpage();
            pack_uint_be(raw_page + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
            return;
        }
        MVal result = get_key(item);
        pack_uint_be(result.end(), NODE_PID_SIZE, value);
    }

    void LeafPtr::init_dirty(Tid new_tid){
        char * raw_page = (char *)mpage();
        memset(raw_page + sizeof(DataPage), 0, page_size - sizeof(DataPage));
        mpage()->tid = new_tid;
        mpage()->free_end_offset = page_size;
    }

    void LeafPtr::compact(size_t item_size){
        if(LEAF_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset)
            return;
        char buf[page_size]; // TODO - remove copy from stack
        memmove(buf, page, page_size);
        CLeafPtr my_copy(page_size, (LeafPage *)buf);
        clear();
        append_range(my_copy, 0, my_copy.size());
    }
    void LeafPtr::insert_at(PageOffset insert_index, Val key, Val value){
        ass(insert_index <= mpage()->item_count, "Cannot insert at this index");
        size_t item_size = get_item_size(key, value);
        compact(item_size);
        ass(LEAF_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset, "No space to insert in node");
        MVal new_key = mpage()->insert_at(page_size, false, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, insert_index, key, item_size);
        auto valuesizesize = write_u64_sqlite4(value.size, new_key.end());
        memmove(new_key.end() + valuesizesize, value.data, value.size);
    }

    PageOffset CLeafPtr::get_item_size(Val key, Val value)const{
        size_t kv_size = sizeof(PageOffset) + get_compact_size_sqlite4(key.size) + key.size + get_compact_size_sqlite4(value.size) + value.size;
        if( kv_size <= capacity() )
            return kv_size;
        throw std::runtime_error("Item does not fit in leaf");
    }
    ValVal CLeafPtr::get_kv(PageOffset item)const{
        ValVal result;
        result.key = get_key(item);
        uint64_t valuesize;
        auto valuesizesize = read_u64_sqlite4(valuesize, result.key.end());
        result.value = Val(result.key.end() + valuesizesize, valuesize);
        return result;
    }

    void test_node_page(){
        const uint32_t page_size = 128;
        NodePtr pa(page_size, (NodePage *)malloc(page_size));
        pa.init_dirty(10);
        std::map<std::string, Pid> mirror;
        pa.set_value(-1, 123456);
        for(int i = 0; i != 1000; ++i){
            std::string key = "key" + std::to_string(rand() % 100);
            Pid val = rand() % 100000;
            bool same_key;
            PageOffset existing_item = pa.lower_bound_item(Val(key), &same_key);
            bool remove_existing = rand() % 2;
            if( same_key ){
                if( !remove_existing )
                    continue;
                pa.erase(existing_item);
                mirror.erase(key);
            }
            size_t new_kvsize = pa.get_item_size(Val(key), val);
            bool add_new = rand() % 2;
            if( add_new && pa.free_capacity() >= new_kvsize ){
                pa.insert_at(existing_item, Val(key), val);
                mirror[key] = val;
            }
        }
        std::cout << "Special value=" << pa.get_value(-1) << std::endl;
        std::cout << "Mirror" << std::endl;
        for(auto && ma : mirror)
            std::cout << ma.first << ":" << ma.second << std::endl;
        std::cout << "Page" << std::endl;
        for(int i = 0; i != pa.size(); ++i){
            ValPid va = pa.get_kv(i);
            std::cout << va.key.to_string() << ":" << va.pid << std::endl;
        }
    }
    void test_data_pages(){
        test_node_page();
        const uint32_t page_size = 256;
        LeafPtr pa(page_size, (LeafPage *)malloc(page_size));
        pa.init_dirty(10);
        std::map<std::string, std::string> mirror;
        for(int i = 0; i != 1000; ++i){
            std::string key = "key" + std::to_string(rand() % 100);
            std::string val = "value" + std::to_string(rand() % 100000);
            bool same_key;
            PageOffset existing_item = pa.lower_bound_item(Val(key), &same_key);
            bool remove_existing = rand() % 2;
            if( same_key ){
                if( !remove_existing )
                    continue;
                pa.erase(existing_item);
                mirror.erase(key);
            }
            size_t new_kvsize = pa.get_item_size(Val(key), Val(val));
            bool add_new = rand() % 2;
            if( add_new && new_kvsize <= pa.free_capacity() ){
                pa.insert_at(existing_item, Val(key), Val(val));
                mirror[key] = val;
            }
        }
        std::cout << "Mirror" << std::endl;
        for(auto && ma : mirror)
            std::cout << ma.first << ":" << ma.second << std::endl;
        std::cout << "Page" << std::endl;
        for(int i = 0; i != pa.size(); ++i){
            ValVal va = pa.get_kv(i);
            std::cout << va.key.to_string() << ":" << va.value.to_string() << std::endl;
        }
    }

}
