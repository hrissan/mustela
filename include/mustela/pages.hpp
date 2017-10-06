#ifndef pages_hpp
#define pages_hpp

#include <string>
#include <cstring>
#include "utils.hpp"

namespace mustela {

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
    constexpr PageOffset NODE_HEADER_SIZE = sizeof(Pid) + sizeof(Tid) + 3*sizeof(PageOffset);
    struct NodePage : public DataPage {
        //uint64_t total_item_count; // with child pages
        PageOffset item_count;
        PageOffset items_size; // bytes keys+values + their sizes occupy. for branch pages instead of svalue we store pagenum
        PageOffset free_end_offset; // we can have a bit of gaps, will compact when free middle space not enough to store new item
        PageOffset item_offsets[1];
        // each NodePage has NODE_PID_SIZE bytes at the end, storing the -1 indexed link to child, which has no associated key
        // Branch page // TODO insert total_item_count
        // header [io0, io1, io2] free_middle [skey2 page_be2, gap, skey0 page_be0, gap, skey1 page_be1] page_last

        /*void init_dirty(uint32_t page_size, Tid tid);
        void clear(uint32_t page_size){
            init_dirty(page_size, this->tid);
        }
        PageOffset lower_bound_item(uint32_t page_size, Val key, bool * found = nullptr)const{
            return DataPage::lower_bound_item(page_size, item_offsets, item_count, key, found);
        }
        PageOffset upper_bound_item(uint32_t page_size, Val key)const{
            return DataPage::upper_bound_item(page_size, item_offsets, item_count, key);
        }*/
//        PageOffset find_item(uint32_t page_size, Val key)const{
//            return DataPage::find_item(page_size, item_offsets, item_count, key);
//        }
  /*      MVal get_item_key(uint32_t page_size, PageOffset item){
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
        }*/
    };
    struct CNodePtr {
        uint32_t page_size;
        const NodePage * page;
        
        CNodePtr():page_size(0), page(nullptr)
        {}
        CNodePtr(uint32_t page_size, const NodePage * page):page_size(page_size), page(page)
        {}
        PageOffset size()const{ return page->item_count; }
        Val get_key(PageOffset item)const{
            return page->get_item_key(page_size, page->item_offsets, page->item_count, item);
        }
        Pid get_value(PageOffset item)const;
        ValPid get_kv(PageOffset item)const;
        size_t get_item_size(PageOffset item)const{
            return page->item_size(page_size, false, page->item_offsets, page->item_count, item);
        }
        PageOffset lower_bound_item(Val key, bool * found = nullptr)const{
            return page->lower_bound_item(page_size, page->item_offsets, page->item_count, key, found);
        }
        PageOffset upper_bound_item(Val key)const{
            return page->upper_bound_item(page_size, page->item_offsets, page->item_count, key);
        }

        PageOffset kv_size(Val key, Pid value)const;
      
        bool enough_size_for_elements(PageOffset count, size_t total_kvsize)const{
            size_t free_size = page_size - NODE_HEADER_SIZE - NODE_PID_SIZE;
            return free_size >= count * sizeof(PageOffset) + total_kvsize;
        }
        bool has_enough_free_space(size_t required_size)const{
            size_t free_size = page_size - NODE_HEADER_SIZE - NODE_PID_SIZE - page->items_size - page->item_count * sizeof(PageOffset);
            return free_size >= required_size;
        }
        PageOffset half_size()const{
            return (page_size - NODE_HEADER_SIZE - NODE_PID_SIZE)/2;
        }
        PageOffset data_size()const{
            return page->items_size + page->item_count * sizeof(PageOffset);
        }
/*        PageOffset kv_size(uint32_t page_size, Val key, Pid value)const;
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
        }*/
    };
    struct NodePtr : public CNodePtr {
        NodePtr():CNodePtr(0, nullptr)
        {}
        NodePtr(uint32_t page_size, NodePage * page):CNodePtr(page_size, page)
        {}
        NodePage * mpage()const { return const_cast<NodePage *>(page); }

        void init_dirty(Tid tid);
        void clear(){
            init_dirty(page->tid);
        }
        MVal get_key(PageOffset item){
            return mpage()->get_item_key(page_size, page->item_offsets, page->item_count, item);
        }
        void set_value(PageOffset item, Pid value);
        void erase(PageOffset to_remove_item){
            mpage()->remove_simple(page_size, false, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, to_remove_item);
            if( mpage()->item_count == 0)
                mpage()->free_end_offset = page_size - NODE_PID_SIZE; // compact on last delete :)
        }
        void compact(size_t kv_size2);
        void insert_at(PageOffset insert_index, Val key, Pid value){
            ass(insert_index <= mpage()->item_count, "Cannot insert at this index");
            size_t kv_size2 = kv_size(key, value);
            compact(kv_size2);
            ass(NODE_HEADER_SIZE + sizeof(PageOffset)*(page->item_count + 1) + kv_size2 <= page->free_end_offset, "No space to insert in node");
            MVal new_key = mpage()->insert_at(page_size, false, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, insert_index, key, kv_size2);
            pack_uint_be((unsigned char *)new_key.end(), NODE_PID_SIZE, value);
        }
        void append(Val key, Pid value){
            insert_at(page->item_count, key, value);
        }
        void insert_range(PageOffset insert_index, const CNodePtr & other, PageOffset begin, PageOffset end){
            ass(begin <= end, "Invalid range at insert_range");
             // TODO - compact at start if needed midway, move all page offsets at once
            for(;begin != end; ++begin){
                auto kv = other.get_kv(begin);
                insert_at(insert_index++, kv.key, kv.pid);
            }
        }
        void append_range(const CNodePtr & other, PageOffset begin, PageOffset end){
            ass(begin <= end, "Invalid range at append_range");
            for(;begin != end; ++begin){
                auto kv = other.get_kv(begin);
                append(kv.key, kv.pid);
            }
        }
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
        void append(uint32_t page_size, Val key, Val value){
            insert_at(page_size, item_count, key, value);
        }
        void append_range(uint32_t page_size, const LeafPage * other, PageOffset begin, PageOffset end){
            ass(begin <= end, "Invalid range at append");
            for(;begin != end; ++begin){
                Val val;
                Val key = other->get_item_kv(page_size, begin, val);
                append(page_size, key, val);
            }
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
        
    };
#pragma pack(pop)

    void test_data_pages();
}

#endif /* pages_hpp */
