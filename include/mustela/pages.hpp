#pragma once

#include <string>
#include <cstring>
#include "utils.hpp"

namespace mustela {
	
#pragma pack(push, 1)
	struct BucketDesc {
		uint64_t root_page;
		uint64_t height; // 0 when root is leaf
		uint64_t count;
		uint64_t leaf_page_count;
		uint64_t node_page_count;
		uint64_t overflow_page_count;
	};
	struct MetaPage { // We use uint64_t here independent of Page and PageOffset to make meta pages readable across platforms
		uint64_t pid;
		uint64_t tid;
		uint64_t magic;
		uint32_t version;
		uint32_t page_size;
		uint64_t page_count; // excess pages in file are all free
		BucketDesc meta_bucket; // All other bucket descs are stored in meta_bucket together with freelist
		uint64_t tid2; // protect against write shredding (if tid does not match tid2, use lowest of two as an effective tid)
//		bool dirty; // We do not commit transaction if dirty=false. If dirty, set to false then commit to disk
		
		Tid effective_tid()const { return std::min(tid, tid2); }
//		bool check(uint32_t system_page_size, uint64_t file_size)const;
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
		//        size_t get_item_size(uint32_t page_size, bool is_leaf, const PageOffset * item_offsets, PageOffset item_count, PageOffset item)const;
		void remove_simple(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset to_remove_item, size_t item_size);
		MVal insert_at(uint32_t page_size, bool is_leaf, PageOffset * item_offsets, PageOffset & item_count, PageOffset & items_size, PageOffset & free_end_offset, PageOffset insert_index, Val key, size_t item_size);
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
	};
	constexpr PageOffset LEAF_HEADER_SIZE = sizeof(Pid)+sizeof(Tid)+3*sizeof(PageOffset);
	struct LeafPage : public DataPage {
		PageOffset item_count;
		PageOffset items_size; // bytes keys+values + their sizes occupy. for branch pages instead of svalue we store pagenum
		PageOffset free_end_offset; // we can have a bit of gaps, will shift when free middle space not enough to store new item
		PageOffset item_offsets[1];
		// Leaf page
		// header [io0, io1, io2] free_middle [skey2 svalue2, gap, skey0 svalue0, gap, skey1 svalue1]
	};
#pragma pack(pop)
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
		size_t get_item_size(PageOffset item)const;
		PageOffset lower_bound_item(Val key, bool * found = nullptr)const{
			return page->lower_bound_item(page_size, page->item_offsets, page->item_count, key, found);
		}
		PageOffset upper_bound_item(Val key)const{
			return page->upper_bound_item(page_size, page->item_offsets, page->item_count, key);
		}
		PageOffset get_item_size(Val key, Pid value)const;
		PageOffset capacity()const{
			return page_size - NODE_HEADER_SIZE - NODE_PID_SIZE;
		}
		PageOffset max_key()const{
			constexpr int KEY_COUNT = 2; // min keys per page
			PageOffset space = capacity()/KEY_COUNT - NODE_PID_SIZE;
			space -= get_compact_size_sqlite4(space);
			return space;
		}
		PageOffset free_capacity()const{
			return capacity() - data_size();
		}
		PageOffset data_size()const{
			return page->items_size;
		}
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
			size_t item_size = get_item_size(to_remove_item);
			mpage()->remove_simple(page_size, false, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, to_remove_item, item_size);
			if( mpage()->item_count == 0)
				mpage()->free_end_offset = page_size - NODE_PID_SIZE; // compact on last delete :)
		}
		void erase(PageOffset begin, PageOffset end){
			ass(begin <= end, "Invalid range at erase");
			for(PageOffset it = end; it-- > begin; )
				erase(it);
		}
		void compact(size_t item_size);
		void insert_at(PageOffset insert_index, Val key, Pid value){
			ass(insert_index <= mpage()->item_count, "Cannot insert at this index");
			if(insert_index < mpage()->item_count){
				ValPid right_kv = get_kv(insert_index);
				ass(key < right_kv.key, "Wrong insert order 1");
			}
			if( insert_index > 0){
				ValPid left_kv = get_kv(insert_index - 1);
				ass(left_kv.key < key, "Wrong insert order 2");
			}
			size_t item_size = get_item_size(key, value);
			compact(item_size);
			ass(NODE_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset, "No space to insert in node");
			MVal new_key = mpage()->insert_at(page_size, false, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, insert_index, key, item_size);
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
			insert_range(page->item_count, other, begin, end);
		}
	};
	
	struct CLeafPtr {
		uint32_t page_size;
		const LeafPage * page;
		
		CLeafPtr():page_size(0), page(nullptr)
		{}
		CLeafPtr(uint32_t page_size, const LeafPage * page):page_size(page_size), page(page)
		{}
		PageOffset size()const{ return page->item_count; }
		Val get_key(PageOffset item)const{
			return page->get_item_key(page_size, page->item_offsets, page->item_count, item);
		}
		//        Val get_value(PageOffset item)const;
		ValVal get_kv(PageOffset item, Pid & overflow_page)const;
		size_t get_item_size(PageOffset item, Pid & overflow_page, Pid & overflow_count)const;
		size_t get_item_size(PageOffset item)const{
			Pid a; return get_item_size(item, a, a);
		}
		PageOffset lower_bound_item(Val key, bool * found = nullptr)const{
			return page->lower_bound_item(page_size, page->item_offsets, page->item_count, key, found);
		}
		//        PageOffset upper_bound_item(Val key)const{
		//            return page->upper_bound_item(page_size, page->item_offsets, page->item_count, key);
		//        }
		PageOffset get_item_size(Val key, size_t value_size, bool & overflow)const;
		PageOffset capacity()const{
			return page_size - LEAF_HEADER_SIZE;
		}
		PageOffset free_capacity()const{
			return capacity() - data_size();
		}
		PageOffset data_size()const{
			return page->items_size;
		}
	};
	struct LeafPtr : public CLeafPtr {
		LeafPtr():CLeafPtr(0, nullptr)
		{}
		LeafPtr(uint32_t page_size, LeafPage * page):CLeafPtr(page_size, page)
		{}
		LeafPage * mpage()const { return const_cast<LeafPage *>(page); }
		
		void init_dirty(Tid tid);
		void clear(){
			init_dirty(page->tid);
		}
		MVal get_key(PageOffset item){
			return mpage()->get_item_key(page_size, page->item_offsets, page->item_count, item);
		}
		//        void set_value(PageOffset item, Pid value);
		void erase(PageOffset to_remove_item, Pid & overflow_page, Pid & overflow_count){
			size_t item_size = get_item_size(to_remove_item, overflow_page, overflow_count);
			mpage()->remove_simple(page_size, true, mpage()->item_offsets, mpage()->item_count, mpage()->items_size, mpage()->free_end_offset, to_remove_item, item_size);
			if( mpage()->item_count == 0)
				mpage()->free_end_offset = page_size; // compact on last delete :)
		}
		void compact(size_t item_size);
		char * insert_at(PageOffset insert_index, Val key, size_t value_size, bool & overflow);
		void insert_at(PageOffset insert_index, Val key, Val value){
			bool overflow = false;
			char * dst = insert_at(insert_index, key, value.size, overflow);
			memcpy(dst, value.data, overflow ? NODE_PID_SIZE : value.size);
		}
		void append(Val key, Val value){
			insert_at(page->item_count, key, value);
		}
		void insert_range(PageOffset insert_index, const CLeafPtr & other, PageOffset begin, PageOffset end){
			ass(begin <= end, "Invalid range at insert_range");
			// TODO - compact at start if needed midway, move all page offsets at once
			for(;begin != end; ++begin){
				Pid overflow_page;
				auto kv = other.get_kv(begin, overflow_page);
				insert_at(insert_index++, kv.key, kv.value);
			}
		}
		void append_range(const CLeafPtr & other, PageOffset begin, PageOffset end){
			insert_range(page->item_count, other, begin, end);
		}
	};
	
	void test_data_pages();
}

