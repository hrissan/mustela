#pragma once

#include <string>
#include <cstring>
#include "utils.hpp"

namespace mustela {
	
	class DB;
	class TX;
	class FreeList;
	class Cursor;
	class Bucket;

	constexpr int MIN_KEY_COUNT = 2;
	constexpr uint32_t OUR_VERSION = 3;
	typedef uint16_t PageOffset;
	typedef int16_t PageIndex; // we use -1 to indicate special left value in nodes

	constexpr uint64_t META_MAGIC = 0x58616c657473754d; // MustelaX in LE
//	constexpr uint64_t META_MAGIC_ALTENDIAN = 0x4d757374656c6158;
	constexpr int META_PAGES_COUNT = 3; // We might end up using 2 like lmdb
	constexpr int NODE_PID_SIZE = 5;
	constexpr size_t MIN_PAGE_SIZE = 128;
	constexpr size_t GOOD_PAGE_SIZE = 4096;
	constexpr size_t MAX_PAGE_SIZE = 1 << 8*sizeof(PageOffset);
	// 4 bytes to store page index will result in ~4 billion pages limit, or 16TB max for 4KB pages
	// TODO - move NODE_PID_SIZE into MetaPage
	constexpr int MAX_DEPTH = 40; // TODO - calculate from NODE_PID_SIZE, use for Cursor::path
	// fixed pid size allows simple logic when replacing page in node index
	
	constexpr bool CLEAR_FREE_SPACE = true;
	constexpr bool DEBUG_PAGES = true;
	constexpr bool DEBUG_MIRROR = true;

#pragma pack(push, 1)
	struct BucketDesc {
		uint64_t root_page;
		uint64_t height; // 0 when root is leaf
		uint64_t count;
		uint64_t leaf_page_count;
		uint64_t node_page_count;
		uint64_t overflow_page_count;
		void unpack(const char * buf, size_t size);
		void pack(char * buf, size_t size);
	};
	struct MetaPage { // We use uint64_t here independent of Page and PageOffset to make meta pages readable across platforms
		uint64_t pid;
		uint64_t tid;
		uint64_t magic;
		uint64_t page_count; // excess pages in file are all free
		BucketDesc meta_bucket; // All other bucket descs are stored in meta_bucket together with freelist
		uint32_t version;
		uint32_t page_size;
		uint32_t pid_size; // TODO - implement
		uint32_t crc32; // Must be last one
	};
	// TODO - detect hot copy made with "cp" utility
	struct DataPage {
		// Our design makes no use of Pid in pages
		Tid s_tid;
		// We do a lot of ==, != comparions between tids, might optimize them...
		Tid tid()const{ // transaction which did write the page
			Tid result;
			unpack_uint_le((const unsigned char * )&s_tid, sizeof(Tid), result);
			return result;
		}
		void set_tid(Tid t){
			pack_uint_le((unsigned char * )&s_tid, sizeof(Tid), t);
		}
	};
	inline size_t unpack_page_object(const void * vbuf){
		const unsigned char * buf = (const unsigned char * )vbuf;
		return (size_t(buf[1]) << 8) + buf[0];
	}
	inline void pack_page_object(size_t c, void * vbuf){
		unsigned char * buf = (unsigned char * )vbuf;
		buf[0] = static_cast<unsigned char>(c);
		buf[1] = static_cast<unsigned char>(c >> 8);
	}
	struct KeysPage : public DataPage {
		PageIndex s_item_count;
		PageOffset s_items_size; // bytes keys+values + their sizes occupy. for branch pages instead of svalue we store pagenum
		PageOffset s_free_end_offset; // we can have a bit of gaps, will compact when free middle space not enough to store new item
		PageOffset s_item_offsets[20];
		
		int item_count()const { return (int)unpack_page_object(&s_item_count); }
		void set_item_count(int c) { pack_page_object((size_t)c, &s_item_count); }
		size_t items_size()const { return unpack_page_object(&s_items_size); }
		void set_items_size(size_t c) { pack_page_object(c, &s_items_size); }
		size_t free_end_offset()const { return unpack_page_object(&s_free_end_offset); }
		void set_free_end_offset(size_t c) { pack_page_object(c, &s_free_end_offset); }
		size_t item_offsets(int item)const { return unpack_page_object(static_cast<const PageOffset *>(s_item_offsets) + item); }
		void set_item_offsets(int item, size_t c) { pack_page_object(c, static_cast<PageOffset *>(s_item_offsets) + item); }

		MVal get_item_key(size_t page_size, int item);
		Val get_item_key(size_t page_size, int item)const;
		Val get_item_key_no_check(size_t page_size, int item)const;
		int lower_bound_item(size_t page_size, Val key, bool * found)const;
		int upper_bound_item(size_t page_size, Val key)const;
		void erase_item(size_t page_size, int to_remove_item, size_t item_size);
		MVal insert_item_at(size_t page_size, int insert_index, Val key, size_t item_size);
	};

	struct NodePage : public KeysPage {
		// each NodePage has NODE_PID_SIZE bytes at the end, storing the -1 indexed link to child, which has no associated key
		// header [io0, io1, io2] free_middle [skey2 page_be2, gap, skey0 page_be0, gap, skey1 page_be1] page_last
	};
	constexpr size_t NODE_HEADER_SIZE = sizeof(NodePage) - sizeof(KeysPage::s_item_offsets);
	static_assert(sizeof(KeysPage) < MIN_PAGE_SIZE, "Array of offsets does not fit into page (used for debugging only).");

	inline size_t node_capacity(size_t page_size){
		return page_size - NODE_HEADER_SIZE - NODE_PID_SIZE;
	}
	inline size_t max_key_size(size_t page_size){
		size_t space = (page_size - NODE_HEADER_SIZE - NODE_PID_SIZE)/MIN_KEY_COUNT - NODE_PID_SIZE - sizeof(PageOffset);
		space -= get_compact_size_sqlite4(space);
		return space;
	}
	size_t get_item_size(size_t page_size, Val key, Pid value);

	struct LeafPage : public KeysPage {
		// Leaf page
		// header [io0, io1, io2] free_middle [skey2 svalue2, gap, skey0 svalue0, gap, skey1 svalue1]
	};
	constexpr size_t LEAF_HEADER_SIZE = sizeof(LeafPage) - sizeof(KeysPage::s_item_offsets);
	inline size_t leaf_capacity(size_t page_size){
		return page_size - LEAF_HEADER_SIZE;
	}
#pragma pack(pop)

	static_assert(MIN_KEY_COUNT == 2, "Should be 2 for invariants, do not change");
	static_assert(MIN_PAGE_SIZE >= sizeof(MetaPage), "Metapage does not fit into page size");
	static_assert(MIN_PAGE_SIZE >= (NODE_PID_SIZE + 1 + sizeof(PageOffset))*MIN_KEY_COUNT + NODE_PID_SIZE + NODE_HEADER_SIZE, "Node page with min keys does not fit into page size");

	struct CNodePtr {
		size_t page_size;
		const NodePage * page;
		
		CNodePtr():page_size(0), page(nullptr)
		{}
		CNodePtr(size_t page_size, const NodePage * page):page_size(page_size), page(page)
		{}
		int size()const{ return page->item_count(); }
		Val get_key(int item)const{
			return page->get_item_key(page_size, item);
		}
		Pid get_value(int item)const;
		ValPid get_kv(int item)const;
		size_t get_item_size(int item)const;
		int lower_bound_item(Val key, bool * found)const{
			return page->lower_bound_item(page_size, key, found);
		}
		int upper_bound_item(Val key)const{
			return page->upper_bound_item(page_size, key);
		}
	 	size_t capacity()const{
	 		return node_capacity(page_size);
	 	}
		size_t free_capacity()const{
			return capacity() - data_size();
		}
		size_t data_size()const{
			return page->items_size();
		}
	};
	struct NodePtr : public CNodePtr {
		NodePtr():CNodePtr(0, nullptr)
		{}
		NodePtr(size_t page_size, NodePage * page):CNodePtr(page_size, page)
		{}
		NodePage * mpage()const { return const_cast<NodePage *>(page); }
		
		void init_dirty(Tid tid);
		MVal get_key(int item){
			return mpage()->get_item_key(page_size, item);
		}
		void set_value(int item, Pid value);
		void erase(int to_remove_item){
			size_t item_size = get_item_size(to_remove_item);
			mpage()->erase_item(page_size, to_remove_item, item_size);
			if( mpage()->item_count() == 0)
				mpage()->set_free_end_offset(page_size - NODE_PID_SIZE); // compact on last delete :)
		}
		void erase(int begin, int end){
			ass2(begin <= end, "Invalid range at erase", DEBUG_PAGES);
			for(int it = end; it-- > begin; )
				erase(it);
		}
		void compact(size_t item_size);
		void insert_at(int insert_index, Val key, Pid value){
			ass2(insert_index >= 0 && insert_index <= mpage()->item_count(), "Cannot insert at this index", DEBUG_PAGES);
			if(insert_index < mpage()->item_count()){
				ValPid right_kv = get_kv(insert_index);
				ass2(key < right_kv.key, "Wrong insert order 1", DEBUG_PAGES);
			}
			if( insert_index > 0){
				ValPid left_kv = get_kv(insert_index - 1);
				ass2(left_kv.key < key, "Wrong insert order 2", DEBUG_PAGES);
			}
			size_t item_size = mustela::get_item_size(page_size, key, value);
			compact(item_size);
			ass2(NODE_HEADER_SIZE + sizeof(PageOffset)*static_cast<size_t>(page->item_count()) + item_size <= page->free_end_offset(), "No space to insert in node", DEBUG_PAGES);
			MVal new_key = mpage()->insert_item_at(page_size, insert_index, key, item_size);
			pack_uint_le((unsigned char *)new_key.end(), NODE_PID_SIZE, value);
		}
		void insert_at(int insert_index, ValPid kv){
			insert_at(insert_index, kv.key, kv.pid);
		}
		void append(Val key, Pid value){
			insert_at(page->item_count(), key, value);
		}
		void append(ValPid kv){
			append(kv.key, kv.pid);
		}
		void insert_range(int insert_index, const CNodePtr & other, int begin, int end){
			ass2(begin <= end, "Invalid range at insert_range", DEBUG_PAGES);
			// TODO - compact at start if needed midway, move all page offsets at once
			for(;begin != end; ++begin){
				auto kv = other.get_kv(begin);
				insert_at(insert_index++, kv.key, kv.pid);
			}
		}
		void append_range(const CNodePtr & other, int begin, int end){
			insert_range(page->item_count(), other, begin, end);
		}
	};
	
	struct CLeafPtr {
		size_t page_size;
		const LeafPage * page;
		
		CLeafPtr():page_size(0), page(nullptr)
		{}
		CLeafPtr(size_t page_size, const LeafPage * page):page_size(page_size), page(page)
		{}
		int size()const{ return page->item_count(); }
		Val get_key(int item)const{
			return page->get_item_key(page_size, item);
		}
		ValVal get_kv(int item, Pid & overflow_page)const;
		size_t get_item_size(int item, Pid & overflow_page, Pid & overflow_count, Tid & overflow_tid)const;
		size_t get_item_size(int item)const{
			Pid a; Tid b; return get_item_size(item, a, a, b);
		}
		int lower_bound_item(Val key, bool * found)const{
			return page->lower_bound_item(page_size, key, found);
		}
		size_t get_item_size(Val key, size_t value_size, bool & overflow)const;
		size_t capacity()const{
			return leaf_capacity(page_size);
		}
		size_t free_capacity()const{
			return capacity() - data_size();
		}
		size_t data_size()const{
			return page->items_size();
		}
	};
	struct LeafPtr : public CLeafPtr {
		LeafPtr():CLeafPtr(0, nullptr)
		{}
		LeafPtr(size_t page_size, LeafPage * page):CLeafPtr(page_size, page)
		{}
		LeafPage * mpage()const { return const_cast<LeafPage *>(page); }
		
		void init_dirty(Tid tid);
		MVal get_key(int item){
			return mpage()->get_item_key(page_size, item);
		}
		void erase(int to_remove_item, Pid & overflow_page, Pid & overflow_count, Tid & overflow_tid){
			size_t item_size = get_item_size(to_remove_item, overflow_page, overflow_count, overflow_tid);
			mpage()->erase_item(page_size, to_remove_item, item_size);
			if( mpage()->item_count() == 0)
				mpage()->set_free_end_offset(page_size); // compact on last delete :)
		}
		void erase(int begin, int end){
			Pid overflow_page, overflow_count;
			Tid overflow_tid;
			ass2(begin <= end, "Invalid range at erase", DEBUG_PAGES);
			for(int it = end; it-- > begin; )
				erase(it, overflow_page, overflow_count, overflow_tid);
		}
		void compact(size_t item_size);
		char * insert_at(int insert_index, Val key, size_t value_size, bool & overflow);
		void insert_at(int insert_index, Val key, Val value){
			bool overflow = false;
			char * dst = insert_at(insert_index, key, value.size, overflow);
			memcpy(dst, value.data, overflow ? NODE_PID_SIZE + sizeof(Tid) : value.size);
		}
		void append(Val key, Val value){
			insert_at(page->item_count(), key, value);
		}
		void append(ValVal kv){
			insert_at(page->item_count(), kv.key, kv.value);
		}
		void insert_range(int insert_index, const CLeafPtr & other, int begin, int end){
			ass2(begin <= end, "Invalid range at insert_range", DEBUG_PAGES);
			// TODO - compact at start if needed midway, move all page offsets at once
			for(;begin != end; ++begin){
				Pid overflow_page;
				auto kv = other.get_kv(begin, overflow_page);
				insert_at(insert_index++, kv.key, kv.value);
			}
		}
		void append_range(const CLeafPtr & other, int begin, int end){
			insert_range(page->item_count(), other, begin, end);
		}
	};
	
	void test_data_pages();
}

