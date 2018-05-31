#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"
#include "free_list.hpp"

namespace mustela {
	
	class DB;
	class Cursor;
	class Bucket;
	class TX {
		friend class Cursor;
		friend class FreeList;
		friend class Bucket;

		DB & my_db;
		size_t c_mappings_end_addr = 0; // We keep c_mappings while TX is using it
		std::set<Cursor *> my_cursors;
		std::set<Bucket *> my_buckets;
		Pid meta_page_index = 0;
		Tid oldest_reader_tid = 0;
		std::vector<std::vector<char>> tmp_pages; // We do not store it in stack. Sometimes we need more than one.
		NodePtr push_tmp_copy(const NodePage * other){
			tmp_pages.push_back(std::vector<char>(page_size, 0));
			memcpy(tmp_pages.back().data(), other, page_size);
			return NodePtr(page_size, (NodePage * )tmp_pages.back().data());
		}
		LeafPtr push_tmp_copy(const LeafPage * other){
			tmp_pages.push_back(std::vector<char>(page_size, 0));
			memcpy(tmp_pages.back().data(), other, page_size);
			return LeafPtr(page_size, (LeafPage * )tmp_pages.back().data());
		}
		void clear_tmp_copies(){
			tmp_pages.clear();
		}
		MetaPage meta_page;
		bool meta_page_dirty = false;
		std::map<std::string, BucketDesc> bucket_descs;
		BucketDesc * load_bucket_desc(const Val & name);
		FreeList free_list;
		Pid get_free_page(Pid contigous_count);
		void mark_free_in_future_page(Pid page, Pid contigous_count); // associated with our tx, will be available after no read tx can ever use our tid
		
		DataPage * make_pages_writable(Cursor & cur, size_t height);
		
		void new_merge_node(Cursor & cur, size_t height, NodePtr wr_dap);
		void new_merge_leaf(Cursor & cur, LeafPtr wr_dap);

		void new_increase_height(Cursor & cur);
		void new_insert2node(Cursor & cur, size_t height, ValPid insert_kv1, ValPid insert_kv2 = ValPid());
		char * new_insert2leaf(Cursor & cur, Val insert_key, size_t insert_value_size, bool * overflow);

		CLeafPtr readable_leaf(Pid pa);
		CNodePtr readable_node(Pid pa);
		const char * readable_overflow(Pid pa, Pid count);
		LeafPtr writable_leaf(Pid pa);
		NodePtr writable_node(Pid pa);
		char * writable_overflow(Pid pa, Pid count);
		
		void start_transaction();

		std::string print_db(const BucketDesc * bucket_desc);
		std::string print_db(Pid pa, size_t height, bool parse_meta);
	public:
		const size_t page_size; // copy from my_db
		const bool read_only;
		
		static const char bucket_prefix = 'b';
		static const char freelist_prefix = 'f';
		explicit TX(DB & my_db, bool read_only = false);
		~TX();
		std::vector<Val> get_bucket_names(); // sorted
		bool drop_bucket(const Val & name); // true if dropped, false if did not exist
		void commit(); // after commit, new transaction is started. in destructor we rollback last started transaction

		std::string print_meta_db();
		void print_free_list(){ free_list.print_db(); }
		std::string get_meta_stats();
	};
}

