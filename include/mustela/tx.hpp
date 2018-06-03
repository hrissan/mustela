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
		friend class DB;

		DB & my_db;
		const char * c_file_ptr = nullptr;
		char * wr_file_ptr = nullptr;
		Pid file_page_count = 0;
		size_t used_mapping_size = 0; // r-tx uses 1 mapping, w-tx uses all mappings larger than this
		Tid oldest_reader_tid = 0;
		MetaPage meta_page;
		bool meta_page_dirty = false;

		IntrusiveNode<Cursor> my_cursors;
		std::set<Bucket *> my_buckets;

		std::map<std::string, BucketDesc> bucket_descs;
		BucketDesc * load_bucket_desc(const Val & name, Val * persistent_name, bool create_if_not_exists);
		
		FreeList free_list;
		Pid get_free_page(Pid contigous_count);
		void mark_free_in_future_page(Pid page, Pid contigous_count); // associated with our tx, will be available after no read tx can ever use our tid
		
		DataPage * make_pages_writable(Cursor & cur, size_t height);
		
		void new_merge_node(Cursor & cur, size_t height, NodePtr wr_dap);
		void new_merge_leaf(Cursor & cur, LeafPtr wr_dap);

		void new_increase_height(Cursor & cur);
		void new_insert2node(Cursor & cur, size_t height, ValPid insert_kv1, ValPid insert_kv2 = ValPid());
		char * new_insert2leaf(Cursor & cur, Val insert_key, size_t insert_value_size, bool * overflow);

		const DataPage * readable_page(Pid page, Pid count){
			ass(page + count <= file_page_count, "Constant mapping should always cover the whole file");
			return (const DataPage *)(c_file_ptr + page * page_size);
		}
		DataPage * writable_page(Pid page, Pid count);
		CLeafPtr readable_leaf(Pid pa){
			return CLeafPtr(page_size, (const LeafPage *)readable_page(pa, 1));
		}
		LeafPtr writable_leaf(Pid pa);
		CNodePtr readable_node(Pid pa){
			return CNodePtr(page_size, (const NodePage *)readable_page(pa, 1));
		}
		NodePtr writable_node(Pid pa);
		const char * readable_overflow(Pid pa, Pid count){
			return (const char *)readable_page(pa, count);
		}
		char * writable_overflow(Pid pa, Pid count);

		std::string print_db(const BucketDesc * bucket_desc);
		std::string print_db(Pid pa, size_t height, bool parse_meta);

	 	void check_bucket(BucketDesc * bucket_desc, MergablePageCache * pages);
	 	void check_bucket_page(const BucketDesc * bucket_desc, BucketDesc * stat_bucket_desc, Pid pa, size_t height, Val left_limit, Val right_limit, MergablePageCache * pages);

		void unlink_buckets_and_cursors();

		const bool read_only;
	public:
		const size_t page_size; // copy from my_db
		
		static const char bucket_prefix = 'b';
		static const char freelist_prefix = 'f';
		
		explicit TX(DB & my_db, bool read_only = false);
		~TX();
		Bucket get_bucket(const Val & name, bool create_if_not_exists = true);
		bool drop_bucket(const Val & name); // true if dropped, false if did not exist
		std::vector<Val> get_bucket_names(); // sorted
		
		void check_database(std::function<void(int percent)> on_progress);
		
		// both rollback and commit of read-only transaction are nops
		// commit of r/w transaction writes it to disk, everything remains valid
		// rollback of r/w transaction invalidates buckets and cursors, restarts r/w transaction
		void commit();
		void rollback();

		std::string print_meta_db();
		void print_free_list(){ free_list.print_db(); }
		std::string get_meta_stats();
	};
}

