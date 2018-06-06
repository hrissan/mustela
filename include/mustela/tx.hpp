#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include <functional>
#include "pages.hpp"
#include "lock.hpp"
#include "free_list.hpp"

namespace mustela {
	
	class TX {
	public:
		// We cannot have ove semantic in TX for now because &meta_page.meta_bucket is stored in our cursors and buckets
		explicit TX(DB & my_db, bool read_only = false);
		~TX();
		Tid tid()const{ return meta_page.tid; }
		std::string get_meta_stats();

		Bucket get_bucket(const Val & name, bool create_if_not_exists = true);
		bool drop_bucket(const Val & name); // true if dropped, false if did not exist
		std::vector<Val> get_bucket_names(); // sorted

		// both rollback and commit of read-only transaction are nops
		// commit of r/w transaction writes it to disk, everything remains valid for next commit, etc
		// rollback of r/w transaction invalidates buckets and cursors, restarts r/w transaction
		void commit();
		void rollback();

		// Slow - reads all values
		void check_database(std::function<void(int percent)> on_progress, bool verbose);

		std::string debug_print_meta_db();
		void debug_print_free_list(){
			free_list.load_all_free_pages(this, oldest_reader_tid);
			free_list.debug_print_db();
		}
		int debug_get_mirror_counter()const { return debug_mirror_counter; }
	private:
		friend class Cursor;
		friend class FreeList;
		friend class Bucket;
		friend class DB;

		DB & my_db;
		// For readers & writers
		IntrusiveNode<Cursor> my_cursors;
		std::set<Bucket *> my_buckets;

		const char * c_file_ptr = nullptr;
		Pid file_page_count = 0;
		size_t used_mapping_size = 0; // r-tx uses 1 mapping, w-tx uses all mappings larger than this
		MetaPage meta_page;

		// For readers
		ReaderSlotDesc reader_slot;

		// For writers
		char * wr_file_ptr = nullptr;
		Tid oldest_reader_tid = 0;
		bool meta_page_dirty = false;
		FreeList free_list;

		std::map<std::string, BucketDesc> bucket_descs;
		BucketDesc * load_bucket_desc(const Val & name, Val * persistent_name, bool create_if_not_exists);
		Bucket get_meta_bucket();

		Pid get_free_page(Pid contigous_count);
		void mark_free_in_future_page(Pid page, Pid contigous_count, Tid page_tid); // associated with our tx, will be available after no read tx can ever use our tid
		bool updating_meta_bucket = false;
		void start_update(BucketDesc * bucket_desc);
		void finish_update(BucketDesc * bucket_desc);

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
		const size_t page_size; // copy from my_db

		typedef std::map<std::string, std::pair<std::string, Cursor>> BucketMirror;
		std::map<std::string, BucketMirror> mirror; // model of our DB
		static int debug_mirror_counter;
		void before_mirror_operation();
		void load_mirror();
		void check_mirror();
	};
}

