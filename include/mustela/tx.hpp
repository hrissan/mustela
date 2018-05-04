#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"
#include "free_list.hpp"

namespace mustela {
	
	class DB;
	class TX;
	class Bucket;
	class Cursor {
		friend class TX;
		friend class Bucket;
		friend class FreeList;
		TX & my_txn;
		BucketDesc * bucket_desc;
		std::vector<std::pair<Pid, PageOffset>> path;
		
		void truncate(size_t height){
			while(height < path.size() - 1)
				path[height++].second = -1; // set to special link
			if(height == path.size() - 1)
				path[height++].second = 0; // set to first leaf kv
		}
		void jump_prev();
		void jump_next();
		
		explicit Cursor(TX & my_txn, BucketDesc * bucket_desc);
	public:
		Cursor(Cursor && other);
		Cursor & operator=(Cursor && other)=delete;
		explicit Cursor(Bucket & bucket);
		~Cursor();
		void lower_bound(const Val & key);
		
		bool seek(const Val & key); // sets to key and returns true if key is found, otherwise sets to next key and returns false
		void first();
		void last();
		bool get(Val & key, Val & value);
		void del(); // If you can get, you can del. After del, cursor points to the next item
		void next();
		void prev();
		// for( cur.first(); cur.get(key, val) && key.prefix("a"); cur.next() ) {}
		// for( cur.last(); cur.get(key, val) && key.prefix("a"); cur.prev() ) {}
		
		void on_page_split(size_t height, Pid pa, PageOffset split_index, PageOffset split_index_r, const Cursor & cur2){
			if( path.at(height).first == pa && path.at(height).second != PageOffset(-1) && path.at(height).second > split_index ){
				for(size_t i = height + 1; i != bucket_desc->height + 1; ++i)
					path.at(i) = cur2.path.at(i);
				path.at(height).first = cur2.path.at(height).first;
				path.at(height).second -= split_index_r;
			}
		}
		void on_insert(size_t height, Pid pa, PageOffset insert_index){
			if( path.at(height).first == pa && path.at(height).second != PageOffset(-1) && path.at(height).second >= insert_index ){
				path.at(height).second += 1;
			}
		}
		void on_erase(size_t height, Pid pa, PageOffset erase_index){
			if( path.at(height).first == pa && path.at(height).second != PageOffset(-1) && path.at(height).second > erase_index ){
				path.at(height).second -= 1;
			}
		}
	};
	class TX {
		friend class Cursor;
		friend class FreeList;
		friend class Bucket;

		DB & my_db;
		const Pid c_mappings_end_page; // We keep c_mappings while TX is uinsg it
		std::set<Cursor *> my_cursors;
		std::set<Bucket *> my_buckets;
		size_t meta_page_index;
		Tid oldest_reader_tid;
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
		bool meta_page_dirty;
		std::map<std::string, BucketDesc> bucket_descs;
		BucketDesc * load_bucket_desc(const Val & name);
		FreeList free_list;
		Pid get_free_page(Pid contigous_count);
		void mark_free_in_future_page(Pid page, Pid contigous_count); // associated with our tx, will be available after no read tx can ever use our tid
		
		DataPage * make_pages_writable(Cursor & cur, size_t height);
		
		PageOffset find_split_index(const CNodePtr & wr_dap, int * insert_direction, PageOffset insert_index, Val insert_key, Pid insert_page, size_t add_size_left, size_t add_size_right);
		
		Cursor insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid);
		Cursor force_split_node(Cursor & cur, size_t height, Val insert_key, Pid insert_page);
		char * force_split_leaf(Cursor & cur, Val insert_key, size_t insert_val_size);
		void merge_if_needed_leaf(Cursor & cur, LeafPtr wr_dap);
		void merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap);
		void prune_empty_node(Cursor & cur, size_t height, NodePtr wr_dap);
		
		CLeafPtr readable_leaf(Pid pa);
		CNodePtr readable_node(Pid pa);
		const char * readable_overflow(Pid pa);
		LeafPtr writable_leaf(Pid pa);
		NodePtr writable_node(Pid pa);
		char * writable_overflow(Pid pa, Pid count);
		
		void start_transaction();

		std::string print_db(const BucketDesc * bucket_desc);
		std::string print_db(Pid pa, size_t height, bool parse_meta);
	public:
		const uint32_t page_size; // copy from my_db
		static const char bucket_prefix = 'b';
		static const char freelist_prefix = 'f';
		explicit TX(DB & my_db);
		~TX();
		std::vector<Val> get_buckets(); // order of returned tables can be different each call. meta table not returned
		bool drop_bucket(const Val & name); // true if dropped, false if did not exist
		void commit(); // after commit, new transaction is started. in destructor we rollback last started transaction
	};
	class Bucket {
		friend class Cursor;
		friend class TX;
		friend class FreeList;
		TX & my_txn;
		BucketDesc * bucket_desc;
		std::string debug_name;

		Bucket(TX & my_txn, BucketDesc * bucket_desc):my_txn(my_txn), bucket_desc(bucket_desc) {
			my_txn.my_buckets.insert(this);
		}
	public:
		Bucket(TX & my_txn, const Val & name, bool create = true);
		~Bucket();
		bool exists()const { return bucket_desc != nullptr; }
		char * put(const Val & key, size_t value_size, bool nooverwrite); // danger! db will alloc space for key/value in db and return address for you to copy value to
		bool put(const Val & key, const Val & value, bool nooverwrite); // false if nooverwrite and key existed
		bool get(const Val & key, Val & value);
		bool del(const Val & key, bool must_exist);
		std::string print_db(){
			return bucket_desc ? my_txn.print_db(bucket_desc) : std::string();
		}
		std::string get_stats()const;
	};
}

