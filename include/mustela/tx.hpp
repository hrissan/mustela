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
	struct Cursor {
		TX & my_txn;
		TableDesc & table;
		std::vector<std::pair<Pid, PageOffset>> path;
		
		void truncate(size_t height){
			while(height < path.size() - 1)
				path[height++].second = -1; // set to special link
			if(height == path.size() - 1)
				path[height++].second = 0; // set to first leaf kv
		}
		//bool erased_left = false;
		//bool erased_right = false;
	public:
		Cursor(Cursor && other);
		Cursor & operator=(Cursor && other)=delete;
		explicit Cursor(TX & my_txn, TableDesc & table);
		~Cursor();
		void on_page_split(size_t height, Pid pa, PageOffset split_index, PageOffset split_index_r, const Cursor & cur2){
			if( path.at(height).first == pa && path.at(height).second != PageOffset(-1) && path.at(height).second > split_index ){
				for(size_t i = height + 1; i != table.height + 1; ++i)
					//                for(size_t i = 0; i != height; ++i)
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
	class Bucket;
	class TX {
		DB & my_db;
		const Pid c_mappings_end_page; // We keep c_mappings while TX is uinsg it
		std::set<Cursor *> my_cursors;
		friend class Cursor;
		friend class FreeList;
		friend class Bucket;
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
		//        void pop_tmp_copy(){
		//            tmp_pages.pop_back();
		//        }
		void clear_tmp_copies(){
			tmp_pages.clear();
		}
		MetaPage meta_page;
		std::map<std::string, TableDesc> tables;
		TableDesc * load_table_desc(const Val & table){
			auto tit = tables.find(table.to_string());
			if( tit != tables.end() )
				return &tit->second;
			std::string key = "table/" + table.to_string();
			Val value;
			if( !get(meta_page.meta_table, Val(key), value) )
				return nullptr;
			TableDesc & td = tables[table.to_string()];
			ass(value.size == sizeof(td), "TableDesc size in DB is wrong");
			memmove(&td, value.data, value.size);
			return &td;
		}
		
		FreeList free_list;
		Pid get_free_page(Pid contigous_count);
		void mark_free_in_future_page(Pid page, Pid contigous_count); // associated with our tx, will be available after no read tx can ever use our tid
		
		void lower_bound(Cursor & cur, const Val & key);
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
		std::string print_db(Pid pa, size_t height);
		
		//        Cursor main_cursor;
		char * put(TableDesc & table, const Val & key, size_t value_size, bool nooverwrite);
		bool put(TableDesc & table, const Val & key, const Val & value, bool nooverwrite) {
			char * dst = put(table, key, value.size, nooverwrite);
			if( dst )
				memcpy(dst, value.data, value.size);
			return dst != nullptr;
		}
		bool get(TableDesc & table, const Val & key, Val & value);
		bool get_next(TableDesc & table, Val & key, Val & value);
		bool del(TableDesc & table, const Val & key, bool must_exist);
		std::string print_db(const TableDesc & table);
		std::string get_stats(const TableDesc & table, std::string name);
	public:
		const uint32_t page_size; // copy from my_db
		explicit TX(DB & my_db);
		~TX();
		std::string print_db(){
			return print_db(meta_page.meta_table);
		}
		std::string print_db(const Val & table){
			TableDesc * td = load_table_desc(table);
			if(!td)
				return std::string();
			return print_db(*td);
		}
		std::string get_stats(){
			return get_stats(meta_page.meta_table, std::string());
		}
		std::string get_stats(const Val & table){
			TableDesc * td = load_table_desc(table);
			if(!td)
				return std::string();
			return get_stats(*td, table.to_string());
		}
		bool create_table(const Val & table){ // true if just created, false if already existed
			if( load_table_desc(table) )
				return false;
			TableDesc & td = tables[table.to_string()];
			td = TableDesc{};
			td.root_page = get_free_page(1);
			td.leaf_page_count = 1;
			// We will put it in DB on commit
			return true;
		}
		bool drop_table(const Val & table){ // true if dropped, false if did not exist
			if( load_table_desc(table) )
				return false;
			// TODO - iterate cursors and throw if any points to this table
			// TODO - mark all table pages as free
			std::string key = "table/" + table.to_string();
			ass(del(meta_page.meta_table, Val(key), true), "Error while dropping table");
			tables.erase(key);
			return true;
		}
		bool put(const Val & table, const Val & key, const Val & value, bool nooverwrite) { // false if nooverwrite and key existed
			char * dst = put(table, key, value.size, nooverwrite);
			if( dst )
				memcpy(dst, value.data, value.size);
			return dst != nullptr;
		}
		char * put(const Val & table, const Val & key, size_t value_size, bool nooverwrite){ // danger! db will alloc space for key/value in db and return address for you to copy value to
			TableDesc * td = load_table_desc(table);
			if(!td)
				return nullptr;
			return put(*td, key, value_size, nooverwrite);
		}
		bool get(const Val & table, const Val & key, Val & value){
			TableDesc * td = load_table_desc(table);
			if(!td)
				return false;
			return get(*td, key, value);
		}
		bool del(const Val & table, const Val & key, bool must_exist){
			TableDesc * td = load_table_desc(table);
			if(!td)
				return false;
			return del(*td, key, must_exist);
		}
		void commit(); // after commit, new transaction is started. in destructor we rollback last started transaction
	};
	/*    class Bucket {
	 TX & tx;
	 TableDesc * td;
	 public:
	 Bucket(TX & tx, const Val & table, bool create_if_not_exists = true):tx(tx), td( tx.load_table_desc(table) )
	 {
	 if(!td && create_if_not_exists){
	 // Create
	 }
	 }
	 bool put(const Val & key, const Val & value, bool nooverwrite) { // false if nooverwrite and key existed
	 char * dst = put(key, value.size, nooverwrite);
	 if( dst )
	 memcpy(dst, value.data, value.size);
	 return dst != nullptr;
	 }
	 char * put(const Val & key, size_t value_size, bool nooverwrite){ // danger! db will alloc space for key/value in db and return address for you to copy value to
	 if(!td)
	 return nullptr;
	 return tx.put(*td, key, value_size, nooverwrite);
	 }
	 bool get(const Val & table, const Val & key, Val & value){
	 if(!td)
	 return false;
	 return tx.get(*td, key, value);
	 }
	 bool del(const Val & table, const Val & key, bool must_exist){
	 if(!td)
	 return false;
	 return tx.del(*td, key, must_exist);
	 }
	 };
	 
	 
	 
	 */
}

