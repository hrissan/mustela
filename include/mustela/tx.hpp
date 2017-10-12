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
        std::vector<std::pair<Pid, PageOffset>> path;
        
        void truncate(size_t height){
            while(height < path.size() - 1)
                path[height++].second = -1; // set to special link
            if(height == path.size() - 1)
                path[height++].second = 0; // set to first leaf kv
        }
        //bool erased_left = false;
        //bool erased_right = false;

//        explicit Cursor(TX & my_txn);
//        ~Cursor();
    };
    class TX {
        DB & my_db;
        const uint32_t page_size; // copy from my_db

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
        void pop_tmp_copy(){
            tmp_pages.pop_back();
        }
        void clear_tmp_copies(){
            tmp_pages.clear();
        }
        MetaPage meta_page;
        
        FreeList free_list;
        Pid get_free_page(Pid contigous_count);
        void mark_free_in_future_page(Pid page, Pid contigous_count); // associated with our tx, will be available after no read tx can ever use our tid
        
        void lower_bound(Cursor & cur, const Val & key);
        DataPage * make_pages_writable(Cursor & cur, size_t height);
        
        PageOffset find_split_index(const CNodePtr & wr_dap, int * insert_direction, PageOffset insert_index, Val insert_key, Pid insert_page, size_t add_size_left, size_t add_size_right);

        void insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid);
        NodePage * force_split_node(Cursor & cur, size_t height, Val insert_key, Pid insert_page);
        char * force_split_leaf(Cursor & cur, size_t height, Val insert_key, size_t insert_val_size);
        void merge_if_needed_leaf(Cursor & cur, LeafPtr wr_dap);
        void merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap);
        void prune_empty_node(Cursor & cur, size_t height, NodePtr wr_dap);

        CLeafPtr readable_leaf(Pid pa);
        CNodePtr readable_node(Pid pa);
        LeafPtr writable_leaf(Pid pa);
        NodePtr writable_node(Pid pa);

        void start_transaction();
        std::string print_db(Pid pa, size_t height);
        
        Cursor main_cursor;
    public:
        explicit TX(DB & my_db);
        ~TX();
        std::string print_db();
        std::string get_stats()const;
        bool put(const Val & key, const Val & value, bool nooverwrite) { // false if nooverwrite and key existed
            char * dst = put(key, value.size, nooverwrite);
            if( dst )
                memcpy(dst, value.data, value.size);
            return dst != nullptr;
        }
        char * put(const Val & key, size_t value_size, bool nooverwrite); // danger! db will alloc space for key/value in db and return address for you to copy value to
        bool get(const Val & key, Val & value);
        bool del(const Val & key, bool must_exist);
        void commit(); // after commit, new transaction is started. in destructor we rollback last started transaction
    };
}

