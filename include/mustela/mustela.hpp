#ifndef mustela_hpp
#define mustela_hpp

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {

    struct Mapping {
        Pid begin_page;
        Pid end_page;
        char * addr;
        explicit Mapping(Pid begin_page, Pid end_page, char * addr):begin_page(begin_page), addr(addr), end_page(end_page)
        {}
    };
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
        std::vector<std::vector<char>> tmp_pages; // We do not store it in stack. Sometimes we need more than one.
        NodePage * push_tmp_copy(const NodePage * other){
            tmp_pages.push_back(std::vector<char>(page_size, 0));
            memmove(tmp_pages.back().data(), other, page_size);
            return (NodePage * )tmp_pages.back().data();
        }
        LeafPage * push_tmp_copy(const LeafPage * other){
            tmp_pages.push_back(std::vector<char>(page_size, 0));
            memmove(tmp_pages.back().data(), other, page_size);
            return (LeafPage * )tmp_pages.back().data();
        }
        void pop_tmp_copy(){
            tmp_pages.pop_back();
        }
        void clear_tmp_copies(){
            tmp_pages.clear();
        }
        MetaPage meta_page;
        
        std::vector<Pid> free_pages_cache;
        Pid get_free_page(Pid contigous_count);
        void mark_free_in_future_page(Pid page); // associated with our tx, will be available after no read tx can ever use our tid
        
        void lower_bound(Cursor & cur, const Val & key);
        DataPage * make_pages_writable(Cursor & cur, size_t height);
        
        PageOffset find_split_index(const CNodePtr & wr_dap, int * insert_direction, PageOffset insert_index, Val insert_key, Pid insert_page, size_t add_size_left, size_t add_size_right);

        void insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid);
        NodePage * force_split_node(Cursor & cur, size_t height, Val insert_key, Pid insert_page);
        LeafPage * force_split_leaf(Cursor & cur, size_t height, Val insert_key, Val insert_val);
        void merge_if_needed_leaf(Cursor & cur, LeafPtr wr_dap);
        void merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap);

        const LeafPage * readable_leaf(Pid pa);
        const NodePage * readable_node(Pid pa);
        LeafPage * writable_leaf(Pid pa);
        NodePage * writable_node(Pid pa);

        void start_transaction();
        std::string print_db(Pid pa, size_t height);
        
        Cursor main_cursor;
    public:
        explicit TX(DB & my_db);
        ~TX();
        std::string print_db();
        bool put(const Val & key, const Val & value, bool nooverwrite); // false if nooverwrite and key existed
        bool get(const Val & key, Val & value);
        bool del(const Val & key, bool must_exist);
        void commit(); // after commit, new transaction is started. in destructor we rollback last started transaction
    };
//{'branch_pages': 1040L,
//    'depth': 4L,
//    'entries': 3761848L,
//    'leaf_pages': 73658L,
//    'overflow_pages': 0L,
//    'psize': 4096L}
    class DB {
        friend class TX;
        //friend class Cursor;
        int fd = -1;
        const uint32_t page_size;
        const uint32_t physical_page_size; // We allow to work with smaller pages when reading file from different platform (or portable variant)
        const bool read_only;
        char * c_mapping = nullptr;
        std::vector<Mapping> mappings;
        size_t last_meta_page_index()const;
        size_t oldest_meta_page_index()const;
        void grow_mappings(Pid new_page_count);
        DataPage * writable_page(Pid page)const;
        const DataPage * readable_page(Pid page)const { return (const DataPage * )(c_mapping + page_size * page); }
        const MetaPage * readable_meta_page(size_t index)const { return (const MetaPage * )readable_page(index); }
        MetaPage * writable_meta_page(size_t index)const { return (MetaPage * )writable_page(index); }
        void create_db();
    public:
        explicit DB(const std::string & file_path, bool read_only = false); // If file does not exist,
        ~DB();
    };
}

#endif /* mustela_hpp */
