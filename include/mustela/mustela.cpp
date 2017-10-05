#include "mustela.hpp"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <iostream>
#include <algorithm>

namespace mustela {

    DB::DB(const std::string & file_path, bool read_only):read_only(read_only), page_size(128), physical_page_size(static_cast<decltype(physical_page_size)>(sysconf(_SC_PAGESIZE))){
        fd = open(file_path.c_str(), (read_only ? O_RDONLY : O_RDWR) | O_CREAT, (mode_t)0600);
        if( fd == -1)
            throw Exception("file open failed");
        uint64_t file_size = lseek(fd, 0, SEEK_END);
        if( file_size == -1)
            throw Exception("file lseek SEEK_END failed");
        void * cm = mmap(0, MAX_MAPPING_SIZE, PROT_READ, MAP_SHARED, fd, 0);
        if (cm == MAP_FAILED)
            throw Exception("mmap PROT_READ failed");
        c_mapping = (char *)cm;
        if( file_size < sizeof(MetaPage) ){
            create_db();
            return;
        }
//        throw Exception("mmap file_size too small (corrupted or different platform)");
        const MetaPage * meta_pages[3]{readable_meta_page(0), readable_meta_page(1), readable_meta_page(2)};
        if( meta_pages[0]->magic != META_MAGIC && meta_pages[0]->magic != META_MAGIC_ALTENDIAN )
            throw Exception("file is either not mustela DB or corrupted");
        if( meta_pages[0]->magic != META_MAGIC && meta_pages[0]->page_size != page_size )
            throw Exception("file is from incompatible platform, should convert before opening");
        if( meta_pages[0]->version != OUR_VERSION )
            throw Exception("file is from older or newer version, should convert before opening");
        if( file_size < page_size * 5 ){
            create_db(); // We could just not finish the last create_db call
            return;
        }
        if( !meta_pages[0]->check(page_size, file_size) || !meta_pages[1]->check(page_size, file_size) || !meta_pages[2]->check(page_size, file_size))
            throw Exception("meta_pages failed to check - (corrupted)");
//        if( meta_pages[last_meta_page()]->page_count * page_size > file_size )
//            throw Exception("file too short for last transaction (corrupted)");
    }
    DB::~DB(){
        
    }
    size_t DB::last_meta_page_index()const{
        size_t result = 0;
        if( readable_meta_page(1)->tid > readable_meta_page(result)->tid )
            result = 1;
        if( readable_meta_page(2)->tid > readable_meta_page(result)->tid )
            result = 2;
        return result;
    }
    size_t DB::oldest_meta_page_index()const{
        size_t result = 0;
        if( readable_meta_page(1)->tid < readable_meta_page(result)->tid )
            result = 1;
        if( readable_meta_page(2)->tid < readable_meta_page(result)->tid )
            result = 2;
        return result;
    }

    void DB::create_db(){
        if( lseek(fd, 0, SEEK_SET) == -1 )
            throw Exception("file seek failed");
        {
            char meta_buf[page_size];
            memset(meta_buf, 0, page_size); // C++ standard URODI "variable size object cannot be initialized"
            MetaPage * mp = (MetaPage *)meta_buf;
            mp->magic = META_MAGIC;
            mp->version = OUR_VERSION;
            mp->page_size = page_size;
            mp->page_count = 5;
            mp->pid = 0;
            mp->tid = 0;
            mp->main_root_page = 3;
            mp->main_height = 0;
            mp->free_root_page = 4;
            if( write(fd, meta_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
            mp->pid = 1;
            if( write(fd, meta_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
            mp->pid = 2;
            if( write(fd, meta_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
        }
        {
            char data_buf[page_size];
            LeafPage * mp = (LeafPage *)data_buf;
            mp->pid = 3;
            mp->init_dirty(page_size, 0);
            if( write(fd, data_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
            mp->pid = 4;
            if( write(fd, data_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
        }
        if( fsync(fd) == -1 )
            throw Exception("fsync failed in create_db");
    }
    DataPage * DB::writable_page(Pid page)const{
        auto it = std::upper_bound(mappings.begin(), mappings.end(), page, [](Pid p, const Mapping & ma) {
            return p < ma.end_page;
        });
        if( it == mappings.end() )
            throw Exception("writable_page out of range");
        return (DataPage *)(it->addr + (page - it->begin_page)*page_size);
    }

    void DB::grow_mappings(Pid new_page_count){
        auto old_page_count = mappings.empty() ? 0 : mappings.back().end_page;
        if( new_page_count <= old_page_count )
            return;
        new_page_count = ((3 + new_page_count) * 5 / 4); // reserve 20% excess space, make first mapping include at least 3 meta pages
        size_t additional_granularity = 1;// 65536;  // on Windows mmapped regions should be aligned to 65536
        if( page_size < additional_granularity )
            new_page_count = ((new_page_count * page_size + additional_granularity - 1) / additional_granularity) * additional_granularity / page_size;
        if( page_size < physical_page_size )
            new_page_count = ((new_page_count * page_size + physical_page_size - 1) / physical_page_size) * physical_page_size / page_size;
        uint64_t new_file_size = new_page_count * page_size;
        if( lseek(fd, new_file_size - 1, SEEK_SET) == -1 )
            throw Exception("file seek failed in grow_mappings");
        if( write(fd, "", 1) != 1 )
            throw Exception("file write failed in grow_mappings");
        uint64_t map_size = new_file_size - old_page_count * page_size;
        uint64_t map_offset = old_page_count * page_size;
        void * wm = mmap(0, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_offset);
        if (wm == MAP_FAILED)
            throw Exception("mmap PROT_READ | PROT_WRITE failed");
        mappings.push_back(Mapping(old_page_count, new_page_count, (char *)wm));
    }
    
    TX::TX(DB & my_db):my_db(my_db) {
        start_transaction();
    }
    void TX::start_transaction(){
        meta_page_index = my_db.oldest_meta_page_index(); // Overwrite oldest page
        auto newest_page_index = my_db.last_meta_page_index(); // by newest
        meta_page = *(my_db.readable_meta_page(newest_page_index));
        meta_page.tid += 1;
    }

    TX::~TX(){
        // rollback is nop
        //if( !cursors.empty() )
        //    throw Exception("cursors should be destroyed before transaction they are in");
    }
    const LeafPage * TX::readable_leaf(Pid pa){
        return (const LeafPage *)my_db.readable_page(pa);
    }
    const NodePage * TX::readable_node(Pid pa){
        return (const NodePage *)my_db.readable_page(pa);
    }
    LeafPage * TX::writable_leaf(Pid pa){
        LeafPage * result = (LeafPage *)my_db.writable_page(pa);
        ass(result->tid == meta_page.tid, "writable_leaf is not from our transaction");
        return result;
    }
    NodePage * TX::writable_node(Pid pa){
        NodePage * result = (NodePage *)my_db.writable_page(pa);
        ass(result->tid == meta_page.tid, "writable_node is not from our transaction");
        return result;
    }
    void TX::mark_free_in_future_page(Pid page){
        // NOP for now
    }
    Pid TX::get_free_page(Pid contigous_count){
        if( contigous_count == 1 && !free_pages_cache.empty()){ // TODO - think about merging adjacent free pages and returning them as contigous region
            Pid pa = free_pages_cache.back();
            free_pages_cache.pop_back();
            return pa;
        }
        my_db.grow_mappings(meta_page.page_count + contigous_count);
        Pid pa = meta_page.page_count;
        meta_page.page_count += contigous_count;
        DataPage * new_pa = my_db.writable_page(pa);
        new_pa->pid = pa;
        new_pa->tid = meta_page.tid;
        return pa;
    }
    DataPage * TX::make_pages_writable(Cursor & cur, size_t height){
        //const bool is_leaf = (height == cur.path.size() - 1);
        auto & path_el = cur.path.at(height);
        const DataPage * dap = my_db.readable_page(path_el.first);
        if( dap->tid == meta_page.tid ){
            DataPage * wr_dap = my_db.writable_page(path_el.first);
            return wr_dap;
        }
        mark_free_in_future_page(path_el.first);
        path_el.first = get_free_page(1);
        DataPage * wr_dap = my_db.writable_page(path_el.first);
        memmove(wr_dap, dap, my_db.page_size);
        wr_dap->pid = path_el.first;
        wr_dap->tid = meta_page.tid;
        if(height == 0){ // node is root
            meta_page.main_root_page = path_el.first;
            return wr_dap;
        }
        NodePage * wr_parent = (NodePage *)make_pages_writable(cur, height - 1);
        wr_parent->set_item_value(my_db.page_size, cur.path.at(height - 1).second, path_el.first);
        return wr_dap;
    }
    void TX::insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid){
        if( height == size_t(-1)){ // making new root
            //cur.path.insert(cur.path.begin(), std::make_pair(root_pid, PageOffset(-1))); // update all cursors with new root
            Pid root_pid = get_free_page(1);
            NodePage * wr_root = writable_node(root_pid);
            wr_root->init_dirty(my_db.page_size, meta_page.tid);
            wr_root->set_item_value(my_db.page_size, -1, meta_page.main_root_page);
            wr_root->insert_at(my_db.page_size, wr_root->item_count, key, new_pid);
            meta_page.main_root_page = root_pid;
            meta_page.main_height += 1;
            // TODO - check that cur is correctly updated
            return;
        }
        auto & path_el = cur.path.at(height);
        NodePage * wr_dap = writable_node(path_el.first);
        size_t required_size = sizeof(PageOffset) + wr_dap->kv_size(my_db.page_size, key, new_pid);
        if( !wr_dap->has_enough_free_space(my_db.page_size, required_size) ){
            wr_dap = force_split_node(cur, height, key, new_pid);
//            Cursor cur2 = lower_bound(key);
//            if( cur.path != cur2.path )
//                std::cout << "cur != cur2" << std::endl;
            return;
        }
        wr_dap->insert_at(my_db.page_size, path_el.second, key, new_pid);
    }
    PageOffset TX::find_split_index(const NodePage * wr_dap, int * insert_direction, PageOffset insert_index, Val insert_key, Pid insert_page, size_t add_size_left, size_t add_size_right){
        size_t required_size = sizeof(PageOffset) + wr_dap->kv_size(my_db.page_size, insert_key, insert_page);
        PageOffset best_split_index = -1;
        int best_disbalance = 0;
        size_t left_sigma_size = 0;
        for(PageOffset i = 0; i != wr_dap->item_count; ++i){ // Split element cannot be last one if it is insert_key
            size_t split_item_size = wr_dap->item_size(my_db.page_size, i);
            size_t right_sigma_size = wr_dap->items_size - left_sigma_size - split_item_size; // do not count split item size
            bool enough_left = wr_dap->enough_size_for_elements(my_db.page_size, i, left_sigma_size + required_size + add_size_left);
            bool enough_right = wr_dap->enough_size_for_elements(my_db.page_size, wr_dap->item_count - 1 - i, right_sigma_size + required_size + add_size_right);
            if( i >= insert_index && enough_left ){ // split index can equal insert index
                int disbalance = std::abs(int(right_sigma_size + add_size_right) - int(left_sigma_size + required_size + add_size_left));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_disbalance = disbalance;
                    *insert_direction = -1;
                }
            }
            if( i < insert_index && enough_right ){ // split index cannot equal insert index
                int disbalance = std::abs(int(right_sigma_size + required_size + add_size_right) - int(left_sigma_size - add_size_left));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_disbalance = disbalance;
                    *insert_direction = 1;
                }
            }
            if( i == insert_index ){ // last possibility is splitting at insert_index and moving insert_key into parent
                int disbalance = std::abs(int(right_sigma_size + add_size_right) - int(left_sigma_size + add_size_left));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_disbalance = disbalance;
                    *insert_direction = 0;
                }
            }
            //if( i != wr_dap->item_count) // last iteration has no element
            left_sigma_size += split_item_size;
        }
        return best_split_index;
    }
    NodePage * TX::force_split_node(Cursor & cur, size_t height, Val insert_key, Pid insert_page){
        auto & path_el = cur.path.at(height);
        NodePage * wr_dap = writable_node(path_el.first);
        ass(wr_dap->item_count >= 3, "Node being split contains < 3 items");
        size_t required_size = sizeof(PageOffset) + wr_dap->kv_size(my_db.page_size, insert_key, insert_page);
        // Find split index so that either of split pages will fit new value and disbalance is minimal
        PageOffset insert_index = path_el.second;
        PageOffset best_split_index = -1;
        PageOffset best_split_index_r = -1;
        int insert_direction = 0;
        int best_disbalance = 0;
        size_t left_sigma_size = 0;
        for(PageOffset i = 0; i != wr_dap->item_count; ++i){ // Split element cannot be last one if it is insert_key
            size_t split_item_size = wr_dap->item_size(my_db.page_size, i);
            size_t right_sigma_size = wr_dap->items_size - left_sigma_size - split_item_size; // do not count split item size
            bool enough_left = wr_dap->enough_size_for_elements(my_db.page_size, i, left_sigma_size + required_size);
            bool enough_right = wr_dap->enough_size_for_elements(my_db.page_size, wr_dap->item_count - 1 - i, right_sigma_size + required_size);
            if( i >= insert_index && enough_left ){ // split index can equal insert index
                int disbalance = std::abs(int(right_sigma_size) - int(left_sigma_size + required_size));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_split_index_r = i + 1;
                    best_disbalance = disbalance;
                    insert_direction = -1;
                }
            }
            if( i < insert_index && enough_right ){ // split index cannot equal insert index
                int disbalance = std::abs(int(right_sigma_size + required_size) - int(left_sigma_size));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_split_index_r = i + 1;
                    best_disbalance = disbalance;
                    insert_direction = 1;
                }
            }
            if( i == insert_index ){ // last possibility is splitting at insert_index and moving insert_key into parent
                int disbalance = std::abs(int(right_sigma_size) - int(left_sigma_size));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_split_index_r = i;
                    best_disbalance = disbalance;
                    insert_direction = 0;
                }
            }
            //if( i != wr_dap->item_count) // last iteration has no element
            left_sigma_size += split_item_size;
        }
        //PageOffset split_index = best_split_index;
        //ass(split_index >= 0 && split_index < wr_dap->item_count, "Could not find split index for node");
        
        Pid right_pid = get_free_page(1);
        NodePage * wr_right = writable_node(right_pid);
        wr_right->init_dirty(my_db.page_size, meta_page.tid);
//        PageOffset split_item = wr_dap->item_count / 2;
        for(PageOffset i = best_split_index_r; i != wr_dap->item_count; ++i){
            Pid value;
            Val key = wr_dap->get_item_kv(my_db.page_size, i, value);
            wr_right->insert_at(my_db.page_size, wr_right->item_count, key, value);
        }
        char raw_my_copy[my_db.page_size];
        memmove(raw_my_copy, wr_dap, my_db.page_size);
        NodePage * my_copy = (NodePage *)raw_my_copy;
        wr_dap->clear(my_db.page_size);
        wr_dap->set_item_value(my_db.page_size, -1, my_copy->get_item_value(my_db.page_size, -1));
        for(PageOffset i = 0; i != best_split_index; ++i){
            Pid value;
            Val key = my_copy->get_item_kv(my_db.page_size, i, value);
            wr_dap->insert_at(my_db.page_size, wr_dap->item_count, key, value);
        }
        //Cursor cur2 = cur;
        if( height != 0 )
            cur.path.at(height - 1).second += 1;
        NodePage * result_page = nullptr;
        if( insert_direction == -1 ) {
            Pid split_value;
            Val split_key = my_copy->get_item_kv(my_db.page_size, best_split_index, split_value);
            wr_right->set_item_value(my_db.page_size, -1, split_value);
            result_page = wr_dap;
            result_page->insert_at(my_db.page_size, insert_index, insert_key, insert_page);
            insert_pages_to_node(cur, height - 1, split_key, right_pid);
        }else if( insert_direction == 1 ) {
            path_el.first = right_pid;
            path_el.second -= best_split_index_r;
            Pid split_value;
            Val split_key = my_copy->get_item_kv(my_db.page_size, best_split_index, split_value);
            wr_right->set_item_value(my_db.page_size, -1, split_value);
            result_page = wr_right;
            result_page->insert_at(my_db.page_size, insert_index - best_split_index_r, insert_key, insert_page);
            insert_pages_to_node(cur, height - 1, split_key, right_pid);
        }else{
            path_el.first = right_pid;
            path_el.second -= best_split_index_r;
            wr_right->set_item_value(my_db.page_size, -1, insert_page);
            result_page = wr_right; // wr_parent :)
            insert_pages_to_node(cur, height - 1, insert_key, right_pid);
        }
        return result_page;
    }
    LeafPage * TX::force_split_leaf(Cursor & cur, size_t height, Val insert_key, Val insert_val){
        auto & path_el = cur.path.at(height);
        LeafPage * wr_dap = writable_leaf(path_el.first);
        size_t required_size = sizeof(PageOffset) + wr_dap->kv_size(my_db.page_size, insert_key, insert_val);
        // Find split index so that either of split pages will fit new value and disbalance is minimal
        PageOffset insert_index = path_el.second;
        PageOffset best_split_index = -1;
        int insert_direction = 0;
        int best_disbalance = 0;
        size_t left_sigma_size = 0;
        for(PageOffset i = 0; i != wr_dap->item_count + 1; ++i){ // We can split at the end too
            size_t right_sigma_size = wr_dap->items_size - left_sigma_size;
            bool enough_left = wr_dap->enough_size_for_elements(my_db.page_size, i, left_sigma_size + required_size);
            bool enough_right = wr_dap->enough_size_for_elements(my_db.page_size, wr_dap->item_count - i, right_sigma_size + required_size);
            if( i >= insert_index && enough_left ){ // should insert only to the left}
                int disbalance = std::abs(int(right_sigma_size) - int(left_sigma_size + required_size));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_disbalance = disbalance;
                    insert_direction = -1;
                }
            }
            if( i <= insert_index && enough_right ){
                int disbalance = std::abs(int(right_sigma_size + required_size) - int(left_sigma_size));
                if( best_split_index == PageOffset(-1) || disbalance < best_disbalance ){
                    best_split_index = i;
                    best_disbalance = disbalance;
                    insert_direction = 1;
                }
            }
            if( i != wr_dap->item_count) // last iteration has no element
                left_sigma_size += wr_dap->item_size(my_db.page_size, i);
        }
        PageOffset split_index = best_split_index;
        if( split_index == PageOffset(-1) ){ // Perform 3 - split
            split_index = insert_index;
            insert_direction = 0;
        }
        Pid right_pid = get_free_page(1);
        LeafPage * wr_right = writable_leaf(right_pid);
        wr_right->init_dirty(my_db.page_size, meta_page.tid);
        for(PageOffset i = split_index; i != wr_dap->item_count; ++i){
            Val value;
            Val key = wr_dap->get_item_kv(my_db.page_size, i, value);
            wr_right->insert_at(my_db.page_size, wr_right->item_count, key, value);
        }
        char raw_my_copy[my_db.page_size];
        memmove(raw_my_copy, wr_dap, my_db.page_size);
        LeafPage * my_copy = (LeafPage *)raw_my_copy;
        wr_dap->clear(my_db.page_size);
        for(PageOffset i = 0; i != split_index; ++i){
            Val value;
            Val key = my_copy->get_item_kv(my_db.page_size, i, value);
            wr_dap->insert_at(my_db.page_size, wr_dap->item_count, key, value);
        }
        LeafPage * result_page = nullptr;
        if( path_el.first == wr_dap->pid ){
            if( path_el.second >= split_index ){
                path_el.first = right_pid;
                path_el.second -= split_index;
            }
        }
//        Cursor cur2 = cur;
        if( height != 0 )
            cur.path.at(height - 1).second += 1;
        if( insert_direction == -1 ) {
//            path_el.second = wr_dap->item_count;
            result_page = wr_dap;
            result_page->insert_at(my_db.page_size, insert_index, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right->get_item_key(my_db.page_size, 0)), right_pid);
        }else if( insert_direction == 1 ){
//            path_el.first = right_pid;
//            path_el.second = 0;
            result_page = wr_right;
            result_page->insert_at(my_db.page_size, insert_index - split_index, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right->get_item_key(my_db.page_size, 0)), right_pid);
        }else {
            Pid middle_pid = get_free_page(1);
            LeafPage * wr_middle = writable_leaf(middle_pid);
            wr_middle->init_dirty(my_db.page_size, meta_page.tid);
            //path_el.first = middle_pid;
            //path_el.second = 0;
            result_page = wr_middle;
            result_page->insert_at(my_db.page_size, 0, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right->get_item_key(my_db.page_size, 0)), right_pid);
            insert_pages_to_node(cur, height - 1, insert_key, middle_pid);
        }
        return result_page;
    }
    void TX::merge_if_needed_node(Cursor & cur, size_t height, NodePage * wr_dap){
        PageOffset half_size = wr_dap->half_size(my_db.page_size);
        if( !wr_dap->has_enough_free_space(my_db.page_size, half_size) )
            return;
        if(height == 0){ // merging root
            if( wr_dap->item_count != 0)
                return;
            mark_free_in_future_page(meta_page.main_root_page);
            meta_page.main_root_page = wr_dap->get_item_value(my_db.page_size, -1);
            meta_page.main_height -= 1;
            cur.path.erase(cur.path.begin()); // TODO - check
            return;
        }
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path[height - 1];
        NodePage * wr_parent = writable_node(path_pa.first);
        ass(wr_parent->item_count > 0, "Found parent node with 0 items");
        //PageOffset required_size = wr_dap->items_size + wr_dap->item_count * sizeof(PageOffset);
        const NodePage * remember_left_sib = nullptr;
        const NodePage * left_sib = nullptr;
        PageOffset left_data_size = 0;
        Val my_key;
        size_t required_size_for_my_kv = 0;
        const NodePage * remember_right_sib = nullptr;
        const NodePage * right_sib = nullptr;
        PageOffset right_data_size = 0;
        Val right_key;
        if( path_pa.second != PageOffset(-1)){
            Pid my_pid;
            my_key = wr_parent->get_item_kv(my_db.page_size, path_pa.second, my_pid);
            required_size_for_my_kv = wr_parent->kv_size(my_db.page_size, my_key, my_pid);
            ass(my_pid == wr_dap->pid, "merge_if_needed_node my pid in parent does not match");
            Pid left_pid = wr_parent->get_item_value(my_db.page_size, path_pa.second - 1);
            remember_left_sib = left_sib = readable_node(left_pid);
            left_data_size = left_sib->data_size();
            left_data_size += sizeof(PageOffset) + left_sib->kv_size(my_db.page_size, my_key, 0); // will need to insert key from parent. Achtung - 0 works only when fixed-size pids are used
            if( !wr_dap->has_enough_free_space(my_db.page_size, left_data_size) )
                left_sib = nullptr; // forget about left!
        }
        if(path_pa.second == PageOffset(-1) || path_pa.second < wr_parent->item_count - 1){
            Pid right_pid;
            right_key = wr_parent->get_item_kv(my_db.page_size, path_pa.second + 1, right_pid);
            remember_right_sib = right_sib = readable_node(right_pid);
            right_data_size = right_sib->data_size();
            right_data_size += sizeof(PageOffset) + right_sib->kv_size(my_db.page_size, right_key, 0); // will need to insert key from parent! Achtung - 0 works only when fixed-size pids are used
            if( !wr_dap->has_enough_free_space(my_db.page_size, right_data_size) )
                right_sib = nullptr; // forget about right!
        }
        if( left_sib && right_sib && !wr_dap->has_enough_free_space(my_db.page_size, left_data_size + right_data_size) ){ // If cannot merge both, select smallest
            if( left_data_size < right_data_size ) // <= will also work
                right_sib = nullptr;
            else
                left_sib = nullptr;
        }
        if( wr_dap->item_count == 0 && !left_sib && !right_sib) { // Cannot merge, siblings are full and do not fit key from parent, so we borrow!
            if( remember_left_sib && remember_right_sib){ // Select larger one
                if( remember_left_sib->data_size() < remember_right_sib->data_size() )
                    remember_left_sib = nullptr;
                else
                    remember_right_sib = nullptr;
            }
            ass(remember_left_sib || remember_right_sib, "Node is empty and cannot borrow from siblings");
/*            if( remember_left_sib ){
                Pid spec_val = wr_dap->get_item_value(my_db.page_size, -1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(remember_left_sib, &insert_direction, remember_left_sib->item_count, my_key, spec_val, 0, 0);
                if( split_index == 0 )
                    split_index = 1;
                ass(insert_direction != 0 && split_index < remember_left_sib->item_count, "Split index to the right");
                Pid split_val;
                Val split_key = remember_left_sib->get_item_kv(my_db.page_size, split_index, split_val);
                for(PageOffset i = split_index + 1; i != remember_left_sib->item_count; ++i){
                    Pid val;
                    Val key = remember_left_sib->get_item_kv(my_db.page_size, i, val);
                    wr_dap->insert_at(my_db.page_size, i - split_index - 1, key, val);
                }
                wr_dap->insert_at(my_db.page_size, wr_dap->item_count, my_key, spec_val);
                wr_dap->set_item_value(my_db.page_size, -1, split_val);
                wr_parent->remove_simple(my_db.page_size, path_pa.second);
                // TODO split parent if needed
                wr_parent->insert_at(my_db.page_size, path_pa.second, split_key, wr_dap->pid);
            }
            if( remember_right_sib ){
                Pid spec_val = remember_right_sib->get_item_value(my_db.page_size, -1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(remember_right_sib, &insert_direction, 0, right_key, spec_val, 0, 0);
                if( split_index == remember_right_sib->item_count - 1 )
                    split_index -= 1;
                ass(insert_direction != 0 && split_index > 0, "Split index to the right");
                Pid split_val;
                Val split_key = remember_right_sib->get_item_kv(my_db.page_size, split_index, split_val);
                wr_dap->insert_at(my_db.page_size, 0, right_key, spec_val);
                for(PageOffset i = 0; i != split_index; ++i){
                    Pid val;
                    Val key = remember_right_sib->get_item_kv(my_db.page_size, i, val);
                    wr_dap->insert_at(my_db.page_size, wr_dap->item_count, key, val);
                }
                remember_right_sib->set_item_value(my_db.page_size, -1, split_val);
                wr_parent->remove_simple(my_db.page_size, path_pa.second + 1);
                // TODO split parent if needed
                wr_parent->insert_at(my_db.page_size, path_pa.second + 1, split_key, wr_dap->pid);
            }*/
        }
        if( left_sib ){
            Pid spec_val = wr_dap->get_item_value(my_db.page_size, -1);
            wr_dap->insert_at(my_db.page_size, 0, my_key, spec_val);
            wr_dap->set_item_value(my_db.page_size, -1, left_sib->get_item_value(my_db.page_size, -1));
            for(PageOffset i = 0; i != left_sib->item_count; ++i){
                Pid val;
                Val key = left_sib->get_item_kv(my_db.page_size, i, val);
                wr_dap->insert_at(my_db.page_size, i, key, val);
            }
            mark_free_in_future_page(left_sib->pid); // unlink left, point its slot in parent to us, remove our slot in parent
            wr_parent->remove_simple(my_db.page_size, path_pa.second);
            wr_parent->set_item_value(my_db.page_size, path_pa.second - 1, wr_dap->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( right_sib ){
            Pid spec_val = right_sib->get_item_value(my_db.page_size, -1);
            wr_dap->insert_at(my_db.page_size, wr_dap->item_count, right_key, spec_val);
            for(PageOffset i = 0; i != right_sib->item_count; ++i){
                Pid val;
                Val key = right_sib->get_item_kv(my_db.page_size, i, val);
                wr_dap->insert_at(my_db.page_size, wr_dap->item_count, key, val);
            }
            mark_free_in_future_page(right_sib->pid); // unlink right, remove its slot in parent
            wr_parent->remove_simple(my_db.page_size, path_pa.second + 1);
        }
        merge_if_needed_node(cur, height - 1, wr_parent);
    }
    void TX::merge_if_needed_leaf(Cursor & cur, LeafPage * wr_dap){
        PageOffset half_size = wr_dap->half_size(my_db.page_size);
        if( !wr_dap->has_enough_free_space(my_db.page_size, half_size) )
            return;
        if(cur.path.size() <= 1) // root is leaf, cannot merge anyway
            return;
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path[cur.path.size() - 2];
        NodePage * wr_parent = writable_node(path_pa.first);
        ass(wr_parent->item_count > 0, "Found parent node with 0 items");
        //PageOffset required_size = wr_dap->items_size + wr_dap->item_count * sizeof(PageOffset);
        const LeafPage * left_sib = nullptr;
        PageOffset left_data_size = 0;
        //Pid left_pid = 0;
        const LeafPage * right_sib = nullptr;
        PageOffset right_data_size = 0;
        //Pid right_pid = 0;
        if( path_pa.second != PageOffset(-1)){
            Pid left_pid = wr_parent->get_item_value(my_db.page_size, path_pa.second - 1);
            left_sib = readable_leaf(left_pid);
            left_data_size = left_sib->data_size();
            if( !wr_dap->has_enough_free_space(my_db.page_size, left_data_size) )
                left_sib = nullptr; // forget about left!
        }
        if( path_pa.second < wr_parent->item_count - 1){
            Pid right_pid = wr_parent->get_item_value(my_db.page_size, path_pa.second + 1);
            right_sib = readable_leaf(right_pid);
            right_data_size = right_sib->data_size();
            if( !wr_dap->has_enough_free_space(my_db.page_size, right_data_size) )
                right_sib = nullptr; // forget about right!
        }
        if( left_sib && right_sib && !wr_dap->has_enough_free_space(my_db.page_size, left_data_size + right_data_size) ){ // If cannot merge both, select smallest
            if( left_data_size < right_data_size ) // <= will also work
                right_sib = nullptr;
            else
                left_sib = nullptr;
        }
        if( left_sib ){
            for(PageOffset i = 0; i != left_sib->item_count; ++i){
                Val val;
                Val key = left_sib->get_item_kv(my_db.page_size, i, val);
                wr_dap->insert_at(my_db.page_size, i, key, val);
            }
            mark_free_in_future_page(left_sib->pid); // unlink left, point its slot in parent to us, remove our slot in parent
            wr_parent->remove_simple(my_db.page_size, path_pa.second);
            wr_parent->set_item_value(my_db.page_size, path_pa.second - 1, wr_dap->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( right_sib ){
            for(PageOffset i = 0; i != right_sib->item_count; ++i){
                Val val;
                Val key = right_sib->get_item_kv(my_db.page_size, i, val);
                wr_dap->insert_at(my_db.page_size, wr_dap->item_count, key, val);
            }
            mark_free_in_future_page(right_sib->pid); // unlink right, remove its slot in parent
            wr_parent->remove_simple(my_db.page_size, path_pa.second + 1);
        }
        if( left_sib || right_sib )
            merge_if_needed_node(cur, cur.path.size() - 2, wr_parent);
    }

    bool TX::put(const Val & key, const Val & value, bool nooverwrite){
        lower_bound(main_cursor, key);
        const LeafPage * dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        bool same_key = item != dap->item_count && Val(dap->get_item_key(my_db.page_size, item)) == key;
        if( same_key && nooverwrite )
            return false;
        // TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
        LeafPage * wr_dap = (LeafPage *)make_pages_writable(main_cursor, main_cursor.path.size() - 1);
        if( same_key )
            wr_dap->remove_simple(my_db.page_size, main_cursor.path.back().second);
        size_t required_size = sizeof(PageOffset) + wr_dap->kv_size(my_db.page_size, key, value);
        if( !wr_dap->has_enough_free_space(my_db.page_size, required_size) ){
            wr_dap = force_split_leaf(main_cursor, main_cursor.path.size() - 1, key, value);
//            Cursor cur2 = lower_bound(key);
//            if( cur.path != cur2.path )
//                std::cout << "cur != cur2" << std::endl;
            return true;
        }
        wr_dap->insert_at(my_db.page_size, main_cursor.path.back().second, key, value);
        merge_if_needed_leaf(main_cursor, wr_dap);
        return true;
    }
    bool TX::get(const Val & key, Val & value){
        lower_bound(main_cursor, key);
        const LeafPage * dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        Val tmp_value;
        bool same_key = item != dap->item_count && Val(dap->get_item_kv(my_db.page_size, item, tmp_value)) == key;
        if( !same_key )
            return false;
        value = tmp_value;
        return true;
    }
    bool TX::del(const Val & key, bool must_exist){
        lower_bound(main_cursor, key);
        const LeafPage * dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        Val tmp_value;
        bool same_key = item != dap->item_count && Val(dap->get_item_kv(my_db.page_size, item, tmp_value)) == key;
        if( !same_key )
            return false;
        LeafPage * wr_dap = (LeafPage *)make_pages_writable(main_cursor, main_cursor.path.size() - 1);
        wr_dap->remove_simple(my_db.page_size, main_cursor.path.back().second);
        merge_if_needed_leaf(main_cursor, wr_dap);
        return true;
    }

    void TX::commit(){
        if( my_db.mappings.empty() )
            return;
        // First sync all our possible writes. We did not modified meta pages, so we can safely msync them also
        for(size_t i = 0; i != my_db.mappings.size(); ++i){
            Mapping & ma = my_db.mappings[i];
            msync(ma.addr, (ma.end_page - ma.begin_page) * my_db.page_size, MS_SYNC);
        }
        // Now modify and sync our meta page
        MetaPage * wr_meta = my_db.writable_meta_page(meta_page_index);
        *wr_meta = meta_page;
        size_t low = meta_page_index * my_db.page_size;
        size_t high = (meta_page_index + 1) * my_db.page_size;
        low = ((low / my_db.physical_page_size)) * my_db.physical_page_size; // find lowest physical page
        high = ((high + my_db.physical_page_size - 1) / my_db.physical_page_size)  * my_db.physical_page_size; // find highest physical page
        msync(my_db.mappings.at(0).addr + low, high - low, MS_SYNC);
        // Now start new transaction
        start_transaction();
    }
    void TX::lower_bound(Cursor & cur, const Val & key){
        Pid pa = meta_page.main_root_page;
        size_t height = meta_page.main_height;
        cur.path.resize(height + 1);
        while(true){
            if( height == 0 ){
                const LeafPage * dap = readable_leaf(pa);
                PageOffset item = dap->lower_bound_item(my_db.page_size, key);
                cur.path[meta_page.main_height - height] = std::make_pair(pa, item);
                break;
            }
            const NodePage * nap = readable_node(pa);
            PageOffset nitem = nap->upper_bound_item(my_db.page_size, key) - 1;
            cur.path[meta_page.main_height - height] = std::make_pair(pa, nitem);
//            result.path.push_back(std::make_pair(pa, nitem));
            //ass(dap->item_count != 0, "lower_bound node contains zero keys");
            //if( off == dap->item_count )
            //    off -= 1;
            pa = nap->get_item_value(my_db.page_size, nitem);
            height -= 1;
        }
    }
    std::string TX::print_db(){
        Pid pa = meta_page.main_root_page;
        size_t height = meta_page.main_height;
        return print_db(pa, height);
    }
    std::string TX::print_db(Pid pid, size_t height){
        std::string result = "{\"keys\":[";
        if( height == 0 ){
            const LeafPage * dap = readable_leaf(pid);
            std::cout << "Leaf pid=" << pid << " [";
            for(int i = 0; i != dap->item_count; ++i){
                if( i != 0)
                    result += ",";
                Val value;
                Val key = dap->get_item_kv(my_db.page_size, i, value);
                std::cout << key.to_string() << ":" << value.to_string() << ", ";
                result += "\"" + key.to_string() + "\""; //  + ":" + value.to_string() +
            }
            std::cout << "]" << std::endl;
            return result + "]}";
        }
        const NodePage * nap = readable_node(pid);
        Pid spec_value = nap->get_item_value(my_db.page_size, -1);
        std::cout << "Node pid=" << pid << " [" << spec_value << ", ";
        for(int i = 0; i != nap->item_count; ++i){
            if( i != 0)
                result += ",";
            Pid value;
            Val key = nap->get_item_kv(my_db.page_size, i, value);
            std::cout << key.to_string() << ":" << value << ", ";
            result += "\"" + key.to_string() + ":" + std::to_string(value) + "\"";
        }
        result += "],\"children\":[";
        std::cout << "]" << std::endl;
        std::string spec_str = print_db(spec_value, height - 1);
        result += spec_str;
        for(int i = 0; i != nap->item_count; ++i){
            Pid value;
            Val key = nap->get_item_kv(my_db.page_size, i, value);
            std::string str = print_db(value, height - 1);
            result += "," + str;
        }
        return result + "]}";
    }

}
