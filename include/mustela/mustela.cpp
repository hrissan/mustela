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
            // TODO - check pages_count, if >5 then corrupted (truncated)
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
            LeafPtr mp(page_size, (LeafPage *)data_buf);
            mp.mpage()->pid = 3;
            mp.init_dirty(0);
            if( write(fd, data_buf, page_size) == -1)
                throw Exception("file write failed in create_db");
            mp.mpage()->pid = 4;
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
    
    TX::TX(DB & my_db):my_db(my_db), page_size(my_db.page_size) {
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
    CLeafPtr TX::readable_leaf(Pid pa){
        return CLeafPtr(page_size, (const LeafPage *)my_db.readable_page(pa));
    }
    CNodePtr TX::readable_node(Pid pa){
        return CNodePtr(page_size, (const NodePage *)my_db.readable_page(pa));
    }
    LeafPtr TX::writable_leaf(Pid pa){
        LeafPage * result = (LeafPage *)my_db.writable_page(pa);
        ass(result->tid == meta_page.tid, "writable_leaf is not from our transaction");
        return LeafPtr(page_size, result);
    }
    NodePtr TX::writable_node(Pid pa){
        NodePage * result = (NodePage *)my_db.writable_page(pa);
        ass(result->tid == meta_page.tid, "writable_node is not from our transaction");
        return NodePtr(page_size, result);
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
        memmove(wr_dap, dap, page_size);
        wr_dap->pid = path_el.first;
        wr_dap->tid = meta_page.tid;
        if(height == 0){ // node is root
            meta_page.main_root_page = path_el.first;
            return wr_dap;
        }
        NodePtr wr_parent(page_size, (NodePage *)make_pages_writable(cur, height - 1));
        wr_parent.set_value(cur.path.at(height - 1).second, path_el.first);
        return wr_dap;
    }
    void TX::insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid){
        if( height == size_t(-1)){ // making new root
            // TODO - check that cur is correctly updated
            NodePtr wr_root = writable_node(get_free_page(1));
            wr_root.init_dirty(meta_page.tid);
            wr_root.set_value(-1, meta_page.main_root_page);
            wr_root.append(key, new_pid);
            meta_page.main_root_page = wr_root.page->pid;
            meta_page.main_height += 1;
            return;
        }
        auto & path_el = cur.path.at(height);
        NodePtr wr_dap = writable_node(path_el.first);
        size_t required_size = wr_dap.get_item_size(key, new_pid);
        if( wr_dap.free_capacity() < required_size ){
            force_split_node(cur, height, key, new_pid);
//            Cursor cur2 = lower_bound(key);
//            if( cur.path != cur2.path )
//                std::cout << "cur != cur2" << std::endl;
            return;
        }
        wr_dap.insert_at(path_el.second, key, new_pid);
    }
    PageOffset TX::find_split_index(const CNodePtr & wr_dap, int * insert_direction, PageOffset insert_index, Val insert_key, Pid insert_page, size_t add_size_left, size_t add_size_right){
        size_t required_size = wr_dap.get_item_size(insert_key, insert_page);
        PageOffset best_split_index = -1;
        int best_disbalance = 0;
        size_t left_sigma_size = 0;
        for(PageOffset i = 0; i != wr_dap.size(); ++i){ // Split element cannot be last one if it is insert_key
            size_t split_item_size = wr_dap.get_item_size(i);
            size_t right_sigma_size = wr_dap.page->items_size - left_sigma_size - split_item_size; // do not count split item size
            bool enough_left = left_sigma_size + required_size + add_size_left <= wr_dap.capacity();
            bool enough_right = right_sigma_size + required_size + add_size_right <= wr_dap.capacity();//(wr_dap.size() - 1 - i, );
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
        NodePtr wr_dap = writable_node(path_el.first);
        ass(wr_dap.size() >= 3, "Node being split contains < 3 items");
        // Find split index so that either of split pages will fit new value and disbalance is minimal
        PageOffset insert_index = path_el.second;
        int insert_direction = 0;
        PageOffset split_index = find_split_index(wr_dap, &insert_direction, insert_index, insert_key, insert_page, 0, 0);
        PageOffset split_index_r = insert_direction == 0 ? split_index : split_index + 1;
        //ass(split_index >= 0 && split_index < wr_dap->item_count, "Could not find split index for node");
        
        NodePtr wr_right = writable_node(get_free_page(1));
        wr_right.init_dirty(meta_page.tid);
        wr_right.append_range(wr_dap, split_index_r, wr_dap.size());
        NodePtr my_copy = push_tmp_copy(wr_dap.page);
        wr_dap.clear();
        wr_dap.set_value(-1, my_copy.get_value(-1));
        wr_dap.append_range(my_copy, 0, split_index);
        //Cursor cur2 = cur;
        if( height != 0 )
            cur.path.at(height - 1).second += 1;
        NodePage * result_page = nullptr;
        if( insert_direction == -1 ) {
            auto split = my_copy.get_kv(split_index);
            wr_right.set_value(-1, split.pid);
            //result_page = wr_dap;
            wr_dap.insert_at(insert_index, insert_key, insert_page);
            insert_pages_to_node(cur, height - 1, split.key, wr_right.page->pid);
        }else if( insert_direction == 1 ) {
            path_el.first = wr_right.page->pid;
            path_el.second -= split_index_r;
            auto split = my_copy.get_kv(split_index);
            wr_right.set_value(-1, split.pid);
            //result_page = wr_right;
            wr_right.insert_at(insert_index - split_index_r, insert_key, insert_page);
            insert_pages_to_node(cur, height - 1, split.key, wr_right.page->pid);
        }else{
            path_el.first = wr_right.page->pid;
            path_el.second -= split_index_r;
            wr_right.set_value(-1, insert_page);
            //result_page = wr_right; // wr_parent :)
            insert_pages_to_node(cur, height - 1, insert_key, wr_right.page->pid);
        }
        return result_page;
    }
    LeafPage * TX::force_split_leaf(Cursor & cur, size_t height, Val insert_key, Val insert_val){
        auto & path_el = cur.path.at(height);
        LeafPtr wr_dap = writable_leaf(path_el.first);
        size_t required_size = wr_dap.get_item_size(insert_key, insert_val);
        // Find split index so that either of split pages will fit new value and disbalance is minimal
        PageOffset insert_index = path_el.second;
        PageOffset best_split_index = -1;
        int insert_direction = 0;
        int best_disbalance = 0;
        size_t left_sigma_size = 0;
        for(PageOffset i = 0; i != wr_dap.size() + 1; ++i){ // We can split at the end too
            size_t right_sigma_size = wr_dap.data_size() - left_sigma_size;
            bool enough_left = left_sigma_size + required_size <= wr_dap.capacity();
            bool enough_right = right_sigma_size + required_size <= wr_dap.capacity();
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
            if( i != wr_dap.size()) // last iteration has no element
                left_sigma_size += wr_dap.get_item_size(i);
        }
        PageOffset split_index = best_split_index;
        if( split_index == PageOffset(-1) ){ // Perform 3 - split
            split_index = insert_index;
            insert_direction = 0;
        }
        LeafPtr wr_right = writable_leaf(get_free_page(1));
        wr_right.init_dirty(meta_page.tid);
        wr_right.append_range(wr_dap, split_index, wr_dap.size());
        LeafPtr my_copy = push_tmp_copy(wr_dap.page);
        wr_dap.clear();
        wr_dap.append_range(my_copy, 0, split_index);
        LeafPage * result_page = nullptr;
        if( path_el.first == wr_dap.page->pid ){
            if( path_el.second >= split_index ){
                path_el.first = wr_right.page->pid;
                path_el.second -= split_index;
            }
        }
//        Cursor cur2 = cur;
        if( height != 0 )
            cur.path.at(height - 1).second += 1;
        if( insert_direction == -1 ) {
//            path_el.second = wr_dap->item_count;
            result_page = wr_dap.mpage();
            wr_dap.insert_at(insert_index, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right.get_key(0)), wr_right.page->pid);
        }else if( insert_direction == 1 ){
//            path_el.first = right_pid;
//            path_el.second = 0;
            result_page = wr_right.mpage();
            wr_right.insert_at(insert_index - split_index, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right.get_key(0)), wr_right.page->pid);
        }else {
            LeafPtr wr_middle = writable_leaf(get_free_page(1));
            wr_middle.init_dirty(meta_page.tid);
            //path_el.first = middle_pid;
            //path_el.second = 0;
            result_page = wr_middle.mpage();
            wr_middle.insert_at(0, insert_key, insert_val);
            insert_pages_to_node(cur, height - 1, Val(wr_right.get_key(0)), wr_right.page->pid);
            insert_pages_to_node(cur, height - 1, insert_key, wr_middle.page->pid);
        }
        return result_page;
    }
    void TX::merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap){
        if( wr_dap.data_size() >= wr_dap.capacity()/2 )
            return;
        if(height == 0){ // merging root
            if( wr_dap.size() != 0) // wait until 1 key remains, make it new root
                return;
            mark_free_in_future_page(meta_page.main_root_page);
            meta_page.main_root_page = wr_dap.get_value(-1);
            meta_page.main_height -= 1;
            cur.path.erase(cur.path.begin()); // TODO - check
            return;
        }
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path[height - 1];
        NodePtr wr_parent = writable_node(path_pa.first);
        ass(wr_parent.size() > 0, "Found parent node with 0 items");
        CNodePtr left_sib;
        bool use_left_sib = false;
        PageOffset left_data_size = 0;
        ValPid my_kv;
        size_t required_size_for_my_kv = 0;
        
        CNodePtr right_sib;
        bool use_right_sib = false;
        PageOffset right_data_size = 0;
        ValPid right_kv;
        if( path_pa.second != PageOffset(-1)){
            my_kv = wr_parent.get_kv(path_pa.second);
            required_size_for_my_kv = wr_parent.get_item_size(my_kv.key, my_kv.pid);
            ass(my_kv.pid == wr_dap.page->pid, "merge_if_needed_node my pid in parent does not match");
            Pid left_pid = wr_parent.get_value(path_pa.second - 1);
            left_sib = readable_node(left_pid);
            left_data_size = left_sib.data_size();
            left_data_size += left_sib.get_item_size(my_kv.key, 0); // will need to insert key from parent. Achtung - 0 works only when fixed-size pids are used
            use_left_sib = left_data_size <= wr_dap.free_capacity();
        }
        if(path_pa.second == PageOffset(-1) || path_pa.second < wr_parent.size() - 1){
            right_kv = wr_parent.get_kv(path_pa.second + 1);
            right_sib = readable_node(right_kv.pid);
            right_data_size = right_sib.data_size();
            right_data_size += right_sib.get_item_size(right_kv.key, 0); // will need to insert key from parent! Achtung - 0 works only when fixed-size pids are used
            use_right_sib = right_data_size <= wr_dap.free_capacity();
        }
        if( use_left_sib && use_right_sib && wr_dap.free_capacity() < left_data_size + right_data_size ){ // If cannot merge both, select smallest
            if( left_data_size < right_data_size ) // <= will also work
                use_right_sib = false;
            else
                use_left_sib = false;
        }
        if( wr_dap.size() == 0 && !use_left_sib && !use_right_sib) { // Cannot merge, siblings are full and do not fit key from parent, so we borrow!
            if( left_sib.page && right_sib.page){ // Select larger one
                if( left_sib.data_size() > right_sib.data_size() )
                    use_left_sib = true;
                else
                    use_right_sib = true;
            }else if( left_sib.page )
                use_left_sib = true;
            else if( right_sib.page )
                use_right_sib = true;
            ass(use_left_sib || use_right_sib, "Node is empty and cannot borrow from siblings");
/*            if( remember_left_sib ){
                Pid spec_val = wr_dap->get_item_value(page_size, -1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(remember_left_sib, &insert_direction, remember_left_sib->item_count, my_key, spec_val, 0, 0);
                if( split_index == 0 )
                    split_index = 1;
                ass(insert_direction != 0 && split_index < remember_left_sib->item_count, "Split index to the right");
                Pid split_val;
                Val split_key = remember_left_sib->get_item_kv(page_size, split_index, split_val);
                for(PageOffset i = split_index + 1; i != remember_left_sib->item_count; ++i){
                    Pid val;
                    Val key = remember_left_sib->get_item_kv(page_size, i, val);
                    wr_dap->insert_at(page_size, i - split_index - 1, key, val);
                }
                wr_dap->insert_at(page_size, wr_dap->item_count, my_key, spec_val);
                wr_dap->set_item_value(page_size, -1, split_val);
                wr_parent->remove_simple(page_size, path_pa.second);
                // TODO split parent if needed
                wr_parent->insert_at(page_size, path_pa.second, split_key, wr_dap->pid);
            }
            if( remember_right_sib ){
                Pid spec_val = remember_right_sib->get_item_value(page_size, -1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(remember_right_sib, &insert_direction, 0, right_key, spec_val, 0, 0);
                if( split_index == remember_right_sib->item_count - 1 )
                    split_index -= 1;
                ass(insert_direction != 0 && split_index > 0, "Split index to the right");
                Pid split_val;
                Val split_key = remember_right_sib->get_item_kv(page_size, split_index, split_val);
                wr_dap->insert_at(page_size, 0, right_key, spec_val);
                for(PageOffset i = 0; i != split_index; ++i){
                    Pid val;
                    Val key = remember_right_sib->get_item_kv(page_size, i, val);
                    wr_dap->insert_at(page_size, wr_dap->item_count, key, val);
                }
                remember_right_sib->set_item_value(page_size, -1, split_val);
                wr_parent->remove_simple(page_size, path_pa.second + 1);
                // TODO split parent if needed
                wr_parent->insert_at(page_size, path_pa.second + 1, split_key, wr_dap->pid);
            }*/
        }
        if( use_left_sib ){
            Pid spec_val = wr_dap.get_value(-1);
            wr_dap.insert_at(0, my_kv.key, spec_val);
            wr_dap.set_value(-1, left_sib.get_value(-1));
            wr_dap.insert_range(0, left_sib, 0, left_sib.size());
            mark_free_in_future_page(left_sib.page->pid); // unlink left, point its slot in parent to us, remove our slot in parent
            wr_parent.erase(path_pa.second);
            wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( use_right_sib ){
            Pid spec_val = right_sib.get_value(-1);
            wr_dap.append(right_kv.key, spec_val);
            wr_dap.append_range(right_sib, 0, right_sib.size());
            mark_free_in_future_page(right_sib.page->pid); // unlink right, remove its slot in parent
            wr_parent.erase(path_pa.second + 1);
        }
        merge_if_needed_node(cur, height - 1, wr_parent);
    }
    void TX::merge_if_needed_leaf(Cursor & cur, LeafPtr wr_dap){
        if( wr_dap.data_size() >= wr_dap.capacity()/2 )
            return;
        if(cur.path.size() <= 1) // root is leaf, cannot merge anyway
            return;
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path[cur.path.size() - 2];
        NodePtr wr_parent = writable_node(path_pa.first);
        ass(wr_parent.size() > 0, "Found parent node with 0 items");
        //PageOffset required_size = wr_dap->items_size + wr_dap->item_count * sizeof(PageOffset);
        CLeafPtr left_sib;
        CLeafPtr right_sib;
        if( path_pa.second != PageOffset(-1)){
            Pid left_pid = wr_parent.get_value(path_pa.second - 1);
            left_sib = readable_leaf(left_pid);
            if( wr_dap.capacity() < left_sib.data_size() )
                left_sib = CLeafPtr(); // forget about left!
        }
        if( path_pa.second < wr_parent.size() - 1){
            Pid right_pid = wr_parent.get_value(path_pa.second + 1);
            right_sib = readable_leaf(right_pid);
            if( wr_dap.capacity() < right_sib.data_size() )
                right_sib = CLeafPtr(); // forget about right!
        }
        if( left_sib.page && right_sib.page && wr_dap.capacity() < left_sib.data_size() + right_sib.data_size() ){ // If cannot merge both, select smallest
            if( left_sib.data_size() < right_sib.data_size() ) // <= will also work
                right_sib = CLeafPtr();
            else
                left_sib = CLeafPtr();
        }
        if( left_sib.page ){
            wr_dap.insert_range(0, left_sib, 0, left_sib.size());
            mark_free_in_future_page(left_sib.page->pid); // unlink left, point its slot in parent to us, remove our slot in parent
            wr_parent.erase(path_pa.second);
            wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( right_sib.page ){
            wr_dap.append_range(right_sib, 0, right_sib.size());
            mark_free_in_future_page(right_sib.page->pid); // unlink right, remove its slot in parent
            wr_parent.erase(path_pa.second + 1);
        }
        if( left_sib.page || right_sib.page )
            merge_if_needed_node(cur, cur.path.size() - 2, wr_parent);
    }

    bool TX::put(const Val & key, const Val & value, bool nooverwrite){
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
        if( same_key && nooverwrite )
            return false;
        // TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
        LeafPtr wr_dap(page_size, (LeafPage *)make_pages_writable(main_cursor, main_cursor.path.size() - 1));
        if( same_key )
            wr_dap.erase(main_cursor.path.back().second);
        size_t required_size = wr_dap.get_item_size(key, value);
        if( required_size <= wr_dap.free_capacity() )
            wr_dap.insert_at(main_cursor.path.back().second, key, value);
        else
            force_split_leaf(main_cursor, main_cursor.path.size() - 1, key, value);
        clear_tmp_copies();
        return true;
    }
    bool TX::get(const Val & key, Val & value){
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        if( item == dap.size() )
            return false;
        auto kv = dap.get_kv(item);
        if( kv.key != key )
            return false;
        value = kv.value;
        return true;
    }
    bool TX::del(const Val & key, bool must_exist){
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.back().first);
        PageOffset item = main_cursor.path.back().second;
        bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
        if( !same_key )
            return !must_exist;
        LeafPtr wr_dap(page_size, (LeafPage *)make_pages_writable(main_cursor, main_cursor.path.size() - 1));
        wr_dap.erase(main_cursor.path.back().second);
        merge_if_needed_leaf(main_cursor, wr_dap);
        clear_tmp_copies();
        return true;
    }

    void TX::commit(){
        if( my_db.mappings.empty() )
            return;
        // First sync all our possible writes. We did not modified meta pages, so we can safely msync them also
        for(size_t i = 0; i != my_db.mappings.size(); ++i){
            Mapping & ma = my_db.mappings[i];
            msync(ma.addr, (ma.end_page - ma.begin_page) * page_size, MS_SYNC);
        }
        // Now modify and sync our meta page
        MetaPage * wr_meta = my_db.writable_meta_page(meta_page_index);
        *wr_meta = meta_page;
        size_t low = meta_page_index * page_size;
        size_t high = (meta_page_index + 1) * page_size;
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
                CLeafPtr dap = readable_leaf(pa);
                PageOffset item = dap.lower_bound_item(key);
                cur.path[meta_page.main_height - height] = std::make_pair(pa, item);
                break;
            }
            CNodePtr nap = readable_node(pa);
            PageOffset nitem = nap.upper_bound_item(key) - 1;
            cur.path[meta_page.main_height - height] = std::make_pair(pa, nitem);
//            result.path.push_back(std::make_pair(pa, nitem));
            //ass(dap->item_count != 0, "lower_bound node contains zero keys");
            //if( off == dap->item_count )
            //    off -= 1;
            pa = nap.get_value(nitem);
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
            CLeafPtr dap = readable_leaf(pid);
            std::cout << "Leaf pid=" << pid << " [";
            for(int i = 0; i != dap.size(); ++i){
                if( i != 0)
                    result += ",";
                auto kv = dap.get_kv(i);
                std::cout << kv.key.to_string() << ":" << kv.value.to_string() << ", ";
                result += "\"" + kv.key.to_string() + "\""; //  + ":" + value.to_string() +
            }
            std::cout << "]" << std::endl;
            return result + "]}";
        }
        CNodePtr nap = readable_node(pid);
        Pid spec_value = nap.get_value(-1);
        std::cout << "Node pid=" << pid << " [" << spec_value << ", ";
        for(int i = 0; i != nap.size(); ++i){
            if( i != 0)
                result += ",";
            ValPid va = nap.get_kv(i);
            std::cout << va.key.to_string() << ":" << va.pid << ", ";
            result += "\"" + va.key.to_string() + ":" + std::to_string(va.pid) + "\"";
        }
        result += "],\"children\":[";
        std::cout << "]" << std::endl;
        std::string spec_str = print_db(spec_value, height - 1);
        result += spec_str;
        for(int i = 0; i != nap.size(); ++i){
            ValPid va = nap.get_kv(i);
            std::string str = print_db(va.pid, height - 1);
            result += "," + str;
        }
        return result + "]}";
    }

}
