#include "tx.hpp"
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
    
    Cursor::Cursor(TX & my_txn, TableDesc & table):my_txn(my_txn), table(table){
        my_txn.my_cursors.insert(this);
    }
    Cursor::~Cursor(){
        my_txn.my_cursors.erase(this);
    }
    Cursor::Cursor(Cursor && other):my_txn(other.my_txn), table(other.table){
        my_txn.my_cursors.insert(this);
        path = std::move(other.path);
    }
/*    Cursor & Cursor::operator=(Cursor && other){
        my_txn.my_cursors.erase(this);
        my_txn = other.my_txn;
        path = other.path;
        my_txn.my_cursors.insert(this);
        return *this;
    }*/

    TX::TX(DB & my_db):my_db(my_db), page_size(my_db.page_size) {
        start_transaction();
    }
    void TX::start_transaction(){
        meta_page_index = my_db.oldest_meta_page_index(); // Overwrite oldest page
        auto newest_page_index = my_db.last_meta_page_index(); // by newest
        oldest_reader_tid = my_db.oldest_meta_page_index(); // TODO ask from reader-writer lock
        meta_page = *(my_db.readable_meta_page(newest_page_index));
        meta_page.tid += 1;
        meta_page.tid2 = meta_page.tid;
    }

    TX::~TX(){
        // rollback is nop
        ass( my_cursors.empty(), "All cursors should be destroyed before transaction they are in"); // potential throw in destructor ahaha
    }
    CLeafPtr TX::readable_leaf(Pid pa){
        return CLeafPtr(page_size, (const LeafPage *)my_db.readable_page(pa));
    }
    CNodePtr TX::readable_node(Pid pa){
        return CNodePtr(page_size, (const NodePage *)my_db.readable_page(pa));
    }
    LeafPtr TX::writable_leaf(Pid pa){
        LeafPage * result = (LeafPage *)my_db.writable_page(pa, 1);
        ass(result->tid == meta_page.tid, "writable_leaf is not from our transaction");
        return LeafPtr(page_size, result);
    }
    NodePtr TX::writable_node(Pid pa){
        NodePage * result = (NodePage *)my_db.writable_page(pa, 1);
        ass(result->tid == meta_page.tid, "writable_node is not from our transaction");
        return NodePtr(page_size, result);
    }
    const char * TX::readable_overflow(Pid pa){
        return (const char *)my_db.readable_page(pa);
    }
    char * TX::writable_overflow(Pid pa, Pid count){
        return (char *)my_db.writable_page(pa, count);
    }
    void TX::mark_free_in_future_page(Pid page, Pid contigous_count){
        free_list.mark_free_in_future_page(page, contigous_count);
        // NOP for now
    }
    Pid TX::get_free_page(Pid contigous_count){
        Pid pa = free_list.get_free_page(*this, contigous_count, oldest_reader_tid);
        if( pa )
            return pa;
        my_db.grow_mappings(meta_page.page_count + contigous_count);
        pa = meta_page.page_count;
        meta_page.page_count += contigous_count;
        DataPage * new_pa = my_db.writable_page(pa, contigous_count);
        new_pa->pid = pa;
        new_pa->tid = meta_page.tid;
        return pa;
    }
    DataPage * TX::make_pages_writable(Cursor & cur, size_t height){
        //const bool is_leaf = (height == cur.path.size() - 1);
        Pid old_page = cur.path.at(height).first;
        const DataPage * dap = my_db.readable_page(old_page);
        if( dap->tid == meta_page.tid ){ // Reached already writable page
            DataPage * wr_dap = my_db.writable_page(old_page, 1);
            return wr_dap;
        }
        mark_free_in_future_page(old_page, 1);
        Pid new_page = get_free_page(1);
        for(auto && c : my_cursors)
            if( c->path.at(height).first == old_page )
                c->path.at(height).first = new_page;
        DataPage * wr_dap = my_db.writable_page(new_page, 1);
        memcpy(wr_dap, dap, page_size);
        wr_dap->pid = new_page;
        wr_dap->tid = meta_page.tid;
        if(height == cur.table.height){ // node is root
            cur.table.root_page = new_page;
            return wr_dap;
        }
        NodePtr wr_parent(page_size, (NodePage *)make_pages_writable(cur, height + 1));
        wr_parent.set_value(cur.path.at(height + 1).second, new_page);
        return wr_dap;
    }
    Cursor TX::insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid){
        if( height == cur.table.height + 1){ // making new root
            NodePtr wr_root = writable_node(get_free_page(1));
            cur.table.node_page_count += 1;
            wr_root.init_dirty(meta_page.tid);
            Pid previous_root = cur.table.root_page;
            wr_root.set_value(-1, previous_root);
            wr_root.append(key, new_pid);
            cur.table.root_page = wr_root.page->pid;
            cur.table.height += 1;
            for(auto && c : my_cursors)
                c->path.push_back(std::make_pair(cur.table.root_page, -1) );
//                ass(c->path.at[0].first == new_pid || c->path.at[0].first == previous_root, "Increasing height, but stray cursor found");
//                bool was_left = c->path.at[0].first == previous_root;
            Cursor cur2(*this, cur.table);
            cur2.path = cur.path;//.resize(meta_page.main_height, std::make_pair(-1, -1));
//            cur2.path[meta_page.main_height - 1] = std::make_pair(new_pid, 0);
            cur2.path.at(cur.table.height) = std::make_pair(cur.table.root_page, 0);
            return cur2;
        }
        auto path_el = cur.path.at(height);
        NodePtr wr_dap = writable_node(path_el.first);
        size_t required_size = wr_dap.get_item_size(key, new_pid);
        if( wr_dap.free_capacity() < required_size ){
            Cursor cur3 = force_split_node(cur, height, key, new_pid);
            //cur3.path[height] = std::make_pair(new_pid, 0);
            return cur3;
        }
        wr_dap.insert_at(path_el.second + 1, key, new_pid);
        for(auto && c : my_cursors)
            c->on_insert(height, path_el.first, path_el.second + 1);
        Cursor cur2(*this, cur.table);
        cur2.path = cur.path;//.insert(cur2.path.end(), cur.path.begin(), cur.path.begin() + height + 1);
        cur2.path.at(height).second += 1;// = std::make_pair(new_pid, 0);
        return cur2;
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
    Cursor TX::force_split_node(Cursor & cur, size_t height, Val insert_key, Pid insert_page){
        auto path_el = cur.path.at(height);
        NodePtr wr_dap = writable_node(path_el.first);
        ass(wr_dap.size() >= 3, "Node being split contains < 3 items");
        // Find split index so that either of split pages will fit new value and disbalance is minimal
        PageOffset insert_index = path_el.second + 1;
        int insert_direction = 0;
        PageOffset split_index = find_split_index(wr_dap, &insert_direction, insert_index, insert_key, insert_page, 0, 0);
        PageOffset split_index_r = insert_direction == 0 ? split_index : split_index + 1;
        //ass(split_index >= 0 && split_index < wr_dap->item_count, "Could not find split index for node");
        
        NodePtr wr_right = writable_node(get_free_page(1));
        cur.table.node_page_count += 1;
        wr_right.init_dirty(meta_page.tid);
        wr_right.append_range(wr_dap, split_index_r, wr_dap.size());
        NodePtr my_copy = push_tmp_copy(wr_dap.page);
        ValPid split;
        if( insert_direction != 0)
            split = my_copy.get_kv(split_index);
        Cursor cur2 = insert_pages_to_node(cur, height + 1, insert_direction != 0 ? split.key : insert_key, wr_right.page->pid);
        for(auto && c : my_cursors)
            c->on_page_split(height, path_el.first, split_index, split_index_r, cur2);
        wr_dap.clear();
        wr_dap.set_value(-1, my_copy.get_value(-1));
        wr_dap.append_range(my_copy, 0, split_index);
        if( insert_direction == -1 ) {
            wr_right.set_value(-1, split.pid);
            wr_dap.insert_at(insert_index, insert_key, insert_page);
            for(auto && c : my_cursors)
                c->on_insert(height, path_el.first, insert_index);
//            cur2.path.assign(cur.path.begin(), cur.path.begin() + height + 1);
            cur2.path.at(height) = std::make_pair(path_el.first, insert_index);
            return cur2;
        }
        if( insert_direction == 1 ) {
            wr_right.set_value(-1, split.pid);
            wr_right.insert_at(insert_index - split_index_r, insert_key, insert_page);
            for(auto && c : my_cursors)
                c->on_insert(height, wr_right.page->pid, insert_index - split_index_r);
            cur2.path.at(height) = std::make_pair(wr_right.page->pid, insert_index - split_index_r);
            return cur2;
        }
        wr_right.set_value(-1, insert_page);
        cur2.path.at(height) = std::make_pair(insert_page, -1);
        return cur2;
//        insert_pages_to_node(cur, height - 1, insert_key, wr_right.page->pid);
    }
    char * TX::force_split_leaf(Cursor & cur, Val insert_key, size_t insert_val_size){
        auto path_el = cur.path.at(0);
        LeafPtr wr_dap = writable_leaf(path_el.first);
        bool overflow;
        size_t required_size = wr_dap.get_item_size(insert_key, insert_val_size, overflow);
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
        cur.table.leaf_page_count += 1;
        wr_right.init_dirty(meta_page.tid);
        wr_right.append_range(wr_dap, split_index, wr_dap.size());
        Val right_key0 = (insert_direction == 1 && insert_index == split_index) ? insert_key : wr_right.get_key(0);
        Cursor cur2 = insert_pages_to_node(cur, 1, right_key0, wr_right.page->pid);
        for(auto && c : my_cursors)
            c->on_page_split(0, path_el.first, split_index, split_index, cur2);
        LeafPtr my_copy = push_tmp_copy(wr_dap.page);
        wr_dap.clear();
        wr_dap.append_range(my_copy, 0, split_index);
        char * result = nullptr;
        if( insert_direction == -1 ) {
            result = wr_dap.insert_at(insert_index, insert_key, insert_val_size, overflow);
            for(auto && c : my_cursors)
                c->on_insert(0, path_el.first, insert_index);
        }else if( insert_direction == 1 ){
            result = wr_right.insert_at(insert_index - split_index, insert_key, insert_val_size, overflow);
            for(auto && c : my_cursors)
                c->on_insert(0, wr_right.page->pid, insert_index - split_index);
        }else {
            LeafPtr wr_middle = writable_leaf(get_free_page(1));
            cur.table.leaf_page_count += 1;
            wr_middle.init_dirty(meta_page.tid);
            result = wr_middle.insert_at(0, insert_key, insert_val_size, overflow);
            insert_pages_to_node(cur, 1, insert_key, wr_middle.page->pid);
        }
        return result;
    }
    void TX::merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap){
        if( wr_dap.data_size() >= wr_dap.capacity()/2 )
            return;
        if(height == cur.table.height){ // merging root
            if( wr_dap.size() != 0) // wait until 1 key remains, make it new root
                return;
            mark_free_in_future_page(cur.table.root_page, 1);
            cur.table.node_page_count -= 1;
            cur.table.root_page = wr_dap.get_value(-1);
            cur.table.height -= 1;
            for(auto && c : my_cursors)
                c->path.pop_back();
            return;
        }
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path.at(height + 1);
        NodePtr wr_parent = writable_node(path_pa.first);
        //ass(wr_parent.size() > 0, "Found parent node with 0 items");
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
        if(PageOffset(path_pa.second + 1) < wr_parent.size()){
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
//            std::cout << "Node with 0 items and cannot merge :)" << std::endl;
/*            if( left_sib.page && right_sib.page){ // Select larger one
                if( left_sib.page->tid == meta_page.tid && right_sib.page->tid == meta_page.tid ){
                    if( left_sib.data_size() > right_sib.data_size() ) // both writable and left > right
                        use_left_sib = true;
                    else
                        use_right_sib = true;
                }else
                    if( left_sib.page->tid == meta_page.tid ) // left writable
                        use_left_sib = true;
                    else
                        use_right_sib = true;
            }else if( left_sib.page )
                use_left_sib = true;
            else if( right_sib.page )
                use_right_sib = true;
            ass(use_left_sib || use_right_sib, "Node is empty and cannot borrow from siblings");
            if( use_left_sib ){
                NodePtr wr_left;
                if( left_sib.page->tid == meta_page.tid ) // already writable
                    wr_left = writable_node(left_sib.page->pid);
                else{ // Not writable yet
                    Cursor left_cur = cur;
                    left_cur.path[height - 1].second -= 1;
                    left_cur.truncate(height);
                    wr_left = NodePtr(page_size, (NodePage *)make_pages_writable(left_cur, left_cur.path.size() - 1)); // TODO - check correctness
                }
                Pid spec_val = wr_dap.get_value(-1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(wr_left, &insert_direction, wr_left.size(), my_kv.key, spec_val, 0, 0);
                if( split_index == 0 )
                    split_index = 1;
                ass(insert_direction != 0 && split_index < wr_left.size(), "Split index to the right");
                auto split_kv = wr_left.get_kv(split_index);
                wr_dap.append_range(wr_left, split_index + 1, wr_left.size());
                wr_dap.append(my_kv.key, spec_val);
                wr_dap.set_value(-1, split_kv.pid);
                wr_parent.erase(path_pa.second);
                insert_pages_to_node(cur, height - 1, split_kv.key, wr_dap.page->pid);
                wr_left.erase(split_index, wr_left.size()); // will invalidate split_kv pointed into wr_left
            }
            if( use_right_sib ){
                NodePtr wr_right;
                if( right_sib.page->tid == meta_page.tid ) // already writable
                    wr_right = writable_node(right_sib.page->pid);
                else{ // Not writable yet
                    Cursor right_cur = cur;
                    right_cur.path[height - 1].second += 1;
                    right_cur.truncate(height);
                    wr_right = NodePtr(page_size, (NodePage *)make_pages_writable(right_cur, right_cur.path.size() - 1)); // TODO - check correctness
                }
                Pid spec_val = wr_right.get_value(-1);
                int insert_direction = 0;
                PageOffset split_index = find_split_index(wr_right, &insert_direction, 0, right_kv.key, spec_val, 0, 0);
                if( split_index == wr_right.size() - 1 ) // TODO check
                    split_index -= 1;
                ass(insert_direction != 0 && split_index > 0, "Split index to the right");
                ValPid split_kv = wr_right.get_kv(split_index);
                wr_dap.append(right_kv.key, spec_val);
                wr_dap.append_range(wr_right, 0, split_index);
                wr_right.set_value(-1, split_kv.pid);
                wr_parent.erase(path_pa.second + 1);
                cur.path[height - 1].second += 1;
                insert_pages_to_node(cur, height - 1, split_kv.key, wr_right.page->pid);
                wr_right.erase(0, split_index + 1);
            }
            merge_if_needed_node(cur, height - 1, wr_parent);
            return;*/
        }
        if( use_left_sib ){
            Pid spec_val = wr_dap.get_value(-1);
            wr_dap.insert_at(0, my_kv.key, spec_val);
            wr_dap.set_value(-1, left_sib.get_value(-1));
            wr_dap.insert_range(0, left_sib, 0, left_sib.size());
            mark_free_in_future_page(left_sib.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
            cur.table.node_page_count -= 1;
            wr_parent.erase(path_pa.second);
            wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( use_right_sib ){
            Pid spec_val = right_sib.get_value(-1);
            wr_dap.append(right_kv.key, spec_val);
            wr_dap.append_range(right_sib, 0, right_sib.size());
            mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
            cur.table.node_page_count -= 1;
            wr_parent.erase(path_pa.second + 1);
        }
        merge_if_needed_node(cur, height + 1, wr_parent);
    }
    void TX::prune_empty_node(Cursor & cur, size_t height, NodePtr wr_dap){
        mark_free_in_future_page(wr_dap.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
        cur.table.node_page_count -= 1;
        auto & path_el = cur.path.at(height);
        auto & path_pa = cur.path.at(height + 1);
        NodePtr wr_parent = writable_node(path_pa.first);
        if( wr_parent.size() == 0){
            prune_empty_node(cur, height + 1, wr_parent);
            return;
        }
        if( path_pa.second == PageOffset(-1) ){
            ValPid zero_val = wr_parent.get_kv(0);
            wr_parent.erase(0);
            wr_parent.set_value(-1, zero_val.pid);
        }else{
            wr_parent.erase(path_pa.second);
        }
        merge_if_needed_node(cur, height + 1, wr_parent);
    }
    void TX::merge_if_needed_leaf(Cursor & cur, LeafPtr wr_dap){
        if( wr_dap.data_size() >= wr_dap.capacity()/2 )
            return;
        if(cur.path.size() <= 1) // root is leaf, cannot merge anyway
            return;
        // TODO - redistribute value between siblings instead of outright merging
        auto & path_pa = cur.path.at(1);
        NodePtr wr_parent = writable_node(path_pa.first);
        int c = 0;
        if( wr_dap.size() == 0)
            c = 1;
        if( wr_dap.size() == 0 && wr_parent.size() == 0 ){ // Prune empty leaf + nodes
            mark_free_in_future_page(wr_dap.page->pid, 1);
            cur.table.leaf_page_count -= 1;
            prune_empty_node(cur, 1, wr_parent);
            return;
        }
        //ass(wr_parent.size() > 0, "Found parent node with 0 items");
        //PageOffset required_size = wr_dap->items_size + wr_dap->item_count * sizeof(PageOffset);
        CLeafPtr left_sib;
        CLeafPtr right_sib;
        if( path_pa.second != PageOffset(-1)){
            Pid left_pid = wr_parent.get_value(path_pa.second - 1);
            left_sib = readable_leaf(left_pid);
            if( wr_dap.free_capacity() < left_sib.data_size() )
                left_sib = CLeafPtr(); // forget about left!
        }
        if( PageOffset(path_pa.second + 1) < wr_parent.size()){
            Pid right_pid = wr_parent.get_value(path_pa.second + 1);
            right_sib = readable_leaf(right_pid);
            if( wr_dap.free_capacity() < right_sib.data_size() )
                right_sib = CLeafPtr(); // forget about right!
        }
        if( left_sib.page && right_sib.page && wr_dap.free_capacity() < left_sib.data_size() + right_sib.data_size() ){ // If cannot merge both, select smallest
            if( left_sib.data_size() < right_sib.data_size() ) // <= will also work
                right_sib = CLeafPtr();
            else
                left_sib = CLeafPtr();
        }
        if( wr_dap.size() == 0){
            ass(left_sib.page || right_sib.page, "Cannot merge leaf with 0 items" );
//            std::cout << "We could optimize by unlinking our leaf" << std::endl;
        }
        if( left_sib.page ){
            wr_dap.insert_range(0, left_sib, 0, left_sib.size());
            mark_free_in_future_page(left_sib.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
            cur.table.leaf_page_count -= 1;
            wr_parent.erase(path_pa.second);
            wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
            path_pa.second -= 1; // fix cursor
        }
        if( right_sib.page ){
            wr_dap.append_range(right_sib, 0, right_sib.size());
            mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
            cur.table.leaf_page_count -= 1;
            wr_parent.erase(path_pa.second + 1);
        }
        if( left_sib.page || right_sib.page )
            merge_if_needed_node(cur, 1, wr_parent);
    }

    char * TX::put(TableDesc & table, const Val & key, size_t value_size, bool nooverwrite){
        Cursor main_cursor(*this, table);
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.at(0).first);
        PageOffset item = main_cursor.path.at(0).second;
        bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
        if( same_key && nooverwrite )
            return nullptr;
        meta_page.dirty = true;
        // TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
        LeafPtr wr_dap(page_size, (LeafPage *)make_pages_writable(main_cursor, 0));
        if( same_key ){
            Pid overflow_page, overflow_count;
            wr_dap.erase(main_cursor.path.at(0).second, overflow_page, overflow_count);
            if( overflow_page ){
                table.overflow_page_count -= overflow_count;
                mark_free_in_future_page(overflow_page, overflow_count);
            }
        }
        bool overflow;
        size_t required_size = wr_dap.get_item_size(key, value_size, overflow);
        char * result = nullptr;
        if( required_size <= wr_dap.free_capacity() )
            result = wr_dap.insert_at(main_cursor.path.at(0).second, key, value_size, overflow);
        else
            result = force_split_leaf(main_cursor, key, value_size);
        if( overflow ){
            Pid overflow_count = (value_size + page_size - 1)/page_size;
            Pid opa = get_free_page(overflow_count);
            table.overflow_page_count += overflow_count;
            pack_uint_be(result, NODE_PID_SIZE, opa);
            result = writable_overflow(opa, overflow_count);
        }
        if( !same_key )
            table.count += 1;
        clear_tmp_copies();
        return result;
    }
    bool TX::get(TableDesc & table, const Val & key, Val & value){
        Cursor main_cursor(*this, table);
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.at(0).first);
        PageOffset item = main_cursor.path.at(0).second;
        if( item == dap.size() )
            return false;
        Pid overflow_page;
        auto kv = dap.get_kv(item, overflow_page);
        if( kv.key != key )
            return false;
        if( overflow_page )
            kv.value.data = readable_overflow(overflow_page);
        value = kv.value;
        return true;
    }
    bool TX::del(TableDesc & table, const Val & key, bool must_exist){
        Cursor main_cursor(*this, table);
        lower_bound(main_cursor, key);
        CLeafPtr dap = readable_leaf(main_cursor.path.at(0).first);
        PageOffset item = main_cursor.path.at(0).second;
        bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
        if( !same_key )
            return !must_exist;
        meta_page.dirty = true;
        LeafPtr wr_dap(page_size, (LeafPage *)make_pages_writable(main_cursor, 0));
        Pid overflow_page, overflow_count;
        wr_dap.erase(main_cursor.path.at(0).second, overflow_page, overflow_count);
        if( overflow_page ) {
            table.overflow_page_count -= overflow_count;
            mark_free_in_future_page(overflow_page, overflow_count);
        }
        merge_if_needed_leaf(main_cursor, wr_dap);
        table.count -= 1;
        clear_tmp_copies();
        return true;
    }

    void TX::commit(){
        if( my_db.mappings.empty() || !meta_page.dirty )
            return;
        for(auto && tit : tables){ // First write all dirty table descriptions
            CLeafPtr dap = readable_leaf(tit.second.root_page);
            if( dap.page->tid != meta_page.tid ) // Table not dirty
                continue;
            std::string key = "table/" + tit.first;
            Val value(reinterpret_cast<const char *>(&tit.second), sizeof(TableDesc));
            ass(put(meta_page.free_table, Val(key), value, false), "Writing table desc failed during commit");
        }
        free_list.commit_free_pages(*this, meta_page.tid);
        meta_page.dirty = false;
        // First sync all our possible writes. We did not modified meta pages, so we can safely msync them also
        my_db.trim_old_mappings(); // We do not need writable pages from previous mappings, so we will unmap (if system decides to msync them, no problem. We want that anyway)
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
        Pid pa = cur.table.root_page;
        size_t height = cur.table.height;
        cur.path.resize(height + 1);
        while(true){
            if( height == 0 ){
                CLeafPtr dap = readable_leaf(pa);
                PageOffset item = dap.lower_bound_item(key);
                cur.path.at(height) = std::make_pair(pa, item);
                break;
            }
            CNodePtr nap = readable_node(pa);
            PageOffset nitem = nap.upper_bound_item(key) - 1;
            cur.path.at(height) = std::make_pair(pa, nitem);
//            result.path.push_back(std::make_pair(pa, nitem));
            //ass(dap->item_count != 0, "lower_bound node contains zero keys");
            //if( off == dap->item_count )
            //    off -= 1;
            pa = nap.get_value(nitem);
            height -= 1;
        }
    }
    //{'branch_pages': 1040L,
        //    'depth': 4L,
        //    'entries': 3761848L,
        //    'leaf_pages': 73658L,
        //    'overflow_pages': 0L,
        //    'psize': 4096L}
    std::string TX::get_stats(const TableDesc & table, std::string name){
        std::string result;
        result += "{'branch_pages': " + std::to_string(table.node_page_count) +
        ",\n\t'depth': " + std::to_string(table.height) +
        ",\n\t'entries': " + std::to_string(table.count) +
        ",\n\t'leaf_pages': " + std::to_string(table.leaf_page_count) +
        ",\n\t'overflow_pages': " + std::to_string(table.overflow_page_count) +
        ",\n\t'psize': " + std::to_string(page_size) +
        ",\n\t'table': '" + name + "'}";
        return result;
    }
    std::string TX::print_db(const TableDesc & table){
        Pid pa = table.root_page;
        size_t height = table.height;
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
                Pid overflow_page;
                auto kv = dap.get_kv(i, overflow_page);
                if( overflow_page )
                    kv.value.data = readable_overflow(overflow_page);
//                std::cout << kv.key.to_string() << ":" << kv.value.to_string() << ", ";
                std::cout << kv.key.to_string() << ", ";
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
