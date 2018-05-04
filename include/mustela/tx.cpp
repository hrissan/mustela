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
	
	Cursor::Cursor(TX & my_txn, BucketDesc * bucket_desc):my_txn(my_txn), bucket_desc(bucket_desc){
		my_txn.my_cursors.insert(this);
	}
	Cursor::Cursor(Bucket & bucket):my_txn(bucket.my_txn), bucket_desc(bucket.bucket_desc){
		my_txn.my_cursors.insert(this);
	}
	Cursor::~Cursor(){
		my_txn.my_cursors.erase(this);
	}
	Cursor::Cursor(Cursor && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc){
		my_txn.my_cursors.insert(this);
		path = std::move(other.path);
	}
	void Cursor::jump_prev(){
		auto path_el = path.at(0);
//		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		ass(path_el.second == 0, "Cursor jump from leaf element != 0 at Cursor::jump_prev");
		size_t height = 1;
		Pid pa = 0;
		while(true){
			if( height == path.size() ){
				path.clear();
				return;
			}
			CNodePtr nap = my_txn.readable_node(path.at(height).first);
			if( path.at(height).second != PageOffset(-1) ){
				path.at(height).second -= 1;
				pa = nap.get_value(path.at(height).second);
				height -= 1;
				break;
			}
			height += 1;
		}
		while(true){
			if( height == 0 ){
				CLeafPtr dap = my_txn.readable_leaf(pa);
				ass(dap.size() > 0, "Empty leaf page in Cursor::last");
				path.at(height) = std::make_pair(pa, dap.size() - 1);
				break;
			}
			CNodePtr nap = my_txn.readable_node(pa);
			PageOffset nitem = nap.size() - 1;
			path.at(height) = std::make_pair(pa, nitem);
			pa = nap.get_value(nitem);
			height -= 1;
		}
	}
	void Cursor::jump_next(){
		auto path_el = path.at(0);
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		ass(path_el.second == dap.size(), "Cursor jump from exisiting leaf element at Cursor::jump_next");
		size_t height = 1;
		Pid pa = 0;
		while(true){
			if( height == path.size() ){
				path.clear();
				return;
			}
			CNodePtr nap = my_txn.readable_node(path.at(height).first);
			if( PageOffset(path.at(height).second + 1) < nap.size() ){
				path.at(height).second += 1;
				pa = nap.get_value(path.at(height).second);
				height -= 1;
				break;
			}
			height += 1;
		}
		while(true){
			if( height == 0 ){
				path.at(height) = std::make_pair(pa, 0);
				break;
			}
			CNodePtr nap = my_txn.readable_node(pa);
			PageOffset nitem = -1;
			path.at(height) = std::make_pair(pa, nitem);
			pa = nap.get_value(nitem);
			height -= 1;
		}
	}
	void Cursor::lower_bound(const Val & key){
		Pid pa = bucket_desc->root_page;
		size_t height = bucket_desc->height;
		path.resize(height + 1);
		while(true){
			if( height == 0 ){
				CLeafPtr dap = my_txn.readable_leaf(pa);
				PageOffset item = dap.lower_bound_item(key);
				path.at(height) = std::make_pair(pa, item);
				break;
			}
			CNodePtr nap = my_txn.readable_node(pa);
			PageOffset nitem = nap.upper_bound_item(key) - 1;
			path.at(height) = std::make_pair(pa, nitem);
			pa = nap.get_value(nitem);
			height -= 1;
		}
	}
	bool Cursor::seek(const Val & key){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		lower_bound(key);
		auto path_el = path.at(0);
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		bool same_key = path_el.second != dap.size() && Val(dap.get_key(path_el.second)) == key;
		if( path_el.second == dap.size() )
			jump_next();
		return same_key;
	}
	void Cursor::first(){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		Pid pa = bucket_desc->root_page;
		size_t height = bucket_desc->height;
		path.resize(height + 1);
		while(true){
			if( height == 0 ){
				path.at(height) = std::make_pair(pa, 0);
				break;
			}
			CNodePtr nap = my_txn.readable_node(pa);
			PageOffset nitem = -1;
			path.at(height) = std::make_pair(pa, nitem);
			pa = nap.get_value(nitem);
			height -= 1;
		}
	}
	void Cursor::last(){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		if( bucket_desc->count == 0){
			path.clear();
			return;
		}
		Pid pa = bucket_desc->root_page;
		size_t height = bucket_desc->height;
		path.resize(height + 1);
		while(true){
			if( height == 0 ){
				CLeafPtr dap = my_txn.readable_leaf(pa);
				ass(dap.size() > 0, "Empty leaf page in Cursor::last");
				path.at(height) = std::make_pair(pa, dap.size() - 1);
				break;
			}
			CNodePtr nap = my_txn.readable_node(pa);
			PageOffset nitem = nap.size() - 1;
			path.at(height) = std::make_pair(pa, nitem);
			pa = nap.get_value(nitem);
			height -= 1;
		}
	}
	bool Cursor::get(Val & key, Val & value){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		if( path.empty() )
			return false;
		auto path_el = path.at(0);
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		if( path_el.second == dap.size() ) // After seek("zzz"), when all keys are less than "z"
			return false;
		Pid overflow_page;
		auto kv = dap.get_kv(path_el.second, overflow_page);
		if( overflow_page )
			kv.value.data = my_txn.readable_overflow(overflow_page);
		key = kv.key;
		value = kv.value;
		return true;
	}
	void Cursor::del(){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		if( my_txn.read_only )
			throw Exception("Attempt to modify read-only transaction in Cursor::del");
		if( path.empty() )
			return;
		auto & path_el = path.at(0);
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		if( path_el.second == dap.size() ) // After seek("zzz"), when all keys are less than "z"
			return;
		my_txn.meta_page_dirty = true;
		LeafPtr wr_dap(my_txn.page_size, (LeafPage *)my_txn.make_pages_writable(*this, 0));
		Pid overflow_page, overflow_count;
		wr_dap.erase(path_el.second, overflow_page, overflow_count);
		if( overflow_page ) {
			bucket_desc->overflow_page_count -= overflow_count;
			my_txn.mark_free_in_future_page(overflow_page, overflow_count);
		}
		my_txn.merge_if_needed_leaf(*this, wr_dap);
		bucket_desc->count -= 1;
		my_txn.clear_tmp_copies();
		dap = my_txn.readable_leaf(path_el.first);
		if( path_el.second == dap.size() )
			jump_next();
	}
	void Cursor::next(){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		if( path.empty() )
			return first();
		auto & path_el = path.at(0);
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		ass(path_el.second < dap.size(), "Cursor points beyond last leaf element");
		path_el.second += 1;
		if( path_el.second == dap.size() )
			jump_next();
	}
	void Cursor::prev(){
		ass(bucket_desc, "Cursor not valid (using after tx commit?)");
		if( path.empty() )
			return last();
		auto & path_el = path.at(0);
		if( path_el.second == 0 ) {
			jump_prev();
//			if (path.empty()) // jump_prev from begin clears cursor
//				return;
			return;
		}
		CLeafPtr dap = my_txn.readable_leaf(path_el.first);
		ass(path_el.second > 0 && path_el.second <= dap.size(), "Cursor points beyond last leaf element");
		path_el.second -= 1;
	}
	TX::TX(DB & my_db, bool read_only):my_db(my_db), page_size(my_db.page_size), read_only(read_only) {
		if( !read_only && my_db.read_only)
			throw Exception("Read-write transaction impossible on read-only DB");
		start_transaction();
	}
	void TX::start_transaction(){
		c_mappings_end_page = my_db.c_mappings.back().end_page;
		my_db.c_mappings.back().ref_count += 1;

		meta_page_index = my_db.oldest_meta_page_index(); // Overwrite oldest page
		auto oldest_meta_page = my_db.readable_meta_page(meta_page_index);
		oldest_reader_tid = oldest_meta_page->tid; // TODO ask from reader-writer lock
		auto newest_page_index = my_db.last_meta_page_index(); // by newest
		meta_page = *(my_db.readable_meta_page(newest_page_index));
		meta_page.tid += 1;
		meta_page.tid2 = meta_page.tid;
		meta_page_dirty = false;
	}
	
	TX::~TX(){
		// rollback is nop
		ass( my_cursors.empty(), "All cursors should be destroyed before transaction they are in"); // potential throw in destructor ahaha
		my_db.trim_old_c_mappings(c_mappings_end_page);
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
	}
	Pid TX::get_free_page(Pid contigous_count){
		Pid pa = free_list.get_free_page(*this, contigous_count, oldest_reader_tid);
		if( !pa ){
			my_db.grow_file(meta_page.page_count + contigous_count);
			pa = meta_page.page_count;
			meta_page.page_count += contigous_count;
		}
		if( 3731 >= pa && 3731 < pa + contigous_count )
			pa = pa;
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
		if(height == cur.bucket_desc->height){ // node is root
			cur.bucket_desc->root_page = new_page;
			return wr_dap;
		}
		NodePtr wr_parent(page_size, (NodePage *)make_pages_writable(cur, height + 1));
		wr_parent.set_value(cur.path.at(height + 1).second, new_page);
		return wr_dap;
	}
	Cursor TX::insert_pages_to_node(Cursor & cur, size_t height, Val key, Pid new_pid){
		if( height == cur.bucket_desc->height + 1){ // making new root
			NodePtr wr_root = writable_node(get_free_page(1));
			cur.bucket_desc->node_page_count += 1;
			wr_root.init_dirty(meta_page.tid);
			Pid previous_root = cur.bucket_desc->root_page;
			wr_root.set_value(-1, previous_root);
			wr_root.append(key, new_pid);
			cur.bucket_desc->root_page = wr_root.page->pid;
			cur.bucket_desc->height += 1;
			for(auto && c : my_cursors)
				c->path.push_back(std::make_pair(cur.bucket_desc->root_page, -1) );
			//                ass(c->path.at[0].first == new_pid || c->path.at[0].first == previous_root, "Increasing height, but stray cursor found");
			//                bool was_left = c->path.at[0].first == previous_root;
			Cursor cur2(*this, cur.bucket_desc);
			cur2.path = cur.path;//.resize(meta_page.main_height, std::make_pair(-1, -1));
			//            cur2.path[meta_page.main_height - 1] = std::make_pair(new_pid, 0);
			cur2.path.at(cur.bucket_desc->height) = std::make_pair(cur.bucket_desc->root_page, 0);
			cur2.path.at(height - 1) = std::make_pair(new_pid, 0);
			return cur2;
		}
		auto path_el = cur.path.at(height);
		NodePtr wr_dap = writable_node(path_el.first);
		size_t required_size = wr_dap.get_item_size(key, new_pid);
		if( wr_dap.free_capacity() < required_size ){
			Cursor cur3 = force_split_node(cur, height, key, new_pid);
			cur3.path.at(height - 1) = std::make_pair(new_pid, 0);
			return cur3;
		}
		wr_dap.insert_at(path_el.second + 1, key, new_pid);
		for(auto && c : my_cursors)
			c->on_insert(height, path_el.first, path_el.second + 1);
		Cursor cur2(*this, cur.bucket_desc);
		cur2.path = cur.path;//.insert(cur2.path.end(), cur.path.begin(), cur.path.begin() + height + 1);
		cur2.path.at(height).second += 1;// = std::make_pair(new_pid, 0);
		cur2.path.at(height - 1) = std::make_pair(new_pid, 0);
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
		cur.bucket_desc->node_page_count += 1;
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
		cur2.path.at(height) = std::make_pair(wr_right.page->pid, -1);
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
		cur.bucket_desc->leaf_page_count += 1;
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
			cur.bucket_desc->leaf_page_count += 1;
			wr_middle.init_dirty(meta_page.tid);
			result = wr_middle.insert_at(0, insert_key, insert_val_size, overflow);
			insert_pages_to_node(cur, 1, insert_key, wr_middle.page->pid);
		}
		return result;
	}
	void TX::merge_if_needed_node(Cursor & cur, size_t height, NodePtr wr_dap){
		if( wr_dap.data_size() >= wr_dap.capacity()/2 )
			return;
		if(height == cur.bucket_desc->height){ // merging root
			if( wr_dap.size() != 0) // wait until 1 key remains, make it new root
				return;
			mark_free_in_future_page(cur.bucket_desc->root_page, 1);
			cur.bucket_desc->node_page_count -= 1;
			cur.bucket_desc->root_page = wr_dap.get_value(-1);
			cur.bucket_desc->height -= 1;
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
			cur.bucket_desc->node_page_count -= 1;
			wr_parent.erase(path_pa.second);
			wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
			path_pa.second -= 1; // fix cursor
		}
		if( use_right_sib ){
			Pid spec_val = right_sib.get_value(-1);
			wr_dap.append(right_kv.key, spec_val);
			wr_dap.append_range(right_sib, 0, right_sib.size());
			mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
			cur.bucket_desc->node_page_count -= 1;
			wr_parent.erase(path_pa.second + 1);
		}
		merge_if_needed_node(cur, height + 1, wr_parent);
	}
	void TX::prune_empty_node(Cursor & cur, size_t height, NodePtr wr_dap){
		mark_free_in_future_page(wr_dap.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
		cur.bucket_desc->node_page_count -= 1;
		//        auto & path_el = cur.path.at(height);
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
			cur.bucket_desc->leaf_page_count -= 1;
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
			cur.bucket_desc->leaf_page_count -= 1;
			wr_parent.erase(path_pa.second);
			wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
			path_pa.second -= 1; // fix cursor
		}
		if( right_sib.page ){
			wr_dap.append_range(right_sib, 0, right_sib.size());
			mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
			cur.bucket_desc->leaf_page_count -= 1;
			wr_parent.erase(path_pa.second + 1);
		}
		if( left_sib.page || right_sib.page )
			merge_if_needed_node(cur, 1, wr_parent);
	}
/*	bool TX::get_next(BucketDesc * table, Val & key, Val & value){
		Val key1 = key, key2 = key;
		Val value1, value2;
		bool r2 = get_next_old(table, key2, value2);
//		Bucket bucket(*this, table);
		Cursor main_cursor(*this, table);
		main_cursor.seek(key1);
		bool r1 = main_cursor.get(key1, value1);
		ass( r1 == r2 && key1 == key2 && value1 == value2, "get_next wrong" );
		key = key1;
		value = value1;
		return r1;
	}
	bool TX::get_next_old(BucketDesc * table, Val & key, Val & value){
		Cursor main_cursor(*this, table);
		main_cursor.lower_bound(key);
		CLeafPtr dap = readable_leaf(main_cursor.path.at(0).first);
		PageOffset item = main_cursor.path.at(0).second;
		if( item == dap.size() )
			return false;
		Pid overflow_page;
		auto kv = dap.get_kv(item, overflow_page);
		key = kv.key;
		if( overflow_page )
			kv.value.data = readable_overflow(overflow_page);
		value = kv.value;
		return true;
	}*/
	void TX::commit(){
		if( meta_page_dirty ) {
			{
				Bucket meta_bucket(*this, &meta_page.meta_bucket);
				for (auto &&tit : bucket_descs) { // First write all dirty table descriptions
					CLeafPtr dap = readable_leaf(tit.second.root_page);
					if (dap.page->tid != meta_page.tid) // Table not dirty
						continue;
					std::string key = bucket_prefix + tit.first;
					Val value(reinterpret_cast<const char *>(&tit.second), sizeof(BucketDesc));
					ass(meta_bucket.put(Val(key), value, false), "Writing table desc failed during commit");
				}
			}
			free_list.commit_free_pages(*this, meta_page.tid);
			meta_page_dirty = false;
			// First sync all our possible writes. We did not modified meta pages, so we can safely msync them also
			my_db.trim_old_mappings(); // We do not need writable pages from previous mappings, so we will unmap (if system decides to msync them, no problem. We want that anyway)
			for (size_t i = 0; i != my_db.mappings.size(); ++i) {
				Mapping &ma = my_db.mappings[i];
				msync(ma.addr, ma.end_page * page_size, MS_SYNC);
			}
			// Now modify and sync our meta page
			MetaPage *wr_meta = my_db.writable_meta_page(meta_page_index);
			*wr_meta = meta_page;
			size_t low = meta_page_index * page_size;
			size_t high = (meta_page_index + 1) * page_size;
			low = ((low / my_db.physical_page_size)) * my_db.physical_page_size; // find lowest physical page
			high = ((high + my_db.physical_page_size - 1) / my_db.physical_page_size) *
				   my_db.physical_page_size; // find highest physical page
			msync(my_db.mappings.at(0).addr + low, high - low, MS_SYNC);
		}
		// Now invalidate all cursors and buckets
		for(auto && c : my_cursors) {
			c->bucket_desc = nullptr;
			c->path.clear();
		}
		for(auto && b : my_buckets) {
			b->bucket_desc = nullptr;
			b->debug_name = std::string();
		}
		// Now start new transaction
		my_db.trim_old_c_mappings(c_mappings_end_page);
		start_transaction();
	}
	//{'branch_pages': 1040L,
	//    'depth': 4L,
	//    'entries': 3761848L,
	//    'leaf_pages': 73658L,
	//    'overflow_pages': 0L,
	//    'psize': 4096L}
    std::vector<Val> TX::get_bucket_names(){
		std::vector<Val> results;
		for(auto && tit : bucket_descs) // First write all dirty table descriptions
			results.push_back(Val(tit.first));
		Cursor cur(*this, &meta_page.meta_bucket);
		Val c_key, c_value, c_tail;
		char ch = bucket_prefix;
		const Val prefix(&ch, 1);
		for(cur.seek(prefix); cur.get(c_key, c_value) && c_key.has_prefix(prefix, c_tail); cur.next())
			if(bucket_descs.count(c_tail.to_string()) == 0)
				results.push_back(c_tail);
		return results;
	}
//	bool TX::create_table(const Val & table){
//		if( load_table_desc(table) )
//			return false;
//		return true;
//	}
	bool TX::drop_bucket(const Val & name){
		if( read_only )
			throw Exception("Attempt to modify read-only transaction");
		BucketDesc * td = load_bucket_desc(name);
		if( !td )
			return false;
		for(auto && c : my_cursors) // All cursor to that table become set to end
			if( c->bucket_desc == td ) {
				c->bucket_desc = nullptr;
				c->path.clear();
			}
		for(auto && b : my_buckets)
			if( b->bucket_desc == td ) {
				b->bucket_desc = nullptr;
				b->debug_name = std::string();
			}
		// TODO - mark all table pages as free
		std::string key = bucket_prefix + name.to_string();
		Bucket meta_bucket(*this, &meta_page.meta_bucket);
		ass(meta_bucket.del(Val(key), true), "Error while dropping table");
		bucket_descs.erase(key);
		return true;
	}
	BucketDesc * TX::load_bucket_desc(const Val & name){
//		if(name.empty())
//			return nullptr;
		auto tit = bucket_descs.find(name.to_string());
		if( tit != bucket_descs.end() )
			return &tit->second;
		std::string key = bucket_prefix + name.to_string();
		Val value;
		Bucket meta_bucket(*this, &meta_page.meta_bucket);
		if( !meta_bucket.get(Val(key), value) )
			return nullptr;
		BucketDesc & td = bucket_descs[name.to_string()];
		ass(value.size == sizeof(td), "BucketDesc size in DB is wrong");
		memmove(&td, value.data, value.size);
		return &td;
	}
	static std::string trim_key(const std::string & key, bool parse_meta){
		if( parse_meta && !key.empty() && key[0] == TX::freelist_prefix ){
			Tid next_record_tid;
			uint64_t next_record_batch;
			size_t p1 = 1;
			p1 += read_u64_sqlite4(next_record_tid, key.data() + p1);
			p1 += read_u64_sqlite4(next_record_batch, key.data() + p1);
			return "f" + std::to_string(next_record_tid) + ":" + std::to_string(next_record_batch);
		}
		std::string result;
		for(auto && ch : key)
			if( std::isprint(ch) && ch != '"')
				result += ch;
			else
				result += "$" + std::to_string(unsigned(ch));
		return result;
	}
	std::string TX::print_db(const BucketDesc * bucket_desc){
		Pid pa = bucket_desc->root_page;
		size_t height = bucket_desc->height;
		return print_db(pa, height, bucket_desc == &meta_page.meta_bucket);
	}
	std::string TX::print_db(Pid pid, size_t height, bool parse_meta){
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
				std::cout << trim_key(kv.key.to_string(), parse_meta) << ", ";
				result += "\"" + trim_key(kv.key.to_string(), parse_meta) + "\""; //  + ":" + value.to_string() +
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
			std::cout << trim_key(va.key.to_string(), parse_meta) << ":" << va.pid << ", ";
			result += "\"" + trim_key(va.key.to_string(), parse_meta) + ":" + std::to_string(va.pid) + "\"";
		}
		result += "],\"children\":[";
		std::cout << "]" << std::endl;
		std::string spec_str = print_db(spec_value, height - 1, parse_meta);
		result += spec_str;
		for(int i = 0; i != nap.size(); ++i){
			ValPid va = nap.get_kv(i);
			std::string str = print_db(va.pid, height - 1, parse_meta);
			result += "," + str;
		}
		return result + "]}";
	}
	std::string TX::print_meta_db(){
		Bucket meta_bucket(*this, &meta_page.meta_bucket);
		return meta_bucket.print_db();
	}
	std::string TX::get_meta_stats(){
		Bucket meta_bucket(*this, &meta_page.meta_bucket);
		return meta_bucket.get_stats();
	}
	Bucket::Bucket(TX & my_txn, const Val & name, bool create):my_txn(my_txn), bucket_desc(my_txn.load_bucket_desc(name)), debug_name(name.to_string()) {
		if( !bucket_desc && create){
			if( my_txn.read_only )
				throw Exception("Attempt to modify read-only transaction");
			BucketDesc & td = my_txn.bucket_descs[name.to_string()];
			td = BucketDesc{};
			td.root_page = my_txn.get_free_page(1);
			td.leaf_page_count = 1;
			bucket_desc = &td;
			// We will put it in DB on commit
		}
		my_txn.my_buckets.insert(this);
	}
	Bucket::~Bucket(){
		my_txn.my_buckets.erase(this);
	}
	char * Bucket::put(const Val & key, size_t value_size, bool nooverwrite){
		if( my_txn.read_only )
			throw Exception("Attempt to modify read-only transaction");
		ass(bucket_desc, "Bucket not valid (using after tx commit?)");
		Cursor main_cursor(my_txn, bucket_desc);
		main_cursor.lower_bound(key);
		CLeafPtr dap = my_txn.readable_leaf(main_cursor.path.at(0).first);
		PageOffset item = main_cursor.path.at(0).second;
		bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
		if( same_key && nooverwrite )
			return nullptr;
		my_txn.meta_page_dirty = true;
		// TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
		LeafPtr wr_dap(my_txn.page_size, (LeafPage *)my_txn.make_pages_writable(main_cursor, 0));
		if( same_key ){
			Pid overflow_page, overflow_count;
			wr_dap.erase(main_cursor.path.at(0).second, overflow_page, overflow_count);
			if( overflow_page ){
				bucket_desc->overflow_page_count -= overflow_count;
				my_txn.mark_free_in_future_page(overflow_page, overflow_count);
			}
		}
		bool overflow;
		size_t required_size = wr_dap.get_item_size(key, value_size, overflow);
		char * result = nullptr;
		if( required_size <= wr_dap.free_capacity() )
			result = wr_dap.insert_at(main_cursor.path.at(0).second, key, value_size, overflow);
		else
			result = my_txn.force_split_leaf(main_cursor, key, value_size);
		if( overflow ){
			Pid overflow_count = (value_size + my_txn.page_size - 1)/my_txn.page_size;
			Pid opa = my_txn.get_free_page(overflow_count);
			bucket_desc->overflow_page_count += overflow_count;
			pack_uint_be(result, NODE_PID_SIZE, opa);
			result = my_txn.writable_overflow(opa, overflow_count);
		}
		if( !same_key )
			bucket_desc->count += 1;
		my_txn.clear_tmp_copies();
		return result;
//				return my_txn.put(table, key, value_size, nooverwrite);
	}
	bool Bucket::put(const Val & key, const Val & value, bool nooverwrite) { // false if nooverwrite and key existed
		char * dst = put(key, value.size, nooverwrite);
		if( dst )
			memcpy(dst, value.data, value.size);
		return dst != nullptr;
	}
	bool Bucket::get(const Val & key, Val & value){
		ass(bucket_desc, "Bucket not valid (using after tx commit?)");
		Cursor main_cursor(my_txn, bucket_desc);
		if( !main_cursor.seek(key) )
			return false;
		Val c_key;
		return main_cursor.get(c_key, value);
	}
	bool Bucket::del(const Val & key, bool must_exist){
		if( my_txn.read_only )
			throw Exception("Attempt to modify read-only transaction");
		ass(bucket_desc, "Bucket not valid (using after tx commit?)");
		Cursor main_cursor(my_txn, bucket_desc);
		if( !main_cursor.seek(key) )
			return !must_exist;
		main_cursor.del();
		return true;
	}
	std::string Bucket::get_stats()const{
		std::string result;
		ass(bucket_desc, "Bucket not valid (using after tx commit?)");
		result += "{'branch_pages': " + std::to_string(bucket_desc->node_page_count) +
		",\n\t'depth': " + std::to_string(bucket_desc->height) +
		",\n\t'entries': " + std::to_string(bucket_desc->count) +
		",\n\t'leaf_pages': " + std::to_string(bucket_desc->leaf_page_count) +
		",\n\t'overflow_pages': " + std::to_string(bucket_desc->overflow_page_count) +
		",\n\t'psize': " + std::to_string(my_txn.page_size) +
		",\n\t'table': '" + debug_name + "'}";
		return result;
	}
}
