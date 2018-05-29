#include "mustela.hpp"
#include <iostream>
#include <sys/mman.h>

using namespace mustela;

static const bool BULK_LOADING = true;
	

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
	Pid old_page = cur.path.at(height).first;
	const DataPage * dap = my_db.readable_page(old_page);
	if( dap->tid == meta_page.tid ){ // Reached already writable page
		DataPage * wr_dap = my_db.writable_page(old_page, 1);
		return wr_dap;
	}
	mark_free_in_future_page(old_page, 1);
	Pid new_page = get_free_page(1);
	for(auto && c : my_cursors)
		if( c->bucket_desc == cur.bucket_desc && c->path.at(height).first == old_page )
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
void TX::new_increase_height(Cursor & cur){
	NodePtr wr_root = writable_node(get_free_page(1));
	cur.bucket_desc->node_page_count += 1;
	wr_root.init_dirty(meta_page.tid);
	Pid previous_root = cur.bucket_desc->root_page;
	wr_root.set_value(-1, previous_root);
//		wr_root.append(key, new_pid);
	cur.bucket_desc->root_page = wr_root.page->pid;
	cur.bucket_desc->height += 1;
	for(auto && c : my_cursors)
		if(c->bucket_desc == cur.bucket_desc)
			c->path.push_back(std::make_pair(cur.bucket_desc->root_page, -1) );
}
static size_t get_item_size_with_insert(const NodePtr & wr_dap, PageIndex pos, PageIndex insert_pos, size_t required_size1, size_t required_size2){
	if(pos == insert_pos)
		return required_size1;
	if(required_size2 != 0){
		if(pos == insert_pos + 1)
			return required_size2;
		if(pos > insert_pos + 1)
			pos -= 2;
	}else{
		if(pos > insert_pos)
			pos -= 1;
	}
	return wr_dap.get_item_size(pos);
}
static ValPid get_kv_with_insert(const NodePtr & wr_dap, PageIndex pos, PageIndex insert_pos, ValPid insert_kv1, ValPid insert_kv2){
	if(pos == insert_pos)
		return insert_kv1;
	if(insert_kv2.key.data){
		if(pos == insert_pos + 1)
			return insert_kv2;
		if(pos > insert_pos + 1)
			pos -= 2;
	}else{
		if(pos > insert_pos)
			pos -= 1;
	}
	return wr_dap.get_kv(pos);
}
void TX::new_insert2node(Cursor & cur, size_t height, ValPid insert_kv1, ValPid insert_kv2){
	auto path_el = cur.path.at(height);
	NodePtr wr_dap = writable_node(path_el.first);
	size_t required_size1 = wr_dap.get_item_size(insert_kv1.key, insert_kv1.pid);
	size_t required_size2 = insert_kv2.key.data ? wr_dap.get_item_size(insert_kv2.key, insert_kv2.pid) : 0;
	if( wr_dap.free_capacity() >= required_size1 + required_size2 ){
		wr_dap.insert_at(path_el.second, insert_kv1.key, insert_kv1.pid);
		if(insert_kv2.key.data)
			wr_dap.insert_at(path_el.second + 1, insert_kv2.key, insert_kv2.pid);
		return;
	}
	ass(wr_dap.size() >= 2, "Cannot split node with <2 keys");
	if(cur.bucket_desc->height == height)
		new_increase_height(cur);
	path_el = cur.path.at(height); // Could change in increase height
	auto path_pa = cur.path.at(height + 1);
	const PageIndex size_with_insert = wr_dap.size() + 1 + (insert_kv2.key.data ? 1 : 0);
	const PageIndex insert_index = path_el.second;
	// We must leave at least 1 key to the left and to the right
	size_t left_size = get_item_size_with_insert(wr_dap, 0, insert_index, required_size1, required_size2);
	size_t right_size = get_item_size_with_insert(wr_dap, size_with_insert - 1, insert_index, required_size1, required_size2);
	PageIndex left_split = 1;
	PageIndex right_split = size_with_insert - 1;
	size_t left_add = get_item_size_with_insert(wr_dap, left_split, insert_index, required_size1, required_size2);
	size_t right_add = get_item_size_with_insert(wr_dap, right_split - 1, insert_index, required_size1, required_size2);
	while(left_split + 1 != right_split){
		if( left_size + left_add <= wr_dap.capacity() && left_size + left_add <= right_size + right_add ){
			left_split += 1;
			left_size += left_add;
			left_add = get_item_size_with_insert(wr_dap, left_split, insert_index, required_size1, required_size2);
			continue;
		}
		if( right_size + right_add <= wr_dap.capacity() && right_size + right_add <= left_size + left_add ){
			right_split -= 1;
			right_size += left_add;
			right_add = get_item_size_with_insert(wr_dap, right_split - 1, insert_index, required_size1, required_size2);
			continue;
		}
		ass(false, "Failed to find node split");
	}
	if( BULK_LOADING && insert_index == wr_dap.size() ){ // Bulk loading?
		bool right_sibling = false; // No right sibling when inserting into root node
		if( cur.path.size() > height + 1 ){
			auto & path_pa = cur.path.at(height + 1);
			CNodePtr wr_parent = readable_node(path_pa.first);
			right_sibling = path_pa.second + 1 < wr_parent.size();
		}
		if( !right_sibling) {
			right_split = size_with_insert - 1; // Insert at inset_index would lead to empty right node
			left_split = right_split - 1;
		}
	}
	NodePtr wr_right = writable_node(get_free_page(1));
	cur.bucket_desc->node_page_count += 1;
	wr_right.init_dirty(meta_page.tid);
	for(PageIndex i = right_split; i != size_with_insert; ++i)
		wr_right.append(get_kv_with_insert(wr_dap, i, insert_index, insert_kv1, insert_kv2));
	for(auto && c : my_cursors){
		c->on_insert(cur.bucket_desc, height + 1, path_pa.first, path_pa.second + 1);
		c->on_split(cur.bucket_desc, height, path_el.first, wr_right.page->pid, right_split);
	}
	ValPid split_kv = get_kv_with_insert(wr_dap, left_split, insert_index, insert_kv1, insert_kv2);
	wr_right.set_value(-1, split_kv.pid);
	cur.path.at(height + 1).second = path_pa.second + 1; // original item
	new_insert2node(cur, height + 1, ValPid(split_kv.key, wr_right.page->pid));
	// split_kv will not be valid after code below
	{ //if( split_index != wr_dap.size() ){ // TODO - optimize out nop
		NodePtr my_copy = push_tmp_copy(wr_dap.page);
		wr_dap.clear();
		for(PageIndex i = 0; i != left_split; ++i)
			wr_dap.append(get_kv_with_insert(my_copy, i, insert_index, insert_kv1, insert_kv2));
		wr_dap.set_value(-1, my_copy.get_value(-1));
	}
}
static size_t get_item_size_with_insert(const LeafPtr & wr_dap, PageIndex pos, PageIndex insert_pos, size_t required_size){
	if(pos == insert_pos)
		return required_size;
	if(pos > insert_pos)
		pos -= 1;
	return wr_dap.get_item_size(pos);
}
static ValVal get_kv_with_insert(const LeafPtr & wr_dap, PageIndex pos, PageIndex insert_pos){
	ass(pos != insert_pos, "Insert2Leaf does not have data for replace item");
	if(pos > insert_pos)
		pos -= 1;
	Pid op;
	return wr_dap.get_kv(pos, op);
}
char * TX::new_insert2leaf(Cursor & cur, Val insert_key, size_t insert_value_size, bool * overflow){
	auto path_el = cur.path.at(0);
	LeafPtr wr_dap = writable_leaf(path_el.first);
	const size_t required_size = wr_dap.get_item_size(insert_key, insert_value_size, *overflow);
	if( required_size <= wr_dap.free_capacity() ) {
		return wr_dap.insert_at(path_el.second, insert_key, insert_value_size, *overflow);
	}
	if(cur.bucket_desc->height == 0)
		new_increase_height(cur);
	path_el = cur.path.at(0); // Could change in increase height
	auto path_pa = cur.path.at(1);
	size_t left_size = 0;
	size_t right_size = 0;
	const PageIndex size_with_insert = wr_dap.size() + 1;
	const PageIndex insert_index = path_el.second;
	PageIndex left_split = 0;
	PageIndex right_split = size_with_insert;
	size_t left_add = get_item_size_with_insert(wr_dap, left_split, insert_index, required_size);
	size_t right_add = get_item_size_with_insert(wr_dap, right_split - 1, insert_index, required_size);
	while(left_split != right_split){
		if( left_size + left_add <= wr_dap.capacity() && left_size + left_add <= right_size + right_add ){
			left_split += 1;
			left_size += left_add;
			left_add = get_item_size_with_insert(wr_dap, left_split, insert_index, required_size);
			continue;
		}
		if( right_size + right_add <= wr_dap.capacity() && right_size + right_add <= left_size + left_add ){
			right_split -= 1;
			right_size += right_add;
			right_add = get_item_size_with_insert(wr_dap, right_split - 1, insert_index, required_size);
			continue;
		}
		ass(left_split + 1 == right_split, "3-split is wrong");
		break;
	}
	if( BULK_LOADING && insert_index == wr_dap.size() ){ // Bulk loading?
		bool right_sibling = false; // No right sibling when height == 0
		if( cur.path.size() > 1 ){
			auto & path_pa = cur.path.at(1);
			CNodePtr wr_parent = readable_node(path_pa.first);
			right_sibling = path_pa.second + 1 < wr_parent.size();
		}
		if( !right_sibling)
			right_split = left_split = size_with_insert - 1;
	}
	LeafPtr wr_right = writable_leaf(get_free_page(1));
	cur.bucket_desc->leaf_page_count += 1;
	wr_right.init_dirty(meta_page.tid);
	char * result = nullptr;
	for(PageIndex i = right_split; i != size_with_insert; ++i)
		if( i == insert_index)
			result = wr_right.insert_at(wr_right.size(), insert_key, insert_value_size, *overflow);
		else
			wr_right.append(get_kv_with_insert(wr_dap, i, insert_index));
	for(auto && c : my_cursors){
		c->on_insert(cur.bucket_desc, 1, path_pa.first, path_pa.second + 1);
		c->on_split(cur.bucket_desc, 0, path_el.first, wr_right.page->pid, right_split);
	}
	LeafPtr wr_middle;
	if(left_split + 1 == right_split){
		wr_middle = writable_leaf(get_free_page(1));
		cur.bucket_desc->leaf_page_count += 1;
		wr_middle.init_dirty(meta_page.tid);
		if( left_split == insert_index)
			result = wr_middle.insert_at(wr_middle.size(), insert_key, insert_value_size, *overflow);
		else
			wr_middle.append(get_kv_with_insert(wr_dap, left_split, insert_index));
		for(auto && c : my_cursors){
			c->on_insert(cur.bucket_desc, 1, path_pa.first, path_pa.second + 1);
			c->on_split(cur.bucket_desc, 0, path_el.first, wr_middle.page->pid, left_split);
		}
	}
	{ //if( split_index != wr_dap.size() ){ // TODO - optimize out nop
		LeafPtr my_copy = push_tmp_copy(wr_dap.page);
		wr_dap.clear();
		for(PageIndex i = 0; i != left_split; ++i)
			if( i == insert_index)
				result = wr_dap.insert_at(wr_dap.size(), insert_key, insert_value_size, *overflow);
			else
				wr_dap.append(get_kv_with_insert(my_copy, i, insert_index));
	}
	cur.path.at(1).second = path_pa.second + 1; // original item
	if(left_split + 1 == right_split){
		new_insert2node(cur, 1, ValPid(wr_middle.get_key(0), wr_middle.page->pid), ValPid(wr_right.get_key(0), wr_right.page->pid));
	}else{
		new_insert2node(cur, 1, ValPid(wr_right.get_key(0), wr_right.page->pid));
	}
	clear_tmp_copies();
	return result;
}
void TX::new_merge_node(Cursor & cur, size_t height, NodePtr wr_dap){
	if( wr_dap.data_size() >= wr_dap.capacity()/2 )
		return;
	if(height == cur.bucket_desc->height){ // merging root
		if( wr_dap.size() != 0) // wait until only key at -1 offset remains, make it new root
			return;
		mark_free_in_future_page(cur.bucket_desc->root_page, 1);
		cur.bucket_desc->node_page_count -= 1;
		cur.bucket_desc->root_page = wr_dap.get_value(-1);
		cur.bucket_desc->height -= 1;
		for(auto && c : my_cursors)
			if( c->bucket_desc == cur.bucket_desc)
				c->path.pop_back();
		return;
	}
	auto path_el = cur.path.at(height);
	auto path_pa = cur.path.at(height + 1);
	NodePtr wr_parent = writable_node(path_pa.first);
	ass(wr_parent.size() > 0, "Found parent node with 0 items");
	ValPid my_kv;
//	size_t required_size_for_my_kv = 0;

	CNodePtr left_sib;
	bool use_left_sib = false;
	size_t left_data_size = 0;
	CNodePtr right_sib;
	bool use_right_sib = false;
	size_t right_data_size = 0;
	ValPid right_kv;
	if( path_pa.second != -1){
		my_kv = wr_parent.get_kv(path_pa.second);
//		required_size_for_my_kv = wr_parent.get_item_size(my_kv.key, my_kv.pid);
		ass(my_kv.pid == wr_dap.page->pid, "merge_if_needed_node my pid in parent does not match");
		Pid left_pid = wr_parent.get_value(path_pa.second - 1);
		left_sib = readable_node(left_pid);
		left_data_size = left_sib.data_size();
		left_data_size += left_sib.get_item_size(my_kv.key, 0); // will need to insert key from parent. Achtung - 0 works only when fixed-size pids are used
		use_left_sib = left_data_size <= wr_dap.free_capacity();
	}
	if(path_pa.second + 1 < wr_parent.size()){
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
		std::cout << "Borrowing key from sibling" << std::endl;
		ass(left_sib.page || right_sib.page, "Cannot borrow - no siblings for node with 0 items" );
		if(left_sib.page && right_sib.page){
			// TODO zero one that does not fit in parent after rotation
		}
		if(left_sib.page){
		
		}else{
		
		}
		return;
	}
	if( use_left_sib && use_right_sib )
		std::cout << "3-way merge" << std::endl;
	if( use_left_sib ){
		Pid spec_val = wr_dap.get_value(-1);
		wr_dap.insert_at(0, my_kv.key, spec_val);
		wr_dap.set_value(-1, left_sib.get_value(-1));
		wr_dap.insert_range(0, left_sib, 0, left_sib.size());
		mark_free_in_future_page(left_sib.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
		cur.bucket_desc->node_page_count -= 1;
		wr_parent.erase(path_pa.second);
		wr_parent.set_value(path_pa.second - 1, wr_dap.page->pid);
		for(auto && c : my_cursors){
			c->on_erase(cur.bucket_desc, height + 1, path_pa.first, path_pa.second - 1);
			c->on_insert(cur.bucket_desc, height, path_el.first, -1, left_sib.size() + 1);
			c->on_merge(cur.bucket_desc, height, left_sib.page->pid, path_el.first, 0);
		}
		path_el = cur.path.at(0); // path_pa was modified by code above
		path_pa = cur.path.at(1); // path_pa was modified by code above
	}
	if( use_right_sib ){
		for(auto && c : my_cursors){
			c->on_erase(cur.bucket_desc, height + 1, path_pa.first, path_pa.second);
			c->on_merge(cur.bucket_desc, height, right_sib.page->pid, path_el.first, wr_dap.size() + 1);
		}
		Pid spec_val = right_sib.get_value(-1);
		wr_dap.append(right_kv.key, spec_val);
		wr_dap.append_range(right_sib, 0, right_sib.size());
		mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
		cur.bucket_desc->node_page_count -= 1;
		wr_parent.erase(path_pa.second + 1);
	}
	if( use_left_sib || use_right_sib )
		new_merge_node(cur, height + 1, wr_parent);
}
void TX::new_merge_leaf(Cursor & cur, LeafPtr wr_dap){
	if( wr_dap.data_size() >= wr_dap.capacity()/2 )
		return;
	if(cur.bucket_desc->height == 0) // root is leaf, cannot merge anyway
		return;
	auto path_el = cur.path.at(0);
	auto path_pa = cur.path.at(1);
	NodePtr wr_parent = writable_node(path_pa.first);
	CLeafPtr left_sib;
	CLeafPtr right_sib;
	if( path_pa.second != -1){
		Pid left_pid = wr_parent.get_value(path_pa.second - 1);
		left_sib = readable_leaf(left_pid);
		if( wr_dap.free_capacity() < left_sib.data_size() )
			left_sib = CLeafPtr(); // forget about left!
	}
	if( path_pa.second + 1 < wr_parent.size()){
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
	if( left_sib.page && right_sib.page )
		std::cout << "3-way merge" << std::endl;
	if( left_sib.page ){
		wr_dap.insert_range(0, left_sib, 0, left_sib.size());
		mark_free_in_future_page(left_sib.page->pid, 1); // unlink left, point its slot in parent to us, remove our slot in parent
		cur.bucket_desc->leaf_page_count -= 1;
		wr_parent.erase(path_pa.second);
		wr_parent.set_value(path_pa.second - 1, path_el.first);
		for(auto && c : my_cursors){
			c->on_erase(cur.bucket_desc, 1, path_pa.first, path_pa.second - 1);
			c->on_insert(cur.bucket_desc, 0, path_el.first, 0, left_sib.size());
			c->on_merge(cur.bucket_desc, 0, left_sib.page->pid, path_el.first, 0);
		}
		path_el = cur.path.at(0); // path_pa was modified by code above
		path_pa = cur.path.at(1); // path_pa was modified by code above
	}
	if( right_sib.page ){
		for(auto && c : my_cursors){
			c->on_erase(cur.bucket_desc, 1, path_pa.first, path_pa.second);
			c->on_merge(cur.bucket_desc, 0, right_sib.page->pid, path_el.first, wr_dap.size());
		}
		wr_dap.append_range(right_sib, 0, right_sib.size());
		mark_free_in_future_page(right_sib.page->pid, 1); // unlink right, remove its slot in parent
		cur.bucket_desc->leaf_page_count -= 1;
		wr_parent.erase(path_pa.second + 1);
	}
	if( left_sib.page || right_sib.page )
		new_merge_node(cur, 1, wr_parent);
	clear_tmp_copies();
}
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
	BucketDesc * bucket_desc = load_bucket_desc(name);
	if( !bucket_desc )
		return false;
	{
		Cursor cursor(*this, bucket_desc);
		cursor.first();
		while(bucket_desc->count != 0){
			cursor.del();
		}
		ass(bucket_desc->leaf_page_count == 1 && bucket_desc->node_page_count == 0 && bucket_desc->overflow_page_count == 0 && bucket_desc->height == 0, "Bucket in wrong state after deleting all keys");
		mark_free_in_future_page(bucket_desc->root_page, 1);
	}
	for(auto && c : my_cursors) // All cursor to that table become set to end
		if( c->bucket_desc == bucket_desc ) {
			c->bucket_desc = nullptr;
			c->path.clear();
		}
	for(auto && b : my_buckets)
		if( b->bucket_desc == bucket_desc ) {
			b->bucket_desc = nullptr;
			b->debug_name = std::string();
		}
	// TODO - mark all table pages as free
	std::string key = bucket_prefix + name.to_string();
	Bucket meta_bucket(*this, &meta_page.meta_bucket);
	ass(meta_bucket.del(Val(key), true), "Error while dropping table");
	bucket_descs.erase(name.to_string());
	return true;
}
BucketDesc * TX::load_bucket_desc(const Val & name){
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
