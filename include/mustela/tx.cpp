#include "mustela.hpp"
#include <algorithm>
#include <iostream>
#include <sys/mman.h>

using namespace mustela;

static const bool BULK_LOADING = true;

static const char bucket_prefix = 'b';

int TX::debug_mirror_counter = 0;

TX::TX(DB & my_db, bool read_only):my_db(my_db), read_only(read_only), page_size(my_db.page_size) {
	if( !read_only && my_db.options.read_only)
		throw Exception("Read-write transaction impossible on read-only DB");
	my_db.start_transaction(this);
	if(DEBUG_MIRROR)
		load_mirror();
}

TX::~TX(){
	my_db.finish_transaction(this);
	unlink_buckets_and_cursors();
}
DataPage * TX::writable_page(Pid page, Pid count){
	ass(page + count <= file_page_count, "Mapping should always cover the whole file");
	return (DataPage *)(wr_file_ptr + page * page_size);
}

LeafPtr TX::writable_leaf(Pid pa){
	LeafPage * result = (LeafPage *)writable_page(pa, 1);
	ass(result->tid() == meta_page.tid, "writable_leaf is not from our transaction");
	return LeafPtr(page_size, result);
}
NodePtr TX::writable_node(Pid pa){
	NodePage * result = (NodePage *)writable_page(pa, 1);
	ass(result->tid() == meta_page.tid, "writable_node is not from our transaction");
	return NodePtr(page_size, result);
}
char * TX::writable_overflow(Pid pa, Pid count){
	return (char *)writable_page(pa, count);
}
void TX::mark_free_in_future_page(Pid page, Pid contigous_count, Tid page_tid){
	free_list.mark_free_in_future_page(page, contigous_count, this->tid() == page_tid);
}
void TX::start_update(BucketDesc * bucket_desc){
	if(bucket_desc != &meta_page.meta_bucket)
		return;
	ass(!updating_meta_bucket, "Double start of update meta bucket");
	updating_meta_bucket = true;
	free_list.ensure_have_several_pages(this, oldest_reader_tid);
}
void TX::finish_update(BucketDesc * bucket_desc){
	if(bucket_desc != &meta_page.meta_bucket)
		return;
	ass(updating_meta_bucket, "Double finish of update meta bucket");
	updating_meta_bucket = false;
}

Pid TX::get_free_page(Pid contigous_count){
	Pid pa = free_list.get_free_page(this, contigous_count, oldest_reader_tid, updating_meta_bucket);
	if( !pa ){
		if(meta_page.page_count + contigous_count > file_page_count)
			my_db.grow_transaction(this, meta_page.page_count + contigous_count);
		ass(meta_page.page_count + contigous_count <= file_page_count, "grow_transaction failed to increase file size");
		pa = meta_page.page_count;
		meta_page.page_count += contigous_count;
		free_list.add_to_future_from_end_of_file(pa);
	}
	DataPage * new_pa = writable_page(pa, contigous_count);
//	new_pa->pid = pa;
	new_pa->set_tid(meta_page.tid);
	return pa;
}
DataPage * TX::make_pages_writable(Cursor & cur, size_t height){
	Pid old_page = cur.at(height).first;
	const DataPage * dap = readable_page(old_page, 1);
	if( dap->tid() == meta_page.tid ){ // Reached already writable page
		DataPage * wr_dap = writable_page(old_page, 1);
		return wr_dap;
	}
	mark_free_in_future_page(old_page, 1, dap->tid());
	Pid new_page = get_free_page(1);
	for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
		if( c->get_current()->bucket_desc == cur.bucket_desc && c->get_current()->at(height).first == old_page )
			c->get_current()->at(height).first = new_page;
	DataPage * wr_dap = writable_page(new_page, 1);
	memcpy(wr_dap, dap, page_size);
//	wr_dap->pid = new_page;
	wr_dap->set_tid(meta_page.tid);
	if(height == cur.bucket_desc->height){ // node is root
		cur.bucket_desc->root_page = new_page;
		return wr_dap;
	}
	NodePtr wr_parent(page_size, (NodePage *)make_pages_writable(cur, height + 1));
	wr_parent.set_value(cur.at(height + 1).second, new_page);
	return wr_dap;
}
void TX::new_increase_height(Cursor & cur){
	ass(cur.bucket_desc->height + 1 <= MAX_HEIGHT, "Maximum bucket height reached, congratulation!");
	const Pid wr_root_pid = get_free_page(1);
	NodePtr wr_root = writable_node(wr_root_pid);
	cur.bucket_desc->node_page_count += 1;
	wr_root.init_dirty(meta_page.tid);
	Pid previous_root = cur.bucket_desc->root_page;
	wr_root.set_value(-1, previous_root);
	cur.bucket_desc->root_page = wr_root_pid;
	cur.bucket_desc->height += 1;
	for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
		if(c->get_current()->bucket_desc == cur.bucket_desc)
			c->get_current()->at(cur.bucket_desc->height) = std::make_pair(cur.bucket_desc->root_page, -1);
}
static size_t get_item_size_with_insert(const NodePtr & wr_dap, int pos, int insert_pos, size_t required_size1, size_t required_size2){
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
static ValPid get_kv_with_insert(const NodePtr & wr_dap, int pos, int insert_pos, ValPid insert_kv1, ValPid insert_kv2){
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
static void find_best_node_split(int & left_split, int & right_split, const NodePtr & wr_dap, int insert_index, size_t required_size1, size_t required_size2){
	const int size_with_insert = wr_dap.size() + 1 + (required_size2 != 0 ? 1 : 0);
	size_t left_size = get_item_size_with_insert(wr_dap, 0, insert_index, required_size1, required_size2);
	size_t right_size = get_item_size_with_insert(wr_dap, size_with_insert - 1, insert_index, required_size1, required_size2);
	left_split = 1;
	right_split = size_with_insert - 1;
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
			right_size += right_add;
			right_add = get_item_size_with_insert(wr_dap, right_split - 1, insert_index, required_size1, required_size2);
			continue;
		}
		ass(false, "Failed to find node split");
	}
}
void TX::new_insert2node(Cursor & cur, size_t height, ValPid insert_kv1, ValPid insert_kv2){
	auto path_el = cur.at(height);
	NodePtr wr_dap = writable_node(path_el.first);
	const size_t required_size1 = get_item_size(page_size, insert_kv1.key, insert_kv1.pid);
	const size_t required_size2 = insert_kv2.key.data ? get_item_size(page_size, insert_kv2.key, insert_kv2.pid) : 0;
	if( wr_dap.free_capacity() >= required_size1 + required_size2 ){
		wr_dap.insert_at(path_el.second, insert_kv1.key, insert_kv1.pid);
		if(insert_kv2.key.data)
			wr_dap.insert_at(path_el.second + 1, insert_kv2.key, insert_kv2.pid);
		return;
	}
	const int size_with_insert = wr_dap.size() + 1 + (required_size2 != 0 ? 1 : 0);
	ass(size_with_insert >= 3, "Cannot split node with <3 keys");
	if(cur.bucket_desc->height == height)
		new_increase_height(cur);
	path_el = cur.at(height); // Could change in increase height
	auto path_pa = cur.at(height + 1);
	const int insert_index = path_el.second;
	int left_split = 0, right_split = 0;
	find_best_node_split(left_split, right_split, wr_dap, insert_index, required_size1, required_size2);
	// We must leave at least 1 key to the left and to the right
	if( BULK_LOADING && insert_index == wr_dap.size() ){ // Bulk loading?
		bool right_sibling = false; // No right sibling when inserting into root node
		if( cur.bucket_desc->height > height ){
//			auto & path_pa = cur.at(height + 1);
			CNodePtr wr_parent = readable_node(path_pa.first);
			right_sibling = path_pa.second + 1 < wr_parent.size();
		}
		if( !right_sibling) {
			right_split = size_with_insert - 1; // Insert at inset_index would lead to empty right node
			left_split = right_split - 1;
		}
	}
	const Pid wr_right_pid = get_free_page(1);
	NodePtr wr_right = writable_node(wr_right_pid);
	cur.bucket_desc->node_page_count += 1;
	wr_right.init_dirty(meta_page.tid);
	for(int i = right_split; i != size_with_insert; ++i)
		wr_right.append(get_kv_with_insert(wr_dap, i, insert_index, insert_kv1, insert_kv2));
	for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
		c->get_current()->on_insert(cur.bucket_desc, height + 1, path_pa.first, path_pa.second + 1);
		c->get_current()->on_split(cur.bucket_desc, height, path_el.first, wr_right_pid, left_split, 1); // !!!
	}
	ValPid split_kv = get_kv_with_insert(wr_dap, left_split, insert_index, insert_kv1, insert_kv2);
	wr_right.set_value(-1, split_kv.pid);
	cur.at(height + 1).second = path_pa.second + 1; // original item.
	cur.debug_set_truncated_validity_guard();
	new_insert2node(cur, height + 1, ValPid(split_kv.key, wr_right_pid));
	// split_kv will not be valid after code below
	{
//		NodePtr my_copy = push_tmp_copy(wr_dap.page);
//		wr_dap.clear();
//		for(int i = 0; i != left_split; ++i)
//			wr_dap.append(get_kv_with_insert(my_copy, i, insert_index, insert_kv1, insert_kv2));
//		wr_dap.set_value(-1, my_copy.get_value(-1));
		int insert_counter = (left_split > insert_index) ? 1 : 0;
		if(insert_kv2.key.data)
			insert_counter += (left_split > insert_index + 1) ? 1 : 0;
		wr_dap.erase(left_split - insert_counter, wr_dap.size());
		if(insert_counter > 0)
			wr_dap.insert_at(insert_index, insert_kv1);
		if(insert_counter > 1)
			wr_dap.insert_at(insert_index + 1, insert_kv2);
	}
}
static size_t get_item_size_with_insert(const LeafPtr & wr_dap, int pos, int insert_pos, size_t required_size){
	if(pos == insert_pos)
		return required_size;
	if(pos > insert_pos)
		pos -= 1;
	return wr_dap.get_item_size(pos);
}
static ValVal get_kv_with_insert(const LeafPtr & wr_dap, int pos, int insert_pos){
	ass(pos != insert_pos, "Insert2Leaf does not have data for replace item");
	if(pos > insert_pos)
		pos -= 1;
	Pid op;
	return wr_dap.get_kv(pos, op);
}
char * TX::new_insert2leaf(Cursor & cur, Val insert_key, size_t insert_value_size, bool * overflow){
	auto path_el = cur.at(0);
	LeafPtr wr_dap = writable_leaf(path_el.first);
	const size_t required_size = wr_dap.get_item_size(insert_key, insert_value_size, *overflow);
	if( required_size <= wr_dap.free_capacity() ) {
		return wr_dap.insert_at(path_el.second, insert_key, insert_value_size, *overflow);
	}
	if(cur.bucket_desc->height == 0)
		new_increase_height(cur);
	path_el = cur.at(0); // Could change in increase height
	auto path_pa = cur.at(1);
	size_t left_size = 0;
	size_t right_size = 0;
	const int size_with_insert = wr_dap.size() + 1;
	const int insert_index = path_el.second;
	int left_split = 0;
	int right_split = size_with_insert;
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
		if( cur.bucket_desc->height > 0 ){
//			auto & path_pa = cur.at(1);
			CNodePtr wr_parent = readable_node(path_pa.first);
			right_sibling = path_pa.second + 1 < wr_parent.size();
		}
		if( !right_sibling)
			right_split = left_split = size_with_insert - 1;
	}
	const Pid wr_right_pid = get_free_page(1);
	LeafPtr wr_right = writable_leaf(wr_right_pid);
	cur.bucket_desc->leaf_page_count += 1;
	wr_right.init_dirty(meta_page.tid);
	char * result = nullptr;
	for(int i = right_split; i != size_with_insert; ++i)
		if( i == insert_index)
			result = wr_right.insert_at(wr_right.size(), insert_key, insert_value_size, *overflow);
		else
			wr_right.append(get_kv_with_insert(wr_dap, i, insert_index));
	for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
		c->get_current()->on_insert(cur.bucket_desc, 1, path_pa.first, path_pa.second + 1);
		c->get_current()->on_split(cur.bucket_desc, 0, path_el.first, wr_right_pid, right_split, 0);
	}
	Pid wr_middle_pid = 0;
	LeafPtr wr_middle;
	if(left_split + 1 == right_split){
		wr_middle_pid = get_free_page(1);
		wr_middle = writable_leaf(wr_middle_pid);
		cur.bucket_desc->leaf_page_count += 1;
		wr_middle.init_dirty(meta_page.tid);
		if( left_split == insert_index)
			result = wr_middle.insert_at(wr_middle.size(), insert_key, insert_value_size, *overflow);
		else
			wr_middle.append(get_kv_with_insert(wr_dap, left_split, insert_index));
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
			c->get_current()->on_insert(cur.bucket_desc, 1, path_pa.first, path_pa.second + 1);
			c->get_current()->on_split(cur.bucket_desc, 0, path_el.first, wr_middle_pid, left_split, 0);
		}
	}
	{
//		LeafPtr my_copy = push_tmp_copy(wr_dap.page);
//		wr_dap.clear();
//		for(int i = 0; i != left_split; ++i)
//			if( i == insert_index)
//				result = wr_dap.insert_at(wr_dap.size(), insert_key, insert_value_size, *overflow);
//			else
//				wr_dap.append(get_kv_with_insert(my_copy, i, insert_index));
		if(insert_index >= left_split)
			wr_dap.erase(left_split, wr_dap.size());
		else{
			wr_dap.erase(left_split - 1, wr_dap.size());
			result = wr_dap.insert_at(insert_index, insert_key, insert_value_size, *overflow);
		}
	}
	Cursor truncated_validity(cur); // truncated_validity will not be valid below height
	truncated_validity.at(1).second = path_pa.second + 1; // original item
	truncated_validity.debug_set_truncated_validity_guard();
	if(left_split + 1 == right_split){
		new_insert2node(truncated_validity, 1, ValPid(wr_middle.get_key(0), wr_middle_pid), ValPid(wr_right.get_key(0), wr_right_pid));
	}else{
		new_insert2node(truncated_validity, 1, ValPid(wr_right.get_key(0), wr_right_pid));
	}
	return result;
}
void TX::new_merge_node(Cursor & cur, size_t height, NodePtr wr_dap){
	if( wr_dap.data_size() >= wr_dap.capacity()/2 )
		return;
	if(height == cur.bucket_desc->height){ // merging root
		if( wr_dap.size() != 0) // wait until only key at -1 offset remains, make it new root
			return;
		const DataPage * dap = readable_page(cur.bucket_desc->root_page, 1);
		mark_free_in_future_page(cur.bucket_desc->root_page, 1, dap->tid());
		cur.bucket_desc->node_page_count -= 1;
		cur.bucket_desc->root_page = wr_dap.get_value(-1);
		cur.bucket_desc->height -= 1;
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
			if( c->get_current()->bucket_desc == cur.bucket_desc)
				c->get_current()->at(height) = std::make_pair(0, 0);
		return;
	}
	auto path_el = cur.at(height);
	auto path_pa = cur.at(height + 1);
	NodePtr wr_parent = writable_node(path_pa.first);
	ass(wr_parent.size() > 0, "Found parent node with 0 items");
	ValPid my_kv;
	
	Pid left_sib_pid = 0;
	CNodePtr left_sib;
	bool use_left_sib = false;
	size_t left_data_size = 0;
	CNodePtr right_sib;
	bool use_right_sib = false;
	size_t right_data_size = 0;
	ValPid right_kv;
	if( path_pa.second != -1){
		my_kv = wr_parent.get_kv(path_pa.second);
		ass(my_kv.pid == path_el.first, "merge_if_needed_node my pid in parent does not match");
		left_sib_pid = wr_parent.get_value(path_pa.second - 1);
		left_sib = readable_node(left_sib_pid);
		left_data_size = left_sib.data_size();
		left_data_size += get_item_size(page_size, my_kv.key, Pid{}); // will need to insert key from parent. Achtung - 0 works only when fixed-size pids are used
		use_left_sib = left_data_size <= wr_dap.free_capacity();
	}
	if(path_pa.second + 1 < wr_parent.size()){
		right_kv = wr_parent.get_kv(path_pa.second + 1);
		right_sib = readable_node(right_kv.pid);
		right_data_size = right_sib.data_size();
		right_data_size += get_item_size(page_size, right_kv.key, Pid{}); // will need to insert key from parent! Achtung - 0 works only when fixed-size pids are used
		use_right_sib = right_data_size <= wr_dap.free_capacity();
	}
	if( use_left_sib && use_right_sib && wr_dap.free_capacity() < left_data_size + right_data_size ){ // If cannot merge both, select smallest
		if( left_data_size < right_data_size ) // <= will also work
			use_right_sib = false;
		else
			use_left_sib = false;
	}
	if( wr_dap.size() == 0 && !use_left_sib && !use_right_sib) { // Cannot merge, siblings are full and do not fit key from parent, so we borrow!
//		std::cerr << "Borrowing key from sibling" << std::endl;
		ass(left_sib.page || right_sib.page, "Cannot borrow - no siblings for node with 0 items" );
		if(left_sib.page && right_sib.page){
			if(left_sib.data_size() < right_sib.data_size())
				right_sib = CNodePtr();
			else
				left_sib = CNodePtr();
			// TODO idea - zero one that does not fit in parent after rotation
		}
		if(left_sib.page){
			Cursor cur2(cur);
			cur2.at(height + 1).second -= 1;
			cur2.at(height) = std::make_pair(left_sib_pid, -1);
			cur2.debug_set_truncated_validity_guard();
			NodePtr wr_left(page_size, (NodePage *)make_pages_writable(cur2, height));
			const Pid wr_left_pid = cur2.at(height).first;

			const size_t required_size1 = get_item_size(page_size, my_kv.key, my_kv.pid);
			int left_split = 0, right_split = 0;
			find_best_node_split(left_split, right_split, wr_left, wr_left.size(), required_size1, 0);
			for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
				c->get_current()->on_insert(cur.bucket_desc, height, path_el.first, -1, wr_left.size() - right_split + 1);
				c->get_current()->on_rotate_right(cur.bucket_desc, height, wr_left_pid, path_el.first, left_split);
			}
			wr_dap.insert_at(0, my_kv.key, wr_dap.get_value(-1)); // on_insert
			wr_dap.insert_range(0, wr_left, right_split, wr_left.size()); // on_insert
			auto split_kv = wr_left.get_kv(left_split);
			wr_dap.set_value(-1, split_kv.pid);
			wr_parent.erase(path_pa.second);
			Cursor truncated_validity(cur); // truncated_validity will not be valid below height
			truncated_validity.debug_set_truncated_validity_guard();
			new_insert2node(truncated_validity, height + 1, ValPid(split_kv.key, path_el.first)); // split_kv is only valid here
			wr_left.erase(left_split, wr_left.size());
		}else{
			Cursor cur2(cur); // This cursor will be invalid below height
			cur2.at(height + 1).second += 1;
			cur2.at(height) = std::make_pair(right_kv.pid, right_sib.size() - 1);
			cur2.debug_set_truncated_validity_guard();
			NodePtr wr_right(page_size, (NodePage *)make_pages_writable(cur2, height));
			const Pid wr_right_pid = cur2.at(height).first;

			const size_t required_size1 = get_item_size(page_size, right_kv.key, right_kv.pid);
			int left_split = 0, right_split = 0;
			find_best_node_split(left_split, right_split, wr_right, 0, required_size1, 0);
			for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
				c->get_current()->on_rotate_left(cur.bucket_desc, height, wr_right_pid, path_el.first, left_split - 1);
				c->get_current()->on_erase(cur.bucket_desc, height, wr_right_pid, -1, left_split);
			}
			wr_dap.insert_at(0, right_kv.key, wr_right.get_value(-1));
			wr_dap.insert_range(1, wr_right, 0, left_split - 1);
			auto split_kv = wr_right.get_kv(left_split - 1);
			wr_right.set_value(-1, split_kv.pid);
			wr_parent.erase(path_pa.second + 1);
			new_insert2node(cur2, height + 1, ValPid(split_kv.key, wr_right_pid)); // split_kv is only valid here
			wr_right.erase(0, right_split - 1);
		}
		return;
	}
//	if( use_left_sib && use_right_sib )
//		std::cerr << "3-way merge" << std::endl;
	if( use_left_sib ){
		Pid spec_val = wr_dap.get_value(-1);
		wr_dap.insert_at(0, my_kv.key, spec_val);
		wr_dap.set_value(-1, left_sib.get_value(-1));
		wr_dap.insert_range(0, left_sib, 0, left_sib.size());
		mark_free_in_future_page(left_sib_pid, 1, left_sib.page->tid()); // unlink left, point its slot in parent to us, remove our slot in parent
		cur.bucket_desc->node_page_count -= 1;
		wr_parent.erase(path_pa.second);
		wr_parent.set_value(path_pa.second - 1, path_el.first);
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
			c->get_current()->on_erase(cur.bucket_desc, height + 1, path_pa.first, path_pa.second - 1);
			c->get_current()->on_insert(cur.bucket_desc, height, path_el.first, -1, left_sib.size() + 1);
			c->get_current()->on_merge(cur.bucket_desc, height, left_sib_pid, path_el.first, 0);
		}
		path_el = cur.at(height); // path_pa was modified by code above
		path_pa = cur.at(height + 1); // path_pa was modified by code above
	}
	if( use_right_sib ){
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
			c->get_current()->on_erase(cur.bucket_desc, height + 1, path_pa.first, path_pa.second);
			c->get_current()->on_merge(cur.bucket_desc, height, right_kv.pid, path_el.first, wr_dap.size() + 1);
		}
		Pid spec_val = right_sib.get_value(-1);
		wr_dap.append(right_kv.key, spec_val);
		wr_dap.append_range(right_sib, 0, right_sib.size());
		mark_free_in_future_page(right_kv.pid, 1, right_sib.page->tid()); // unlink right, remove its slot in parent
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
	auto path_el = cur.at(0);
	auto path_pa = cur.at(1);
	NodePtr wr_parent = writable_node(path_pa.first);
	Pid left_sib_pid = 0;
	CLeafPtr left_sib;
	Pid right_sib_pid = 0;
	CLeafPtr right_sib;
	if( path_pa.second != -1){
		left_sib_pid = wr_parent.get_value(path_pa.second - 1);
		left_sib = readable_leaf(left_sib_pid);
		if( wr_dap.free_capacity() < left_sib.data_size() )
			left_sib = CLeafPtr(); // forget about left!
	}
	if( path_pa.second + 1 < wr_parent.size()){
		right_sib_pid = wr_parent.get_value(path_pa.second + 1);
		right_sib = readable_leaf(right_sib_pid);
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
		//            std::cerr << "We could optimize by unlinking our leaf" << std::endl;
	}
//	if( left_sib.page && right_sib.page )
//		std::cerr << "3-way merge" << std::endl;
	if( left_sib.page ){
		wr_dap.insert_range(0, left_sib, 0, left_sib.size());
		mark_free_in_future_page(left_sib_pid, 1, left_sib.page->tid()); // unlink left, point its slot in parent to us, remove our slot in parent
		cur.bucket_desc->leaf_page_count -= 1;
		wr_parent.erase(path_pa.second);
		wr_parent.set_value(path_pa.second - 1, path_el.first);
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
			c->get_current()->on_erase(cur.bucket_desc, 1, path_pa.first, path_pa.second - 1);
			c->get_current()->on_insert(cur.bucket_desc, 0, path_el.first, 0, left_sib.size());
			c->get_current()->on_merge(cur.bucket_desc, 0, left_sib_pid, path_el.first, 0);
		}
		path_el = cur.at(0); // path_pa was modified by code above
		path_pa = cur.at(1); // path_pa was modified by code above
	}
	if( right_sib.page ){
		for(IntrusiveNode<Cursor> * c = &my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors)){
			c->get_current()->on_erase(cur.bucket_desc, 1, path_pa.first, path_pa.second);
			c->get_current()->on_merge(cur.bucket_desc, 0, right_sib_pid, path_el.first, wr_dap.size());
		}
		wr_dap.append_range(right_sib, 0, right_sib.size());
		mark_free_in_future_page(right_sib_pid, 1, right_sib.page->tid()); // unlink right, remove its slot in parent
		cur.bucket_desc->leaf_page_count -= 1;
		wr_parent.erase(path_pa.second + 1);
	}
	if( left_sib.page || right_sib.page )
		new_merge_node(cur, 1, wr_parent);
}
void TX::commit(){
	if(read_only)
		return;
	if( meta_page_dirty ) {
		Bucket meta_bucket = get_meta_bucket();
		for (auto &&tit : bucket_descs) { // First write all dirty table descriptions
			CLeafPtr dap = readable_leaf(tit.second.root_page);
			if (dap.page->tid() != meta_page.tid) // Table not dirty
				continue;
			std::string key = bucket_prefix + tit.first;
			char buf[sizeof(BucketDesc)];
			Val value(buf, sizeof(BucketDesc));
			tit.second.pack(buf, sizeof(BucketDesc));
			ass(meta_bucket.put(Val(key), value, false), "Writing table desc failed during commit");
		}
		free_list.commit_free_pages(this);
		my_db.commit_transaction(this, meta_page);
	}
	meta_page_dirty = false;
}
void TX::unlink_buckets_and_cursors(){
	// Now invalidate all cursors and buckets
	while(!my_cursors.is_end()){
		Cursor * c = my_cursors.get_current();
		c->my_txn = nullptr;
		c->bucket_desc = nullptr;
		c->tx_cursors.unlink(&Cursor::tx_cursors);
	}
	while(!my_buckets.is_end()){
		Bucket * c = my_buckets.get_current();
		c->my_txn = nullptr;
		c->bucket_desc = nullptr;
		c->tx_buckets.unlink(&Bucket::tx_buckets);
	}
	bucket_descs.clear();
}

void TX::rollback(){
	if(read_only)
		return;
	free_list.clear();
	meta_page_dirty = false;
	my_db.finish_transaction(this);
	unlink_buckets_and_cursors();
	my_db.start_transaction(this);
	if(DEBUG_MIRROR)
		load_mirror();
}

//{'branch_pages': 1040L,
//    'depth': 4L,
//    'entries': 3761848L,
//    'leaf_pages': 73658L,
//    'overflow_pages': 0L,
//    'psize': 4096L}
std::vector<Val> TX::get_bucket_names(){
	std::vector<Val> results;
	Cursor cur(this, &meta_page.meta_bucket, Val{});
	Val c_key, c_value, c_tail;
	const Val prefix(&bucket_prefix, 1);
	for(cur.seek(prefix); cur.get(&c_key, &c_value) && c_key.has_prefix(prefix, &c_tail); cur.next()){
		Val persistent_name;
		load_bucket_desc(c_tail, &persistent_name, false);
		results.push_back(persistent_name);
	}
	return results;
}
//	bool TX::create_table(const Val & table){
//		if( load_table_desc(table) )
//			return false;
//		return true;
//	}
Bucket TX::get_bucket(const Val & name, bool create_if_not_exists){
	Val persistent_name;
	BucketDesc * bucket_desc = load_bucket_desc(name, &persistent_name, create_if_not_exists);
	ass(!DEBUG_MIRROR || (debug_mirror.count(name.to_string()) != 0) == (bucket_desc != 0), "mirror violation in get_bucket");
	return Bucket(this, bucket_desc, persistent_name);
}

bool TX::drop_bucket(const Val & name){
	if( read_only )
		throw Exception("Attempt to modify read-only transaction");
	Val persistent_name;
	BucketDesc * bucket_desc = load_bucket_desc(name, &persistent_name, false);
	if(DEBUG_MIRROR){
		ass(debug_mirror.count(name.to_string()) == (bucket_desc != 0), "mirror violation in drop_bucket");
		before_mirror_operation();
	}
	if( !bucket_desc ){
		return false;
	}
	{
		// TODO - delete page by page
		Cursor cursor(this, bucket_desc, persistent_name);
		cursor.first();
		while(bucket_desc->count != 0){
			cursor.del();
		}
		ass(bucket_desc->leaf_page_count == 1 && bucket_desc->node_page_count == 0 && bucket_desc->overflow_page_count == 0 && bucket_desc->height == 0, "Bucket in wrong state after deleting all keys");
		const DataPage * dap = readable_page(bucket_desc->root_page, 1);
		mark_free_in_future_page(bucket_desc->root_page, 1, dap->tid());
	}
	for(IntrusiveNode<Cursor> * cit = &my_cursors; !cit->is_end();){
		Cursor * c = cit->get_current();
		if( c->bucket_desc == bucket_desc ){
			c->my_txn = nullptr;
			c->bucket_desc = nullptr;
			cit->unlink(&Cursor::tx_cursors);
		}else
			cit = cit->get_next(&Cursor::tx_cursors);
	}
	if(DEBUG_MIRROR)
		ass(debug_mirror.erase(name.to_string()) != 0, "inconsistency with mirror in drop_bucket");
	for(IntrusiveNode<Bucket> * cit = &my_buckets; !cit->is_end();){
		Bucket * c = cit->get_current();
		if( c->bucket_desc == bucket_desc ){
			c->my_txn = nullptr;
			c->bucket_desc = nullptr;
			cit->unlink(&Bucket::tx_buckets);
		}else
			cit = cit->get_next(&Bucket::tx_buckets);
	}
	std::string key = bucket_prefix + name.to_string();
	Bucket meta_bucket = get_meta_bucket();
	ass(meta_bucket.del(Val(key)), "Error while dropping table");
	ass(bucket_descs.erase(name.to_string()) == 1, "bucket_desc not found during erase");
	return true;
}
BucketDesc * TX::load_bucket_desc(const Val & name, Val * persistent_name, bool create_if_not_exists){
	const std::string str_name = name.to_string();
	auto tit = bucket_descs.find(str_name);
	if( tit != bucket_descs.end() ){
		*persistent_name = Val(tit->first);
		return &tit->second;
	}
	const std::string key = bucket_prefix + str_name;
	Val value;
	Bucket meta_bucket = get_meta_bucket();
	if( meta_bucket.get(Val(key), &value) ){
		tit = bucket_descs.insert(std::make_pair(name.to_string(), BucketDesc{})).first;
		*persistent_name = Val(tit->first);
		tit->second.unpack(value.data, value.size);
		return &tit->second;
	}
	if(!create_if_not_exists)
		return nullptr;
	if( read_only )
		throw Exception("Attempt to modify read-only transaction");
	if(DEBUG_MIRROR){
		ass(debug_mirror.insert(std::make_pair(name.to_string(), BucketMirror{})).second, "mirror violation in load_bucket");
		before_mirror_operation();
	}
	tit = bucket_descs.insert(std::make_pair(str_name, BucketDesc{})).first;
	*persistent_name = Val(tit->first);
	tit->second.root_page = get_free_page(1);
	LeafPtr wr_root = writable_leaf(tit->second.root_page);
	wr_root.init_dirty(meta_page.tid);
	tit->second.leaf_page_count = 1;
	char buf[sizeof(BucketDesc)];
	value = Val(buf, sizeof(BucketDesc));
	tit->second.pack(buf, sizeof(BucketDesc));
	ass(meta_bucket.put(Val(key), value, true), "Writing table desc failed during bucket creation");
	return &tit->second;
}
Bucket TX::get_meta_bucket(){
	return Bucket(this, &meta_page.meta_bucket);
}

void TX::check_bucket(BucketDesc * bucket_desc, MergablePageCache * pages){
	Pid pa = bucket_desc->root_page;
	size_t height = bucket_desc->height;
	BucketDesc stat_bucket_desc{};
	std::string largest_element(my_db.max_key_size(), char(0xFF));
	check_bucket_page(bucket_desc, &stat_bucket_desc, pa, height, Val(), Val(largest_element), pages);
	ass(stat_bucket_desc.count == bucket_desc->count && stat_bucket_desc.leaf_page_count == bucket_desc->leaf_page_count &&
		stat_bucket_desc.node_page_count == bucket_desc->node_page_count && stat_bucket_desc.overflow_page_count == bucket_desc->overflow_page_count, "Bucket stats differ");
}
void TX::check_bucket_page(const BucketDesc * bucket_desc, BucketDesc * stat_bucket_desc, Pid pa, size_t height, Val left_limit, Val right_limit, MergablePageCache * pages){
	pages->add_to_cache(pa, 1);
	if( height == 0 ){
		stat_bucket_desc->leaf_page_count += 1;
		CLeafPtr dap = readable_leaf(pa);
		ass(bucket_desc->height == 0 || dap.size() > 0, "leaf with 0 keys found");
		stat_bucket_desc->count += static_cast<size_t>(dap.size());
		Val prev_key;
		for(int pi = 0; pi != dap.size(); ++pi){
			Pid overflow_page = 0;
			ValVal val = dap.get_kv(pi, overflow_page);
			if( overflow_page != 0 ){
				Pid overflow_count = (val.value.size + page_size - 1) / page_size;
				stat_bucket_desc->overflow_page_count += overflow_count;
				pages->add_to_cache(overflow_page, overflow_count);
			}
			if( pi == 0)
				ass(val.key >= left_limit, "first leaf element < left_limit");
			else
				ass(val.key > prev_key, "leaf elements are in wrong order");
			prev_key = val.key;
		}
		ass(stat_bucket_desc->count == bucket_desc->count || prev_key < right_limit, "last leaf element >= right_limit");
		// last element is the only one allowed to be equal to largest possible key
		return;
	}
	stat_bucket_desc->node_page_count += 1;
	CNodePtr nap = readable_node(pa);
	ass(nap.size() > 0, "node with 0 keys found");
	for(int pi = -1; pi != nap.size(); ++pi){
		Val prev_limit = (pi == -1) ? left_limit : nap.get_key(pi);
		Val next_limit = (pi + 1 < nap.size()) ? nap.get_key(pi + 1) : right_limit;
		ass(prev_limit < next_limit, "node with wrong keys order found");
		check_bucket_page(bucket_desc, stat_bucket_desc, nap.get_value(pi), height - 1, prev_limit, next_limit, pages);
	}
}
void TX::check_database(std::function<void(int percent)> on_progress, bool verbose){
	MergablePageCache pages(false);
	free_list.get_all_free_pages(this, &pages);
	if (verbose) {
		std::cerr << "Free Pages" << std::endl;
		pages.debug_print_db();
	}

	MergablePageCache meta_pages(false);
	Bucket meta_bucket = get_meta_bucket();
	check_bucket(meta_bucket.bucket_desc, &meta_pages);

	if (verbose) {
        std::cerr << "Meta pages " << std::endl;
        meta_pages.debug_print_db();
	}
	pages.merge_from(meta_pages);
	
	for(auto bname : get_bucket_names()){
		MergablePageCache busy_pages(false);
		Bucket bucket = get_bucket(bname);
		check_bucket(bucket.bucket_desc, &busy_pages);
		if (verbose) {
            std::cerr << "Pages from " << bname.to_string() << std::endl;
            busy_pages.debug_print_db();
		}
		pages.merge_from(busy_pages);
	}
	if (verbose) {
        std::cerr << "All pages " << std::endl;
        pages.debug_print_db();
	}
	Pid remaining_pages = meta_page.page_count - pages.defrag_end(meta_page.page_count);
	ass(pages.empty(), "After defrag free pages left");
	ass(remaining_pages == META_PAGES_COUNT, "There should be exactly meta pages count left after removing everything from database");
}

static std::string trim_key(const Val & key, bool parse_meta){
	Tid next_record_tid;
	uint64_t next_record_batch;
	if( parse_meta && FreeList::parse_free_record_key(key, &next_record_tid, &next_record_batch) )
		return "f" + std::to_string(next_record_tid) + ":" + std::to_string(next_record_batch);
	std::string result;
	for(auto && ch : key.to_string())
		if( std::isprint(ch) && ch != '"' && ch != '\\' && ch != '<' && ch != '>' && ch != '{')
			result += ch;
		else
			result += std::string("$") + "0123456789abcdef"[static_cast<unsigned char>(ch) >> 4] + "0123456789abcdef"[static_cast<unsigned char>(ch) & 0xf];
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
		std::cerr << "Leaf pid=" << pid << " [";
		for(int i = 0; i != dap.size(); ++i){
			if( i != 0)
				result += ",";
			Pid overflow_page;
			auto kv = dap.get_kv(i, overflow_page);
			if( overflow_page ){
				Pid overflow_count = (kv.value.size + page_size - 1)/page_size;
				kv.value.data = readable_overflow(overflow_page, overflow_count);
			}
			//                std::cerr << kv.key.to_string() << ":" << kv.value.to_string() << ", ";
			std::cerr << trim_key(kv.key, parse_meta) << "(" << kv.value.size << ")" << (overflow_page ? "->" + std::to_string(overflow_page) : "") << ", ";
			result += "\"" + trim_key(kv.key, parse_meta) + "(" + std::to_string(kv.value.size) + ")\""; //  + ":" + value.to_string() +
		}
		std::cerr << "]" << std::endl;
		return result + "]}";
	}
	CNodePtr nap = readable_node(pid);
	Pid spec_value = nap.get_value(-1);
	std::cerr << "Node pid=" << pid << " [" << spec_value << ", ";
	for(int i = 0; i != nap.size(); ++i){
		if( i != 0)
			result += ",";
		ValPid va = nap.get_kv(i);
		std::cerr << trim_key(va.key, parse_meta) << "-" << va.pid << ", ";
		if( i == 0)
			result += "\"" + std::to_string(spec_value) + " ";
		else
			result += "\"";
		result += trim_key(va.key, parse_meta) + " " + std::to_string(va.pid) + "\"";
	}
	if(nap.size() == 0)
		result += "\"" + std::to_string(spec_value) + "\"";
	result += "],\"children\":[";
	std::cerr << "]" << std::endl;
	std::string spec_str = print_db(spec_value, height - 1, parse_meta);
	result += spec_str;
	for(int i = 0; i != nap.size(); ++i){
		ValPid va = nap.get_kv(i);
		std::string str = print_db(va.pid, height - 1, parse_meta);
		result += "," + str;
	}
	return result + "]}";
}
std::string TX::debug_print_meta_db(){
	return get_meta_bucket().debug_print_db();
}
std::string TX::get_meta_stats(){
	return get_meta_bucket().get_stats();
}
void TX::before_mirror_operation(){
	debug_mirror_counter += 1;
//	if( debug_mirror_counter == 115)
//		std::cerr << "before_mirror_operation" << std::endl;
}
void TX::load_mirror(){
	debug_mirror.clear();
	for(auto bn : get_bucket_names()){
		ass(debug_mirror.count(bn.to_string()) == 0, "Duplicate bucket name in mirror");
		auto & part = debug_mirror[bn.to_string()];
		mustela::Bucket bucket = get_bucket(bn);
		mustela::Cursor cur = bucket.get_cursor();
		mustela::Val c_key, c_value;
		for (cur.first(); cur.get(&c_key, &c_value); cur.next())
			ass(part.insert(std::make_pair(c_key.to_string(), std::make_pair(c_value.to_string(), cur))).second, "BAD mirror insert");
		BucketMirror part2_back;
		for (cur.last(); cur.get(&c_key, &c_value); cur.prev())
			ass(part2_back.insert(std::make_pair(c_key.to_string(), std::make_pair(c_value.to_string(), cur))).second, "BAD mirror insert");
		ass(part == part2_back, "Inconsistent forward/backward iteration");
	}
}
void TX::check_mirror(){
	for(auto && part : debug_mirror){
		mustela::Bucket bucket = get_bucket(Val(part.first), false);
		for(auto && ma : part.second){
			mustela::Val value, c_key, c_value;
			bool result = bucket.get(mustela::Val(ma.first), &value);
			ma.second.second.debug_check_cursor_path_up();
			bool c_result = ma.second.second.get(&c_key, &c_value);
			if( !result || !c_result || c_key.to_string() != ma.first || value.to_string() != ma.second.first || c_value.to_string() != ma.second.first ){
				std::string json = bucket.debug_print_db();
				std::cerr << "Main table: " << json << std::endl;
				ass(false, "Bad mirror check ma");
			}
		}
	}
}
