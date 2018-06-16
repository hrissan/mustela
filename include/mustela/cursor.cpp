#include "mustela.hpp"
#include <iostream>

using namespace mustela;
	
Cursor::Cursor(TX * my_txn, BucketDesc * bucket_desc, Val name):my_txn(my_txn), bucket_desc(bucket_desc), persistent_name(name){
	ass(my_txn && bucket_desc, "get_cursor called on invalid bucket");
    my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
	before_first();
}
Cursor::~Cursor(){
	unlink();
}
void Cursor::unlink(){
	tx_cursors.unlink(&Cursor::tx_cursors);
	my_txn = nullptr;
	bucket_desc = nullptr;
	persistent_name = Val{};
}
Cursor::Cursor(Cursor && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), persistent_name(other.persistent_name), path(std::move(other.path)){
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
}
Cursor::Cursor(const Cursor & other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), persistent_name(other.persistent_name), path(other.path){
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
}
Cursor & Cursor::operator=(Cursor && other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	persistent_name = other.persistent_name;
	path = std::move(other.path);
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
	return *this;
}
Cursor & Cursor::operator=(const Cursor & other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	persistent_name = other.persistent_name;
	path = other.path;
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
	return *this;
}

bool Cursor::operator==(const Cursor & other)const{
	if(my_txn != other.my_txn || bucket_desc != other.bucket_desc)
		return false;
	if(!is_valid()) // both are invalid here, and invalid cursors are equal
		return true;
	if(is_before_first() && other.is_before_first() )
		return true;
	if(is_before_first() != other.is_before_first() )
		return false;
	bool end1 = const_cast<Cursor &>(*this).fix_cursor_after_last_item();
	bool end2 = const_cast<Cursor &>(other).fix_cursor_after_last_item();
	if(end1 != end2) // fix_cursor returns end for both our end indicators
		return false;
	for(size_t i = 0; i != bucket_desc->height + 1; ++i)
		if(at(i).pid != other.at(i).pid || at(i).item != other.at(i).item)
			return false;
	return true;
}
Bucket Cursor::get_bucket(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	return Bucket(my_txn, bucket_desc, persistent_name);
}

bool Cursor::seek(const Val & key){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	Pid pa = bucket_desc->root_page;
	size_t height = bucket_desc->height;
	while(true){
		if( height == 0 ){
			CLeafPtr dap = my_txn->readable_leaf(pa);
			bool found;
			int item = dap.lower_bound_item(key, &found);
			at(height) = Element{pa, item};
			return found;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		int nitem = nap.upper_bound_item(key) - 1;
		at(height) = Element{pa, nitem};
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
bool Cursor::fix_cursor_after_last_item(){
	if( is_before_first() )
		return false;
	auto path_el = at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.pid);
	if(path_el.item < dap.size())
		return true;
	ass(path_el.item == dap.size(), "Cursor corrupted at Cursor::fix_cursor_after_last_item");
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == bucket_desc->height + 1 )
			return false;
		CNodePtr nap = my_txn->readable_node(at(height).pid);
		if( at(height).item + 1 < nap.size() ){
			at(height).item += 1;
			pa = nap.get_value(at(height).item);
			height -= 1;
			break;
		}
		height += 1;
	}
	set_at_direction(height, pa, -1);
	return true;
}
void Cursor::set_at_direction(size_t height, Pid pa, int dir){
	while(true){
		if( height == 0 ){
			CLeafPtr dap = my_txn->readable_leaf(pa);
			ass(bucket_desc->item_count == 0 || dap.size() > 0, "Empty leaf page in Cursor::set_at_direction");
			at(height) = Element{pa, dir > 0 ? dap.size() : 0};
			break;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		int nitem = dir > 0 ? nap.size() - 1 : -1;
		at(height) = Element{pa, nitem};
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
void Cursor::end(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	set_at_direction(bucket_desc->height, bucket_desc->root_page, 1);
}
void Cursor::before_first(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	at(0).pid = 0;
}

void Cursor::first(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	set_at_direction(bucket_desc->height, bucket_desc->root_page, -1);
}
void Cursor::last(){
	end();
	prev();
}
bool Cursor::get(Val * key, Val * value){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( !fix_cursor_after_last_item() )
		return false;
	auto path_el = at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.pid);
	ass( path_el.item < dap.size(), "fix_cursor_after_last_item failed at Cursor::get" );
	Pid overflow_page;
	auto kv = dap.get_kv(path_el.item, overflow_page);
	if( overflow_page ){
		Pid overflow_count = (kv.value.size + my_txn->page_size - 1)/my_txn->page_size;
		kv.value.data = my_txn->readable_overflow(overflow_page, overflow_count);
	}
	*key = kv.key;
	*value = kv.value;
	return true;
}
bool Cursor::del(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( my_txn->read_only )
		Exception::th("Attempt to modify read-only transaction in Cursor::del");
	if( !fix_cursor_after_last_item() )
		return false;
	if(DEBUG_MIRROR && bucket_desc != &my_txn->meta_page.meta_bucket){
		Val c_key, c_value;
		ass(get(&c_key, &c_value), "cursor get failed in del");
 		auto & part = my_txn->debug_mirror.at(persistent_name.to_string());
 		auto mit = part.find(c_key.to_string());
		ass(mit != part.end(), "inconsistent mirror in cursor del");
		ass(mit->second.first == c_value.to_string(), "inconsistent mirror in cursor del");
		ass(mit->second.second == *this, "inconsistent mirror in cursor del");
		mit = part.erase(mit);
		my_txn->before_mirror_operation(bucket_desc, persistent_name);
	}
	my_txn->meta_page_dirty = true;
	LeafPtr wr_dap(my_txn->page_size, (LeafPage *)my_txn->make_pages_writable(*this, 0));
	auto path_el = at(0);
	ass( path_el.item < wr_dap.size(), "fix_cursor_after_last_item failed at Cursor::del" );
	Pid overflow_page, overflow_count;
	Tid overflow_tid;
	wr_dap.erase(path_el.item, overflow_page, overflow_count, overflow_tid);
	if( overflow_page ) {
		bucket_desc->overflow_page_count -= overflow_count;
		my_txn->mark_free_in_future_page(overflow_page, overflow_count, overflow_tid);
	}
	for(IntrusiveNode<Cursor> * c = &my_txn->my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
		c->get_current()->on_erase(bucket_desc, 0, path_el.pid, path_el.item);
	my_txn->start_update(bucket_desc);
	my_txn->new_merge_leaf(*this, wr_dap);
	my_txn->finish_update(bucket_desc);
	bucket_desc->item_count -= 1;
	if(DEBUG_MIRROR && bucket_desc != &my_txn->meta_page.meta_bucket)
		my_txn->check_mirror();
	return true;
}
void Cursor::next(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( is_before_first())
		return first();
	if( !fix_cursor_after_last_item() )
		return;
	auto & path_el = at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.pid);
	ass( path_el.item < dap.size(), "fix_cursor_after_last_item failed at Cursor::next" );
	path_el.item += 1;
}
void Cursor::prev(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( is_before_first())
		return;
	if( at(0).item > 0 ) {
		// TODO - fast check here
//			CLeafPtr dap = my_txn.readable_leaf(path_el.first);
//			ass(path_el.second > 0 && path_el.second <= dap.size(), "Cursor points beyond last leaf element");
		at(0).item -= 1;
		return;
	}
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == bucket_desc->height + 1 )
			return before_first();
		CNodePtr nap = my_txn->readable_node(at(height).pid);
		if( at(height).item != -1 ){
			at(height).item -= 1;
			pa = nap.get_value(at(height).item);
			height -= 1;
			break;
		}
		height += 1;
	}
	set_at_direction(height, pa, 1);
	ass(at(0).item > 0, "Invalid cursor after set_at_direction in Cursor::prev");
	at(0).item -= 1;
}
void Cursor::debug_make_pages_writable(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( my_txn->read_only )
		Exception::th("Attempt to modify read-only transaction in Cursor::make_pages_writable");
	if( !fix_cursor_after_last_item() )
		return;
	my_txn->meta_page_dirty = true;
	LeafPtr wr_dap(my_txn->page_size, (LeafPage *)my_txn->make_pages_writable(*this, 0));
}

void Cursor::debug_check_cursor_path_up(){
	ass(is_valid(), "Cursor not valid (using after tx commit?)");
	if( is_before_first())
		return;
	for(size_t i = 0; i != bucket_desc->height; ++i){
		auto path_pa = path.at(i+1);
		CNodePtr nap = my_txn->readable_node(path_pa.pid);
		ass(path_pa.item < nap.size(), "check cursor failed");
		Pid pa = nap.get_value(path_pa.item);
		ass(pa == path.at(i).pid, "check cursor failed 2");
	}
}
