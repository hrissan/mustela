#include "mustela.hpp"
#include <iostream>

using namespace mustela;
	
Cursor::Cursor(TX * my_txn, BucketDesc * bucket_desc):my_txn(my_txn), bucket_desc(bucket_desc){
	ass(my_txn && bucket_desc, "get_cursor called on invalid bucket");
    my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
//	path.resize(bucket_desc->height + 1);
	end();
}
//Cursor::Cursor(Bucket & bucket):my_txn(bucket.my_txn), bucket_desc(bucket.bucket_desc){
//	ass(my_txn->my_cursors.insert(this).second, "Double insert");
//	path.resize(bucket_desc->height + 1);
//	end();
//}
Cursor::~Cursor(){
	unlink();
}
void Cursor::unlink(){
//	if(my_txn)
	tx_cursors.unlink(&Cursor::tx_cursors);
	my_txn = nullptr;
	bucket_desc = nullptr;
}
Cursor::Cursor(Cursor && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), path(std::move(other.path)){
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
}
Cursor::Cursor(const Cursor & other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), path(other.path){
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
}
Cursor & Cursor::operator=(Cursor && other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	path = std::move(other.path);
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
	return *this;
}
Cursor & Cursor::operator=(const Cursor & other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	path = other.path;
	if(my_txn)
    	my_txn->my_cursors.insert_after_this(this, &Cursor::tx_cursors);
	return *this;
}

bool Cursor::operator==(const Cursor & other)const{
	if(my_txn != other.my_txn || bucket_desc != other.bucket_desc)
		return false;
	if(!bucket_desc) // invalid cursors are equal
		return true;
	bool end1 = const_cast<Cursor &>(*this).fix_cursor_after_last_item();
	bool end2 = const_cast<Cursor &>(other).fix_cursor_after_last_item();
	if(end1 != end2) // fix_cursor returns end for both our end indicators
		return false;
	return path == other.path; // compare up to the height + 1 only
}

bool Cursor::seek(const Val & key){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	Pid pa = bucket_desc->root_page;
	size_t height = bucket_desc->height;
	while(true){
		if( height == 0 ){
			CLeafPtr dap = my_txn->readable_leaf(pa);
			bool found;
			PageIndex item = dap.lower_bound_item(key, &found);
			at(height) = std::make_pair(pa, item);
			return found;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		PageIndex nitem = nap.upper_bound_item(key) - 1;
		at(height) = std::make_pair(pa, nitem);
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
bool Cursor::fix_cursor_after_last_item(){
	auto path_el = at(0);
	if( path_el.first == 0 ) // fast end indicator
		return false;
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	if(path_el.second < dap.size())
		return true;
	ass(path_el.second == dap.size(), "Cursor corrupted at Cursor::fix_cursor_after_last_item");
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == bucket_desc->height + 1 )
			return false;
		CNodePtr nap = my_txn->readable_node(at(height).first);
		if( at(height).second + 1 < nap.size() ){
			at(height).second += 1;
			pa = nap.get_value(at(height).second);
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
			ass(bucket_desc->count == 0 || dap.size() > 0, "Empty leaf page in Cursor::set_at_direction");
			at(height) = std::make_pair(pa, dir > 0 ? dap.size() : 0);
			break;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		PageIndex nitem = dir > 0 ? nap.size() - 1 : -1;
		at(height) = std::make_pair(pa, nitem);
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
void Cursor::end(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	at(0).first = 0; // fast end indicator
//	set_at_direction(bucket_desc->height, bucket_desc->root_page, 1); <- this sets to true end
}
void Cursor::first(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	set_at_direction(bucket_desc->height, bucket_desc->root_page, -1);
}
void Cursor::last(){
	end();
	prev();
}
bool Cursor::get(Val * key, Val * value){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if( !fix_cursor_after_last_item() )
		return false;
	auto path_el = at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	ass( path_el.second < dap.size(), "fix_cursor_after_last_item failed at Cursor::get" );
	Pid overflow_page;
	auto kv = dap.get_kv(path_el.second, overflow_page);
	if( overflow_page ){
		Pid overflow_count = (kv.value.size + my_txn->page_size - 1)/my_txn->page_size;
		kv.value.data = my_txn->readable_overflow(overflow_page, overflow_count);
	}
	*key = kv.key;
	*value = kv.value;
	return true;
}
bool Cursor::del(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if( my_txn->read_only )
		throw Exception("Attempt to modify read-only transaction in Cursor::del");
	if( !fix_cursor_after_last_item() )
		return false;
	my_txn->meta_page_dirty = true;
	LeafPtr wr_dap(my_txn->page_size, (LeafPage *)my_txn->make_pages_writable(*this, 0));
	auto path_el = at(0);
	ass( path_el.second < wr_dap.size(), "fix_cursor_after_last_item failed at Cursor::del" );
	Pid overflow_page, overflow_count;
	wr_dap.erase(path_el.second, overflow_page, overflow_count);
	if( overflow_page ) {
		bucket_desc->overflow_page_count -= overflow_count;
		my_txn->mark_free_in_future_page(overflow_page, overflow_count);
	}
	for(IntrusiveNode<Cursor> * c = &my_txn->my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
		c->get_current()->on_erase(bucket_desc, 0, path_el.first, path_el.second);
	my_txn->new_merge_leaf(*this, wr_dap);
	bucket_desc->count -= 1;
	return true;
}
void Cursor::next(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if( !fix_cursor_after_last_item() )
		return first();
	auto & path_el = at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	ass( path_el.second < dap.size(), "fix_cursor_after_last_item failed at Cursor::next" );
	path_el.second += 1;
}
void Cursor::prev(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if(bucket_desc->count == 0)
		return;
	if( at(0).first == 0) // fast end indicator, set at true end
		set_at_direction(bucket_desc->height, bucket_desc->root_page, 1);
	if( at(0).second > 0 ) {
//			CLeafPtr dap = my_txn.readable_leaf(path_el.first);
//			ass(path_el.second > 0 && path_el.second <= dap.size(), "Cursor points beyond last leaf element");
		at(0).second -= 1;
		return;
	}
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == bucket_desc->height + 1 ){
			end();
			return;
		}
		CNodePtr nap = my_txn->readable_node(at(height).first);
		if( at(height).second != -1 ){
			at(height).second -= 1;
			pa = nap.get_value(at(height).second);
			height -= 1;
			break;
		}
		height += 1;
	}
	set_at_direction(height, pa, 1);
	ass(at(0).second > 0, "Invalid cursor after set_at_direction in Cursor::prev");
	at(0).second -= 1;
}
