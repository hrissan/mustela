#include "mustela.hpp"
#include <iostream>

using namespace mustela;
	
Cursor::Cursor(TX * my_txn, BucketDesc * bucket_desc):my_txn(my_txn), bucket_desc(bucket_desc){
	ass(my_txn->my_cursors.insert(this).second, "Double insert");
	path.resize(bucket_desc->height + 1);
	end();
}
Cursor::Cursor(Bucket & bucket):my_txn(bucket.my_txn), bucket_desc(bucket.bucket_desc){
	ass(my_txn->my_cursors.insert(this).second, "Double insert");
	path.resize(bucket_desc->height + 1);
	end();
}
Cursor::~Cursor(){
	unlink();
}
void Cursor::unlink(){
	if(my_txn)
		ass(my_txn->my_cursors.erase(this) == 1, "Double etase");
	my_txn = nullptr;
	bucket_desc = nullptr;
}
Cursor::Cursor(Cursor && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), path(std::move(other.path)){
	if(my_txn)
		ass(my_txn->my_cursors.insert(this).second, "Double insert");
	other.unlink();
}
Cursor::Cursor(const Cursor & other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), path(other.path){
	if(my_txn)
		ass(my_txn->my_cursors.insert(this).second, "Double insert");
}
Cursor & Cursor::operator=(Cursor && other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	path = std::move(other.path);
	if(my_txn)
		ass(my_txn->my_cursors.insert(this).second, "Double insert");
	other.unlink();
	return *this;
}
Cursor & Cursor::operator=(const Cursor & other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	path = other.path;
	if(my_txn)
		ass(my_txn->my_cursors.insert(this).second, "Double insert");
	return *this;
}

bool Cursor::operator==(const Cursor & other)const{
	const_cast<Cursor &>(*this).fix_cursor_after_last_item();
	const_cast<Cursor &>(other).fix_cursor_after_last_item();
//		Cursor a(*this);
//		Cursor b(other);
//		a.fix_cursor_after_last_item(); // fixes are lazy
//		b.fix_cursor_after_last_item(); // fixes are lazy
	return my_txn == other.my_txn && bucket_desc == other.bucket_desc && path == other.path;
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
			path.at(height) = std::make_pair(pa, item);
			return found;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		PageIndex nitem = nap.upper_bound_item(key) - 1;
		path.at(height) = std::make_pair(pa, nitem);
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
bool Cursor::fix_cursor_after_last_item(){
	auto path_el = path.at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	if(path_el.second < dap.size())
		return true;
	ass(path_el.second == dap.size(), "Cursor corrupted at Cursor::fix_cursor_after_last_item");
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == path.size() )
			return false;
		CNodePtr nap = my_txn->readable_node(path.at(height).first);
		if( path.at(height).second + 1 < nap.size() ){
			path.at(height).second += 1;
			pa = nap.get_value(path.at(height).second);
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
			path.at(height) = std::make_pair(pa, dir > 0 ? dap.size() : 0);
			break;
		}
		CNodePtr nap = my_txn->readable_node(pa);
		PageIndex nitem = dir > 0 ? nap.size() - 1 : -1;
		path.at(height) = std::make_pair(pa, nitem);
		pa = nap.get_value(nitem);
		height -= 1;
	}
}
void Cursor::end(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	set_at_direction(bucket_desc->height, bucket_desc->root_page, 1);
}
void Cursor::first(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	set_at_direction(bucket_desc->height, bucket_desc->root_page, -1);
}
void Cursor::last(){
	end();
	prev();
}
bool Cursor::get(Val & key, Val & value){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if( !fix_cursor_after_last_item() )
		return false;
	auto path_el = path.at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	ass( path_el.second < dap.size(), "fix_cursor_after_last_item failed at Cursor::get" );
	Pid overflow_page;
	auto kv = dap.get_kv(path_el.second, overflow_page);
	if( overflow_page ){
		Pid overflow_count = (kv.value.size + my_txn->page_size - 1)/my_txn->page_size;
		kv.value.data = my_txn->readable_overflow(overflow_page, overflow_count);
	}
	key = kv.key;
	value = kv.value;
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
	auto path_el = path.at(0);
	ass( path_el.second < wr_dap.size(), "fix_cursor_after_last_item failed at Cursor::del" );
	Pid overflow_page, overflow_count;
	wr_dap.erase(path_el.second, overflow_page, overflow_count);
	if( overflow_page ) {
		bucket_desc->overflow_page_count -= overflow_count;
		my_txn->mark_free_in_future_page(overflow_page, overflow_count);
	}
	for(auto && c : my_txn->my_cursors)
		c->on_erase(bucket_desc, 0, path_el.first, path_el.second);
	my_txn->new_merge_leaf(*this, wr_dap);
	bucket_desc->count -= 1;
	return true;
}
void Cursor::next(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if( !fix_cursor_after_last_item() )
		return first();
	auto & path_el = path.at(0);
	CLeafPtr dap = my_txn->readable_leaf(path_el.first);
	ass( path_el.second < dap.size(), "fix_cursor_after_last_item failed at Cursor::next" );
	path_el.second += 1;
}
void Cursor::prev(){
	ass(bucket_desc, "Cursor not valid (using after tx commit?)");
	if(bucket_desc->count == 0)
		return;
	if( path.at(0).second > 0 ) {
//			CLeafPtr dap = my_txn.readable_leaf(path_el.first);
//			ass(path_el.second > 0 && path_el.second <= dap.size(), "Cursor points beyond last leaf element");
		path.at(0).second -= 1;
		return;
	}
	size_t height = 1;
	Pid pa = 0;
	while(true){
		if( height == path.size() ){
			end();
			return;
		}
		CNodePtr nap = my_txn->readable_node(path.at(height).first);
		if( path.at(height).second != -1 ){
			path.at(height).second -= 1;
			pa = nap.get_value(path.at(height).second);
			height -= 1;
			break;
		}
		height += 1;
	}
	set_at_direction(height, pa, 1);
	ass(path.at(0).second > 0, "Invalid cursor after set_at_direction in Cursor::prev");
	path.at(0).second -= 1;
}
