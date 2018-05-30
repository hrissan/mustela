#include "mustela.hpp"

using namespace mustela;


Bucket::Bucket(TX * my_txn, BucketDesc * bucket_desc):my_txn(my_txn), bucket_desc(bucket_desc) {
	ass(my_txn->my_buckets.insert(this).second, "Double insert");
}
Bucket::Bucket(TX & my_txn_r, const Val & name, bool create):my_txn(&my_txn_r), bucket_desc(my_txn->load_bucket_desc(name)), debug_name(name.to_string()) {
	if( !bucket_desc && create){
		if( my_txn->read_only )
			throw Exception("Attempt to modify read-only transaction");
		BucketDesc & td = my_txn->bucket_descs[name.to_string()];
		td = BucketDesc{};
		td.root_page = my_txn->get_free_page(1);
		LeafPtr wr_root = my_txn->writable_leaf(td.root_page);
		wr_root.init_dirty(my_txn->meta_page.tid);
		td.leaf_page_count = 1;
		bucket_desc = &td;
		std::string key = TX::bucket_prefix + name.to_string();
		Val value(reinterpret_cast<const char *>(bucket_desc), sizeof(BucketDesc));
		Bucket meta_bucket(my_txn, &my_txn->meta_page.meta_bucket);
		ass(meta_bucket.put(Val(key), value, true), "Writing table desc failed during bucket creation");
	}
	ass(my_txn->my_buckets.insert(this).second, "Double insert");
}
Bucket::~Bucket(){
	unlink();
}
void Bucket::unlink(){
	if(my_txn)
		ass(my_txn->my_buckets.erase(this) == 1, "Double etase");
	my_txn = nullptr;
	bucket_desc = nullptr;
}
Bucket::Bucket(Bucket && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc){
	if(my_txn)
		ass(my_txn->my_buckets.insert(this).second, "Double insert");
	other.unlink();
}
Bucket::Bucket(const Bucket & other):my_txn(other.my_txn), bucket_desc(other.bucket_desc){
	if(my_txn)
		ass(my_txn->my_buckets.insert(this).second, "Double insert");
}
Bucket & Bucket::operator=(Bucket && other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	if(my_txn)
		ass(my_txn->my_buckets.insert(this).second, "Double insert");
	other.unlink();
	return *this;
}
Bucket & Bucket::operator=(const Bucket & other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	if(my_txn)
		ass(my_txn->my_buckets.insert(this).second, "Double insert");
	return *this;
}

char * Bucket::put(const Val & key, size_t value_size, bool nooverwrite){
	if( my_txn->read_only )
		throw Exception("Attempt to modify read-only transaction");
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	if(key.size > CNodePtr::max_key_size(my_txn->page_size))
		throw Exception("Key size too big in Bucket::put");
	Cursor main_cursor(my_txn, bucket_desc);
	bool same_key = main_cursor.seek(key);
//		CLeafPtr dap = my_txn.readable_leaf(main_cursor.path.at(0).first);
//		bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
	if( same_key && nooverwrite )
		return nullptr;
	my_txn->meta_page_dirty = true;
	// TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
	LeafPtr wr_dap(my_txn->page_size, (LeafPage *)my_txn->make_pages_writable(main_cursor, 0));
	auto path_el = main_cursor.path.at(0);
	if( same_key ){
		Pid overflow_page, overflow_count;
		wr_dap.erase(path_el.second, overflow_page, overflow_count);
		if( overflow_page ){
			bucket_desc->overflow_page_count -= overflow_count;
			my_txn->mark_free_in_future_page(overflow_page, overflow_count);
		}
	}else{
		for(auto && c : my_txn->my_cursors)
			c->on_insert(bucket_desc, 0, path_el.first, path_el.second);
		ass(main_cursor.path.at(0).second == path_el.second + 1, "Main cursor was unaffectet by on_insert");
		main_cursor.path.at(0).second = path_el.second;
	}
	bool overflow;
	char * result = my_txn->new_insert2leaf(main_cursor, key, value_size, &overflow);
	if( overflow ){
		Pid overflow_count = (value_size + my_txn->page_size - 1)/my_txn->page_size;
		Pid opa = my_txn->get_free_page(overflow_count);
		bucket_desc->overflow_page_count += overflow_count;
		pack_uint_be(result, NODE_PID_SIZE, opa);
		result = my_txn->writable_overflow(opa, overflow_count);
	}
	if( !same_key )
		bucket_desc->count += 1;
	return result;
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
	if( my_txn->read_only )
		throw Exception("Attempt to modify read-only transaction");
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	Cursor main_cursor(my_txn, bucket_desc);
	if( !main_cursor.seek(key) )
		return !must_exist;
	main_cursor.del();
	return true;
}
std::string Bucket::print_db(){
	return bucket_desc ? my_txn->print_db(bucket_desc) : std::string();
}

std::string Bucket::get_stats()const{
	std::string result;
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	result += "{'branch_pages': " + std::to_string(bucket_desc->node_page_count) +
	",\n\t'depth': " + std::to_string(bucket_desc->height) +
	",\n\t'entries': " + std::to_string(bucket_desc->count) +
	",\n\t'leaf_pages': " + std::to_string(bucket_desc->leaf_page_count) +
	",\n\t'overflow_pages': " + std::to_string(bucket_desc->overflow_page_count) +
	",\n\t'psize': " + std::to_string(my_txn->page_size) +
	",\n\t'table': '" + debug_name + "'}";
	return result;
}
