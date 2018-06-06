#include "mustela.hpp"

using namespace mustela;


Bucket::Bucket(TX * my_txn, BucketDesc * bucket_desc, Val name):my_txn(my_txn), bucket_desc(bucket_desc), persistent_name(name) {
	ass(my_txn && bucket_desc, "get_bucket called on invalid transaction");
   	my_txn->my_buckets.insert_after_this(this, &Bucket::tx_buckets);
}
Bucket::~Bucket(){
	unlink();
}
void Bucket::unlink(){
	tx_buckets.unlink(&Bucket::tx_buckets);
	my_txn = nullptr;
	bucket_desc = nullptr;
	persistent_name = Val{};
}
Bucket::Bucket(Bucket && other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), persistent_name(other.persistent_name){
	if(my_txn)
		my_txn->my_buckets.insert_after_this(this, &Bucket::tx_buckets);
}
Bucket::Bucket(const Bucket & other):my_txn(other.my_txn), bucket_desc(other.bucket_desc), persistent_name(other.persistent_name){
	if(my_txn)
		my_txn->my_buckets.insert_after_this(this, &Bucket::tx_buckets);
}
Bucket & Bucket::operator=(Bucket && other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	persistent_name = other.persistent_name;
	if(my_txn)
   		my_txn->my_buckets.insert_after_this(this, &Bucket::tx_buckets);
	return *this;
}
Bucket & Bucket::operator=(const Bucket & other){
	unlink();
	my_txn = other.my_txn;
	bucket_desc = other.bucket_desc;
	persistent_name = other.persistent_name;
	if(my_txn)
   		my_txn->my_buckets.insert_after_this(this, &Bucket::tx_buckets);
	return *this;
}

char * Bucket::put(const Val & key, size_t value_size, bool nooverwrite){
	if( my_txn->read_only )
		throw Exception("Attempt to modify read-only transaction");
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	if(key.size > max_key_size(my_txn->page_size))
		throw Exception("Key size too big in Bucket::put");
	Cursor main_cursor(my_txn, bucket_desc, persistent_name);
	const bool same_key = main_cursor.seek(key);
//		CLeafPtr dap = my_txn.readable_leaf(main_cursor.path.at(0).first);
//		bool same_key = item != dap.size() && Val(dap.get_key(item)) == key;
	TX::BucketMirror * bu = nullptr;
	if(DEBUG_MIRROR && bucket_desc != &my_txn->meta_page.meta_bucket){
	 	bu = &my_txn->debug_mirror.at(persistent_name.to_string());
		ass(bu->count(key.to_string()) == size_t(same_key), "Mirror key different in bucket put");
		my_txn->before_mirror_operation();
	}
	if( same_key && nooverwrite )
		return nullptr;
	my_txn->meta_page_dirty = true;
	// TODO - optimize - if page will split and it is not writable yet, we can save make_page_writable
	LeafPtr wr_dap(my_txn->page_size, (LeafPage *)my_txn->make_pages_writable(main_cursor, 0));
	auto path_el = main_cursor.path.at(0);
	if( same_key ){
		Pid overflow_page, overflow_count;
		Tid overflow_tid;
		wr_dap.erase(path_el.second, overflow_page, overflow_count, overflow_tid);
		if( overflow_page ){
			bucket_desc->overflow_page_count -= overflow_count;
			my_txn->mark_free_in_future_page(overflow_page, overflow_count, overflow_tid);
		}
	}else{
		for(IntrusiveNode<Cursor> * c = &my_txn->my_cursors; !c->is_end(); c = c->get_next(&Cursor::tx_cursors))
			c->get_current()->on_insert(bucket_desc, 0, path_el.first, path_el.second);
		ass(main_cursor.path.at(0).second == path_el.second + 1, "Main cursor was unaffectet by on_insert");
		main_cursor.path.at(0).second = path_el.second;
	}
	bool overflow;
	my_txn->start_update(bucket_desc);
	char * result = my_txn->new_insert2leaf(main_cursor, key, value_size, &overflow);
	if( overflow ){
		Pid overflow_count = (value_size + my_txn->page_size - 1)/my_txn->page_size;
		Pid opa = my_txn->get_free_page(overflow_count);
		bucket_desc->overflow_page_count += overflow_count;
		pack_uint_le(result, NODE_PID_SIZE, opa);
		pack_uint_le(result + NODE_PID_SIZE, sizeof(Tid), my_txn->tid());
		result = my_txn->writable_overflow(opa, overflow_count);
	}
	my_txn->finish_update(bucket_desc);
	if( !same_key )
		bucket_desc->count += 1;
	if(DEBUG_MIRROR && bucket_desc != &my_txn->meta_page.meta_bucket){
		if(same_key) // Update only value, existing cursor should stay pointing to the same key-value
			bu->at(key.to_string()).first = std::string();
		else
			ass(bu->insert(std::make_pair(key.to_string(), std::make_pair(std::string(), main_cursor))).second, "inconsistent mirror");
	}
	return result;
}
bool Bucket::put(const Val & key, const Val & value, bool nooverwrite) { // false if nooverwrite and key existed
	char * dst = put(key, value.size, nooverwrite);
	if( dst )
		memcpy(dst, value.data, value.size);
	if(DEBUG_MIRROR && bucket_desc != &my_txn->meta_page.meta_bucket){
	 	auto & part = my_txn->debug_mirror.at(persistent_name.to_string());
	 	part.at(key.to_string()).first = value.to_string();
		my_txn->check_mirror();
	}
	return dst != nullptr;
}
bool Bucket::get(const Val & key, Val * value)const{
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	Cursor main_cursor(my_txn, bucket_desc, persistent_name);
	if( !main_cursor.seek(key) )
		return false;
	Val c_key;
	return main_cursor.get(&c_key, value);
}
bool Bucket::del(const Val & key){
	if( my_txn->read_only )
		throw Exception("Attempt to modify read-only transaction");
	ass(bucket_desc, "Bucket not valid (using after tx commit?)");
	Cursor main_cursor(my_txn, bucket_desc, persistent_name);
	if( !main_cursor.seek(key) )
		return false;
	ass(main_cursor.del(), "Cursor del returned false after successfull seek");
	return true;
}
std::string Bucket::debug_print_db(){
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
	",\n\t'table': '" + persistent_name.to_string() + "'}";
	return result;
}
