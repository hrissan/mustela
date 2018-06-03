#include "pages.hpp"
#include <map>
#include <iostream>

using namespace mustela;
	
//	bool MetaPage::check(uint32_t system_page_size, uint64_t file_size)const{
//		return magic == META_MAGIC && version == OUR_VERSION && page_size == system_page_size && page_count * page_size <= file_size && meta_bucket.root_page < page_count;
//	}
static unsigned char read_our_sqlite4(uint64_t & val, const void * vptr){
	const unsigned char * ptr = (const unsigned char * )vptr;
	unsigned char a0 = *ptr;
	if (a0 <= 240) {
		val = a0;
		return 1;
	}
	if(a0 <= 248) {
		unsigned char a1 = *(ptr + 1);
		val = 240 + 256 * (a0 - 241) + a1;
		return 2;
	}
	const unsigned char * buf = ptr + 1;
	val = 2288 + 256 * buf[0] + buf[1];
	return 3;
}
Val KeysPage::get_item_key(size_t page_size, PageIndex item)const{
	return const_cast<KeysPage *>(this)->get_item_key(page_size, item);
}
MVal KeysPage::get_item_key(size_t page_size, PageIndex item){
	ass(item < item_count, "get_item_key item too large");
	char * raw_this = (char *)this;
	PageOffset item_offset = item_offsets[item];
	uint64_t keysize;
	auto keysizesize = read_our_sqlite4(keysize, raw_this + item_offset);
	ass(item_offset + keysizesize + keysize <= page_size, "get_item_key key spills over page");
	return MVal(raw_this + item_offset + keysizesize, keysize);
}
PageIndex KeysPage::lower_bound_item(size_t page_size, Val key, bool * found)const{
	PageIndex first = 0;
	PageIndex count = item_count;
	while (count > 0) {
		PageIndex step = count / 2;
		PageIndex it = first + step;
		Val itkey = get_item_key(page_size, it);
		if (itkey < key) {
			first = it + 1;
			count -= step + 1;
		}
		else
			count = step;
	}
	if( found ){
		if( first == item_count)
			*found = false;
		else // TODO - we can save get_item_key here 50% of time
			*found = Val(get_item_key(page_size, first)) == key;
	}
	return first;
}
PageIndex KeysPage::upper_bound_item(size_t page_size, Val key)const{
	PageIndex first = 0;
	PageIndex count = item_count;
	while (count > 0) {
		PageIndex step = count / 2;
		PageIndex it = first + step;
		Val itkey = get_item_key(page_size, it);
		if (!(key < itkey)) {
			first = it + 1;
			count -= step + 1;
		} else
			count = step;
	}
	return first;
}

void KeysPage::erase_item(size_t page_size, PageIndex to_remove_item, size_t item_size){
	char * raw_this = (char *)this;
	auto kv_size = item_size - sizeof(PageOffset);
	memset(raw_this + item_offsets[to_remove_item], 0, kv_size); // clear unused part
	if( item_offsets[to_remove_item] == free_end_offset )
		free_end_offset += kv_size; // Luck, removed item is after free middle space
	for(PageIndex pos = to_remove_item; pos != item_count - 1; ++pos)
		item_offsets[pos] = item_offsets[pos+1];
	// TODO - use memmove
	items_size -= item_size;
	item_count -= 1;
	item_offsets[item_count] = 0; // clear unused part
}

MVal KeysPage::insert_item_at(size_t page_size, PageIndex insert_index, Val key, size_t item_size){
	char * raw_this = (char *)this;
	auto kv_size = item_size - sizeof(PageOffset);
	for(PageIndex pos = item_count; pos-- > insert_index;)
		item_offsets[pos + 1] = item_offsets[pos];
	auto insert_offset = free_end_offset - kv_size;
	item_offsets[insert_index] = insert_offset;
	free_end_offset -= kv_size;
	items_size += item_size;
	item_count += 1;
	auto keysizesize = write_u64_sqlite4(key.size, raw_this + insert_offset);
	memcpy(raw_this + insert_offset + keysizesize, key.data, key.size);
	return MVal(raw_this + insert_offset + keysizesize, key.size);
}

PageOffset mustela::get_item_size(size_t page_size, Val key, Pid value){
	size_t item_size = sizeof(PageOffset) + get_compact_size_sqlite4(key.size) + key.size + NODE_PID_SIZE;
	if( item_size <= node_capacity(page_size) )
		return item_size;
	throw std::runtime_error("Item does not fit in node");
}

void NodePtr::init_dirty(Tid new_tid){
	char * raw_page = (char *)page;
	memset(raw_page + sizeof(DataPage), 0, page_size - sizeof(DataPage));
	mpage()->tid = new_tid;
	mpage()->free_end_offset = page_size - NODE_PID_SIZE;
}
void NodePtr::compact(size_t item_size){
	if(NODE_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset)
		return;
	char buf[page_size]; // This fun is always last call in recursion, so not a problem
	memcpy(buf, page, page_size);
	CNodePtr my_copy(page_size, (NodePage *)buf);
	clear();
	set_value(-1, my_copy.get_value(-1));
	append_range(my_copy, 0, my_copy.size());
}

size_t CNodePtr::get_item_size(PageIndex item)const{
	ass(item >= 0 && item < page->item_count, "item_size item too large");
	const char * raw_page = (const char *)page;
	PageOffset item_offset = page->item_offsets[item];
	uint64_t keysize;
	auto keysizesize = read_u64_sqlite4(keysize, raw_page + item_offset);
	return sizeof(PageOffset) + keysizesize + keysize + NODE_PID_SIZE;
}

Pid CNodePtr::get_value(PageIndex item)const{
	Pid value;
	if( item == -1 ){
		const char * raw_page = (const char *)page;
		unpack_uint_be(raw_page + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
		return value;
	}
	Val result = get_key(item);
	unpack_uint_be(result.end(), NODE_PID_SIZE, value);
	return value;
}
ValPid CNodePtr::get_kv(PageIndex item)const{
	ValPid result(get_key(item), 0);
	unpack_uint_be(result.key.end(), NODE_PID_SIZE, result.pid);
	return result;
}

void NodePtr::set_value(PageIndex item, Pid value){
	if( item == -1 ){
		char * raw_page = (char *)mpage();
		pack_uint_be(raw_page + page_size - NODE_PID_SIZE, NODE_PID_SIZE, value);
		return;
	}
	MVal result = get_key(item);
	pack_uint_be(result.end(), NODE_PID_SIZE, value);
}

void LeafPtr::init_dirty(Tid new_tid){
	char * raw_page = (char *)mpage();
	memset(raw_page + sizeof(DataPage), 0, page_size - sizeof(DataPage));
	mpage()->tid = new_tid;
	mpage()->free_end_offset = page_size;
}

void LeafPtr::compact(size_t item_size){
	if(LEAF_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset)
		return;
	char buf[page_size]; // This is last call in recursion, so we might just keep this allocation in stack
	memcpy(buf, page, page_size);
	CLeafPtr my_copy(page_size, (LeafPage *)buf);
	clear();
	append_range(my_copy, 0, my_copy.size());
}
char * LeafPtr::insert_at(PageIndex insert_index, Val key, size_t value_size, bool & overflow){
	ass(insert_index >= 0 && insert_index <= mpage()->item_count, "Cannot insert at this index");
	size_t item_size = get_item_size(key, value_size, overflow);
	compact(item_size);
	ass(LEAF_HEADER_SIZE + sizeof(PageOffset)*page->item_count + item_size <= page->free_end_offset, "No space to insert in node");
	MVal new_key = mpage()->insert_item_at(page_size, insert_index, key, item_size);
	auto valuesizesize = write_u64_sqlite4(value_size, new_key.end());
	return new_key.end() + valuesizesize;
}

PageOffset CLeafPtr::get_item_size(Val key, size_t value_size, bool & overflow)const{
	size_t kvs_size = sizeof(PageOffset) + get_compact_size_sqlite4(key.size) + key.size + get_compact_size_sqlite4(value_size);
	if( kvs_size + value_size <= leaf_capacity(page_size) ){
		overflow = false;
		return kvs_size + value_size;
	}
	overflow = true;
	return kvs_size + NODE_PID_SIZE;// std::runtime_error("Item does not fit in leaf");
}
size_t CLeafPtr::get_item_size(PageIndex item, Pid & overflow_page, Pid & overflow_count)const{
	ass(item >= 0 && item < page->item_count, "item_size item too large");
	const char * raw_page = (const char *)page;
	PageOffset item_offset = page->item_offsets[item];
	uint64_t keysize;
	auto keysizesize = read_u64_sqlite4(keysize, raw_page + item_offset);
	uint64_t valuesize;
	auto valuesizesize = read_u64_sqlite4(valuesize, raw_page + item_offset + keysizesize + keysize);
	size_t kvs_size = sizeof(PageOffset) + keysizesize + keysize + valuesizesize;
	if( kvs_size + valuesize <= leaf_capacity(page_size) ){
		overflow_page = 0;
		overflow_count = 0;
		return kvs_size + valuesize;
	}
	unpack_uint_be(raw_page + item_offset + keysizesize + keysize + valuesizesize, NODE_PID_SIZE, overflow_page);
	overflow_count = (valuesize + page_size - 1)/page_size;
	return kvs_size + NODE_PID_SIZE;
}
ValVal CLeafPtr::get_kv(PageIndex item, Pid & overflow_page)const{
	ValVal result;
	result.key = get_key(item);
	uint64_t valuesize;
	auto valuesizesize = read_u64_sqlite4(valuesize, result.key.end());
	size_t kvs_size = sizeof(PageOffset) + get_compact_size_sqlite4(result.key.size) + result.key.size + valuesizesize;
	if( kvs_size + valuesize <= leaf_capacity(page_size) ){
		overflow_page = 0;
		result.value = Val(result.key.end() + valuesizesize, valuesize);
	}else{
		unpack_uint_be(result.key.end() + valuesizesize, NODE_PID_SIZE, overflow_page);
		result.value = Val(result.key.end() + valuesizesize, valuesize);
	}
	return result;
}

void test_node_page(){
	const size_t page_size = 128;
	NodePtr pa(page_size, (NodePage *)malloc(page_size));
	pa.init_dirty(10);
	std::map<std::string, Pid> mirror;
	pa.set_value(-1, 123456);
	for(int i = 0; i != 1000; ++i){
		std::string key = "key" + std::to_string(rand() % 100);
		Pid val = rand() % 100000;
		bool same_key;
		PageIndex existing_item = pa.lower_bound_item(Val(key), &same_key);
		bool remove_existing = rand() % 2;
		if( same_key ){
			if( !remove_existing )
				continue;
			pa.erase(existing_item);
			mirror.erase(key);
		}
		size_t new_kvsize = get_item_size(page_size, Val(key), val);
		bool add_new = rand() % 2;
		if( add_new && pa.free_capacity() >= new_kvsize ){
			pa.insert_at(existing_item, Val(key), val);
			mirror[key] = val;
		}
	}
	std::cerr << "Special value=" << pa.get_value(-1) << std::endl;
	std::cerr << "Mirror" << std::endl;
	for(auto && ma : mirror)
		std::cerr << ma.first << ":" << ma.second << std::endl;
	std::cerr << "Page" << std::endl;
	for(int i = 0; i != pa.size(); ++i){
		ValPid va = pa.get_kv(i);
		std::cerr << va.key.to_string() << ":" << va.pid << std::endl;
	}
	for(int i = -5; i != 0; ++i)
		for(int j = -5; j != 0; ++j){
			std::string key1 = std::string(max_key_size(page_size) + i, 'A');
			std::string key2 = std::string(max_key_size(page_size) + j, 'B');
			pa.init_dirty(10);
			pa.insert_at(0, Val(key1), 0);
			pa.insert_at(1, Val(key2), 0);
		}
}
void test_data_pages(){
	test_node_page();
	const size_t page_size = 256;
	LeafPtr pa(page_size, (LeafPage *)malloc(page_size));
	pa.init_dirty(10);
	std::map<std::string, std::string> mirror;
	for(int i = 0; i != 1000; ++i){
		std::string key = "key" + std::to_string(rand() % 100);
		std::string val = "value" + std::to_string(rand() % 100000);
		bool same_key;
		PageIndex existing_item = pa.lower_bound_item(Val(key), &same_key);
		bool remove_existing = rand() % 2;
		if( same_key ){
			if( !remove_existing )
				continue;
			Pid overflow_page, overflow_count;
			pa.erase(existing_item, overflow_page, overflow_count);
			ass(overflow_page == 0, "This test should not use overflow");
			mirror.erase(key);
		}
		bool overflow;
		size_t new_kvsize = pa.get_item_size(Val(key), Val(val).size, overflow);
		ass(!overflow, "This test should not use overflow");
		bool add_new = rand() % 2;
		if( add_new && new_kvsize <= pa.free_capacity() ){
			pa.insert_at(existing_item, Val(key), Val(val));
			mirror[key] = val;
		}
	}
	std::cerr << "Mirror" << std::endl;
	for(auto && ma : mirror)
		std::cerr << ma.first << ":" << ma.second << std::endl;
	std::cerr << "Page" << std::endl;
	for(int i = 0; i != pa.size(); ++i){
		Pid overflow_page;
		ValVal va = pa.get_kv(i, overflow_page);
		ass(overflow_page == 0, "This test should not use overflow");
		std::cerr << va.key.to_string() << ":" << va.value.to_string() << std::endl;
	}
}
