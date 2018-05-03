#include "free_list.hpp"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <iostream>
#include <algorithm>
#include "tx.hpp"

using namespace mustela;

constexpr size_t RECORD_SIZE = NODE_PID_SIZE + 1;
// Use store fixed-size records of NODE_PID_SIZE page + 1-byte count
static size_t get_records_count(Pid count){
	return ((count + 254) / 255);
}

void FreeList::add_to_size_index(Pid page, Pid count){
	//        if(count == 1)
	//            return;
	auto & sitem = size_index[count];
	bool ins = sitem.insert(page).second;
	ass(ins, "Page is twice in size_index");
}
void FreeList::remove_from_size_index(Pid page, Pid count){
	//        if(count == 1)
	//            return;
	auto siit = size_index.find(count);
	ass(siit != size_index.end(), "Size index find count failed");
	ass(siit->second.erase(page) == 1, "Size index erase page failed");
	if(siit->second.empty())
		size_index.erase(siit);
}

void FreeList::add_to_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & record_count, bool update_index){
	auto it = cache.lower_bound(page);
	ass(it == cache.end() || it->first != page, "adding existing page to cache");
	if(it != cache.end() && it->first == page + count){
		if( update_index)
			remove_from_size_index(it->first, it->second);
		count += it->second;
		record_count -= get_records_count(it->second);
		it = cache.erase(it);
	}
	if( it != cache.begin() ){
		--it;
		if( it->first + it->second == page){
			if( update_index)
				remove_from_size_index(it->first, it->second);
			page = it->first;
			count += it->second;
			record_count -= get_records_count(it->second);
			it = cache.erase(it);
		}
	}
	record_count += get_records_count(count);
	if( count == 0 || count > 1000000000)
		count = count;
	cache.insert(std::make_pair(page, count));
	if( update_index)
		add_to_size_index(page, count);
}
void FreeList::remove_from_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & record_count, bool update_index){
	auto it = cache.find(page);
	ass(it != cache.end() && it->second >= count, "invalid remove from cache");
	if( update_index )
		remove_from_size_index(it->first, it->second);
	if( count == it->second ){
		record_count -= get_records_count(it->second);
		cache.erase(it);
		return;
	}
	auto old_count = it->second;
	record_count -= get_records_count(old_count);
	cache.erase(it);
	old_count -= count;
	page += count;
	if( update_index )
		add_to_size_index(page, old_count);
	record_count += get_records_count(old_count);
	if( old_count == 0 || old_count > 1000000000)
		old_count = old_count;
	cache.insert(std::make_pair(page, old_count));
}
Pid FreeList::get_free_page(TX & tx, Pid contigous_count, Tid oldest_read_tid){
	while( true ){
		auto siit = size_index.lower_bound(contigous_count);
		if( siit != size_index.end() ){
			Pid pa = *(siit->second.begin());
			remove_from_cache(pa, contigous_count, free_pages, free_pages_record_count, true);
			back_from_future_pages.insert(pa);
			if(pa < 3)
				std::cout << "Achtung!" << std::endl;
			return pa;
		}
		if( next_record_tid >= oldest_read_tid ) // End of free list reached during last get_free_page
			break;
		char keybuf[20]={TX::freelist_prefix};
		size_t p1 = 1;
		p1 += write_u64_sqlite4(next_record_tid, keybuf + p1);
		p1 += write_u64_sqlite4(next_record_batch, keybuf + p1);
		Val key(keybuf, p1);
		Val value;
		{
			Cursor main_cursor(tx, &tx.meta_page.meta_bucket);
			main_cursor.seek(key);
			if( !main_cursor.get(key, value) )
				break;
		}
//		if( !tx.get_next(&tx.meta_page.meta_bucket, key, value))
//			break;
		if( key.size < 1 || key.data[0] != TX::freelist_prefix ) // Free list finished
			break;
		//            Tid tid;
		//            uint64_t batch;
		p1 = 1;
		p1 += read_u64_sqlite4(next_record_tid, key.data + p1);
		p1 += read_u64_sqlite4(next_record_batch, key.data + p1);
		if( next_record_tid >= oldest_read_tid )
			break;
		std::cout << "FreeList read " << next_record_tid << ":" << next_record_batch << std::endl;
		records_to_delete.push_back(std::make_pair(next_record_tid, next_record_batch));
		next_record_batch += 1;
		size_t rec = 0;
		for(;(rec + 1) * RECORD_SIZE <= value.size; ++rec){
			Pid page;
			unpack_uint_be(value.data + rec * RECORD_SIZE, NODE_PID_SIZE, page);
			Pid count = static_cast<unsigned char>(value.data[rec * RECORD_SIZE + NODE_PID_SIZE]);
			if( count == 0) // Marker of unused space
				break;
			add_to_cache(page, count, free_pages, free_pages_record_count, true);
		}
		if( !free_pages.empty() ){ // Try to defrag the end of file
			auto fit = free_pages.end();
			--fit;
			Pid last_page = fit->first;
			Pid last_count = fit->second;
			if( last_page + last_count == tx.meta_page.page_count){
				tx.meta_page.page_count -= last_count;
				remove_from_cache(last_page, last_count, free_pages, free_pages_record_count, true);
			}
		}
	}
	return 0;
}
void FreeList::mark_free_in_future_page(Pid page, Pid count){
	if(page < 3)
		std::cout << "Achtung!" << std::endl;
	auto bfit = back_from_future_pages.find(page);
	if( bfit != back_from_future_pages.end()){
		back_from_future_pages.erase(bfit);
		add_to_cache(page, count, free_pages, free_pages_record_count, true);
		return;
	}
	add_to_cache(page, count, future_pages, future_pages_record_count, false);
}
void FreeList::fill_record_space(TX & tx, Tid tid, std::vector<MVal> & space, const std::map<Pid, Pid> & pages){
	size_t space_count = 0;
	size_t space_pos = 0;
	for(auto && pa : pages){
		Pid pid = pa.first;
		Pid count = pa.second;
		while( count > 0 ){
			Pid r_count = std::min<Pid>(count, 255);
			ass(space_count < space.size(), "No space to save free list, though  enough space was allocated");
			pack_uint_be(space[space_count].data + space_pos, NODE_PID_SIZE, pid); space_pos += NODE_PID_SIZE;
			space[space_count].data[space_pos] = r_count; space_pos += 1;
			ass( space_pos <= space[space_count].size, "Overshoot of space_pos while writing free list");
			if( space_pos == space[space_count].size){
				space_pos = 0;
				space_count += 1;
			}
			count -= r_count;
		}
	}
	for(;space_count < space.size(); space_count += 1){
		memset(space[space_count].data + space_pos, 0, space[space_count].size - space_pos);
		space_pos = 0;
	}
}
void FreeList::grow_record_space(TX & tx, Tid tid, uint32_t & batch, std::vector<MVal> & space, size_t & space_record_count, size_t record_count){
	Bucket meta_bucket(tx, &tx.meta_page.meta_bucket);
	const size_t page_records = tx.page_size / RECORD_SIZE;
	while(space_record_count < record_count){
		char keybuf[20]={TX::freelist_prefix};
		size_t p1 = 1;
		p1 += write_u64_sqlite4(tid, keybuf + p1);
		p1 += write_u64_sqlite4(batch, keybuf + p1);
		size_t recs = std::min(page_records, record_count - space_record_count);
		std::cout << "FreeList write recs=" << recs << " tid:batch=" << tid << ":" << batch << std::endl;
		recs = page_records;
		batch += 1;
		char * raw_space = meta_bucket.put(Val(keybuf, p1), recs * RECORD_SIZE, true);
		//        std::cout << tx.print_db() << std::endl;
		space.push_back(MVal(raw_space, recs * RECORD_SIZE));
		space_record_count += recs;
	}
}
void FreeList::commit_free_pages(TX & tx, Tid write_tid){
	Bucket meta_bucket(tx, Val());
	uint32_t old_batch = 0;
	size_t old_record_count = 0;
	std::vector<MVal> old_space;
	uint32_t future_batch = 0;
	size_t future_record_count = 0;
	std::vector<MVal> future_space;
	//        const size_t page_records = tx.page_size / RECORD_SIZE;
	while(old_record_count < free_pages_record_count || future_record_count < future_pages_record_count || !records_to_delete.empty()){
		while(!records_to_delete.empty()){
			char keybuf[20]={TX::freelist_prefix};
			size_t p1 = 1;
			p1 += write_u64_sqlite4(records_to_delete.back().first, keybuf + p1);
			p1 += write_u64_sqlite4(records_to_delete.back().second, keybuf + p1);
			std::cout << "FreeList del " << records_to_delete.back().first << ":" << records_to_delete.back().second << std::endl;
			records_to_delete.pop_back();
			ass(meta_bucket.del(Val(keybuf, p1), true), "Failed to delete free list records after reading");
			//                std::cout << tx.print_db() << std::endl;
		}
		grow_record_space(tx, 0, old_batch, old_space, old_record_count, free_pages_record_count);
		grow_record_space(tx, write_tid, future_batch, future_space, future_record_count, future_pages_record_count);
	}
	//        std::cout << tx.print_db() << std::endl;
	fill_record_space(tx, 0, old_space, free_pages);
	//        std::cout << tx.print_db() << std::endl;
	fill_record_space(tx, write_tid, future_space, future_pages);
	//        std::cout << tx.print_db() << std::endl;
	back_from_future_pages.clear();
	free_pages.clear();
	future_pages.clear();
	size_index.clear();
	free_pages_record_count = 0;
	future_pages_record_count = 0;
	next_record_tid = 0;
	next_record_batch = 0;
}
void FreeList::print_db(){
	std::cout << "FreeList future pages:";
	int counter = 0;
	for(auto && it : future_pages){
		if( counter++ % 10 == 0)
			std::cout << std::endl;
		std::cout << "[" << it.first << ":" << it.second << "] ";
	}
	std::cout << std::endl;
	std::cout << "FreeList free pages:";
	counter = 0;
	for(auto && it : free_pages){
		if( counter++ % 10 == 0)
			std::cout << std::endl;
		std::cout << "[" << it.first << ":" << it.second << "] ";
	}
	std::cout << std::endl;
}
void FreeList::test(){
	FreeList list;
	list.mark_free_in_future_page(4, 2);
	list.mark_free_in_future_page(10, 4);
	list.mark_free_in_future_page(7, 2);
	list.mark_free_in_future_page(6, 1);
	list.mark_free_in_future_page(9, 1);
	list.print_db();
}

