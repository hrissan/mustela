#include "mustela.hpp"
#include <iostream>

using namespace mustela;

constexpr bool FREE_LIST_VERBOSE_PRINT = false;

// We use variable-encoding for count, leaving free worst-case free bytes at the end of page
// TODO - select lowest free page range that has enough space

static size_t get_record_packed_size(Pid page, Pid count){
	return get_compact_size_sqlite4(page) + get_compact_size_sqlite4(count);
}

static size_t get_max_record_packed_size(){
	return 2*get_max_compact_size_sqlite4();
}

void MergablePageCache::clear(){
	cache.clear();
	record_count = 0;
	page_count = 0;
	lo_page_count = 0;
	packed_size = 0;
	size_index_lo.clear();
	size_index_hi.clear();
}

Pid MergablePageCache::get_packed_page_count(size_t page_size)const{
	size_t reduced_size = (page_size - get_max_record_packed_size());
	return (packed_size + reduced_size - 1)/reduced_size;
}

void MergablePageCache::add_to_size_index(Pid page, Pid count, Pid meta_page_count){
	bool high = page >= meta_page_count*3/4;
	if(!high)
		lo_page_count += count;
	auto & size_index = high ? size_index_hi : size_index_lo;
	auto & sitem = size_index[count];
	bool ins = sitem.insert(page).second;
	ass(ins, "Page is twice in size_index");
}
bool MergablePageCache::remove_from_size_index(std::map<Pid, std::set<Pid>> &size_index, Pid page, Pid count){
	auto siit = size_index.find(count);
	if(siit == size_index.end() || siit->second.erase(page) == 0)
		return false;
//	ass(siit != size_index.end(), "Size index find count failed");
//	ass(, "Size index erase page failed");
	if(siit->second.empty())
		size_index.erase(siit);
	return true;
}
void MergablePageCache::remove_from_size_index(Pid page, Pid count){
	if(remove_from_size_index(size_index_lo, page, count)){
		lo_page_count -= count;
		return;
	}
	ass(remove_from_size_index(size_index_hi, page, count), "Page is neither in lo nor in hi size index");
}

void MergablePageCache::add_to_cache(Pid page, Pid count, Pid meta_page_count){
//	if( count == 255 )
//		std::cerr << "MergablePageCache::add_to_cache i=" << int(update_index) << " " << page << ":" << count << std::endl;
	auto it = cache.lower_bound(page);
	ass(it == cache.end() || it->first != page, "adding existing page to cache");
	ass(it == cache.end() || it->first >= page + count, "adding overlapping page to cache (to the right)");
	if(it != cache.end() && it->first == page + count){
		if( update_index)
			remove_from_size_index(it->first, it->second);
		record_count -= 1;
		page_count -= it->second;
		packed_size -= get_record_packed_size(it->first, it->second);
		count += it->second;
		it = cache.erase(it);
	}
	if( it != cache.begin() ){
		--it;
		ass(it->first + it->second <= page, "adding overlapping page to cache (to the left)");
		if( it->first + it->second == page){
			if( update_index)
				remove_from_size_index(it->first, it->second);
			record_count -= 1;
			page_count -= it->second;
			packed_size -= get_record_packed_size(it->first, it->second);
			page = it->first;
			count += it->second;
			it = cache.erase(it);
		}
	}
	record_count += 1;
	page_count += count;
	packed_size += get_record_packed_size(page, count);
	cache.insert(std::make_pair(page, count));
	if( update_index)
		add_to_size_index(page, count, meta_page_count);
}

// TODO - 2 size indexes
void MergablePageCache::remove_from_cache(Pid page, Pid count, Pid meta_page_count){
	auto it = cache.find(page);
	ass(it != cache.end() && it->second >= count, "invalid remove from cache");
	if( update_index )
		remove_from_size_index(it->first, it->second);
	if( count == it->second ){
		record_count -= 1;
		page_count -= it->second;
		packed_size -= get_record_packed_size(it->first, it->second);
		cache.erase(it);
		return;
	}
	auto old_count = it->second;
	record_count -= 1;
	page_count -= it->second;
	packed_size -= get_record_packed_size(it->first, it->second);
	cache.erase(it);
	old_count -= count;
	page += count;
	if( update_index)
		add_to_size_index(page, old_count, meta_page_count);
	record_count += 1;
	page_count += old_count;
	packed_size += get_record_packed_size(page, old_count);
	cache.insert(std::make_pair(page, old_count));
}

static size_t lala_counter = 0;

Pid MergablePageCache::get_free_page(Pid contigous_count, bool high, Pid meta_page_count){
	auto & size_index = high ? size_index_hi : size_index_lo;
	auto siit = size_index.lower_bound(contigous_count);
	if( siit == size_index.end() )
		return 0;
	Pid pa = *(siit->second.begin());
	++siit;
	size_t cou = 0;
	for(;siit != size_index.end(); ++siit){
		Pid try_pa = *(siit->second.begin());
		if( try_pa < pa )
			pa = try_pa;
		cou += 1;
		if( cou > 4)
			break;
	}
	if( contigous_count == 1 && cache.begin()->first < pa )
		pa = cache.begin()->first;
	if(cou > lala_counter){
		lala_counter = cou;
		std::cout << "lala_counter=" << lala_counter << std::endl;
	}
	remove_from_cache(pa, contigous_count, meta_page_count);
	ass(pa >= META_PAGES_COUNT, "Meta somehow got into freelist");
	// TODO - check tid of the page?
	return pa;
}
Pid MergablePageCache::defrag_end(Pid meta_page_count){
	if( cache.empty() )
		return 0;
	auto fit = cache.end();
	--fit;
	Pid last_page = fit->first;
	Pid last_count = fit->second;
	ass(last_page + last_count <= meta_page_count, "free list spans last page");
	if( last_page + last_count != meta_page_count)
		return 0;
	remove_from_cache(last_page, last_count, meta_page_count);
	return last_count;
}
void MergablePageCache::debug_print_db()const{
	int counter = 0;
	std::vector<Pid> histogram;
	Pid last_page = cache.empty() ? 0 : (--cache.end())->first + (--cache.end())->second;
	const size_t CHUNK = 10000;
	histogram.resize((last_page + CHUNK - 1)/CHUNK);
	for(auto && it : cache){
		if( ++counter % 100 == 0)
			std::cerr << std::endl;
		std::cerr << "[" << it.first << ":" << it.second << "] ";
		for(size_t hi = 0; hi != histogram.size(); ++hi){
			Pid start = hi * CHUNK;
			Pid finish = (hi + 1) * CHUNK;
//			if(it.first >= finish || it.first + it.second < start)
//				continue;
			Pid mi = std::max(it.first, start);
			Pid ma = std::min(it.first + it.second, finish);
			if( ma > mi)
				histogram[hi] += ma - mi;
		}
//		histogram[histogram.size() * it.first / last_page] += it.second;
	}
	Pid sum = 0;
	std::cerr << std::endl;
	for(auto && hi : histogram){
		std::cerr << hi << std::endl;
		sum += hi;
	}
	std::cerr << "Total free=" << sum << std::endl;
}

void MergablePageCache::read_packed_page(Val value, Pid meta_page_count){
	size_t pos = 0;
	while(pos + get_max_record_packed_size() <= value.size) {
		Pid page;
		Pid count;
		pos += read_u64_sqlite4(page, value.data + pos);
		pos += read_u64_sqlite4(count, value.data + pos);
		if( count == 0) // Marker of unused space
			break;
		ass(page >= META_PAGES_COUNT, "Meta somehow got into freelist - detected while reading");
		add_to_cache(page, count, meta_page_count);
	}
}

void MergablePageCache::fill_packed_pages(TX * tx, Tid tid, const std::vector<MVal> & all_space)const{
	if(all_space.empty()){
		ass(cache.empty(), "Empty space for non empty free list");
		return;
	}
	// cache may be empty here (all free pages used while puttung all_space into DB), but we must zero-mark all space
	size_t space_index = 0;
	ass(space_index < all_space.size(), "No space to save free list, though  enough space was allocated");
	MVal space = all_space.at(space_index);
	ass(space.size >= get_max_record_packed_size(), "Must have place for at least 1 record in space item");
	for(auto && pa : cache){
		const Pid pid = pa.first;
		const Pid count = pa.second;
		if(space.size < get_max_record_packed_size()){
			memset(space.data, 0, CLEAR_FREE_SPACE ? space.size : std::min<size_t>(2, space.size));
			space_index += 1;
			ass(space_index < all_space.size(), "No space to save free list, though  enough space was allocated");
			space = all_space.at(space_index);
			ass(space.size >= get_max_record_packed_size(), "Must have place for at least 1 record in space item");
		}
		size_t s1 = write_u64_sqlite4(pid, space.data);
		size_t s2 = write_u64_sqlite4(count, space.data + s1);
		space.data += s1 + s2;
		space.size -= s1 + s2;
	}
	memset(space.data, 0, CLEAR_FREE_SPACE ? space.size : std::min<size_t>(2, space.size));
	space_index += 1;
	for(;space_index < all_space.size(); space_index += 1){
		space = all_space.at(space_index);
		memset(space.data, 0, CLEAR_FREE_SPACE ? space.size : std::min<size_t>(2, space.size));
	}
}

void MergablePageCache::merge_from(const MergablePageCache & other){
	for(auto && pa : other.cache){
		Pid pid = pa.first;
		Pid count = pa.second;
		add_to_cache(pid, count, 0);
	}
}

static const Val freelist_prefix("f", 1);

Val FreeList::fill_free_record_key(char * keybuf, Tid tid, uint64_t batch){
	memcpy(keybuf, freelist_prefix.data, freelist_prefix.size);
	size_t p1 = freelist_prefix.size;
	p1 += write_u64_sqlite4(tid, keybuf + p1);
	p1 += write_u64_sqlite4(batch, keybuf + p1);
	return Val(keybuf, p1);
}

bool FreeList::parse_free_record_key(Val key, Tid * tid, uint64_t * batch){
	if(!key.has_prefix(freelist_prefix))
		return false;
	size_t p1 = freelist_prefix.size;
	p1 += read_u64_sqlite4(*tid, key.data + p1);
	p1 += read_u64_sqlite4(*batch, key.data + p1);
	return p1 == key.size;
}

bool FreeList::read_record_space(TX * tx, Tid oldest_read_tid){
	if( next_record_tid >= oldest_read_tid ) // End of free list reached during last get_free_page
		return false;
	char keybuf[32];
	Val key = fill_free_record_key(keybuf, next_record_tid, next_record_batch);
	Val value;
	{
		Cursor main_cursor = tx->get_meta_bucket().get_cursor();
		main_cursor.seek(key);
		if( !main_cursor.get(&key, &value) || !parse_free_record_key(key, &next_record_tid, &next_record_batch) || next_record_tid >= oldest_read_tid ){
			next_record_tid = oldest_read_tid; // Fast subsequent checks
			return false;
		}
	}
	if(FREE_LIST_VERBOSE_PRINT)
		std::cerr << "FreeList read " << next_record_tid << ":" << next_record_batch << std::endl;
	records_to_delete.push_back(std::make_pair(next_record_tid, next_record_batch));
	next_record_batch += 1;
	free_pages.read_packed_page(value, tx->meta_page.page_count);
	Pid defrag = free_pages.defrag_end(tx->meta_page.page_count);
	tx->meta_page.page_count -= defrag;
	if(defrag != 0 && FREE_LIST_VERBOSE_PRINT)
		std::cerr << "FreeList defrag " << defrag << " pages, now meta.page_count=" << tx->meta_page.page_count << std::endl;
	return true;
}

void FreeList::load_all_free_pages(TX * tx, Tid oldest_read_tid){
	while( read_record_space(tx, oldest_read_tid) )
		;
	if(FREE_LIST_VERBOSE_PRINT)
		std::cerr << "FreeList meta.page_count=" << tx->meta_page.page_count << std::endl;
}

Pid FreeList::get_free_page(TX * tx, Pid contigous_count, Tid oldest_read_tid, bool updating_meta_bucket){
//	load_all_free_pages(tx, oldest_read_tid); // TODO - remove
	while( true ){
		Pid pa = free_pages.get_free_page(contigous_count, false, tx->meta_page.page_count);
		if( pa != 0){
			ass(debug_back_from_future_pages.insert(pa).second, "Back from Future double addition");
			return pa;
		}
		if( updating_meta_bucket) // We want to prevent reading while putting
			return 0;
		for(size_t j = 0; j != page_jump_counter - 1; ++j)
			if( !read_record_space(tx, oldest_read_tid) )
				break;
		if( !read_record_space(tx, oldest_read_tid) ){
			pa = free_pages.get_free_page(contigous_count, true, tx->meta_page.page_count);
			if( pa != 0)
				ass(debug_back_from_future_pages.insert(pa).second, "Back from Future double addition");
			return pa;
		}else
			page_jump_counter = page_jump_counter * 2;
	}
}

void FreeList::get_all_free_pages(TX * tx, MergablePageCache * pages)const{
	pages->merge_from(free_pages);
	pages->merge_from(future_pages);

	char keybuf[32];
	Val free_key = fill_free_record_key(keybuf, next_record_tid, next_record_batch);
	Val key, value;
	Cursor main_cursor = tx->get_meta_bucket().get_cursor();
	main_cursor.seek(free_key);
	Tid tid;
	uint64_t batch;
	for(;main_cursor.get(&key, &value) && parse_free_record_key(key, &tid, &batch); main_cursor.next() )
		pages->read_packed_page(value, tx->meta_page.page_count);
}

void FreeList::add_to_future_from_end_of_file(Pid page){
	ass(debug_back_from_future_pages.insert(page).second, "Back from Future double addition from end of file");
}

void FreeList::mark_free_in_future_page(TX * tx, Pid page, Pid count, bool is_from_current_tid){
	ass(page >= META_PAGES_COUNT, "Adding meta to freelist"); // TODO - constant
	auto bfit = debug_back_from_future_pages.find(page);
	ass((bfit != debug_back_from_future_pages.end()) == is_from_current_tid, "back from future failed to detect");
	if( bfit != debug_back_from_future_pages.end()){
		bfit = debug_back_from_future_pages.erase(bfit);
		free_pages.add_to_cache(page, count, tx->meta_page.page_count);
		return;
	}
	future_pages.add_to_cache(page, count, tx->meta_page.page_count);
}

MVal FreeList::grow_record_space(TX * tx, Tid tid, uint32_t & batch){
	Bucket meta_bucket = tx->get_meta_bucket();
	while(true){ // step over collisions in zero tid batches
		char keybuf[32];
		Val key = fill_free_record_key(keybuf, tid, batch);
		batch += 1;
		char * raw_space = meta_bucket.put(key, tx->page_size, true);
		if(raw_space){
			if(FREE_LIST_VERBOSE_PRINT)
				std::cerr << "FreeList write " << tid << ":" << batch - 1 << std::endl;
			return MVal(raw_space, tx->page_size);
		}
		if(FREE_LIST_VERBOSE_PRINT)
			std::cerr << "FreeList write conflict " << tid << ":" << batch - 1 << std::endl;
		// TODO - test case for this case
	}
}

void FreeList::ensure_have_several_pages(TX * tx, Tid oldest_read_tid){
	while(free_pages.get_lo_page_count() < tx->meta_page.meta_bucket.height + 8 && read_record_space(tx, oldest_read_tid))
		;
}

void FreeList::commit_free_pages(TX * tx){
	Bucket meta_bucket = tx->get_meta_bucket();
	uint32_t old_batch = 0;
	std::vector<MVal> old_space;
	uint32_t future_batch = 0;
	std::vector<MVal> future_space;
	while(old_space.size() < free_pages.get_packed_page_count(tx->page_size) || future_space.size() < future_pages.get_packed_page_count(tx->page_size) || !records_to_delete.empty()){
		while(!records_to_delete.empty()){
			char keybuf[32];
			Val key = fill_free_record_key(keybuf, records_to_delete.back().first, records_to_delete.back().second);
			if(FREE_LIST_VERBOSE_PRINT)
				std::cerr << "FreeList del " << records_to_delete.back().first << ":" << records_to_delete.back().second << std::endl;
			records_to_delete.pop_back();
			ass(meta_bucket.del(key), "Failed to delete free list records after reading");
		}
		while(old_space.size() < free_pages.get_packed_page_count(tx->page_size))
			old_space.push_back(grow_record_space(tx, 0, old_batch));
		while(future_space.size() < future_pages.get_packed_page_count(tx->page_size))
			future_space.push_back(grow_record_space(tx, tx->tid(), future_batch));
	}
	//        std::cerr << tx.print_db() << std::endl;
	free_pages.fill_packed_pages(tx, 0, old_space);
	//        std::cerr << tx.print_db() << std::endl;
	future_pages.fill_packed_pages(tx, tx->tid(), future_space);
	//        std::cerr << tx.print_db() << std::endl;
	clear();
}
void FreeList::clear(){
	records_to_delete.clear();
	debug_back_from_future_pages.clear();
	free_pages.clear();
	future_pages.clear();
	next_record_tid = 0;
	next_record_batch = 0;
	page_jump_counter = 1;
}

void FreeList::debug_print_db(){
	std::cerr << "FreeList future pages:";
	future_pages.debug_print_db();
	std::cerr << "FreeList free pages:";
	free_pages.debug_print_db();
}
void FreeList::debug_test(){
	for(int i = 0; i != 1; ++i ){
		FreeList list;
//		list.mark_free_in_future_page(4, 8, false);
//		list.mark_free_in_future_page(6, 2, false);

//		list.mark_free_in_future_page(6, 2, false);
//		list.mark_free_in_future_page(4, 8, false);

//		list.mark_free_in_future_page(6, 2, false);
//		list.mark_free_in_future_page(7, 2, false);
//		list.mark_free_in_future_page(5, 2, false);

//		list.mark_free_in_future_page(4, 2, i != 0);
//		list.mark_free_in_future_page(10, 4, i != 0);
//		list.mark_free_in_future_page(7, 2, i != 0);
//		list.mark_free_in_future_page(6, 1, i != 0);
//		list.mark_free_in_future_page(9, 1, i != 0);
		list.debug_print_db();
	}
}

