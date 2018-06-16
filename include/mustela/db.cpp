#include "mustela.hpp"
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <memory>
#include <unistd.h> // sleep

using namespace mustela;

std::string DB::lib_version(){
	return "0.02";
}

// TODO - detect R/O filesystem
DB::DB(const std::string & file_path, DBOptions options):
	readonly_fs(os::file_exists_on_readonly_partition(file_path)),
	options(options),
	map_granularity(os::get_map_granularity()),
	data_file(file_path, readonly_fs || options.read_only),
	lock_file(file_path + ".lock", readonly_fs) { // lock slots are writable on writable fs
	if((options.new_db_page_size & (options.new_db_page_size - 1)) != 0)
		Exception::th("new_db_page_size must be power of 2");
	file_size = data_file.get_size();
	if( file_size == 0 ){ // One or more threads will get into "if" when DB file is just created
		os::FileLock data_lock(data_file);
		page_size = options.new_db_page_size == 0 ? GOOD_PAGE_SIZE : options.new_db_page_size;
		file_size = data_file.get_size();
		if( file_size == 0 ) // while we were waiting for lock, another thread created file already
			create_db();
		else
			std::cout << "Avoided race during create_db" << std::endl;
	}
	MetaPage newest_mp{};
	if( !open_db(&newest_mp) ){
		os::FileLock data_lock(data_file); // Database was just being created?
		std::cout << "Avoided race during open_db" << std::endl;
		if( !open_db(&newest_mp))
			Exception::th("Database corrupted (possibly truncated or meta pages are mismatched)");
	}
	if(newest_mp.version != OUR_VERSION)
		Exception::th("Incompatible database version");
	if(newest_mp.pid_size != NODE_PID_SIZE)
		Exception::th("Incompatible pid size");
}
DB::~DB(){
	ass(r_transactions_counter == 0, "Some reader TX still exist while in DB::~DB");
	ass(!wr_transaction, "Write transaction still in progress while in DB::~DB");
	while(!c_mappings.empty()) {
		ass(c_mappings.back().ref_count == 0, "c_mappings ref counts inconsistent in DB::~DB");
		data_file.munmap(c_mappings.back().addr, c_mappings.back().size);
		c_mappings.pop_back();
	}
	while(!wr_mappings.empty()) {
		data_file.munmap(wr_mappings.back().addr, wr_mappings.back().size);
		wr_mappings.pop_back();
	}
}
Tid DB::read_meta_page_tid(Pid index)const {
	ass(!c_mappings.empty() && (index + 1)*page_size <= c_mappings.at(0).size, "readable_page out of range");
	if( (index + 1) * page_size > file_size )
		return 0;
	const volatile MetaPage * mp = (const volatile MetaPage *)(c_mappings.at(0).addr + page_size * index);
	return mp->tid;
}

MetaPage DB::read_meta_page(Pid index)const {
	ass(!c_mappings.empty() && (index + 1)*page_size <= c_mappings.at(0).size, "readable_page out of range");
	if( (index + 1) * page_size > file_size )
		return MetaPage{};
	const volatile MetaPage * mp = (const volatile MetaPage *)(c_mappings.at(0).addr + page_size * index);
	while(true) {
		MetaPage result;
		result.tid = mp->tid;
		__sync_synchronize();
		result.magic = mp->magic;
		result.page_count = mp->page_count;
		result.meta_bucket.root_page = mp->meta_bucket.root_page;
		result.meta_bucket.height = mp->meta_bucket.height;
		result.meta_bucket.item_count = mp->meta_bucket.item_count;
		result.meta_bucket.leaf_page_count = mp->meta_bucket.leaf_page_count;
		result.meta_bucket.node_page_count = mp->meta_bucket.node_page_count;
		result.meta_bucket.overflow_page_count = mp->meta_bucket.overflow_page_count;
		result.pid = mp->pid;
		result.version = mp->version;
		result.page_size = mp->page_size;
		result.pid_size = mp->pid_size;
		result.crc32 = mp->crc32;
		__sync_synchronize();
		if(mp->tid == result.tid)
			return result;
	}
}
void DB::write_meta_page(Pid index, const MetaPage & new_mp){
	ass(!wr_mappings.empty() && (index + 1)*page_size <= file_size, "writable_page out of range");
	volatile MetaPage * mp = (volatile MetaPage *)(wr_mappings.at(0).addr + page_size * index);
	mp->magic = new_mp.magic;
	mp->page_count = new_mp.page_count;
	mp->meta_bucket.root_page = new_mp.meta_bucket.root_page;
	mp->meta_bucket.height = new_mp.meta_bucket.height;
	mp->meta_bucket.item_count = new_mp.meta_bucket.item_count;
	mp->meta_bucket.leaf_page_count = new_mp.meta_bucket.leaf_page_count;
	mp->meta_bucket.node_page_count = new_mp.meta_bucket.node_page_count;
	mp->meta_bucket.overflow_page_count = new_mp.meta_bucket.overflow_page_count;
	mp->pid = new_mp.pid;
	mp->version = new_mp.version;
	mp->page_size = new_mp.page_size;
	mp->pid_size = new_mp.pid_size;
	mp->crc32 = new_mp.crc32;
	__sync_synchronize();
	mp->tid = new_mp.tid;
}

bool DB::is_valid_meta(Pid index, const MetaPage & mp)const{
	if( mp.pid != index || mp.magic != META_MAGIC)
		return false; // throw Exception("file is either not mustela DB or corrupted - wrong meta page");
	if( mp.pid_size < 4 || mp.pid_size > 8 || mp.page_size != page_size || mp.page_count < 4 )
		return false;
	if( mp.crc32 != crc32c(0, &mp, sizeof(MetaPage) - sizeof(uint32_t)))
		return false;
	return true;
}
bool DB::is_valid_meta_strict(const MetaPage & mp)const{
	if( mp.meta_bucket.root_page >= mp.page_count )
		return false;
	if( mp.version != OUR_VERSION || mp.pid_size != NODE_PID_SIZE )
		return false;
	return true;
}
void DB::debug_print_meta_page(Pid i, const MetaPage & mp)const{
	std::cerr << "  meta page " << i << ": ";
	bool eof = (i + 1) * page_size > file_size;
	bool crc_ok = mp.crc32 == crc32c(0, &mp, sizeof(MetaPage) - sizeof(uint32_t));
	std::cerr << (eof ? "BEYOND EOF" : is_valid_meta(i, mp) ? "GOOD" : crc_ok ? "INVALID" : "WRONG CRC");
	std::cerr << " pid=" << mp.pid << " tid=" << mp.tid << " page_count=" << mp.page_count << " ver=" << mp.version << " pid_size=" << mp.pid_size << std::endl;;
	std::cerr << "    meta bucket: height=" << mp.meta_bucket.height << " items=" << mp.meta_bucket.item_count << " leafs=" << mp.meta_bucket.leaf_page_count << " nodes=" << mp.meta_bucket.node_page_count << " overflows=" << mp.meta_bucket.overflow_page_count << " root_page=" << mp.meta_bucket.root_page << " strict=" << is_valid_meta_strict(mp) << std::endl;
}
Pid DB::get_worst_meta_page(Tid * earliest_tid)const{
	MetaPage mpp[META_PAGES_COUNT];
	bool valid_mpp[META_PAGES_COUNT];
	Pid worst_pid = META_PAGES_COUNT;
	Tid worst_tid = 0;
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		mpp[i] = read_meta_page(i);
		valid_mpp[i] = is_valid_meta(i, mpp[i]) && is_valid_meta_strict(mpp[i]);
		if(valid_mpp[i] && mpp[i].page_count * page_size > file_size)
			valid_mpp[i] = false; // This fun is called from writer, file size cannot change while write tx is going
		if( !valid_mpp[i] ){
			worst_pid = i;
			worst_tid = 0; // So no valid page will overwrite worst_pid
		}
		if( worst_pid == META_PAGES_COUNT || mpp[i].tid < worst_tid){
			worst_pid = i;
			worst_tid = mpp[i].tid;
		}
	}
	Pid earliest_pid = META_PAGES_COUNT;
	for(Pid j = 0; j != META_PAGES_COUNT; ++j)
		if( worst_pid != j && valid_mpp[j] && (earliest_pid == META_PAGES_COUNT || mpp[j].tid < *earliest_tid)){
			earliest_pid = j;
			*earliest_tid = mpp[j].tid;
		}
	ass(earliest_pid != META_PAGES_COUNT, "In get_worst_meta_page earliest_tid not found");
	return worst_pid;
}

bool DB::get_newest_meta_page(MetaPage * newest_mp, Tid * earliest_tid, bool strict){
	MetaPage mpp[META_PAGES_COUNT];
	bool valid_mpp[META_PAGES_COUNT];
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		mpp[i] = read_meta_page(i);
		valid_mpp[i] = is_valid_meta(i, mpp[i]) && (!strict || is_valid_meta_strict(mpp[i]));
		if(valid_mpp[i] && mpp[i].page_count * page_size > file_size){
			file_size = data_file.get_size(); // file grew while we were not looking
			valid_mpp[i] = mpp[i].page_count * page_size <= file_size;
		}
	}
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		if(!valid_mpp[i])
			continue;
		Pid j = 0;
		for(; j != META_PAGES_COUNT; ++j)
			if( i != j && valid_mpp[j] && mpp[j].tid > mpp[i].tid)
				break;
		if(j != META_PAGES_COUNT)
			continue;
		*earliest_tid = mpp[i].tid;
		for(j = 0; j != META_PAGES_COUNT; ++j)
			if( i != j && valid_mpp[j] && mpp[j].tid < *earliest_tid)
				*earliest_tid = mpp[j].tid;
		*newest_mp = mpp[i];
		return true;
	}
//	for(Pid i = 0; i != META_PAGES_COUNT; ++i)
//		debug_print_meta_page(i, mpp[i]);
//	std::exit(-1);
	return false;
}

void DB::start_transaction(TX * tx){
	std::unique_ptr<std::lock_guard<std::mutex>> local_wr_guard;
	std::unique_ptr<os::FileLock> local_wr_file_lock;
	if(!tx->read_only){
		// write TX from same DB wait on guard
		local_wr_guard = std::make_unique<std::lock_guard<std::mutex>>(wr_mut);
		// write TX from different DB (same or different process) wait on file lock
		local_wr_file_lock = std::make_unique<os::FileLock>(data_file);
//		std::cerr << "Obtained main file write lock " << (size_t)this << std::endl;
//		sleep(3);
	}
	std::unique_lock<std::mutex> lock(mu);
	ass((!wr_transaction && !wr_file_lock) || tx->read_only, "We can have only one write transaction");
	ass(!c_mappings.empty(), "c_mappings should not be empty after db is open");
	{ // Shortest possible lock
//		os::FileLock reader_table_lock(lock_file);
//		std::cerr << "Obtained reader table lock " << (size_t)this << std::endl;
//		sleep(2);
		ass(get_newest_meta_page(&tx->meta_page, &tx->oldest_reader_tid, true), "No meta found in start_transaction - hot corruption of DB");
		if(tx->read_only){
			r_transactions_counter += 1;
			tx->reader_slot = reader_table.create_reader_slot(tx->meta_page.tid, lock_file, map_granularity);
			// Now we read newest meta page again, because it could change while we were grabbing the slot
			ass(get_newest_meta_page(&tx->meta_page, &tx->oldest_reader_tid, true), "No meta found in start_transaction 2 - hot corruption of DB");
		} else {
			wr_transaction = tx;
			tx->meta_page.tid += 1;
			tx->meta_page.pid = META_PAGES_COUNT; // So we do not forget to set it before write
			tx->oldest_reader_tid = reader_table.find_oldest_tid(tx->oldest_reader_tid, lock_file, map_granularity);
//			tx->oldest_reader_tid = 0;
			ass(tx->meta_page.tid >= tx->oldest_reader_tid, "We should not be able to treat our own pages as free");
		}
//		std::cerr << "Freeing reader table lock " << (size_t)this << std::endl;
	}
	if(!tx->read_only){
		file_size = data_file.get_size();
		grow_wr_mappings(tx->meta_page.page_count, false);
		wr_guard = std::move(local_wr_guard);
		wr_file_lock = std::move(local_wr_file_lock);
	}
	grow_c_mappings();
	tx->c_file_ptr = c_mappings.at(0).addr;
	tx->wr_file_ptr = wr_mappings.empty() ? nullptr : wr_mappings.at(0).addr;
	tx->file_page_count = file_size / page_size; // whole pages
	
	tx->used_mapping_size = c_mappings.at(0).size;
	c_mappings.at(0).ref_count += 1;
}
void DB::grow_transaction(TX * tx, Pid new_file_page_count){
	std::unique_lock<std::mutex> lock(mu);
	ass(wr_transaction && tx == wr_transaction && !tx->read_only, "We can only grow write transaction");
	ass(!c_mappings.empty() && !wr_mappings.empty(), "Mappings should not be empty in grow_transaction");
	grow_wr_mappings(new_file_page_count, true);
	grow_c_mappings();
	tx->c_file_ptr = c_mappings.at(0).addr;
	tx->wr_file_ptr = wr_mappings.at(0).addr;
	tx->file_page_count = file_size / page_size;
}
void DB::commit_transaction(TX * tx, MetaPage meta_page){
	// TODO - do not take this lock on long operation (msync)
	std::unique_lock<std::mutex> lock(mu);
	ass(tx == wr_transaction, "We can only commit write transaction if it started");
	data_file.msync(wr_mappings.at(0).addr, wr_mappings.at(0).size);
	__sync_synchronize();
	
	Pid oldest_meta_index = 0;
	{
//		os::FileLock reader_table_lock(lock_file);
		Pid worst_pid = get_worst_meta_page(&tx->oldest_reader_tid);
		meta_page.pid = worst_pid; // We usually save to different slot
		meta_page.crc32 = crc32c(0, &meta_page, sizeof(MetaPage) - sizeof(uint32_t));
		ass(is_valid_meta(meta_page.pid, meta_page), "");
		ass(is_valid_meta_strict(meta_page), "");
		write_meta_page(worst_pid, meta_page);
		tx->meta_page.tid += 1; // We continue using tx meta_page
		// We locked reader table anyway, take a chance to update oldest_reader_tid
		tx->oldest_reader_tid = reader_table.find_oldest_tid(tx->oldest_reader_tid, lock_file, map_granularity);
		ass(tx->meta_page.tid >= tx->oldest_reader_tid, "We should not be able to treat our own pages as free");
	
		if(options.meta_sync){
			// We can only msync on granularity, find limits
			size_t low = oldest_meta_index * page_size;
			size_t high = (oldest_meta_index + 1) * page_size;
			low = ((low / map_granularity)) * map_granularity;
			high = ((high + map_granularity - 1) / map_granularity) * map_granularity;
			data_file.msync(wr_mappings.at(0).addr + low, high - low);
		}
	}
}
void DB::finish_transaction(TX * tx){
	// TODO - do not take this lock on long operation (munmap)
	std::unique_lock<std::mutex> lock(mu);
	ass(tx->read_only || tx == wr_transaction, "We can only finish write transaction if it started");
	for(auto && ma : c_mappings)
		if( (tx->read_only && ma.size == tx->used_mapping_size) ||
		 	(!tx->read_only && ma.size >= tx->used_mapping_size)) {
			ma.ref_count -= 1;
		}
	tx->c_file_ptr = nullptr;
	tx->wr_file_ptr = nullptr;
	tx->file_page_count = 0;
	tx->used_mapping_size = 0;
	while(c_mappings.size() > 1 && c_mappings.back().ref_count == 0) {
		data_file.munmap(c_mappings.back().addr, c_mappings.back().size);
		c_mappings.pop_back();
	}
	if(tx->read_only){
		// We release slots without blocking, do not care if will be updated later
		reader_table.release_reader_slot(tx->reader_slot);
		r_transactions_counter -= 1;
		ass(r_transactions_counter >= 0, "read transaction finished twice");
		return;
	}
	wr_transaction = nullptr;
	while(wr_mappings.size() > 1) {
//		msync(wr_mappings.back().addr, wr_mappings.back().size, MS_SYNC);
		data_file.munmap(wr_mappings.back().addr, wr_mappings.back().size);
		wr_mappings.pop_back();
	}
//	std::cerr << "Freeing main file write lock " << (size_t)this << std::endl;
	wr_file_lock.reset();
	wr_guard.reset();
//	sleep(1);
}

void DB::debug_print_db(){
	std::cerr << "DB: page_size=" << page_size << " map_granularity=" << map_granularity << " file_size=" << file_size << std::endl;
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		debug_print_meta_page(i, read_meta_page(i));
	}
}
size_t DB::max_key_size()const{
    return mustela::max_key_size(page_size);
}
size_t DB::max_bucket_name_size()const{
    return mustela::max_key_size(page_size) - 1;
}

void DB::remove_db(const std::string & file_path){
    std::remove(file_path.c_str());
    std::remove((file_path + ".lock").c_str());
}

bool DB::open_db(MetaPage * newest_mp){
//	if( file_size < sizeof(MetaPage) )
//		throw Exception("File size less than 1 meta page - corrupted by truncation");
//	os::FileLock reader_table_lock(lock_file); // We read meta pages
//	file_size = data_file.get_size();
	page_size = MAX_PAGE_SIZE; // required for grow_c_mappings
	grow_c_mappings();
	page_size = read_meta_page(0).page_size;
	Tid earliest_tid = 0;
	if(page_size < MIN_PAGE_SIZE || page_size > MAX_PAGE_SIZE || (page_size & (page_size - 1)) != 0 ||
		!get_newest_meta_page(newest_mp, &earliest_tid, false)){
		// If meta page 0 page_size is corrupted, will have to try all page sizes
		for(page_size = MIN_PAGE_SIZE; page_size <= MAX_PAGE_SIZE; page_size *= 2)
			if( get_newest_meta_page(newest_mp, &earliest_tid, false) )
				break;
		if(page_size > MAX_PAGE_SIZE)
			return false;
	}
	return get_newest_meta_page(newest_mp, &earliest_tid, true);
}

void DB::create_db(){
	// Wrong order is deadlock
//	os::FileLock reader_table_lock(lock_file); // We modify meta pages

	grow_wr_mappings(META_PAGES_COUNT + 1, false);
	grow_c_mappings();

//	memset(wr_mappings.at(0).addr, 0, wr_mappings.at(0).size);

	LeafPage * root_page = (LeafPage *)(wr_mappings.at(0).addr + page_size * META_PAGES_COUNT);
	LeafPtr wr_dap(page_size, root_page);
	wr_dap.init_dirty(0);
	
	data_file.msync(wr_mappings.at(0).addr, wr_mappings.at(0).size);

	MetaPage mp{};
	mp.magic = META_MAGIC;
	mp.page_count = META_PAGES_COUNT + 1;
	mp.version = OUR_VERSION;
	mp.page_size = static_cast<uint32_t>(page_size);
	mp.pid_size = NODE_PID_SIZE;
	mp.meta_bucket.leaf_page_count = 1;
	mp.meta_bucket.root_page = META_PAGES_COUNT;
	for(mp.pid = 0; mp.pid != META_PAGES_COUNT; ++mp.pid){
		mp.crc32 = crc32c(0, &mp, sizeof(MetaPage) - sizeof(uint32_t));
		write_meta_page(mp.pid, mp);
	}
	data_file.msync(wr_mappings.at(0).addr, wr_mappings.at(0).size);
}

void DB::grow_c_mappings() {
	if( !c_mappings.empty() && c_mappings.at(0).size >= file_size && c_mappings.at(0).size >= META_PAGES_COUNT * MAX_PAGE_SIZE )
		return;
	uint64_t fs = file_size;
	if( !readonly_fs && !options.read_only )
		fs = std::max<uint64_t>(fs, options.minimal_mapping_size) * 128 / 64; // x1.5
	fs = std::max<uint64_t>(fs, META_PAGES_COUNT * MAX_PAGE_SIZE); // for initial meta discovery in open_db
	fs = os::grow_to_granularity(fs, page_size, map_granularity);
	char * wm = data_file.mmap(0, fs, true, false);
	c_mappings.insert(c_mappings.begin(), Mapping(fs, wm, wr_transaction ? 1 : 0));
}
void DB::grow_wr_mappings(Pid new_file_page_count, bool grow_more){
	uint64_t fs = file_size;
 	fs = std::max<uint64_t>(fs, new_file_page_count * page_size);
	if( grow_more )
	 	fs = std::max<uint64_t>(fs, options.minimal_mapping_size) * 77 / 64; // x1.2
	uint64_t new_fs = os::grow_to_granularity(fs, page_size, map_granularity);
	if(!wr_mappings.empty() && new_fs == file_size && wr_mappings.at(0).size == new_fs)
		return;
	ass(wr_mappings.empty() || wr_mappings.at(0).size < new_fs, "file was shrunk beyond our control - write mapping is now invalid");
	// TODO - be ready to shrinking of file
	if(new_fs != file_size){
		data_file.set_size(new_fs);
		file_size = data_file.get_size();
		ass( new_fs == file_size, "file failed to grow in grow_file");
	}
	char * wm = data_file.mmap(0, new_fs, true, true);
	wr_mappings.insert(wr_mappings.begin(), Mapping(new_fs, wm, 0));
}

