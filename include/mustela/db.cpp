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
		throw Exception("new_db_page_size must be power of 2");
	os::FileLock data_lock(data_file); // We modify data pages
	os::FileLock reader_table_lock(lock_file); // We modify meta pages
	file_size = data_file.get_size();
	if( file_size == 0 ){
		page_size = options.new_db_page_size == 0 ? GOOD_PAGE_SIZE : options.new_db_page_size;
		create_db();
//		return;
	}
	if( file_size < sizeof(MetaPage) )
		throw Exception("File size less than 1 meta page - corrupted by truncation");
	page_size = MAX_PAGE_SIZE;
	grow_c_mappings();
	page_size = readable_meta_page(0)->page_size;
	const MetaPage * newest_meta = nullptr;
	Pid oldest_index = 0;
	Tid earliest_tid = 0;
	if(page_size < MIN_PAGE_SIZE || page_size > MAX_PAGE_SIZE || (page_size & (page_size - 1)) != 0 ||
		!(newest_meta = get_newest_meta_page(&oldest_index, &earliest_tid, false))){
		// If meta page 0 page_size is corrupted, will have to try all page sizes
		for(page_size = MIN_PAGE_SIZE; page_size <= MAX_PAGE_SIZE; page_size *= 2)
			if( (newest_meta = get_newest_meta_page(&oldest_index, &earliest_tid, false)) )
				break;
		if(page_size > MAX_PAGE_SIZE)
			throw Exception("Failed to find valid meta page of any supported page size");
	}
	debug_print_db();
	if(newest_meta->version != OUR_VERSION)
		throw Exception("Incompatible database version");
	if(newest_meta->pid_size != NODE_PID_SIZE)
		throw Exception("Incompatible pid size");
	if( !get_newest_meta_page(&oldest_index, &earliest_tid, true))
		throw Exception("Database corrupted (possibly truncated or meta pages are mismatched)");
}
DB::~DB(){
	ass(r_transactions_counter == 0, "Some reader TX still exist while in DB::~DB");
	for(auto && ma : c_mappings)
		ass(ma.ref_count == 0, "c_mappings ref counts inconsistent in DB::~DB");
	for(auto && ma : wr_mappings)
		ass(ma.ref_count == 0, "wr_mappings ref counts inconsistent in DB::~DB");
}
const MetaPage * DB::readable_meta_page(Pid index)const {
	ass(!c_mappings.empty() && (index + 1)*page_size <= c_mappings.at(0).end_addr, "readable_page out of range");
	return (const MetaPage * )(c_mappings.at(0).addr + page_size * index);
}
MetaPage * DB::writable_meta_page(Pid index) {
	ass(!wr_mappings.empty() && (index + 1)*page_size <= file_size, "writable_page out of range");
	return (MetaPage * )(wr_mappings.at(0).addr + page_size * index);
}
bool DB::is_valid_meta(Pid index, const MetaPage * mp)const{
	if((index + 1) * page_size > file_size )
		return false;
	if( mp->pid != index || mp->magic != META_MAGIC)
		return false; // throw Exception("file is either not mustela DB or corrupted - wrong meta page");
	if( mp->pid_size < 4 || mp->pid_size > 8 || mp->page_size != page_size || mp->page_count < 4 )
		return false;
	if( mp->crc32 != crc32c(0, mp, sizeof(MetaPage) - sizeof(uint32_t)))
		return false;
	return true;
}
bool DB::is_valid_meta_strict(const MetaPage * mp)const{
	if( mp->meta_bucket.root_page >= mp->page_count || mp->page_count * page_size > file_size )
		return false;
	if( mp->version != OUR_VERSION || mp->pid_size != NODE_PID_SIZE )
		return false;
	return true;
}

const MetaPage * DB::get_newest_meta_page(Pid * overwrite_index, Tid * earliest_tid, bool strict)const{
	const MetaPage * newest_mp = nullptr;
	const MetaPage * corrupted_mp = nullptr;
	const MetaPage * oldest_mp = nullptr;
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		const MetaPage * mp = readable_meta_page(i);
		if( !is_valid_meta(i, mp) || (strict && !is_valid_meta_strict(mp)) ){
			corrupted_mp = mp;
			*overwrite_index = i;
			continue;
		}
		if(!newest_mp || mp->tid > newest_mp->tid || (mp->tid == newest_mp->tid && mp->pid > newest_mp->pid)){
			newest_mp = mp;
		}
		if(!oldest_mp || mp->tid < oldest_mp->tid || (mp->tid == oldest_mp->tid && mp->pid < oldest_mp->pid)){
			oldest_mp = mp;
			*earliest_tid = mp->tid;
			if(!corrupted_mp)
				*overwrite_index = i;
		}
	}
	return newest_mp;
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
		os::FileLock reader_table_lock(lock_file);
//		std::cerr << "Obtained reader table lock " << (size_t)this << std::endl;
//		sleep(2);
		Pid oldest_meta_index = 0;
		const MetaPage * newest_meta_page = get_newest_meta_page(&oldest_meta_index, &tx->oldest_reader_tid, true);
		ass(newest_meta_page, "No meta found in start_transaction - hot corruption of DB");
		tx->meta_page = *newest_meta_page;
		tx->meta_page.pid = 0; // So we do not forget to set it before write
		if(tx->read_only){
			r_transactions_counter += 1;
			tx->reader_slot = reader_table.create_reader_slot(tx->meta_page.tid, lock_file, map_granularity);
		} else {
			wr_transaction = tx;
			tx->meta_page.tid += 1;
			tx->oldest_reader_tid = reader_table.find_oldest_tid(tx->meta_page.tid);
//			tx->oldest_reader_tid = 0;
			ass(tx->meta_page.tid >= tx->oldest_reader_tid, "We should not be able to treat our own pages as free");
		}
//		std::cerr << "Freeing reader table lock " << (size_t)this << std::endl;
	}
	if(!tx->read_only){
		grow_wr_mappings(tx->meta_page.page_count, false);
		wr_guard = std::move(local_wr_guard);
		wr_file_lock = std::move(local_wr_file_lock);
	}else{
		if(tx->meta_page.page_count * page_size > file_size){
			file_size = data_file.get_size();
			ass(tx->meta_page.page_count * page_size <= file_size, "Corruption - meta page count spans end of file");
			grow_c_mappings();
		}
	}
	tx->c_file_ptr = c_mappings.at(0).addr;
	tx->wr_file_ptr = wr_mappings.empty() ? nullptr : wr_mappings.at(0).addr;
	tx->file_page_count = file_size / page_size; // whole pages
	
	tx->used_mapping_size = c_mappings.at(0).end_addr;
	c_mappings.at(0).ref_count += 1;
}
void DB::grow_transaction(TX * tx, Pid new_file_page_count){
	std::unique_lock<std::mutex> lock(mu);
	ass(wr_transaction && tx == wr_transaction && !tx->read_only, "We can only grow write transaction");
	ass(!c_mappings.empty() && !wr_mappings.empty(), "Mappings should not be empty in grow_transaction");
	grow_wr_mappings(new_file_page_count, true);
	tx->c_file_ptr = c_mappings.at(0).addr;
	tx->wr_file_ptr = wr_mappings.at(0).addr;
	tx->file_page_count = file_size / page_size;
}
void DB::commit_transaction(TX * tx, MetaPage meta_page){
	// TODO - do not take this lock on long operation (msync)
	std::unique_lock<std::mutex> lock(mu);
	ass(tx == wr_transaction, "We can only commit write transaction if it started");
	data_file.msync(wr_mappings.at(0).addr, wr_mappings.at(0).end_addr);

	Pid oldest_meta_index = 0;
	{
		os::FileLock reader_table_lock(lock_file);
		const MetaPage * newest_meta_page = get_newest_meta_page(&oldest_meta_index, &tx->oldest_reader_tid, true);
		ass(newest_meta_page, "No meta found in start_transaction - hot corruption of DB");
		meta_page.pid = oldest_meta_index; // We usually save to different slot
		meta_page.crc32 = crc32c(0, &meta_page, sizeof(MetaPage) - sizeof(uint32_t));
		MetaPage * wr_meta = writable_meta_page(oldest_meta_index);
		*wr_meta = meta_page;
		tx->meta_page.tid += 1; // We continue using tx meta_page
		// We locked reader table anyway, take a chance to update oldest_reader_tid
		tx->oldest_reader_tid = reader_table.find_oldest_tid(tx->meta_page.tid);
		ass(tx->meta_page.tid >= tx->oldest_reader_tid, "We should not be able to treat our own pages as free");
	}
	if(options.meta_sync){
		// We can only msync on granularity, find limits
		size_t low = oldest_meta_index * page_size;
		size_t high = (oldest_meta_index + 1) * page_size;
		low = ((low / map_granularity)) * map_granularity;
		high = ((high + map_granularity - 1) / map_granularity) * map_granularity;
		data_file.msync(wr_mappings.at(0).addr + low, high - low);
	}
}
void DB::finish_transaction(TX * tx){
	// TODO - do not take this lock on long operation (munmap)
	std::unique_lock<std::mutex> lock(mu);
	ass(tx->read_only || tx == wr_transaction, "We can only finish write transaction if it started");
	for(auto && ma : c_mappings)
		if( (tx->read_only && ma.end_addr == tx->used_mapping_size) ||
		 	(!tx->read_only && ma.end_addr >= tx->used_mapping_size)) {
			ma.ref_count -= 1;
		}
	tx->c_file_ptr = nullptr;
	tx->wr_file_ptr = nullptr;
	tx->file_page_count = 0;
	tx->used_mapping_size = 0;
	while(c_mappings.size() > 1 && c_mappings.back().ref_count == 0) {
		data_file.munmap(c_mappings.back().addr, c_mappings.back().end_addr);
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
//		msync(wr_mappings.back().addr, wr_mappings.back().end_addr, MS_SYNC);
		data_file.munmap(wr_mappings.back().addr, wr_mappings.back().end_addr);
		wr_mappings.pop_back();
	}
//	std::cerr << "Freeing main file write lock " << (size_t)this << std::endl;
	wr_file_lock.reset();
	wr_guard.reset();
	sleep(1);
}

void DB::debug_print_db(){
	std::cerr << "DB: page_size=" << page_size << " map_granularity=" << map_granularity << " file_size=" << file_size << std::endl;
	for(Pid i = 0; i != META_PAGES_COUNT; ++i){
		std::cerr << "  meta page " << i << ": ";
		if((i + 1) * page_size > file_size ){
			std::cerr << "(partially?) BEYOND END OF FILE" << std::endl;;
			continue;
		}
		const MetaPage * mp = readable_meta_page(i);
		bool crc_ok = mp->crc32 == crc32c(0, mp, sizeof(MetaPage) - sizeof(uint32_t));
		std::cerr << (is_valid_meta(i, mp) ? "GOOD" : crc_ok ? "BAD" : "WRONG CRC");
		std::cerr << " pid=" << mp->pid << " tid=" << mp->tid << " page_count=" << mp->page_count << " ver=" << mp->version << " pid_size=" << mp->pid_size << std::endl;;
		std::cerr << "    meta bucket: height=" << mp->meta_bucket.height << " items=" << mp->meta_bucket.count << " leafs=" << mp->meta_bucket.leaf_page_count << " nodes=" << mp->meta_bucket.node_page_count << " overflows=" << mp->meta_bucket.overflow_page_count << " root_page=" << mp->meta_bucket.root_page << std::endl;
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

void DB::create_db(){
	grow_wr_mappings(META_PAGES_COUNT + 1, false);
	//char data_buf[MAX_PAGE_SIZE]; // Variable-length arrays are C99 feature
	memset(wr_mappings.at(0).addr, 0, wr_mappings.at(0).end_addr); // C++ standard URODI "variable size object cannot be initialized"
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
		*writable_meta_page(mp.pid) = mp;
	}
	LeafPtr wr_dap(page_size, (LeafPage *)writable_meta_page(META_PAGES_COUNT)); // hack with typecast
	wr_dap.init_dirty(0);
	data_file.msync(wr_mappings.at(0).addr, wr_mappings.at(0).end_addr);
}

void DB::grow_c_mappings() {
	if( !c_mappings.empty() && c_mappings.at(0).end_addr >= file_size && c_mappings.at(0).end_addr >= META_PAGES_COUNT * MAX_PAGE_SIZE )
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
	file_size = data_file.get_size();
	uint64_t fs = file_size;
 	fs = std::max<uint64_t>(fs, new_file_page_count * page_size);
	if( grow_more )
	 	fs = std::max<uint64_t>(fs, options.minimal_mapping_size) * 77 / 64; // x1.2
	uint64_t new_fs = os::grow_to_granularity(fs, page_size, map_granularity);
	if(!wr_mappings.empty() && new_fs == file_size && wr_mappings.at(0).end_addr == new_fs)
		return; // TODO - ensure file does not grow down in size
	if(new_fs != file_size){
		data_file.set_size(new_fs);
		file_size = data_file.get_size();
		if( new_fs != file_size )
			throw Exception("file failed to grow in grow_file");
	}
	char * wm = data_file.mmap(0, new_fs, true, true);
	wr_mappings.insert(wr_mappings.begin(), Mapping(new_fs, wm, wr_transaction ? 1 : 0));
	grow_c_mappings();
}

