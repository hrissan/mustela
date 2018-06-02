#include "mustela.hpp"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <iostream>
#include <algorithm>

using namespace mustela;
	
const size_t additional_granularity = 1;// 65536;  // on Windows mmapped regions should be aligned to 65536

static uint64_t grow_to_granularity(uint64_t value, uint64_t page_size){
	return ((value + page_size - 1) / page_size) * page_size;
}
static uint64_t grow_to_granularity(uint64_t value, uint64_t a, uint64_t b, uint64_t c){
	return grow_to_granularity(grow_to_granularity(grow_to_granularity(value, a), b), c);
}
DB::FD::~FD(){
	close(fd); fd = -1;
}
std::string DB::lib_version(){
	return "0.01";
}

DB::DB(const std::string & file_path, bool read_only):fd(open(file_path.c_str(), (read_only ? O_RDONLY : O_RDWR) | O_CREAT, (mode_t)0600)), read_only(read_only), page_size(GOOD_PAGE_SIZE), physical_page_size(static_cast<decltype(physical_page_size)>(sysconf(_SC_PAGESIZE))){
	if( fd.fd == -1)
		throw Exception("file open failed");
	file_size = lseek(fd.fd, 0, SEEK_END);
	if( file_size == uint64_t(-1))
		throw Exception("file lseek SEEK_END failed");
	if( file_size == 0 ){
		create_db();
		return;
	}
	if( file_size < sizeof(MetaPage) )
		throw Exception("File size less than 1 meta page - corrupted by truncation");
	grow_c_mappings();
	page_size = readable_meta_page(0)->page_size;
	Pid newest_index = 0, overwrite_index = 0;
	Tid earliest_tid = 0;
	if(page_size >= MIN_PAGE_SIZE && page_size <= MAX_PAGE_SIZE && (page_size & (page_size - 1)) == 0 &&
		file_size >= META_PAGES_COUNT * page_size && get_meta_indices(&newest_index, &overwrite_index, &earliest_tid)){
		print_db();
		return;
	}
	for(page_size = MIN_PAGE_SIZE; page_size <= MAX_PAGE_SIZE; page_size *= 2)
		if( file_size >= META_PAGES_COUNT * page_size && get_meta_indices(&newest_index, &overwrite_index, &earliest_tid) ){
			print_db();
			return;
		}
	throw Exception("Failed to find valid meta page of any supported page size");
}
DB::~DB(){
	for(auto && ma : c_mappings)
		ass(ma.ref_count == 0, "Some TX still exist while in DB::~DB");
}
void DB::print_db(){
	std::cerr << "-+-- DB: page_size=" << page_size << " phys. page_size=" << physical_page_size << " file_size=" << file_size << std::endl;
	for(int i = 0; i != META_PAGES_COUNT; ++i){
		const MetaPage * mp = readable_meta_page(i);
		std::cerr << " +-- meta page " << i << ": ";
		if( !is_valid_meta(mp) ){
			std::cerr << "CORRUPTED" << std::endl;;
			continue;
		}
		std::cerr << "tid=" << mp->tid << " page_count=" << mp->page_count << std::endl;;
		std::cerr << "  +- meta bucket: height=" << mp->meta_bucket.height << " items=" << mp->meta_bucket.count << " leafs=" << mp->meta_bucket.leaf_page_count << " nodes=" << mp->meta_bucket.node_page_count << " overflows=" << mp->meta_bucket.overflow_page_count << " root_page=" << mp->meta_bucket.root_page << std::endl;
	}
}
size_t DB::max_key_size()const{
    return CNodePtr::max_key_size(page_size);
}
size_t DB::max_bucket_name_size()const{
    return CNodePtr::max_key_size(page_size) - 1;
}

void DB::remove_db(const std::string & file_path){
    std::remove(file_path.c_str());
}

bool DB::is_valid_meta(const MetaPage * mp)const{
	if( mp->magic != META_MAGIC || mp->version != OUR_VERSION)
		return false; // throw Exception("file is either not mustela DB or corrupted - wrong meta page");
	if( mp->pid_size > 8 || mp->page_size != page_size || mp->page_count < 4 || mp->meta_bucket.root_page >= mp->page_count || mp->page_count * page_size > file_size )
		return false;
	if( mp->crc32 != crc32c(0, mp, sizeof(MetaPage) - sizeof(uint32_t)))
		return false;
	return true;
}

bool DB::get_meta_indices(Pid * newest_index, Pid * overwrite_index, Tid * earliest_tid)const{
	Tid newest_tid = 0;
	*earliest_tid = std::numeric_limits<Tid>::max();
	bool invalid_found = false;
	for(int i = 0; i != META_PAGES_COUNT; ++i){
		const MetaPage * mp = readable_meta_page(i);
		if( !is_valid_meta(mp) ){
			invalid_found = true;
			*overwrite_index = i;
			continue;
		}
		if(mp->tid > newest_tid){
			newest_tid = mp->tid;
			*newest_index = i;
		}
		if(mp->tid < *earliest_tid){
			*earliest_tid = mp->tid;
			if(!invalid_found)
				*overwrite_index = i;
		}
	}
	return *earliest_tid != std::numeric_limits<Tid>::max();
}

void DB::create_db(){
	if( lseek(fd.fd, 0, SEEK_SET) == -1 )
		throw Exception("file seek SEEK_SET failed");
	char data_buf[page_size];
	memset(data_buf, 0, page_size); // C++ standard URODI "variable size object cannot be initialized"
	MetaPage * mp = (MetaPage *)data_buf;
	mp->magic = META_MAGIC;
	mp->page_count = META_PAGES_COUNT + 1;
	mp->version = OUR_VERSION;
	mp->page_size = static_cast<uint32_t>(page_size);
	mp->pid_size = NODE_PID_SIZE;
	mp->meta_bucket.leaf_page_count = 1;
	mp->meta_bucket.root_page = META_PAGES_COUNT;
	for(mp->pid = 0; mp->pid != META_PAGES_COUNT; ++mp->pid){
		mp->crc32 = crc32c(0, mp, sizeof(MetaPage) - sizeof(uint32_t));
		if( write(fd.fd, data_buf, page_size) == -1)
			throw Exception("file write failed in create_db");
	}
	LeafPtr wr_dap(page_size, (LeafPage *)data_buf);
	wr_dap.mpage()->pid = META_PAGES_COUNT;
	wr_dap.init_dirty(0);
	if( write(fd.fd, data_buf, page_size) == -1)
		throw Exception("file write failed in create_db");
	if( fsync(fd.fd) == -1 )
		throw Exception("fsync failed in create_db");
	file_size = lseek(fd.fd, 0, SEEK_END);
	if( file_size == uint64_t(-1))
		throw Exception("file lseek SEEK_END failed");
	grow_c_mappings();
}

void DB::grow_c_mappings() {
	if( !c_mappings.empty() && c_mappings.back().end_addr >= file_size )
		return;
	uint64_t fs = read_only ? file_size : (file_size + 1024) * 3 / 2; // *1024*1024
	fs = std::max<uint64_t>(fs, META_PAGES_COUNT * MAX_PAGE_SIZE); // for initial meta discovery in open_db
	uint64_t new_fs = grow_to_granularity(fs, page_size, physical_page_size, additional_granularity);
	void * cm = mmap(0, new_fs, PROT_READ, MAP_SHARED, fd.fd, 0);
	if (cm == MAP_FAILED)
		throw Exception("mmap PROT_READ failed");
	c_mappings.push_back(Mapping(new_fs, (char *)cm));
}
const DataPage * DB::readable_page(Pid page, Pid count)const{ // TODO - optimize by moving back() into variables inside DB class
	ass( !c_mappings.empty() && (page + count)*page_size <= c_mappings.back().end_addr, "readable_page out of range");
	return (const DataPage * )(c_mappings.back().addr + page_size * page);
}
void DB::trim_old_c_mappings(size_t end_addr){
	for(auto && ma : c_mappings)
		if( ma.end_addr == end_addr ) {
			ma.ref_count -= 1;
			break;
		}
	while( c_mappings.size() > 1 && c_mappings.front().ref_count == 0) {
		munmap(c_mappings.front().addr, c_mappings.front().end_addr);
		c_mappings.erase(c_mappings.begin());
	}
}
void DB::trim_old_mappings(){
	if( mappings.empty() )
		return;
	for(size_t m = 0; m != mappings.size() - 1; ++m){
		munmap(mappings[m].addr, mappings[m].end_addr);
	}
	mappings.erase(mappings.begin(), mappings.end() - 1);
}

// Mappings cannot be in chunks, because count pages could fall onto the edge between chunkcs
DataPage * DB::writable_page(Pid page, Pid count){
	if( mappings.empty() ){
		uint64_t new_fs = grow_to_granularity(file_size, page_size, physical_page_size, additional_granularity);
		if( new_fs > file_size ){
			if( lseek(fd.fd, new_fs - 1, SEEK_SET) == -1 )
				throw Exception("file seek failed in writable_page");
			if( write(fd.fd, "", 1) != 1 )
				throw Exception("file write failed in writable_page");
			file_size = lseek(fd.fd, 0, SEEK_END);
			if( new_fs != file_size )
				throw Exception("file failed to grow in writable_page");
		}
		void * wm = mmap(0, new_fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd.fd, 0);
		if (wm == MAP_FAILED)
			throw Exception("mmap PROT_READ | PROT_WRITE failed");
		mappings.push_back(Mapping(new_fs, (char *)wm));
	}
	ass( !mappings.empty() && (page + count) * page_size <= mappings.back().end_addr, "writable_page out of range");
	return (DataPage *)(mappings.back().addr + page * page_size);
}
void DB::grow_file(Pid new_page_count){
	if( new_page_count * page_size <= file_size )
		return;
	uint64_t fs = (file_size + 32 * page_size) * 5 / 4; // * 32 grow faster while file size is small
	uint64_t new_fs = grow_to_granularity(fs, page_size, physical_page_size, additional_granularity);
	if( lseek(fd.fd, new_fs - 1, SEEK_SET) == -1 )
		throw Exception("file seek failed in grow_file");
	if( write(fd.fd, "", 1) != 1 )
		throw Exception("file write failed in grow_file");
	file_size = lseek(fd.fd, 0, SEEK_END);
	if( new_fs != file_size )
		throw Exception("file failed to grow in grow_file");
	void * wm = mmap(0, new_fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd.fd, 0);
	if (wm == MAP_FAILED)
		throw Exception("mmap PROT_READ | PROT_WRITE failed");
	mappings.push_back(Mapping(new_fs, (char *)wm));
	grow_c_mappings();
}
