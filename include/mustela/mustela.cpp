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

namespace mustela {
	
	const size_t additional_granularity = 1;// 65536;  // on Windows mmapped regions should be aligned to 65536
	
	static uint64_t grow_to_granularity(uint64_t value, uint64_t page_size){
		return ((value + page_size - 1) / page_size) * page_size;
	}
	static uint64_t grow_to_granularity(uint64_t value, uint64_t a, uint64_t b, uint64_t c){
		return grow_to_granularity(grow_to_granularity(grow_to_granularity(value, a), b), c);
	}
	DB::DB(const std::string & file_path, bool read_only):read_only(read_only), page_size(128), physical_page_size(static_cast<decltype(physical_page_size)>(sysconf(_SC_PAGESIZE))){
		fd = open(file_path.c_str(), (read_only ? O_RDONLY : O_RDWR) | O_CREAT, (mode_t)0600);
		if( fd == -1)
			throw Exception("file open failed");
		file_size = lseek(fd, 0, SEEK_END);
		if( file_size == uint64_t(-1))
			throw Exception("file lseek SEEK_END failed");
		if( file_size < sizeof(MetaPage) ){
			create_db();
			return;
		}
		//        throw Exception("mmap file_size too small (corrupted or different platform)");
		grow_c_mappings();
		const MetaPage * meta_pages[3]{readable_meta_page(0), readable_meta_page(1), readable_meta_page(2)};
		if( meta_pages[0]->magic != META_MAGIC && meta_pages[0]->magic != META_MAGIC_ALTENDIAN )
			throw Exception("file is either not mustela DB or corrupted");
		if( meta_pages[0]->magic != META_MAGIC && meta_pages[0]->page_size != page_size )
			throw Exception("file is from incompatible platform, should convert before opening");
		if( meta_pages[0]->version != OUR_VERSION )
			throw Exception("file is from older or newer version, should convert before opening");
		if( file_size < page_size * 5 ){
			// TODO - check pages_count, if >5 then corrupted (truncated)
			create_db(); // We could just not finish the last create_db call
			return;
		}
		if( !meta_pages[0]->check(page_size, file_size) || !meta_pages[1]->check(page_size, file_size) || !meta_pages[2]->check(page_size, file_size))
			throw Exception("meta_pages failed to check - (corrupted)");
		//        if( meta_pages[last_meta_page()]->page_count * page_size > file_size )
		//            throw Exception("file too short for last transaction (corrupted)");
	}
	DB::~DB(){
		
	}
	size_t DB::last_meta_page_index()const{
		size_t result = 0;
		if( readable_meta_page(1)->effective_tid() > readable_meta_page(result)->effective_tid() )
			result = 1;
		if( readable_meta_page(2)->effective_tid() > readable_meta_page(result)->effective_tid() )
			result = 2;
		return result;
	}
	size_t DB::oldest_meta_page_index()const{
		size_t result = 0;
		if( readable_meta_page(1)->effective_tid() < readable_meta_page(result)->effective_tid() )
			result = 1;
		if( readable_meta_page(2)->effective_tid() < readable_meta_page(result)->effective_tid() )
			result = 2;
		return result;
	}
	
	void DB::create_db(){
		if( lseek(fd, 0, SEEK_SET) == -1 )
			throw Exception("file seek failed");
		{
			char meta_buf[page_size];
			memset(meta_buf, 0, page_size); // C++ standard URODI "variable size object cannot be initialized"
			MetaPage * mp = (MetaPage *)meta_buf;
			mp->magic = META_MAGIC;
			mp->version = OUR_VERSION;
			mp->page_size = page_size;
			mp->page_count = 4;
			mp->meta_table.leaf_page_count = 1;
			mp->meta_table.root_page = 3;
			mp->dirty = false;
			if( write(fd, meta_buf, page_size) == -1)
				throw Exception("file write failed in create_db");
			mp->pid = 1;
			if( write(fd, meta_buf, page_size) == -1)
				throw Exception("file write failed in create_db");
			mp->pid = 2;
			if( write(fd, meta_buf, page_size) == -1)
				throw Exception("file write failed in create_db");
		}
		{
			char data_buf[page_size];
			LeafPtr mp(page_size, (LeafPage *)data_buf);
			mp.mpage()->pid = 3;
			mp.init_dirty(0);
			if( write(fd, data_buf, page_size) == -1)
				throw Exception("file write failed in create_db");
			mp.mpage()->pid = 4;
			if( write(fd, data_buf, page_size) == -1)
				throw Exception("file write failed in create_db");
		}
		if( fsync(fd) == -1 )
			throw Exception("fsync failed in create_db");
		file_size = lseek(fd, 0, SEEK_END);
		if( file_size == uint64_t(-1))
			throw Exception("file lseek SEEK_END failed");
		grow_c_mappings();
	}
	void DB::grow_c_mappings() {
		if( !c_mappings.empty() && c_mappings.back().end_page * page_size >= file_size )
			return;
		uint64_t fs = read_only ? file_size : (file_size + 1024*1024*1024) * 3 / 2;
		uint64_t new_fs = grow_to_granularity(fs, page_size, physical_page_size, additional_granularity);
		void * cm = mmap(0, new_fs, PROT_READ, MAP_SHARED, fd, 0);
		if (cm == MAP_FAILED)
			throw Exception("mmap PROT_READ failed");
		c_mappings.push_back(Mapping(new_fs / page_size, (char *)cm));
	}
	const DataPage * DB::readable_page(Pid page)const{
		ass( !c_mappings.empty() && page + 1 <= c_mappings.back().end_page, "readable_page out of range");
		return (const DataPage * )(c_mappings.back().addr + page_size * page);
	}
	void DB::trim_old_c_mappings(Pid end_page){
		for(auto && ma : c_mappings)
			if( ma.end_page == end_page ) {
				ma.ref_count -= 1;
				break;
			}
		while( c_mappings.size() > 1 && c_mappings.front().ref_count == 0) {
			munmap(c_mappings.front().addr, c_mappings.front().end_page * page_size);
			c_mappings.erase(c_mappings.begin());
		}
	}
	
	// Mappings cannot be in chunks, because count pages could fall onto the edge between chunkcs
	DataPage * DB::writable_page(Pid page, Pid count){
		if( mappings.empty() ){
			uint64_t new_fs = grow_to_granularity(file_size, page_size, physical_page_size, additional_granularity);
			if( new_fs > file_size ){
				if( lseek(fd, new_fs - 1, SEEK_SET) == -1 )
					throw Exception("file seek failed in writable_page");
				if( write(fd, "", 1) != 1 )
					throw Exception("file write failed in writable_page");
				file_size = lseek(fd, 0, SEEK_END);
				if( new_fs != file_size )
					throw Exception("file failed to grow in writable_page");
			}
			void * wm = mmap(0, new_fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
			if (wm == MAP_FAILED)
				throw Exception("mmap PROT_READ failed");
			mappings.push_back(Mapping(new_fs / page_size, (char *)wm));
		}
		ass( !mappings.empty() && page + count <= mappings.back().end_page, "writable_page out of range");
		return (DataPage *)(mappings.back().addr + page * page_size);
	}
	void DB::grow_file(Pid new_page_count){
		if( new_page_count * page_size <= file_size )
			return;
		uint64_t fs = (file_size + 1024 * page_size) * 5 / 4; // grow faster while file size is small
		uint64_t new_fs = grow_to_granularity(fs, page_size, physical_page_size, additional_granularity);
		if( lseek(fd, new_fs - 1, SEEK_SET) == -1 )
			throw Exception("file seek failed in grow_file");
		if( write(fd, "", 1) != 1 )
			throw Exception("file write failed in grow_file");
		file_size = lseek(fd, 0, SEEK_END);
		if( new_fs != file_size )
			throw Exception("file failed to grow in grow_file");
		void * wm = mmap(0, new_fs, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		if (wm == MAP_FAILED)
			throw Exception("mmap PROT_READ | PROT_WRITE failed");
		mappings.push_back(Mapping(new_fs / page_size, (char *)wm));
		grow_c_mappings();
	}
	void DB::trim_old_mappings(){
		if( mappings.empty() )
			return;
		for(size_t m = 0; m != mappings.size() - 1; ++m){
			munmap(mappings[m].addr, mappings[m].end_page * page_size);
		}
		mappings.erase(mappings.begin(), mappings.end() - 1);
	}
}
