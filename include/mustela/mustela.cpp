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

    DB::DB(const std::string & file_path, bool read_only):read_only(read_only), page_size(128), physical_page_size(static_cast<decltype(physical_page_size)>(sysconf(_SC_PAGESIZE))){
        fd = open(file_path.c_str(), (read_only ? O_RDONLY : O_RDWR) | O_CREAT, (mode_t)0600);
        if( fd == -1)
            throw Exception("file open failed");
        uint64_t file_size = lseek(fd, 0, SEEK_END);
        if( file_size == -1)
            throw Exception("file lseek SEEK_END failed");
        void * cm = mmap(0, MAX_MAPPING_SIZE, PROT_READ, MAP_SHARED, fd, 0);
        if (cm == MAP_FAILED)
            throw Exception("mmap PROT_READ failed");
        c_mapping = (char *)cm;
        if( file_size < sizeof(MetaPage) ){
            create_db();
            return;
        }
//        throw Exception("mmap file_size too small (corrupted or different platform)");
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
    }
    DataPage * DB::writable_page(Pid page, Pid count)const{
/*        auto it = std::upper_bound(mappings.begin(), mappings.end(), page, [](Pid p, const Mapping & ma) {
            return p < ma.end_page;
        });
        ass(it != mappings.end(), "writable_page out of range");
        ass(page + count <= it->end_page, "writable_page range crosses mapping boundary");
        return (DataPage *)(it->addr + (page - it->begin_page)*page_size);
 */
        ass( page + count <= mappings.back().end_page, "writable_page out of range");
        return (DataPage *)(mappings.back().addr + page * page_size);
    }
    void DB::grow_mappings(Pid new_page_count){
        auto old_page_count = mappings.empty() ? 0 : mappings.back().end_page;
        if( new_page_count <= old_page_count )
            return;
        if( new_page_count * page_size < (1 << 20) )
            new_page_count = (3 + new_page_count) * 2; // grow aggressively while file is small
        new_page_count = ((3 + new_page_count) * 5 / 4); // reserve 20% excess space, make first mapping include at least 3 meta pages
        size_t additional_granularity = 1;// 65536;  // on Windows mmapped regions should be aligned to 65536
        if( page_size < additional_granularity )
            new_page_count = ((new_page_count * page_size + additional_granularity - 1) / additional_granularity) * additional_granularity / page_size;
        if( page_size < physical_page_size )
            new_page_count = ((new_page_count * page_size + physical_page_size - 1) / physical_page_size) * physical_page_size / page_size;
        uint64_t new_file_size = new_page_count * page_size;
        if( lseek(fd, new_file_size - 1, SEEK_SET) == -1 )
            throw Exception("file seek failed in grow_mappings");
        if( write(fd, "", 1) != 1 )
            throw Exception("file write failed in grow_mappings");
        void * wm = mmap(0, new_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (wm == MAP_FAILED)
            throw Exception("mmap PROT_READ | PROT_WRITE failed");
        mappings.push_back(Mapping(0, new_page_count, (char *)wm));
/*        uint64_t map_size = new_file_size - old_page_count * page_size;
        uint64_t map_offset = old_page_count * page_size;
        void * wm = mmap(0, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, map_offset);
        if (wm == MAP_FAILED)
            throw Exception("mmap PROT_READ | PROT_WRITE failed");
        mappings.push_back(Mapping(old_page_count, new_page_count, (char *)wm));*/
    }
    void DB::trim_old_mappings(){
        if( mappings.empty() )
            return;
        for(size_t m = 0; m != mappings.size() - 1; ++m){
            munmap(mappings[m].addr, (mappings[m].end_page - mappings[m].begin_page) * page_size);
        }
        mappings.erase(mappings.begin(), mappings.end() - 1);
    }

}
