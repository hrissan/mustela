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

namespace mustela {

    void FreeList::add_to_size_index(Pid page, Pid count){
//        if(count == 1)
//            return;
        auto sitem = size_index[count];
        ass(sitem.insert(page).second, "Page is twice in size_index");
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

    void FreeList::add_to_cache(Pid page, Pid count, Tid tid, uint32_t max_batch){
        auto it = free_pages.lower_bound(page);
        ass(it == free_pages.end() || it->first != page, "adding existing page to cache");
        if(it != free_pages.end() && it->first == page + count){
            remove_from_size_index(it->first, it->second.count);
            count += it->second.count;
            if( tid == it->second.max_tid)
                max_batch = std::max(max_batch, it->second.max_batch);
            else
                tid = std::max(tid, it->second.max_tid);
            it = free_pages.erase(it);
        }
        if( it != free_pages.begin() ){
            --it;
            if( it->first + it->second.count == page){
                remove_from_size_index(it->first, it->second.count);
                page = it->first;
                count += it->second.count;
                if( tid == it->second.max_tid)
                    max_batch = std::max(max_batch, it->second.max_batch);
                else
                    tid = std::max(tid, it->second.max_tid);
            }
        }
        free_pages.insert(std::make_pair(page, FreePageRange(count, tid, max_batch)));
        add_to_size_index(page, count);
    }
    void FreeList::remove_from_cache(Pid page, Pid count){
        auto it = free_pages.find(page);
        ass(it != free_pages.end() && it->second.count >= count, "invalid remove from cache");
        remove_from_size_index(it->first, it->second.count);
        if( count == it->second.count ){
            free_pages.erase(it);
            return;
        }
        FreePageRange remains = it->second;
        remains.count -= count;
        page += count;
        add_to_size_index(page, remains.count);
        free_pages.insert(std::make_pair(page, remains));
    }

    Pid FreeList::get_free_page(Pid contigous_count, Tid oldest_read_tid){
        while( last_scanned_tid < oldest_read_tid ){
            auto siit = size_index.lower_bound(contigous_count);
            if( siit != size_index.end() ){
                Pid pa = *(siit->second.begin());
                remove_from_cache(pa, contigous_count);
                return pa;
            }
            // scan a batch from DB starting from last_scanned_tid, last_scanned_batch
/*            Tid db_tid = 0;
            uint32_t batch_num = 0;
            for(auto && pidcount : pidcounts){
                add_to_cache(pidcount.first, pidcount.second);
            }*/
            break;
        }
        return 0;
    }
    void FreeList::mark_free_in_future_page(Pid page, Pid count){
        auto it = future_pages.lower_bound(page);
        ass(it == future_pages.end() || it->first != page, "adding existing page to future");
        if(it != future_pages.end() && it->first == page + count){
            count += it->second.count;
            it = future_pages.erase(it);
        }
        if( it != future_pages.begin() ){
            --it;
            if( it->first + it->second.count == page){
                page = it->first;
                count += it->second.count;
            }
        }
        future_pages.insert(std::make_pair(page, FreePageRange(count, 0, 0)));
    }
    void FreeList::commit_free_pages(Tid write_tid){
        uint32_t future_batch = 0;
        uint32_t old_batch = 0;
        // while(total_future_keys_space < future_pages.size() && total_old_keys_space < free_pages.size() )
        // while(total_future_keys_space < future_pages.size())
        // future_mvals.push_back( add_key(write_tid, future_batch++) );
        // while(total_old_keys_space < free_pages.size())
        // old_mvals.push_back( add_key(last_scanned_tid - 1, old_batch++) );
        
        // Now fill space with actual pids, and we are set to go
    }

}
