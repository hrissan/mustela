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

using namespace mustela;

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

    void FreeList::add_to_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & raw_size, bool update_index){
        auto it = cache.lower_bound(page);
        ass(it == cache.end() || it->first != page, "adding existing page to cache");
        if(it != cache.end() && it->first == page + count){
            if( update_index)
                remove_from_size_index(it->first, it->second);
            count += it->second;
            raw_size -= NODE_PID_SIZE + get_compact_size_sqlite4(it->second);
            it = cache.erase(it);
        }
        if( it != cache.begin() ){
            --it;
            if( it->first + it->second == page){
                remove_from_size_index(it->first, it->second);
                page = it->first;
                count += it->second;
                raw_size -= NODE_PID_SIZE + get_compact_size_sqlite4(it->second);
                it = cache.erase(it);
            }
        }
        raw_size += NODE_PID_SIZE + get_compact_size_sqlite4(count);
        cache.insert(std::make_pair(page, count));
        if( update_index)
            add_to_size_index(page, count);
    }
    void FreeList::remove_from_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & raw_size, bool update_index){
        auto it = cache.find(page);
        ass(it != free_pages.end() && it->second >= count, "invalid remove from cache");
        if( update_index )
            remove_from_size_index(it->first, it->second);
        if( count == it->second ){
            raw_size -= NODE_PID_SIZE + get_compact_size_sqlite4(it->second);
            cache.erase(it);
            return;
        }
        auto old_count = it->second;
        raw_size -= NODE_PID_SIZE + get_compact_size_sqlite4(it->second);
        cache.erase(it);
        old_count -= count;
        page += count;
        if( update_index )
            add_to_size_index(page, old_count);
        raw_size += NODE_PID_SIZE + get_compact_size_sqlite4(it->second);
        free_pages.insert(std::make_pair(page, old_count));
    }

    Pid FreeList::get_free_page(TX & tx, Pid contigous_count, Tid oldest_read_tid){
        while( last_scanned_tid < oldest_read_tid ){
            auto siit = size_index.lower_bound(contigous_count);
            if( siit != size_index.end() ){
                Pid pa = *(siit->second.begin());
                remove_from_cache(pa, contigous_count, free_pages, free_pages_raw_size, true);
                return pa;
            }
            // scan a batch from DB starting from last_scanned_tid, last_scanned_batch
/*            Tid db_tid = 0;
            uint32_t batch_num = 0;
            for(auto && pidcount : pidcounts){
                add_to_cache(pidcount.first, pidcount.second, free_pages, true);
            }*/
            break;
        }
        return 0;
    }
    void FreeList::mark_free_in_future_page(Pid page, Pid count){
        add_to_cache(page, count, future_pages, future_pages_raw_size, false);
    }
    void FreeList::commit_free_pages(TX & tx, Tid write_tid){
        uint32_t future_batch = 0;
        uint32_t old_batch = 0;
        // while(total_future_keys_space < future_pages.size() && total_old_keys_space < free_pages.size() )
        // while(total_future_keys_space < future_pages.size())
        // future_mvals.push_back( add_key(write_tid, future_batch++) );
        // while(total_old_keys_space < free_pages.size())
        // old_mvals.push_back( add_key(last_scanned_tid-1, old_batch++) );
        
        // Now fill space with actual pids, and we are set to go
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
        list.mark_free_in_future_page(1, 2);
        list.mark_free_in_future_page(7, 4);
        list.mark_free_in_future_page(4, 2);
        list.mark_free_in_future_page(3, 1);
        list.mark_free_in_future_page(6, 1);
        list.print_db();
    }

