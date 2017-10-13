#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {

   class FreeList {
        std::map<Pid, Pid> free_pages;
        std::map<Pid, std::set<Pid>> size_index;
       std::map<Pid, Pid> future_pages;
       //std::set<Pid> back_from_future_pages; // If pages we gave are returned to us, we return them into free_pages instead of future_pages
       void add_to_size_index(Pid page, Pid count);
       void remove_from_size_index(Pid page, Pid count);
       
       void add_to_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, bool update_index);
       void remove_from_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, bool update_index);
       
        Tid last_scanned_tid;
        uint32_t last_scanned_batch;
    public:
        FreeList():last_scanned_tid(0), last_scanned_batch(0)
        {}
        Pid get_free_page(Pid contigous_count, Tid oldest_read_tid);
        void mark_free_in_future_page(Pid page, Pid count);
       void commit_free_pages(Tid write_tid);
       
       void print_db();
       static void test();
    };
}

