#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {

   class FreeList {
        struct FreePageRange {
            Pid count;
            Tid max_tid;
            uint32_t max_batch; // if more than FREE_BATCH pages are freed in transaction, each one gets batch identifier
            explicit FreePageRange(Pid count, Tid max_tid, uint32_t max_batch):count(count), max_tid(max_tid), max_batch(max_batch)
            {}
        };
        std::map<Pid, FreePageRange> free_pages;
        std::map<Pid, std::set<Pid>> size_index;
       std::map<Pid, FreePageRange> future_pages;
       void add_to_size_index(Pid page, Pid count);
       void remove_from_size_index(Pid page, Pid count);
       
       void add_to_cache(Pid page, Pid count, Tid tid, uint32_t max_batch);
       void remove_from_cache(Pid page, Pid count);
       
        Tid last_scanned_tid;
        uint32_t last_scanned_batch;
    public:
        FreeList():last_scanned_tid(0), last_scanned_batch(0)
        {}
        Pid get_free_page(Pid contigous_count, Tid oldest_read_tid);
        void mark_free_in_future_page(Pid page, Pid count);
       void commit_free_pages(Tid write_tid);
    };
}

