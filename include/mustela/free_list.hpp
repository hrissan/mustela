#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {

	// TODO - get rid of std::map and use simpler tree impl
	class TX;
	class FreeList {
		std::map<Pid, Pid> free_pages;
		size_t free_pages_record_count = 0;
		std::map<Pid, std::set<Pid>> size_index;
		std::map<Pid, Pid> future_pages;
		size_t future_pages_record_count = 0;
		std::set<Pid> back_from_future_pages; // If pages we gave are returned to us, we return them into free_pages instead of future_pages
		void add_to_size_index(Pid page, Pid count);
		void remove_from_size_index(Pid page, Pid count);
		
		void add_to_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & record_count, bool update_index);
		void remove_from_cache(Pid page, Pid count, std::map<Pid, Pid> & cache, size_t & record_count, bool update_index);
		
		Tid next_record_tid; 
		uint64_t next_record_batch;
		std::vector<std::pair<Tid, uint64_t>> records_to_delete;
		// TODO - records_to_delete are unnecessary - they form single range for deletion
		
		void fill_record_space(TX * tx, Tid tid, std::vector<MVal> & space, const std::map<Pid, Pid> & pages);
		void grow_record_space(TX * tx, Tid tid, uint32_t & batch, std::vector<MVal> & space, size_t & space_record_count, size_t record_count);
	public:
		FreeList():next_record_tid(0), next_record_batch(0)
		{}
		Pid get_free_page(TX * tx, Pid contigous_count, Tid oldest_read_tid);
		void mark_free_in_future_page(Pid page, Pid count);
		void commit_free_pages(TX * tx, Tid write_tid);
		
		void print_db();
		static void test();
	};
}

