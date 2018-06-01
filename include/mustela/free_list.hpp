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
	class MergablePageCache {
		bool update_index;

		std::map<Pid, Pid> cache;
		size_t record_count = 0;
		std::map<Pid, std::set<Pid>> size_index;

		void add_to_size_index(Pid page, Pid count);
		void remove_from_size_index(Pid page, Pid count);
	public:
		explicit MergablePageCache(bool update_index):update_index(update_index)
		{}
		void clear();
		bool empty()const { return cache.empty() && record_count == 0; }
		size_t get_record_count()const { return record_count; }
		
		void add_to_cache(Pid page, Pid count);
		void remove_from_cache(Pid page, Pid count);

		Pid get_free_page(Pid contigous_count);
		Pid defrag_end(Pid page_count);
		
		void merge_from(const MergablePageCache & other);
		void fill_record_space(TX * tx, Tid tid, std::vector<MVal> & space)const;
		void print_db()const;
	};
	class FreeList {
		MergablePageCache free_pages;
		MergablePageCache future_pages;

		std::set<Pid> back_from_future_pages; // If pages we gave are returned to us, we return them into free_pages instead of future_pages
		
		Tid next_record_tid;
		uint64_t next_record_batch;
		std::vector<std::pair<Tid, uint64_t>> records_to_delete;
		// TODO - records_to_delete are unnecessary - they form single range for deletion
		
		void fill_record_space(TX * tx, Tid tid, std::vector<MVal> & space, const std::map<Pid, Pid> & pages);
		void grow_record_space(TX * tx, Tid tid, uint32_t & batch, std::vector<MVal> & space, size_t & space_record_count, size_t record_count);
	public:
		FreeList():free_pages(true), future_pages(false), next_record_tid(0), next_record_batch(0)
		{}
		Pid get_free_page(TX * tx, Pid contigous_count, Tid oldest_read_tid);
		void get_all_free_pages(TX * tx, MergablePageCache * pages);
		void mark_free_in_future_page(Pid page, Pid count);
		void commit_free_pages(TX * tx, Tid write_tid);
		
		void print_db();
		static void test();
	};
}

