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
		
		Tid next_record_tid = 0;
		uint64_t next_record_batch = 0;
		std::vector<std::pair<Tid, uint64_t>> records_to_delete;
		// TODO - records_to_delete are unnecessary - they form single range for deletion
		
		void read_record_space(TX * tx, Tid oldest_read_tid);
		void fill_record_space(TX * tx, Tid tid, std::vector<MVal> & space, const std::map<Pid, Pid> & pages);
		void grow_record_space(TX * tx, Tid tid, uint32_t & batch, std::vector<MVal> & space, size_t & space_record_count, size_t record_count);
	public:
		FreeList():free_pages(true), future_pages(false)
		{}
		void ensure_have_several_pages(TX * tx, Tid oldest_read_tid); // Called before updates to meta bucket
		Pid get_free_page(TX * tx, Pid contigous_count, Tid oldest_read_tid, bool updating_meta_bucket);
		void mark_free_in_future_page(Pid page, Pid count, bool is_from_current_tid);
		void commit_free_pages(TX * tx);
		void clear();
		
		void add_to_future_from_end_of_file(Pid page); // remove after testing new method of back to future

		void load_all_free_pages(TX * tx, Tid oldest_read_tid);
		void get_all_free_pages(TX * tx, MergablePageCache * pages)const;

		void print_db();
		static void test();

		static Val fill_free_record_key(char * keybuf, Tid tid, uint64_t batch);
		static bool parse_free_record_key(Val key, Tid * tid, uint64_t * batch);
	};
}

