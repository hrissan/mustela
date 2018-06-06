#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {

	// TODO - get rid of std::map and use simpler tree impl
	class MergablePageCache {
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

		void debug_print_db()const;
	private:
		bool update_index;

		std::map<Pid, Pid> cache;
		size_t record_count = 0;
		std::map<Pid, std::set<Pid>> size_index;

		void add_to_size_index(Pid page, Pid count);
		void remove_from_size_index(Pid page, Pid count);
	};
	
	class FreeList {
	public:
		FreeList():free_pages(true), future_pages(false)
		{}
		Pid get_free_page(TX * tx, Pid contigous_count, Tid oldest_read_tid, bool updating_meta_bucket);
		void mark_free_in_future_page(Pid page, Pid count, bool is_from_current_tid);
		void commit_free_pages(TX * tx);
		void clear();
		void ensure_have_several_pages(TX * tx, Tid oldest_read_tid); // Called before updates to meta bucket
		
		void add_to_future_from_end_of_file(Pid page); // remove after testing new method of back to future

		void get_all_free_pages(TX * tx, MergablePageCache * pages)const;
		void load_all_free_pages(TX * tx, Tid oldest_read_tid);
		
		void debug_print_db();
		static void debug_test();

		static Val fill_free_record_key(char * keybuf, Tid tid, uint64_t batch);
		static bool parse_free_record_key(Val key, Tid * tid, uint64_t * batch);
	private:
		MergablePageCache free_pages;
		MergablePageCache future_pages;

		std::set<Pid> debug_back_from_future_pages; // If pages we gave are returned to us, we return them into free_pages instead of future_pages
		
		Tid next_record_tid = 0;
		uint64_t next_record_batch = 0;
		std::vector<std::pair<Tid, uint64_t>> records_to_delete;
		// records_to_delete are necessary for now - we are writting [0:0] [0:2] entries
		// while there could be entries like [0:1] [10:0], we will delete [0:1] next iteration
		// TODO - modify logic to never read free entries during commit
		
		void read_record_space(TX * tx, Tid oldest_read_tid);
		void fill_record_space(TX * tx, Tid tid, std::vector<MVal> & space, const std::map<Pid, Pid> & pages);
		void grow_record_space(TX * tx, Tid tid, uint32_t & batch, std::vector<MVal> & space, size_t & space_record_count, size_t record_count);
	};
}

