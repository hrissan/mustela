#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {
	
	struct ReaderSlotDesc {
		size_t slot = 0;
		uint64_t rand0 = 0;
		uint64_t rand1 = 0;
	};
#pragma pack(push, 1)
	struct ReaderSlot {
		uint64_t deadline; // microseconds as returned by std::steady_clock
		Tid tid;
		uint64_t rand0;
		uint64_t rand1;
		char padding[64 - 3*sizeof(uint64_t) - sizeof(Tid)];
	};
#pragma pack(pop)
	class ReaderTable {
		ReaderSlot * slots = 0;
		size_t mapping_size = 0;
	public:
		explicit ReaderTable();
		~ReaderTable();

		ReaderSlotDesc create_reader_slot(Tid tid, int fd, size_t granularity);
		void update_reader_slot(const ReaderSlotDesc & slot);
		void release_reader_slot(const ReaderSlotDesc & slot);
		Tid find_oldest_tid(Tid writer_tid);
	};
}

