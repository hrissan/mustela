#pragma once

#include "pages.hpp"
#include "os.hpp"

namespace mustela {
	
	struct ReaderSlotDesc {
		size_t slot = 0;
		uint64_t rid = 0;
		uint64_t deadline = 0;
	};
#pragma pack(push, 1)
	struct ReaderSlot {
		uint64_t ridead; // reader id + deadline in seconds as returned by std::steady_clock
		Tid tid;
		char padding[64 - sizeof(uint64_t) - sizeof(Tid)]; // cache line optimization
	};
#pragma pack(pop)
	// All operations in ReaderTable instance is protected by mutex in DB
	class ReaderTable {
		ReaderSlot * slots = 0;
		size_t mapping_size = 0;
		void grow_reader_table(os::File & lock_file, size_t granularity, bool grow_file);
	public:
		explicit ReaderTable();
		~ReaderTable();

		ReaderSlotDesc create_reader_slot(Tid tid, os::File & lock_file, size_t granularity);
		bool update_reader_slot(ReaderSlotDesc & slot); // false if we were too late and slot was grabbed from us
		void release_reader_slot(const ReaderSlotDesc & slot);
		Tid find_oldest_tid(Tid writer_tid, os::File & lock_file, size_t granularity);
	};
}

