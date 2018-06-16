#pragma once

#include "pages.hpp"
#include "os.hpp"

namespace mustela {
	
	struct ReaderSlotDesc {
		size_t slot = 0;
		uint64_t rid = 0;
		uint32_t now = 0;
		uint32_t deadline = 0; // Unix time seconds
	};
#pragma pack(push, 1)
	struct LockFileHeader {
		uint64_t next_reader;
		uint64_t magic;
		char padding[READER_SLOT_SIZE - 2*sizeof(uint64_t)]; // cache line optimization
	};
	static_assert(sizeof(LockFileHeader) == READER_SLOT_SIZE, "");
	struct ReaderSlot {
		uint64_t ridead; // reader id + deadline in seconds as returned by std::steady_clock
		Tid tid;
		char padding[READER_SLOT_SIZE - sizeof(uint64_t) - sizeof(Tid)]; // cache line optimization
	};
	static_assert(sizeof(ReaderSlot) == READER_SLOT_SIZE, "");
#pragma pack(pop)
	// All operations in ReaderTable instance is protected by mutex in DB
	class ReaderTable {
		char * mapping = nullptr;
		size_t mapping_size = 0;
		volatile LockFileHeader * header = nullptr;
		volatile ReaderSlot * slots = 0;
		size_t slots_count = 0;
		
		Random random;
		void grow_reader_table(os::File & lock_file, size_t granularity, bool grow_file);
		void free_mapping();
	public:
		explicit ReaderTable();
		~ReaderTable();
		
		static uint32_t now();

		ReaderSlotDesc create_reader_slot(Tid tid, uint32_t reader_timeout_seconds, os::File & lock_file, size_t granularity);
		bool update_reader_slot(ReaderSlotDesc & slot, uint32_t now, uint32_t reader_timeout_seconds); // false if we were too late and slot was grabbed from us
		void release_reader_slot(const ReaderSlotDesc & slot);
		Tid find_oldest_tid(Tid writer_tid, os::File & lock_file, size_t granularity);
	};
}

