#pragma once

#include <cstdint>

namespace mustela {
	
	// Simple types
	typedef uint64_t Tid;
	typedef uint64_t Pid;

	typedef uint16_t PageOffset;
	typedef int16_t PageIndex; // we use -1 to indicate special left value in nodes

	constexpr int MIN_KEY_COUNT = 2;
	static_assert(MIN_KEY_COUNT == 2, "Should be 2 for invariants, do not change");

	constexpr uint32_t OUR_VERSION = 5;

	constexpr uint64_t META_MAGIC = 0x58616c657473754d; // MustelaX in LE
	
	constexpr int META_PAGES_COUNT = 3; // We might end up using 2 like lmdb
	constexpr int NODE_PID_SIZE = 5; // We use fixed number of bytes for some page references
	
	constexpr size_t MIN_PAGE_SIZE = 128;
	constexpr size_t GOOD_PAGE_SIZE = 4096;
	constexpr size_t MAX_PAGE_SIZE = 1 << 8*sizeof(PageOffset);
	// 4 bytes to store page index will result in ~4 billion pages limit, or 16TB max for 4KB pages
	// TODO - move NODE_PID_SIZE into MetaPage
	
	constexpr int MAX_HEIGHT = 40; // TODO - calculate from NODE_PID_SIZE?
	// fixed pid size allows simple logic when replacing page in node index
	
	constexpr int READER_SLOT_SIZE = 64; // 1 per cache line

// turn on/off health checks
	constexpr bool CLEAR_FREE_SPACE = true;
	constexpr bool DEBUG_PAGES = true;
	constexpr bool DEBUG_MIRROR = false;

// Forward declarations
	class DB;
	class TX;
	class FreeList;
	class Cursor;
	class Bucket;
}

