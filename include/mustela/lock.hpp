#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>
#include <cstring>
#include "pages.hpp"

namespace mustela {
	
	class WriterLock { // Acquires mutually exclusive lock, scans reader table for oldest reader
	public:
		explicit WriterLock()
		{}
		~WriterLock();
		Tid oldest_reader_tid();
	};
	class ReaderLock { // Sets reader tid into reader table slot
	public:
		ReaderLock();
		~ReaderLock();
	};
}

