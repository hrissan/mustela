#pragma once

#include "pages.hpp"

namespace mustela { namespace os {
	
	struct File {
		int fd;
		explicit File(const std::string & file_path, bool read_only);
		uint64_t get_size()const;
		void set_size(uint64_t new_fs);
		char * mmap(uint64_t offset, uint64_t size, bool read, bool write);
		void munmap(char * addr, uint64_t size);
		void msync(char * addr, uint64_t size);

//		explicit File(int fd):fd(fd) {}
		~File();
	};
	struct FileLock {
		int fd;
		explicit FileLock(File & file);
		~FileLock();
	};
	
	bool file_exists_on_readonly_partition(const std::string & file_path); // Do not use reader lock table if it is
	
	size_t get_physical_page_size();
	size_t get_map_granularity(); // Can be larger than page size
	
	inline uint64_t grow_to_granularity(uint64_t value, uint64_t granularity){
		return ((value + granularity - 1) / granularity) * granularity;
	}
	inline uint64_t grow_to_granularity(uint64_t value, uint64_t a, uint64_t b){
		return grow_to_granularity(grow_to_granularity(value, a), b);
	}
}}

