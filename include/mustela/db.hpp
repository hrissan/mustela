#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"
#include "tx.hpp"

namespace mustela {
	
	struct Mapping {
		size_t end_addr;
		char * addr;
		int ref_count;
		explicit Mapping(size_t end_addr, char * addr):end_addr(end_addr), addr(addr), ref_count(0)
		{}
	};
	class TX;
	//{'branch_pages': 1040L,
	//    'depth': 4L,
	//    'entries': 3761848L,
	//    'leaf_pages': 73658L,
	//    'overflow_pages': 0L,
	//    'psize': 4096L}
	class DB {
		struct FD {
			int fd;
			explicit FD(int fd):fd(fd) {}
			~FD();
		};
		FD fd;
		friend class TX;
		uint64_t file_size;
		const bool read_only;
		size_t page_size;
		const size_t physical_page_size; // We allow to work with smaller pages when reading file from different platform (or portable variant)
		
		std::vector<Mapping> c_mappings;
		std::vector<Mapping> mappings;
		bool is_valid_meta(const MetaPage * mp)const;
		bool get_meta_indices(Pid * newest_index, Pid * overwrite_index, Tid * earliest_tid)const;
		
		void grow_file(Pid new_page_count);
		void grow_c_mappings();
		void trim_old_mappings();
		void trim_old_c_mappings(size_t end_addr);
		DataPage * writable_page(Pid page, Pid count);
		const DataPage * readable_page(Pid page)const;
		const MetaPage * readable_meta_page(Pid index)const { return (const MetaPage * )readable_page(index); }
		MetaPage * writable_meta_page(Pid index) { return (MetaPage * )writable_page(index, 1); }
		void create_db();
		bool open_db();
	public:
		explicit DB(const std::string & file_path, bool read_only = false); // If file does not exist,
		size_t max_key_size()const;
		~DB();
	};
}

