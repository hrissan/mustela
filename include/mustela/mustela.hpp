#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"
#include "tx.hpp"

namespace mustela {

    struct Mapping {
        Pid end_page;
        char * addr;
        int ref_count;
        explicit Mapping(Pid end_page, char * addr):end_page(end_page), addr(addr), ref_count(0)
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
        friend class TX;
        int fd = -1;
        uint64_t file_size;
        const bool read_only;
        const uint32_t page_size;
        const uint32_t physical_page_size; // We allow to work with smaller pages when reading file from different platform (or portable variant)

        std::vector<Mapping> c_mappings;
        std::vector<Mapping> mappings;
        size_t last_meta_page_index()const;
        size_t oldest_meta_page_index()const;

        void grow_file(Pid new_page_count);
        void grow_c_mappings();
        void trim_old_mappings();
        void trim_old_c_mappings(Pid end_page);
        DataPage * writable_page(Pid page, Pid count);
        const DataPage * readable_page(Pid page)const;
        const MetaPage * readable_meta_page(size_t index)const { return (const MetaPage * )readable_page(index); }
        MetaPage * writable_meta_page(size_t index) { return (MetaPage * )writable_page(index, 1); }
        void create_db();
    public:
        explicit DB(const std::string & file_path, bool read_only = false); // If file does not exist,
        ~DB();
    };
}

