#pragma once

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include "pages.hpp"
#include "tx.hpp"

namespace mustela {

    struct Mapping {
        Pid begin_page;
        Pid end_page;
        char * addr;
        explicit Mapping(Pid begin_page, Pid end_page, char * addr):begin_page(begin_page), addr(addr), end_page(end_page)
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
        //friend class Cursor;
        int fd = -1;
        const uint32_t page_size;
        const uint32_t physical_page_size; // We allow to work with smaller pages when reading file from different platform (or portable variant)
        const bool read_only;
        char * c_mapping = nullptr;
        std::vector<Mapping> mappings;
        size_t last_meta_page_index()const;
        size_t oldest_meta_page_index()const;
        void grow_mappings(Pid new_page_count);
        void trim_old_mappings();
        DataPage * writable_page(Pid page, Pid count)const;
        const DataPage * readable_page(Pid page)const { return (const DataPage * )(c_mapping + page_size * page); }
        const MetaPage * readable_meta_page(size_t index)const { return (const MetaPage * )readable_page(index); }
        MetaPage * writable_meta_page(size_t index)const { return (MetaPage * )writable_page(index, 1); }
        void create_db();
    public:
        explicit DB(const std::string & file_path, bool read_only = false); // If file does not exist,
        ~DB();
    };
}

