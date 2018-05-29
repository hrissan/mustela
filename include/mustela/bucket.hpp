#pragma once

#include <string>
#include <vector>
#include "pages.hpp"

namespace mustela {
	
	class TX;
	class Cursor;
	class FreeList;
	class Bucket {
		friend class TX;
		friend class Cursor;
		friend class FreeList;
		TX & my_txn;
		BucketDesc * bucket_desc;
		std::string debug_name;

		Bucket(TX & my_txn, BucketDesc * bucket_desc);
	public:
		Bucket(TX & my_txn, const Val & name, bool create = true);
		~Bucket();
		bool is_valid()const { return bucket_desc != nullptr; }
		char * put(const Val & key, size_t value_size, bool nooverwrite); // danger! db will alloc space for key/value in db and return address for you to copy value to
		bool put(const Val & key, const Val & value, bool nooverwrite); // false if nooverwrite and key existed
		bool get(const Val & key, Val & value);
		bool del(const Val & key, bool must_exist);
		std::string print_db();
		std::string get_stats()const;
	};
}

