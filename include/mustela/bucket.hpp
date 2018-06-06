#pragma once

#include <string>
#include <vector>
#include "pages.hpp"
#include "cursor.hpp"

namespace mustela {
	
	class TX;
	class Cursor;
	class FreeList;
	class Bucket {
		friend class TX;
		friend class Cursor;
		friend class FreeList;
		TX * my_txn = nullptr;
		BucketDesc * bucket_desc = nullptr;
		Val persistent_name;
		void unlink();

		Bucket(TX * my_txn, BucketDesc * bucket_desc, Val name = Val());
	public:
		Bucket(){}
		~Bucket();
		Bucket(Bucket && other);
		Bucket(const Bucket & other);
		Bucket & operator=(Bucket && other);
		Bucket & operator=(const Bucket & other);

		bool is_valid()const { return bucket_desc != nullptr; }
		Val get_name()const { return persistent_name; }
		
		Cursor get_cursor()const { return Cursor(my_txn, bucket_desc, persistent_name); } // cursor is set to before_first(), this is the fastest operation
				
		char * put(const Val & key, size_t value_size, bool nooverwrite); // danger! db will alloc space for key/value in db and return address for you to copy value to
		bool put(const Val & key, const Val & value, bool nooverwrite); // false if nooverwrite and key existed
		bool get(const Val & key, Val * value)const;
		bool del(const Val & key);
		
		std::string print_db();
		std::string get_stats()const;
	};
}

