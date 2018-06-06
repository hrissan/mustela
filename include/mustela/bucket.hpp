#pragma once

#include <string>
#include <vector>
#include "pages.hpp"
#include "cursor.hpp"

namespace mustela {
	
	class Bucket {
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
		
		std::string get_stats()const;
	//{'branch_pages': 1040L,
	//    'depth': 4L,
	//    'entries': 3761848L,
	//    'leaf_pages': 73658L,
	//    'overflow_pages': 0L,
	//    'psize': 4096L}

		std::string debug_print_db();
	private:
		friend class TX;
		friend class Cursor;
		Bucket(TX * my_txn, BucketDesc * bucket_desc, Val name = Val());

		TX * my_txn = nullptr;
		BucketDesc * bucket_desc = nullptr;
		Val persistent_name;
		void unlink();
	};
}

