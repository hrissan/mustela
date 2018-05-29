#pragma once

#include <string>
#include <vector>
#include "pages.hpp"

namespace mustela {
	
	class TX;
	class Bucket;
	class FreeList;
	class Cursor {
		friend class TX;
		friend class Bucket;
		friend class FreeList;
		TX & my_txn;
		BucketDesc * bucket_desc;
		std::vector<std::pair<Pid, PageIndex>> path;
		
		void truncate(size_t height){
			while(height < path.size() - 1)
				path[height++].second = -1; // set to special link
			if(height == path.size() - 1)
				path[height++].second = 0; // set to first leaf kv
		}
		
		explicit Cursor(TX & my_txn, BucketDesc * bucket_desc);
		bool fix_cursor_after_last_item(); // true if points to item
		void set_at_direction(size_t height, Pid pa, int dir);
		void end();
	public:
		Cursor(Cursor && other);
		Cursor & operator=(Cursor && other)=delete;
		Cursor(const Cursor & other);
		explicit Cursor(Bucket & bucket);
		~Cursor();
		bool operator==(const Cursor & other)const;
		bool operator!=(const Cursor & other)const{ return !(*this == other); }

		bool seek(const Val & key); // sets to key and returns true if key is found, otherwise sets to next key and returns false
		void first();
		void last();
		bool get(Val & key, Val & value);
		bool del(); // If you can get, you can del. After del, cursor points to the next item
		void next();
		void prev();
		// for( cur.first(); cur.get(key, val) /*&& key.prefix("a")*/; cur.next() ) {}
		// for( cur.last(); cur.get(key, val) /*&& key.prefix("a")*/; cur.prev() ) {}
		
		void on_page_split(size_t height, Pid pa, PageIndex split_index, PageIndex split_index_r, const Cursor & cur2){
			if( !path.empty() && path.at(height).first == pa && path.at(height).second >= split_index ){
				for(size_t i = height + 1; i != bucket_desc->height + 1; ++i)
					path.at(i) = cur2.path.at(i);
				path.at(height).first = cur2.path.at(height).first;
				path.at(height).second -= split_index_r;
			}
		}
		void on_insert(size_t height, Pid pa, PageIndex insert_index, PageIndex insert_count = 1){
			if( !path.empty() && path.at(height).first == pa && path.at(height).second >= insert_index ){
				path.at(height).second += insert_count;
			}
		}
		void on_erase(size_t height, Pid pa, PageIndex erase_index){
			if( !path.empty() && path.at(height).first == pa && path.at(height).second > erase_index ){
				path.at(height).second -= 1;
			}
		}
		void on_split(size_t height, Pid pa, Pid new_pa, PageIndex split_index){
			if( !path.empty() && path.at(height).first == pa && path.at(height).second >= split_index ){
				path.at(height).first = new_pa;
				path.at(height).second -= split_index;
				path.at(height + 1).second += 1;
			}
		}
		void on_merge(size_t height, Pid pa, Pid new_pa, PageIndex new_index){
			if( !path.empty() && path.at(height).first == pa){
				path.at(height).first = new_pa;
				path.at(height).second += new_index;
			}
		}
	};
}

