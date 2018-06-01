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
		TX * my_txn = nullptr;
		BucketDesc * bucket_desc = nullptr;
		void unlink();
		std::vector<std::pair<Pid, PageIndex>> path;
		// All node indices are always [-1..node.size-1]
		// Leaf index is always [0..leaf.size], so can point to the "end" of leaf
		// Cursor is at end() if it is set at last leaf end
		// To speed up Cursor construction, we define another special value for end - path.at(0).first == 0
		
		explicit Cursor(TX * my_txn, BucketDesc * bucket_desc);
		bool fix_cursor_after_last_item(); // true if points to item
		void set_at_direction(size_t height, Pid pa, int dir);

		void on_insert(BucketDesc * desc, size_t height, Pid pa, PageIndex insert_index, PageIndex insert_count = 1){
			if( bucket_desc == desc && path.at(height).first == pa && path.at(height).second >= insert_index ){
				path.at(height).second += insert_count;
			}
		}
		void on_erase(BucketDesc * desc, size_t height, Pid pa, PageIndex erase_index, PageIndex erase_count = 1){
			if( bucket_desc == desc && path.at(height).first == pa && path.at(height).second > erase_index ){
				path.at(height).second -= erase_count;
			}
		}
		void on_split(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, PageIndex split_index){
			if( bucket_desc == desc && path.at(height).first == pa && path.at(height).second >= split_index ){
				path.at(height).first = new_pa;
				path.at(height).second -= split_index;
				path.at(height + 1).second += 1;
			}
		}
		void on_merge(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, PageIndex new_index){
			if( bucket_desc == desc && path.at(height).first == pa){
				path.at(height).first = new_pa;
				path.at(height).second += new_index;
			}
		}
		void on_rotate_right(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, PageIndex split_index){
			if( bucket_desc == desc && path.at(height).first == pa && path.at(height).second >= split_index ){
				path.at(height).first = new_pa;
				path.at(height).second -= split_index + 1;
				path.at(height + 1).second += 1;
			}
		}
		void on_rotate_left(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, PageIndex split_index){
			if( bucket_desc == desc && path.at(height).first == pa && path.at(height).second < split_index ){
				path.at(height).first = new_pa;
				path.at(height).second += 1;
				path.at(height + 1).second -= 1;
			}
		}
	public:
		explicit Cursor()
		{}
		~Cursor();
		Cursor(Cursor && other);
		Cursor(const Cursor & other);
		Cursor & operator=(Cursor && other);
		Cursor & operator=(const Cursor & other);
		bool is_valid()const { return bucket_desc != nullptr; }

		bool operator==(const Cursor & other)const;
		bool operator!=(const Cursor & other)const{ return !(*this == other); }

		bool seek(const Val & key); // sets to key and returns true if key is found, otherwise sets to next key or end() and returns false
		void end(); // sets to end
		void first(); // sets to end(), if db is empty
		void last(); // sets to end(), if db is empty
		bool get(Val & key, Val & value);
		bool del(); // If you can get, you can del. After del, cursor points to the next item, or end() if it was last one
		void next(); // next from last() goes to the end
		void prev(); // prev from first() goes to the end, beware
		// for( cur.first(); cur.get(key, val) /*&& key.prefix("a")*/; cur.next() ) {}
		// for( cur.last(); cur.get(key, val) /*&& key.prefix("a")*/; cur.prev() ) {}
	};
}

