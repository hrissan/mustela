#pragma once

#include <string>
#include <vector>
#include <array>
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
		Val persistent_name; // used for mirror only for now

		IntrusiveNode<Cursor> tx_cursors;

		void unlink();
		std::array<std::pair<Pid, int>, MAX_DEPTH> path{};
		std::pair<Pid, int> & at(size_t height){ return path.at(height); }
		// All node indices are always [-1..node.size-1]
		// Leaf index is always [0..leaf.size], so can point to the "end" of leaf
		// Cursor is at end() if it is set at last leaf end
		// To speed up Cursor construction, we define another special value for end - path.at(0).first == 0
		
		explicit Cursor(TX * my_txn, BucketDesc * bucket_desc, Val name);
		bool fix_cursor_after_last_item(); // true if points to item
		void set_at_direction(size_t height, Pid pa, int dir);

		void on_insert(BucketDesc * desc, size_t height, Pid pa, int insert_index, int insert_count = 1){
			if( bucket_desc == desc && at(height).first == pa && at(height).second >= insert_index ){
				at(height).second += insert_count;
			}
		}
		void on_erase(BucketDesc * desc, size_t height, Pid pa, int erase_index, int erase_count = 1){
			if( bucket_desc == desc && at(height).first == pa && at(height).second > erase_index ){
				at(height).second -= erase_count;
			}
		}
		void on_split(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, int split_index, int is_node){
			if( bucket_desc == desc && at(height).first == pa && at(height).second >= split_index ){
				at(height).first = new_pa;
				at(height).second -= split_index + is_node;
				at(height + 1).second += 1;
			}
		}
		void on_merge(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, int new_index){
			if( bucket_desc == desc && at(height).first == pa){
				at(height).first = new_pa;
				at(height).second += new_index;
			}
		}
		void on_rotate_right(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, int split_index){
			if( bucket_desc == desc && at(height).first == pa && at(height).second >= split_index ){
				at(height).first = new_pa;
				at(height).second -= split_index + 1;
				at(height + 1).second += 1;
			}
		}
		void on_rotate_left(BucketDesc * desc, size_t height, Pid pa, Pid new_pa, int split_index){
			if( bucket_desc == desc && at(height).first == pa && at(height).second < split_index ){
				at(height).first = new_pa;
				at(height).second += 1;
				at(height + 1).second -= 1;
			}
		}
		bool is_before_first()const { return path.at(0).first == 0; }
	public:
		explicit Cursor()
		{}
		~Cursor();
		Cursor(Cursor && other);
		Cursor(const Cursor & other);
		Cursor & operator=(Cursor && other);
		Cursor & operator=(const Cursor & other);
		
		bool is_valid()const { return bucket_desc != nullptr; }
		Val get_bucket_name()const { return persistent_name; }
		
//		Bucket get_bucket(){ // Good idea but classes will reference each other by value :)
//			ass(bucket_desc, "Cursor not valid (using after tx commit?)");
//			return Bucket(my_txn, bucket_desc, persistent_name);
//		}

		bool operator==(const Cursor & other)const;
		bool operator!=(const Cursor & other)const{ return !(*this == other); }

		bool seek(const Val & key); // sets to key and returns true if key is found, otherwise sets to next key or end() and returns false
		void before_first(); // sets before first
		void end(); // sets to end
		void first(); // sets to end(), if db is empty
		void last(); // sets to end(), if db is empty
		
		bool get(Val * key, Val * value); // you can get from any position except end() and before_first()
		bool del(); // If you can get, you can del. After successfull del, cursor points to the next item, or end() if it was last one
		
		void next(); // next from last() goes to the end(), next from end() is nop
		void prev(); // prev from first() goes to the before_first(), prev from before_first() is nop
		// for( cur.first(); cur.get(key, val) /*&& key.prefix("a", &key_tail)*/; cur.next() ) {}
		// for( cur.last(); cur.get(key, val) /*&& key.prefix("a", &key_tail)*/; cur.prev() ) {}
		
		// for debug
		void make_pages_writable();
		void check_cursor_path_up();
	};
}

