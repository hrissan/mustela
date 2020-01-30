// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <unordered_set>
#include <random>
#include <chrono>
#include <thread>
#include <iomanip>
#include <atomic>
#include <unistd.h>
#include "mustela.hpp"
#include "testing.hpp"
#include <immer/map.hpp>
#include <immer/set.hpp>
#include <immer/vector.hpp>

extern "C" {
#include "blake2b.h"
}

namespace mustela {

class DB2;

// Either points to readonly DB page, or contains bytes in a sval
struct AnyValue {
    Val val;
    std::string sval; // used if val.data == nullptr

    Val get_true_value()const { return val.data ? val : Val(sval); }

    bool operator<(const AnyValue & other)const { return get_true_value() < other.get_true_value(); }
    bool operator==(const AnyValue & other)const { return get_true_value() == other.get_true_value(); }
    bool operator!=(const AnyValue & other)const { return !operator==(other); }
};

class Leaf {
public:
    std::map<AnyValue, AnyValue> content;
};

class Node;

struct AnyNode {
    Pid pid;
    std::unique_ptr<Node> node;
    std::unique_ptr<Leaf> leaf;
};

class Node {
public:
    std::map<AnyValue, AnyNode> content;
};

class TX2 {
public:
	TX2();
private:
	// For readers & writers
	DB2 & my_db;

	const char * c_file_ptr = nullptr;
	Pid file_page_count = 0;
	size_t used_mapping_size = 0;
	MetaPage meta_page;

	// For readers
	ReaderSlotDesc reader_slot;

	// For writers
	Tid oldest_reader_tid = 0;
    AnyNode root;
	FreeList free_list;
};

class DB2 {
public:
	explicit DB2();
};

} // namespace must

using namespace mustela;


struct MiniMeta {
	uint64_t tid;
	uint64_t body01;
	uint64_t body02;
	uint64_t tid2;
};

struct AtomicMeta {
	std::atomic<uint64_t> tid;
	std::atomic<uint64_t> body01;
	std::atomic<uint64_t> body02;
	std::atomic<uint64_t> tid2;
};

struct MiniReaderSlot {
	std::atomic<uint64_t> rid_deadline;
	uint64_t tid;
};

AtomicMeta metas[2]{};
enum { MAX_SLOTS = 4096 };
std::atomic<uint64_t> next_rid{};
std::atomic<uint64_t> next_wrid{};
std::atomic<uint64_t> work_result{};
MiniReaderSlot slots[MAX_SLOTS]{};
std::mutex wr_mut;

std::pair<size_t, MiniMeta> grab_newest_meta(){
	MiniMeta newest_meta{};
	for(int i = 0; i != 100;){
		uint64_t tid0 = metas[0].tid;
		uint64_t tid1 = metas[1].tid;
		size_t pid = 0;
		if(tid1 > tid0){
			newest_meta.tid = tid1;
			newest_meta.body01 = metas[1].body01;
			newest_meta.body02 = metas[1].body02;
			newest_meta.tid2 = metas[1].tid2;
			pid = 1;
		}else{
			newest_meta.tid = tid0;
			newest_meta.body01 = metas[0].body01;
			newest_meta.body02 = metas[0].body02;
			newest_meta.tid2 = metas[0].tid2;
			pid = 0;
		}
		if(newest_meta.tid2 != newest_meta.tid)
			continue;
		if(newest_meta.body01 != newest_meta.body02)
			continue;
		return std::make_pair(pid, newest_meta);
	}
	std::cout << "Reader grab meta failed" << std::endl;
	ass(false, "Reader grab meta failed");
	return std::make_pair(0, MiniMeta{});
}

int grab_slot(uint64_t my_rid){
	int jump = rand() % MAX_SLOTS;
	for(int i = 0; i != MAX_SLOTS; ++i){
		int slot = (i + jump) % MAX_SLOTS;
		uint64_t rid_deadline = slots[slot].rid_deadline;
		if( rid_deadline != 0)
			continue;
		uint64_t prev = 0;
		if( slots[slot].rid_deadline.compare_exchange_strong(prev, my_rid) )
			return slot;
	}
	ass(false, "Reader grab slot failed");
	return 0;
}

void run_lockless() {
	while(true){
		const bool reader = rand() % 128 != 0;
		if(reader){
			const uint64_t my_rid = ++next_rid;
			while(true){
				auto grab = grab_newest_meta();
				int slot = grab_slot(my_rid);
				slots[slot].tid = grab.second.tid;
				const uint64_t still_my_rid = slots[slot].rid_deadline;
				if(still_my_rid != my_rid)
					continue;
				grab = grab_newest_meta();
				uint64_t wr = work_result;
				for(int i = 0; i != 900000*slot; ++i){
					wr = (wr + 0x123456789) + 0x987654321;
				}
				work_result += wr;
				// Reading is safe here
//				sleep(1);
				uint64_t prev = my_rid;
				bool freed = slots[slot].rid_deadline.compare_exchange_strong(prev, 0);
				break;
			}
		}else{
			const uint64_t my_wrrid = ++next_wrid;
			std::unique_lock<std::mutex> lock(wr_mut);
			if(my_wrrid % 100000 == 0)
				std::cout << my_wrrid << " " << next_rid << std::endl;
			auto grab = grab_newest_meta();
			uint64_t oldest_tid = grab.second.tid;
			for(int i = 0; i != MAX_SLOTS; ++i){
				uint64_t rid_deadline = slots[i].rid_deadline;
				if( rid_deadline < 100){ // < now
					slots[i].rid_deadline.compare_exchange_strong(rid_deadline, 0);
					continue;
				}
				oldest_tid = std::max(oldest_tid, slots[i].tid);
			}
			uint64_t new_val = static_cast<uint64_t>(rand());
			metas[1 - grab.first].body01 = new_val;
			metas[1 - grab.first].body02 = new_val;
			__sync_synchronize();
			metas[1 - grab.first].tid = grab.second.tid + 1;
			metas[1 - grab.first].tid2 = grab.second.tid + 1;
		}
	}
}

void interactive_test(){
	DBOptions options;
	options.minimal_mapping_size = 1024;
	options.new_db_page_size = 128;
	DB db("test.mustela", options);
	Val main_bucket_name("main");
	
	const int items_counter = 100000;
	std::default_random_engine e;//{r()};
	std::uniform_int_distribution<int> dist(0, 10*items_counter - 1);

	std::vector<std::string> cmds{"ar", "db", "ar", "dr", "ar", "dr", "d", "a", "dr", "ar", "dr", "ar", "d", "ab", "dr", "dr", "dr", "dr", "dr", "dr"};
	while(true){
		TX txn(db);
		Bucket main_bucket = txn.get_bucket(main_bucket_name);
		std::cout << "Enter command (h for help):" << std::endl;
		std::string input;
		if( !cmds.empty()){
			input = cmds.at(0);
			std::cout << input << std::endl;
			cmds.erase(cmds.begin());
		}else
			getline(std::cin, input);
		if( input == "q")
			break;
		if( input == "h"){
			std::cout << "q - quit, p - print, a - add 1M values, d - delete 1M values, ar - add 1M random values, dr - delete 1M random values, ab - add 1M values backwards, db - delete 1M values backwards, f - print free list, c - check DB validity, m - print metas, add 1 to limit operation to single key-value" << std::endl;
			continue;
		}
		if( input == "pp"){
			std::string json = txn.debug_print_meta_db();
			std::cout << "Meta table: " << json << std::endl;
			continue;
		}
		if( input == "p"){
			std::string json = main_bucket.debug_print_db();
			std::cout << "Main table: " << json << std::endl;
			continue;
		}
		if( input == "m"){
			std::cout << txn.get_meta_stats() << std::endl;
			std::cout << main_bucket.get_stats() << std::endl;
			continue;
		}
		if( input == "c"){
			std::cout << "Checking DB: " << std::endl;
			txn.check_database([](int progress){
				std::cout << "Checking... " << progress << "%" << std::endl;
			}, true);
			continue;
		}
		if( input == "f"){
			std::cout << "Free table: " << std::endl;
			txn.debug_print_free_list();
			continue;
		}
		bool new_range = input.find("n") != std::string::npos;
		bool add = input.find("a") != std::string::npos;
		bool ran = input.find("r") != std::string::npos;
		bool back = input.find("b") != std::string::npos;
		bool one = input.find("1") != std::string::npos;
		std::cout << "add=" << int(add) << " ran=" << int(ran) << " back=" << int(back) << std::endl;
		if( input == "t"){
/*			int j = dist(e);
			std::string key = std::to_string(j);
			std::string val = "value" + std::to_string(j);// + std::string(j % 512, '*');
			if( !main_bucket.put(Val(key), Val(val), true) )
				std::cout << "BAD put" << std::endl;
			mirror[key] = val;
			txn.commit();*/
			continue;
		}
		for(int i = 0; i != items_counter; ++i){
			if( i == items_counter / 2)
				txn.commit();
			int j = ran ? dist(e) : back ? items_counter - 1 - i : i;
			if( new_range )
				j += items_counter;
		  	std::string key = std::string(6 - std::to_string(j).length(), '0') + std::to_string(j);
			//std::string key = std::to_string(j) + std::string(4, 'A');
			std::string val = "value" + std::to_string(j);// + std::string(j % 128, '*');
			if(i % 10 == 0)
				val += std::string(j % 512, '*');
			Val got;
			if( add )
				main_bucket.put(Val(key), Val(val), false);
			else
				main_bucket.del(Val(key));
			if(one)
				break;
		}
		txn.commit();
	}
}

std::atomic<uint64_t> reader_counter{};
std::atomic<uint64_t> writer_counter{};
std::atomic<uint64_t> last_tid{};

void run_bank(const std::string & db_path, size_t thread_counter){
	DBOptions options;
	options.minimal_mapping_size = 16*1024;
	options.meta_sync = false;
//	options.data_sync = false;
	DB db(db_path, options);
	Random random(++reader_counter);
	uint64_t ACCOUNTS = 1000;
	uint64_t TOTAL_VALUE = 1000000000;
	const bool read_only = (thread_counter != 0);
	while(true){
//		const bool read_only = (random.rnd() % 64) != 0;
		if(read_only)
			++reader_counter;
		else
			++writer_counter;
		TX txn(db, read_only);
//		txn.check_database([&](int progress){}, false);
		Bucket main_bucket = txn.get_bucket(Val("main"), false);
		if(!main_bucket.is_valid()){
			if(!read_only){
				main_bucket = txn.get_bucket(Val("main"));
				main_bucket.put(Val("bank"), Val(std::to_string(TOTAL_VALUE)), true);
				txn.commit();
			}
			continue;
		}
		Val a_value;
		main_bucket.get(Val("bank"), &a_value);
		uint64_t bank = std::stoull(a_value.to_string());
		uint64_t all_accs = 0;
		if(!read_only && writer_counter % 100 == 0)
			std::cerr << (read_only ? ("R/O " + std::to_string(reader_counter) + " tid=") : "R/W tid=") << txn.tid() << " oldest reader=" << txn.debug_get_oldest_reader_tid() << " bank=" << bank << " reader_counter=" << reader_counter << std::endl;
		if(read_only){
			for(uint64_t i = 0; i != ACCOUNTS; ++i){
	//			if( i == wa)
	//				sleep(1);
	//			if(thread_counter == 1 && i % 100 == 0)
	//				sleep(1);
				if( main_bucket.get(Val(std::to_string(i)), &a_value)){
					uint64_t aaa = std::stoull(a_value.to_string());
					all_accs += aaa;
				}
			}
			ass(bank + all_accs == TOTAL_VALUE, "bank robbed!");
		}
		if(!read_only){
			if( last_tid != 0 ){
				if(last_tid + 1 != txn.tid())
					std::cerr << "Aha" << std::endl;
			}
			last_tid = txn.tid();
			uint64_t acc = random.rnd() % ACCOUNTS;
			uint64_t aaa = 0;
			if( main_bucket.get(Val(std::to_string(acc)), &a_value)){
				aaa = std::stoull(a_value.to_string());
//				uint64_t prev = aaa;
//				bank += aaa / 2;
//				aaa = prev - aaa / 2;
			}
			uint64_t minus = bank/1000000;
			bank -= minus;
			aaa += minus;
			ass(main_bucket.put(Val(std::to_string(acc)), Val(std::to_string(aaa)), false), "Bad put in bank");
			ass(main_bucket.put(Val("bank"), Val(std::to_string(bank)), false), "Bad put in bank");
			txn.commit();
//			sleep(1);
		}
	}
}


void run_benchmark(const std::string & db_path){
	DB::remove_db(db_path);
	DBOptions options;
	options.minimal_mapping_size = 16*1024*1024;
	options.new_db_page_size = 4096;
	DB db(db_path, options);

	const int TEST_COUNT = DEBUG_MIRROR ? 2500 : 1000000;

	{
	auto idea_start  = std::chrono::high_resolution_clock::now();
	TX txn(db);
	Bucket main_bucket = txn.get_bucket(Val("main"));
	uint8_t keybuf[32] = {};
	for(unsigned i = 0; i != TEST_COUNT; ++i){
		unsigned hv = i * 2;
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &hv, sizeof(hv));
		blake2b_final(&ctx, &keybuf);
		main_bucket.put(Val(keybuf,32), Val(keybuf, 32), false);
	}
	txn.commit();
	auto idea_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
	std::cout << "Random insert of " << TEST_COUNT << " hashes, seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	txn.check_database([](int progress){
		std::cout << "Checking... " << progress << "%" << std::endl;
	}, true);
	std::cout << "DB passed all validity checks" << std::endl;
	}
	{
	auto idea_start  = std::chrono::high_resolution_clock::now();
	TX txn(db);
	Bucket main_bucket = txn.get_bucket(Val("main"));
	uint8_t keybuf[32] = {};
	int found_counter = 0;
	for(unsigned i = 0; i != 2 * TEST_COUNT; ++i){
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &i, sizeof(i));
		blake2b_final(&ctx, &keybuf);
		Val value;
		found_counter += main_bucket.get(Val(keybuf,32), &value) ? 1 : 0;
	}
	auto idea_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
	std::cout << "Random lookup of " << TEST_COUNT << " hashes, found " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
	{
	auto idea_start  = std::chrono::high_resolution_clock::now();
	TX txn(db);
	Bucket main_bucket = txn.get_bucket(Val("main"));
	uint8_t keybuf[32] = {};
	int found_counter = 0;
	for(unsigned i = 0; i != 2 * TEST_COUNT; ++i){
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &i, sizeof(i));
		blake2b_final(&ctx, &keybuf);
		Val value;
		found_counter += main_bucket.del(Val(keybuf,32)) ? 1 : 0;
	}
	txn.commit();
	auto idea_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
	std::cout << "Random delete of " << TEST_COUNT << " hashes, found " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	txn.check_database([](int progress){
		std::cout << "Checking... " << progress << "%" << std::endl;
	}, true);
	std::cout << "DB passed all validity checks" << std::endl;
	}
}

static size_t count_zeroes(uint64_t val){
	for(size_t i = 0; i != sizeof(val)*8; ++i)
		if((val & (uint64_t(1) << i)) != 0)
			return i;
	return sizeof(val)*8;
}

static constexpr size_t LEVELS = 10;

template<class T>
class SkipList {
public:
	struct Item { // We allocate only part of it
		T value;
		Item * prev;
		size_t height;
		Item * s_nexts[LEVELS];
		Item * & nexts(size_t i){
			ass(i < height, "out of nexts");
			return s_nexts[i];
		}
	};
	struct InsertPtr {
		std::array<Item *, LEVELS> previous_levels{};
		Item * next()const { return previous_levels.at(0)->nexts(0); }
	};
	SkipList(){
		tail_head.value = T{};
		tail_head.prev = &tail_head;
		tail_head.height = LEVELS;
		for(size_t i = 0; i != LEVELS; ++i)
			tail_head.nexts(i) = &tail_head;
	}
	~SkipList(){
		while(tail_head.prev != &tail_head){
			erase_begin();
//			print();
		}
	}
	int lower_bound(const T & value, InsertPtr * insert_ptr){
		Item * curr = &tail_head;
		size_t current_height = LEVELS - 1;
		int hops = 0;
		Item ** p_levels = insert_ptr->previous_levels.data();
		while(true){
			hops += 1;
			Item * next_curr = curr->s_nexts[current_height];
			if(next_curr == &tail_head || next_curr->value >= value){
				p_levels[current_height] = curr;
				if (current_height == 0)
					break;
				current_height -= 1;
				continue;
			}
			curr = next_curr;
		}
		return hops;
	}
	int count(const T & value) {
		InsertPtr insert_ptr;
		lower_bound(value, &insert_ptr);
		Item * del_item = insert_ptr.next();
		if(del_item == &tail_head || del_item->value != value)
			return 0;
		return 1;
	}
	std::pair<Item *, bool> insert(const T & value){
		InsertPtr insert_ptr;
		lower_bound(value, &insert_ptr);
		Item * next_curr = insert_ptr.next();
		if(next_curr != &tail_head && next_curr->value == value)
			return std::make_pair(next_curr, false);
		static uint64_t keybuf[4] = {};
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &keybuf, sizeof(keybuf));
		blake2b_final(&ctx, &keybuf);

		const size_t height = std::min<size_t>(LEVELS, 1 + count_zeroes(random.rnd())/3);//keybuf[0]
		Item * new_item = (Item *)malloc(sizeof(Item) - (LEVELS - height) * sizeof(Item *)); //new Item{};
		new_item->prev = insert_ptr.previous_levels.at(0);
		next_curr->prev = new_item;
		new_item->height = height;
		size_t i = 0;
		for(; i != height; ++i){
			new_item->nexts(i) = insert_ptr.previous_levels.at(i)->nexts(i);
			insert_ptr.previous_levels.at(i)->nexts(i) = new_item;
		}
//		for(; i != LEVELS; ++i)
//			new_item->nexts(i) = nullptr;
		new_item->value = value;
		return std::make_pair(new_item, true);
	}
	bool erase(const T & value){
		InsertPtr insert_ptr;
		lower_bound(value, &insert_ptr);
		Item * del_item = insert_ptr.next();
		if(del_item == &tail_head || del_item->value != value)
			return false;
		del_item->nexts(0)->prev = del_item->prev;
		del_item->prev = nullptr;
		for(size_t i = 0; i != del_item->height; ++i)
			if(del_item->nexts(i)){
				insert_ptr.previous_levels.at(i)->nexts(i) = del_item->nexts(i);
				del_item->nexts(i) = nullptr;
			}
		free(del_item);// delete del_item;
		return true;
	}
	void erase_begin(){
		Item * del_item = tail_head.nexts(0);
		ass(del_item != &tail_head, "deleting head_tail");
		Item * prev_item = del_item->prev;
		del_item->nexts(0)->prev = prev_item;
		del_item->prev = nullptr;
		for(size_t i = 0; i != del_item->height; ++i) {
			prev_item->nexts(i) = del_item->nexts(i);
			del_item->nexts(i) = nullptr;
		}
		free(del_item);// delete del_item;
	}
	bool empty()const{ return tail_head.prev == &tail_head; }
	Item * end(const T & v);
	void print(){
		Item * curr = &tail_head;
		std::array<size_t, LEVELS> level_counts{};
		std::cerr << "---- list ----" << std::endl;
		while(true){
			if(curr == &tail_head)
				std::cerr << std::setw(4) << "end" << " | ";
			else
				std::cerr << std::setw(4) << curr->value << " | ";
			for(size_t i = 0; i != curr->height; ++i){
				level_counts[i] += 1;
				if(curr == &tail_head || curr->nexts(i) == &tail_head)
					std::cerr << std::setw(4) <<  "end" << " ";
				else
					std::cerr << std::setw(4) << curr->nexts(i)->value << " ";
			}
			for(size_t i = curr->height; i != LEVELS; ++i)
				std::cerr << std::setw(4) <<  "_" << " ";
			if(curr->prev == &tail_head)
				std::cerr << "| " << std::setw(4) << "end" << std::endl;
			else
				std::cerr << "| " << std::setw(4) << curr->prev->value << std::endl;
			if(curr == tail_head.prev)
				break;
			curr = curr->nexts(0);
		}
		std::cerr  << "  #" << " | ";
		for(size_t i = 0; i != LEVELS; ++i){
			std::cerr << std::setw(4) << level_counts[i] << " ";
		}
		std::cerr << "| " << std::endl;
	}
private:
	Item tail_head;
	Random random;
};

struct jsw_node
{
    struct jsw_node *link[2];
    uint64_t data;
};

struct jsw_node *make_node(uint64_t data) {
    struct jsw_node *rn = new jsw_node;

    rn->data = data;
    rn->link[0] = rn->link[1] = NULL;

    return rn;
}

size_t jsw_insert(struct jsw_node *it, uint64_t data) {
	for (;;) {
		if (it->data == data)
			return 0;
		int dir = it->data < data;
		if (it->link[dir] == NULL) {
			it->link[dir] = make_node(data);
			return 1;
		}
		it = it->link[dir];
	}
}

size_t jsw_insert2(struct jsw_node **tree, uint64_t data) {
	for (;;) {
		if (*tree == NULL) {
			*tree = make_node(data);
			return 1;
		}
		struct jsw_node *it = *tree;
		if (it->data == data)
			return 0;
		int dir = it->data < data;
		tree = &it->link[dir];
	}
	// it->link[dir] = make_node(data);
    // return 1;
}
static size_t jsw_find_counter = 0;

size_t jsw_find(struct jsw_node *it, uint64_t data) {
    while (it != NULL) {
//		jsw_find_counter += 1;
        if (it->data == data)
            return 1;
		int dir = it->data < data;

		it = it->link[dir];
    }
    return 0;
}

size_t jsw_remove(struct jsw_node **tree, uint64_t data) {
    if (*tree == NULL)
		return 0;
	struct jsw_node head = { 0 };
	struct jsw_node *it = &head;
	struct jsw_node *p = *tree, *f = NULL;
	int dir = 1;

	it->link[1] = *tree;

	while (it->link[dir] != NULL) {
		p = it;
		it = it->link[dir];
		dir = it->data <= data;

		if (it->data == data) {
			f = it;
		}
	}

	if (f == NULL)
		return 0;
	f->data = it->data;
	p->link[p->link[1] == it] = it->link[it->link[0] == NULL];
	delete it;
	*tree = head.link[1];
    return 1;
}

struct AVLMinusTree {
	jsw_node * root = nullptr;
public:
	std::pair<int, bool> insert(uint64_t value) {
		if (root == NULL) {
			root = make_node(value);
			return {1, true};
		}
		auto result = jsw_insert(root, value);
		return {1, result > 0};
    }
	size_t count(uint64_t value) const {
		return jsw_find(root, value);
	}
	size_t erase(uint64_t value) {
		return jsw_remove(&root, value);
	}
};


// typical benchmark
// skiplist insert of 1000000 hashes, inserted 632459, seconds=1.486
// skiplist get of 1000000 hashes, hops 37.8428, seconds=1.428
// skiplist delete of 1000000 hashes, found 400314, seconds=1.565
// std::set insert of 1000000 hashes, inserted 632459, seconds=0.782
// std::set get of 1000000 hashes, found_counter 1000000, seconds=0.703
// std::set delete of 1000000 hashes, found 400314, seconds=0.906

std::vector<uint64_t> fill_random(uint64_t seed, size_t count) {
	Random random(seed);
	std::vector<uint64_t> result;
	for(size_t i = 0; i != count; ++i)
		result.push_back(random.rnd() % count);
	return result;
}

template<class Op>
void benchmark_op(const char * str, const std::vector<uint64_t> & samples, Op op) {
	size_t found_counter = 0;
	auto idea_start  = std::chrono::high_resolution_clock::now();
	for(const auto & sample : samples)
		found_counter += op(sample);
	auto idea_ms =
		std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
	std::cout << str << " count=" << samples.size() << " hits=" << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
}

int main(int argc, char * argv[]){
	size_t count = 1000000;
	std::vector<uint64_t> to_insert = fill_random(1, count);
	std::vector<uint64_t> to_count = fill_random(2, count);
	std::vector<uint64_t> to_erase = fill_random(3, count);
	
	std::set<uint64_t> test_set;
	benchmark_op("std::set insert ", to_insert, [&](uint64_t sample)->size_t{ return test_set.insert(sample).second; });
	benchmark_op("std::set count ", to_count, [&](uint64_t sample)->size_t{ return test_set.count(sample); });
	benchmark_op("std::set erase ", to_count, [&](uint64_t sample)->size_t{ return test_set.erase(sample); });
	std::unordered_set<uint64_t> test_uset;
	benchmark_op("std::uset insert ", to_insert, [&](uint64_t sample)->size_t{ return test_uset.insert(sample).second; });
	benchmark_op("std::uset count ", to_count, [&](uint64_t sample)->size_t{ return test_uset.count(sample); });
	benchmark_op("std::uset erase ", to_count, [&](uint64_t sample)->size_t{ return test_uset.erase(sample); });

//	SkipList<uint64_t> skip_list;
//	benchmark_op("skip_list insert ", to_insert, [&](uint64_t sample)->size_t{ return skip_list.insert(sample).second; });
//	benchmark_op("skip_list count ", to_count, [&](uint64_t sample)->size_t{ return skip_list.count(sample); });
//	benchmark_op("skip_list erase ", to_count, [&](uint64_t sample)->size_t{ return skip_list.erase(sample); });

	AVLMinusTree test_avl;
	benchmark_op("avl-- insert ", to_insert, [&](uint64_t sample)->size_t{ return test_avl.insert(sample).second; });
	benchmark_op("avl-- count ", to_count, [&](uint64_t sample)->size_t{ return test_avl.count(sample); });
	std::cout << "jsw_find_counter=" << jsw_find_counter << std::endl;
	benchmark_op("avl-- erase ", to_count, [&](uint64_t sample)->size_t{ return test_avl.erase(sample); });

	immer::set<uint64_t> immer_set;
	benchmark_op("immer insert ", to_insert, [&](uint64_t sample)->size_t{
		size_t was_size = immer_set.size();
		immer_set = immer_set.insert(sample);
		return immer_set.size() - was_size;
	});
	benchmark_op("immer count ", to_count, [&](uint64_t sample)->size_t{ return immer_set.count(sample); });
	benchmark_op("immer erase ", to_count, [&](uint64_t sample)->size_t{
		size_t was_size = immer_set.size();
		immer_set = immer_set.erase(sample);
		return was_size - immer_set.size();
	});
//    const auto v0 = immer::vector<int>{};
//    const auto v1 = v0.push_back(13);
//    assert(v0.size() == 0 && v1.size() == 1 && v1[0] == 13);
//
//    const auto v2 = v1.set(0, 42);
//    assert(v1[0] == 13 && v2[0] == 42);

//	benchmark_skiplist(count);
//	benchmark_stdset(count);
	return 0;

	for(size_t i = 0; i != 9; ++i){
		unsigned char buf[8]{};
		pack_uint_le(buf, i, 0x0123456789ABCDEF);
		uint64_t result = 0;
		unpack_uint_le(buf, i, result);
//		std::cerr << "Aha " << std::hex << result << std::endl;
	}

	std::string test;
	std::string benchmark;
	std::string scenario;
	std::string bank;
	std::string lockless;
	std::string backup;
	for(int i = 1; i < argc - 1; ++i){
		if(std::string(argv[i]) == "--test")
			test = argv[i+1];
		if(std::string(argv[i]) == "--scenario")
			scenario = argv[i+1];
		if(std::string(argv[i]) == "--benchmark")
			benchmark = argv[i+1];
		if(std::string(argv[i]) == "--bank")
			bank = argv[i+1];
		if(std::string(argv[i]) == "--lockless")
			lockless = argv[i+1];
		if(std::string(argv[i]) == "--backup")
			backup = argv[i+1];
	}
	if(!backup.empty()){
		DBOptions options;
		options.read_only = true;
		DB src(backup, options);
		options.read_only = false;
		DB dst(backup + "_backup", options);
		TX src_tx(src, true);
		TX dst_tx(dst);
		for(auto buname : src_tx.get_bucket_names()){
			Bucket src_bucket = src_tx.get_bucket(buname, false);
			Bucket dst_bucket = dst_tx.get_bucket(buname);
			Cursor cur = src_bucket.get_cursor();
			Val key, value;
			for(cur.first(); cur.get(&key, &value); cur.next())
				ass(dst_bucket.put(key, value, true), "put failed");
		}
		return 0;
	}
	if(!bank.empty()){
//		DBOptions options;
//		options.minimal_mapping_size = 1;
//		DB db(bank, options);
		std::vector<std::thread> threads;
		for (size_t i = 0; i != 8; ++i)
			threads.emplace_back(&run_bank, bank, i + 1);
		run_bank(bank, 0);
		return 0;
	}
	if(!lockless.empty()){
		std::vector<std::thread> threads;
		for (size_t i = 0; i != 1000; ++i)
			threads.emplace_back(&run_lockless);
		run_lockless();
		return 0;
	}
	if(!benchmark.empty()){
		run_benchmark(benchmark);
		return 0;
	}
	if(!test.empty()){
		if(!scenario.empty()){
	    	auto f = std::ifstream(scenario);
			run_test_driver(test, f);
		}else
			run_test_driver(test, std::cin);
		return 0;
	}
	
//	FreeList::test();
//	test_data_pages();
	interactive_test();
	return 0;
}

class ZipCPU {
	const bool simplified = true;
	std::vector<uint8_t> memory;
	std::array<uint32_t, 32> regs;
	bool cpu_gie = false;
	bool cpu_phase = false;
	bool write_memory(size_t offset, uint32_t value, size_t size){
		if (offset + size > memory.size())
			return false;
		if (offset % size != 0) // unaligned
			return false;
		for(size_t i = 0; i != size; ++i)
			memory.at(offset + i) = static_cast<uint8_t>(value >> (i * 8));
		return true;
	}
	bool read_memory(size_t offset, uint32_t * value, size_t size) {
		if (offset + size > memory.size())
			return false;
		if (offset % size != 0) // unaligned
			return false;
		*value = 0;
		for(size_t i = 0; i != size; ++i)
			*value = (*value << 8) | memory.at(offset + i);
		return true;
	}
public:
	enum Flags {
		CPU_SP_REG = 0xd,
		CPU_CC_REG = 0xe,
		CPU_PC_REG = 0xf,

		CPU_FLAG_Z_BIT = 0,
		CPU_FLAG_C_BIT = 1,
		CPU_FLAG_N_BIT = 2,
		CPU_FLAG_V_BIT = 3,
		CPU_FLAG_SLEEP_BIT = 4,
		CPU_FLAG_GIE_BIT = 5,
		CPU_FLAG_STEP_BIT = 6,
		CPU_FLAG_BREAK_BIT = 7,
		CPU_FLAG_ILL_BIT = 8,
		CPU_FLAG_TRAP_BIT = 9,
		CPU_FLAG_BUSERR_BIT = 10,
		CPU_FLAG_DIVERR_BIT = 11,
		CPU_FLAG_FPUERR_BIT = 12,
		CPU_FLAG_PHASE_BIT = 13,
		CPU_FLAG_CLRCACHE_BIT = 14,

		CPU_OPCODE_BREAK = 0x1c,
		CPU_OPCODE_LOCK = 0x1d,
		CPU_OPCODE_SIM  = 0x1e,
		CPU_OPCODE_NOP  = 0x1f,

		CPU_OPCODE_SUB  = 0x00,
		CPU_OPCODE_AND  = 0x01,
		CPU_OPCODE_ADD  = 0x02,
		CPU_OPCODE_OR   = 0x03,
		CPU_OPCODE_XOR  = 0x04,
		CPU_OPCODE_LSR  = 0x05,
		CPU_OPCODE_LSL  = 0x06,
		CPU_OPCODE_ASR  = 0x07,
		CPU_OPCODE_BREV = 0x08,
		CPU_OPCODE_LDILO =  0x09,
		CPU_OPCODE_MPYUHI = 0x0a,
		CPU_OPCODE_MPYSHI = 0x0b,
		CPU_OPCODE_MPY =  0x0c,
		CPU_OPCODE_MOV =  0x0d,
		CPU_OPCODE_DIVU = 0x0e,
		CPU_OPCODE_DIVS = 0x0f,
		CPU_OPCODE_CMP = 0x10,
		CPU_OPCODE_TST = 0x11,
		CPU_OPCODE_LW =  0x12,
		CPU_OPCODE_SW =  0x13,
		CPU_OPCODE_LH =  0x14,
		CPU_OPCODE_SH =  0x15,
		CPU_OPCODE_LB =  0x16,
		CPU_OPCODE_SB =  0x17,
		CPU_OPCODE_LDI0 = 0x18,
		CPU_OPCODE_LDI1 = 0x19,
		CPU_OPCODE_MOVSU = 0x18,
		CPU_OPCODE_MOVUS = 0x19,
		CPU_OPCODE_FP0 = 0x1a,
		CPU_OPCODE_FP1 = 0x1b,
		CPU_OPCODE_FP2 = 0x1c,
		CPU_OPCODE_FP3 = 0x1d,
		CPU_OPCODE_FP4 = 0x1e,
		CPU_OPCODE_FP5 = 0x1f,

		CIS_OPCODE_SUB = 0,
		CIS_OPCODE_AND = 1,
		CIS_OPCODE_ADD = 2,
		CIS_OPCODE_CMP = 3,
		CIS_OPCODE_LW =  4,
		CIS_OPCODE_SW =  5,
		CIS_OPCODE_LDI = 6,
		CIS_OPCODE_MOV = 7,
		
		ALU_OPCODE_MOV = 1,

		MOV_A_SEL = 18,
		MOV_B_SEL = 13,

		CIS_BIT = 31,
		CISIMMSEL = 23,
		IMMSEL = 18,
		
		CND_ALWAYS = 0,
		CND_Z = 1,
		CND_LT = 2, // N
		CND_C = 3,
		CND_V = 4,
		CND_NZ = 5,
		CND_GE = 6, // Not N
		CND_NC = 7
	};
	explicit ZipCPU(size_t size) {
		memory.resize((size + 3) / 4);
	}
	void copy_memory(size_t offset, const uint8_t * data, size_t size) {
		if (offset + size > memory.size())
			throw std::runtime_error("Copy out of range");
		memcpy(reinterpret_cast<uint8_t *>(memory.data()), data, size);
	}
	void set_register(bool user, size_t offset, uint32_t value) {
		regs.at(offset + (user ? 16 : 0)) = value;
	}
	uint32_t get_register(bool user, size_t offset) {
		return regs.at(offset + (user ? 16 : 0));
	}
	bool get_flag(bool user, size_t flag) {
		return (get_register(user, CPU_CC_REG) & (1 << flag)) != 0;
	}
	static uint32_t set_bit(uint32_t word, size_t pos, bool value){
		uint32_t mask = (1 << pos);
		return (word & ~mask) | (value ? mask : 0);
	}
	static uint32_t get_bits(uint32_t word, size_t pos_hi, size_t pos_lo){
		if (pos_hi >= 32 || pos_lo > pos_hi)
			throw std::runtime_error("get_bits out of range");
		return (word >> pos_lo) & (0xffffffff >> (31 - (pos_hi - pos_lo)));
	}
	static uint32_t get_sign_extend_bits(uint32_t word, size_t pos_hi, size_t pos_lo){
		uint32_t result = get_bits(word, pos_hi, pos_lo);
		if (word & (1 << pos_hi) && pos_hi != 31)
			result |= 0xffffffff << (pos_hi + 1);
		return result;
	}
	void set_flag(bool user, size_t flag, bool value) {
		set_register(user, CPU_CC_REG, set_bit(get_register(user, CPU_CC_REG), flag, value));
	}
	bool check_condition(uint32_t ccc, uint32_t flags) {
		switch (ccc){
		case CND_Z:
			return flags & (1 << CPU_FLAG_Z_BIT);
		case CND_LT:
			return flags & (1 << CPU_FLAG_N_BIT);
		case CND_C:
			return flags & (1 << CPU_FLAG_C_BIT);
		case CND_V:
			return flags & (1 << CPU_FLAG_V_BIT);
		case CND_NZ:
			return (flags & (1 << CPU_FLAG_Z_BIT)) == 0;
		case CND_GE:
			return (flags & (1 << CPU_FLAG_N_BIT)) == 0;
		case CND_NC:
			return (flags & (1 << CPU_FLAG_C_BIT)) == 0;
		default: //case CND_ALWAYS:
			return true;
		}
	}
	void on_exception(bool was_gie, size_t flag, const char * text){
		if (!was_gie)
			throw std::runtime_error(text);
		set_flag(false, flag, true);
		cpu_gie = false;
	}
	void execute_instruction() {
		bool was_gie = cpu_gie;
		uint32_t was_pc = get_register(was_gie, CPU_PC_REG);
		uint32_t was_flags = get_register(was_gie, CPU_CC_REG);
		uint32_t iword = 0;
		if (!read_memory(was_pc, &iword, 4))
			return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error reading PC");
		uint32_t dr = 0;
		bool dr_user = was_gie;
		uint32_t br = 0;
		bool br_user = was_gie;
		bool br_read = false; // false: imm, true: br + imm
		uint32_t cpu_opcode = 0;
		uint32_t imm_value = 0;
		uint32_t ccc = CND_ALWAYS;
		if ((iword & (1U << CIS_BIT)) != 0) {
			uint32_t siword = cpu_phase ? (iword << 16) : iword;

			static uint32_t cis_op_table[8] = {CPU_OPCODE_SUB, CPU_OPCODE_AND, CPU_OPCODE_ADD, CPU_OPCODE_CMP, CPU_OPCODE_LW, CPU_OPCODE_SW, CPU_OPCODE_LDI0, CPU_OPCODE_MOV};
			static bool cis_sp_override[8] = {0, 0, 0, 0, 1, 1, 0, 0};
			auto cis_opcode = get_bits(siword, 26, 24);
			cpu_opcode = cis_op_table[cis_opcode];

			dr = get_bits(siword, 31, 27);
			
			br_read = siword & (1 << CISIMMSEL);
			br = get_bits(siword, 22, 19);
			imm_value = get_sign_extend_bits(siword, br_read ? 18 : 22, 16);
			
			if (!br_read && cis_sp_override[cis_opcode]) {
				br_read = true;
				br = CPU_SP_REG;
			}

			if (!cpu_phase) {
				cpu_phase = true;
			} else {
				cpu_phase = false;
				set_register(was_gie, CPU_PC_REG, was_pc + 4);
			}
		} else {
			dr = get_bits(iword, 30, 27);
			
			br_read = iword & (1 << IMMSEL);
			br = get_bits(iword, 17, 14);
			imm_value = get_sign_extend_bits(iword, br_read ? 13 : 17, 0);
			
			ccc = get_bits(iword, 21, 19);
			
			if (!simplified && !was_gie && cpu_opcode == CPU_OPCODE_MOV) {
				dr_user = iword & (1 << MOV_A_SEL);
				br_user = iword & (1 << MOV_B_SEL);
				br_read = true;
				imm_value = get_sign_extend_bits(iword, 12, 0);
			}
			if (!simplified && (cpu_opcode == CPU_OPCODE_LDI0 || cpu_opcode == CPU_OPCODE_LDI1)) {
				ccc = CND_ALWAYS;
				br_read = false;
				imm_value = get_sign_extend_bits(iword, 22, 0);
			}
			if (simplified && cpu_opcode == CPU_OPCODE_MOVSU) {
				dr_user = true; // MOVSU/US are normal MOVs in user mode
			}
			if (simplified && cpu_opcode == CPU_OPCODE_MOVUS) {
				br_user = true; // MOVSU/US are normal MOVs in user mode
			}
			cpu_opcode = get_bits(iword, 26, 22);
			set_register(was_gie, CPU_PC_REG, was_pc + 4);
		}
		if (simplified && !check_condition(ccc, was_flags))
			return;
		auto special_bits = get_bits(iword, 30, 25);
		bool special = !(iword & (1U << CIS_BIT)) && (special_bits == 0x3f || special_bits == 0x3b);
		if (special) {
//			auto special_opcode = get_bits(iword, 24, 22);
			switch (cpu_opcode) {
			case CPU_OPCODE_NOP:
			case CPU_OPCODE_LOCK:
				break;
			case CPU_OPCODE_SIM:
				std::cout << char(get_bits(iword, 7, 0)) << char(get_bits(iword, 15, 8));
				break;
			case CPU_OPCODE_BREAK:
				return on_exception(was_gie, CPU_FLAG_BREAK_BIT, "Break in supervisor modde");
			}
			return;
		}
		if (!simplified && !check_condition(ccc, was_flags))
			return;
		uint64_t dr_value = get_register(dr_user, dr);
		int64_t dr_value_signed = static_cast<int32_t>(dr_value);
		uint64_t br_value = br_read ? get_register(br_user, br) + imm_value : imm_value;
		int64_t br_value_signed = static_cast<int32_t>(br_value);
		uint64_t fr = 0;
		bool dr_write = true;
		bool presign = dr_value & (1U << 31);
		bool set_ovfl = false;
		bool keep_sgn_on_ovfl = false;
		bool cc = false;
		switch(cpu_opcode){
		case CPU_OPCODE_SUB:
			fr = dr_value - br_value;
			set_ovfl = true;
			keep_sgn_on_ovfl = true;
			cc = fr & (1ULL << 32);
			break;
		case CPU_OPCODE_AND:
			fr = dr_value & br_value;
			set_ovfl = true;
			keep_sgn_on_ovfl = true;
			break;
		case CPU_OPCODE_ADD:
			fr = dr_value + br_value;
			set_ovfl = true;
			keep_sgn_on_ovfl = true;
			cc = fr & (1ULL << 32);
			break;
		case CPU_OPCODE_OR:
			fr = dr_value | br_value;
			break;
		case CPU_OPCODE_XOR:
			fr = dr_value ^ br_value;
		break;
		case CPU_OPCODE_LSR:
			set_ovfl = true;
			if( br_value <= 32) {
				fr = dr_value >> br_value;
				cc = br_value == 0 ? 0 : (dr_value >> (br_value - 1)) & 1;
			}
			break;
		case CPU_OPCODE_ASR:
			if( br_value <= 32) {
				fr = static_cast<uint32_t>(dr_value_signed >> br_value);
				cc = br_value == 0 ? 0 : (dr_value_signed >> (br_value - 1)) & 1;
			}
			break;
		case CPU_OPCODE_LSL:
			set_ovfl = true;
			if( br_value <= 32) {
				fr = dr_value << br_value;
				cc = fr & (1ULL << 32);
			}
			break;
		case CPU_OPCODE_BREV:
			for(size_t i = 0; i != 32; ++i)
				if (dr_value & (1U << i))
					fr |= 1ULL << (31U - i);
			break;
		case CPU_OPCODE_LDILO:
			fr = (dr_value & 0xFFFFFFFF) | (br_value & 0xFFFF);
		break;
		case CPU_OPCODE_MPYUHI:
			fr = (dr_value * br_value) >> 32;
		break;
		case CPU_OPCODE_MPYSHI:
			fr = static_cast<uint32_t>((dr_value_signed * br_value_signed) >> 32);
		break;
		case CPU_OPCODE_MPY:
			fr = dr_value * br_value;
		break;
		case CPU_OPCODE_MOV:
			fr = br_value;
		break;
		case CPU_OPCODE_DIVU:
			if (br_value == 0)
				return on_exception(was_gie, CPU_FLAG_DIVERR_BIT, "Division by 0 in supervisor mode, halt");
			fr = dr_value / br_value;
			break;
		case CPU_OPCODE_DIVS:
			if (br_value == 0)
				return on_exception(was_gie, CPU_FLAG_DIVERR_BIT, "Division by 0 in supervisor mode, halt");
			fr = static_cast<uint32_t>(dr_value_signed / br_value_signed);
			break;
		case CPU_OPCODE_CMP:
			fr = dr_value - br_value;
			dr_write = false;
			set_ovfl = true;
			keep_sgn_on_ovfl = true;
			cc = fr & (1ULL << 32);
			break;
		case CPU_OPCODE_TST:
			fr = dr_value & br_value;
			dr_write = false;
			set_ovfl = true;
			keep_sgn_on_ovfl = true;
			break;
		case CPU_OPCODE_LW: {
			uint32_t val = 0;
			if (!read_memory(br_value, &val, 4))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on LW");
			fr = val;
			break;
			}
		case CPU_OPCODE_SW:
			dr_write = false;
			if (!write_memory(br_value, static_cast<uint32_t>(dr_value), 4))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on SW");
			break;
		case CPU_OPCODE_LH: {
			uint32_t val = 0;
			if (!read_memory(br_value, &val, 2))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on LH");
			fr = val;
			break;
			}
		case CPU_OPCODE_SH:
			dr_write = false;
			if (!write_memory(br_value, static_cast<uint32_t>(dr_value), 2))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on SH");
			break;
		case CPU_OPCODE_LB: {
			uint32_t val = 0;
			if (!read_memory(br_value, &val, 1))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on LB");
				fr = val;
			break;
			}
		case CPU_OPCODE_SB:
			dr_write = false;
			if (!write_memory(br_value, static_cast<uint32_t>(dr_value), 1))
				return on_exception(was_gie, CPU_FLAG_BUSERR_BIT, "Bus error on SB");
			break;
		case CPU_OPCODE_LDI0:
			fr = imm_value;
			break;
		case CPU_OPCODE_LDI1:
			fr = imm_value;
			break;
		default: // CPU_OPCODE_FP*
			throw std::runtime_error("Invalid operation");
		}
		if (dr_write) {
			set_register(dr_user, dr, static_cast<uint32_t>(fr));
			set_flag(was_gie, CPU_FLAG_Z_BIT, (fr & 0xFFFFFFFF) == 0);
			set_flag(was_gie, CPU_FLAG_C_BIT, cc);
			auto n = (fr & (1U << 31)) != 0;
			set_flag(was_gie, CPU_FLAG_V_BIT, set_ovfl && (presign != n));
			auto vx = keep_sgn_on_ovfl && (presign != n);
			set_flag(was_gie, CPU_FLAG_N_BIT, n ^ vx);
			
			if (!was_gie && get_flag(was_gie, CPU_FLAG_GIE_BIT)) {
				set_flag(cpu_gie, CPU_FLAG_GIE_BIT, false);
				cpu_gie = true;
				set_flag(cpu_gie, CPU_FLAG_GIE_BIT, true);
				set_flag(cpu_gie, CPU_FLAG_ILL_BIT, false);
				set_flag(cpu_gie, CPU_FLAG_TRAP_BIT, false);
				set_flag(cpu_gie, CPU_FLAG_BREAK_BIT, false);
				set_flag(cpu_gie, CPU_FLAG_BUSERR_BIT, false);
				set_flag(cpu_gie, CPU_FLAG_DIVERR_BIT, false);
				set_flag(cpu_gie, CPU_FLAG_GIE_BIT, false);
			}
			if (was_gie && !get_flag(was_gie, CPU_FLAG_GIE_BIT)) {
				set_flag(cpu_gie, CPU_FLAG_GIE_BIT, true);
			}
		}
	}
	bool call(bool user, size_t offset) {
		return true;
	}
};
