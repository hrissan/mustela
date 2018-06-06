// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <fstream>
#include <map>
#include <random>
#include <chrono>
#include <iomanip>
#include "mustela.hpp"
#include "testing.hpp"
extern "C" {
#include "blake2b.h"
}

struct Mirror {
	std::string value;
	mustela::Cursor cursor;

	Mirror(const std::string & value, const mustela::Cursor & cursor):value(value), cursor(cursor)
	{}
	bool operator==(const Mirror & other)const{
		return value == other.value && cursor == other.cursor;
	}
	bool operator!=(const Mirror & other)const{ return !(*this == other); }
};

void interactive_test(){
	//	    mustela::DB db("/Users/hrissan/Documents/devbox/mustela/bin/test.mustella");
	//	    mustela::DB db("/Users/user/Desktop/devbox/mustela/bin/test.mustella");
	mustela::DB db("test.mustella");
	mustela::Val main_bucket_name("main");
	
	{
//		mustela::Bucket meta_bucket(txn, mustela::Val());
//		std::string json = txn.debug_print_meta_db();
//		std::cout << "Meta table: " << json << std::endl;
	}
	
//	for(auto && tt : txn.get_bucket_names() )
//		std::cout << "Table: " << tt.to_string() << std::endl;
	
/*	{
		if( !txn.drop_bucket(mustela::Val()) ) {
			mustela::Bucket empty_bucket(txn, mustela::Val());
			std::string long_key(95, 'A');
			empty_bucket.put(mustela::Val("1"), mustela::Val("val1"), false);
			empty_bucket.put(mustela::Val("2"), mustela::Val("val2"), false);
			std::string json = empty_bucket.print_db();
			std::cout << "Empty table: " << json << std::endl;
			empty_bucket.put(mustela::Val(long_key), mustela::Val("vallong"), false);
			json = empty_bucket.print_db();
			std::cout << "Empty table: " << json << std::endl;
		}
		mustela::Bucket evil_bucket(txn, mustela::Val("Evil"));
		for(int i = 0; i != 100; ++i){
			std::string key = std::to_string(i/10) + std::to_string(i%10);
			std::string val = "value" + std::to_string(i);// + std::string(70,'A');
			if( !evil_bucket.put(mustela::Val(key), mustela::Val(val), true) )
				std::cout << "BAD put" << std::endl;
		}
		std::string json = evil_bucket.print_db();
		std::cout << "Evil table: " << evil_bucket.get_stats() << std::endl << json << std::endl;
		mustela::Bucket hren_bucket(txn, mustela::Val("Hren"));
	}*/
	
	std::map<std::string, Mirror> mirror;
	
	const int items_counter = 500;
	std::default_random_engine e;//{r()};
	std::uniform_int_distribution<int> dist(0, items_counter - 1);
	//    for(int i = 0; i != items_counter * 4; ++i){
	//        int j = dist(e);
	//    }
	std::vector<std::string> cmds{"ar", "db", "ar", "dr", "ar", "dr", "ar", "dr", "ar", "dr", "ar", "d", "dr", "a", "dr", "dr", "ar"};
	while(true){
		mustela::TX txn(db);
		mustela::Bucket main_bucket = txn.get_bucket(main_bucket_name);
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
			if( !main_bucket.put(mustela::Val(key), mustela::Val(val), true) )
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
				val += std::string((j*j) % 512, '*');
			mustela::Val got;
//			if( (!add && i == 82 && j == 82)){
//				got.size = 0;
//				std::string json = main_bucket.print_db();
//				std::cout << "Main table: " << json << std::endl;
//			}
//			bool in_db = main_bucket.get(mustela::Val(key), &got);
//			auto mit = mirror.find(key);
//			bool in_mirror = mit != mirror.end();
//			if( in_db != in_mirror )
//				std::cout << "BAD get" << std::endl;
//			if( in_mirror && in_db && mit->second.value != got.to_string() )
//				std::cout << "BAD get value" << std::endl;
			if( add )
				main_bucket.put(mustela::Val(key), mustela::Val(val), false);
			else
				main_bucket.del(mustela::Val(key));
			if(one)
				break;
		}
		txn.commit();
	}
}

void run_benchmark(const std::string & db_path){
	mustela::DB::remove_db(db_path);
	mustela::DBOptions options;
	options.minimal_mapping_size = 16*1024*1024;
	options.new_db_page_size = 4096;
	mustela::DB db(db_path, options);


	const int TEST_COUNT = mustela::DEBUG_MIRROR ? 2500 : 1000000;

	{
	auto idea_start  = std::chrono::high_resolution_clock::now();
	mustela::TX txn(db);
	mustela::Bucket main_bucket = txn.get_bucket(mustela::Val("main"));
	uint8_t keybuf[32] = {};
	for(unsigned i = 0; i != TEST_COUNT; ++i){
		unsigned hv = i * 2;
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &hv, sizeof(hv));
		blake2b_final(&ctx, &keybuf);
		main_bucket.put(mustela::Val(keybuf,32), mustela::Val(keybuf, 32), false);
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
	mustela::TX txn(db);
	mustela::Bucket main_bucket = txn.get_bucket(mustela::Val("main"));
	uint8_t keybuf[32] = {};
	int found_counter = 0;
	for(unsigned i = 0; i != 2 * TEST_COUNT; ++i){
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &i, sizeof(i));
		blake2b_final(&ctx, &keybuf);
		mustela::Val value;
		found_counter += main_bucket.get(mustela::Val(keybuf,32), &value) ? 1 : 0;
	}
	auto idea_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
	std::cout << "Random lookup of " << TEST_COUNT << " hashes, found " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
	{
	auto idea_start  = std::chrono::high_resolution_clock::now();
	mustela::TX txn(db);
	mustela::Bucket main_bucket = txn.get_bucket(mustela::Val("main"));
	uint8_t keybuf[32] = {};
	int found_counter = 0;
	for(unsigned i = 0; i != 2 * TEST_COUNT; ++i){
		auto ctx = blake2b_ctx{};
		blake2b_init(&ctx, 32, nullptr, 0);
		blake2b_update(&ctx, &i, sizeof(i));
		blake2b_final(&ctx, &keybuf);
		mustela::Val value;
		found_counter += main_bucket.del(mustela::Val(keybuf,32)) ? 1 : 0;
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
		if((val & (1 << i)) != 0)
			return i;
	return sizeof(val)*8;
}

static constexpr size_t LEVELS = 20;

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
		size_t current_height = LEVELS;
		int hops = 0;
		Item ** p_levels = insert_ptr->previous_levels.data();
		while(current_height != 0){
			hops += 1;
			Item * next_curr = curr->nexts(current_height - 1);
			if(next_curr == &tail_head || next_curr->value >= value){
//				insert_ptr->previous_levels.at(current_height - 1) = curr;
				p_levels[current_height - 1] = curr;
				current_height -= 1;
				continue;
			}
			curr = next_curr;
		}
		return hops;
	}
	std::pair<Item *, bool> insert(const T & value){
		InsertPtr insert_ptr;
		lower_bound(value, &insert_ptr);
		Item * next_curr = insert_ptr.next();
		if(next_curr != &tail_head && next_curr->value == value)
			return std::make_pair(next_curr, false);
		const size_t height = std::min<size_t>(LEVELS, 1 + count_zeroes(random.rand()));
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
//		Item * next_item = del_item->nexts(0);
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
		std::cerr << "---- list ----" << std::endl;
		while(true){
			if(curr == &tail_head)
				std::cerr << std::setw(4) << "end" << " | ";
			else
				std::cerr << std::setw(4) << curr->value << " | ";
			for(size_t i = 0; i != curr->height; ++i){
				if(curr->nexts(i) == &tail_head)
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
	}
private:
	Item tail_head;
	mustela::Random random;
};

void benchmark_skiplist(size_t TEST_COUNT){
	mustela::Random random;
	SkipList<uint64_t> skip_list;
	skip_list.print();
	{
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random.rand()%TEST_COUNT;
			found_counter += skip_list.insert(val).second;
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "skiplist insert of " << TEST_COUNT << " hashes, inserted " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
//	skip_list.print();
	{
		mustela::Random random2;
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		SkipList<uint64_t>::InsertPtr ptr;
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random2.rand()%TEST_COUNT;
			found_counter += skip_list.lower_bound(val, &ptr);
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "skiplist get of " << TEST_COUNT << " hashes, hops " << double(found_counter)/TEST_COUNT << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
	{
//		mustela::Random random;
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random.rand()%TEST_COUNT;
			found_counter += skip_list.erase(val);
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "skiplist delete of " << TEST_COUNT << " hashes, found " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
}
void benchmark_stdset(size_t TEST_COUNT){
	mustela::Random random;
	std::set<uint64_t> skip_list;
	{
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random.rand()%TEST_COUNT;
			found_counter += skip_list.insert(val).second;
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "std::set insert of " << TEST_COUNT << " hashes, inserted " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
	{
		mustela::Random random2;
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random2.rand()%TEST_COUNT;
			found_counter += skip_list.count(val);
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "std::set get of " << TEST_COUNT << " hashes, found_counter " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
	{
//		mustela::Random random;
		int found_counter = 0;
		auto idea_start  = std::chrono::high_resolution_clock::now();
		for(size_t i = 0; i != TEST_COUNT; ++i){
			uint64_t val = random.rand()%TEST_COUNT;
			found_counter += skip_list.erase(val);
		}
		auto idea_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - idea_start);
		std::cout << "std::set delete of " << TEST_COUNT << " hashes, found " << found_counter << ", seconds=" << double(idea_ms.count()) / 1000 << std::endl;
	}
}
// typical benchmark
// skiplist insert of 1000000 hashes, inserted 632459, seconds=1.486
// skiplist get of 1000000 hashes, hops 37.8428, seconds=1.428
// skiplist delete of 1000000 hashes, found 400314, seconds=1.565
// std::set insert of 1000000 hashes, inserted 632459, seconds=0.782
// std::set get of 1000000 hashes, found_counter 1000000, seconds=0.703
// std::set delete of 1000000 hashes, found 400314, seconds=0.906

int main(int argc, char * argv[]){
//	size_t count = 1000000;
//	benchmark_skiplist(count);
//	benchmark_stdset(count);
//	return 0;

	for(size_t i = 0; i != 9; ++i){
		unsigned char buf[8]{};
		mustela::pack_uint_le(buf, i, 0x0123456789ABCDEF);
		uint64_t result = 0;
		mustela::unpack_uint_le(buf, i, result);
//		std::cerr << "Aha " << std::hex << result << std::endl;
	}

	std::string test;
	std::string benchmark;
	std::string scenario;
	for(int i = 1; i < argc - 1; ++i){
		if(std::string(argv[i]) == "--test")
			test = argv[i+1];
		if(std::string(argv[i]) == "--scenario")
			scenario = argv[i+1];
		if(std::string(argv[i]) == "--benchmark")
			benchmark = argv[i+1];
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
	
//	mustela::FreeList::test();
	mustela::test_data_pages();
	{
		mustela::DBOptions options;
		options.minimal_mapping_size = 1024;
		options.new_db_page_size = 128;
		mustela::DB db("test.mustella", options);
		mustela::TX txn(db);
		mustela::Bucket main_bucket = txn.get_bucket(mustela::Val("main"), false);
		auto ab = txn.get_bucket_names();
		mustela::Bucket zhu_bucket = txn.get_bucket(mustela::Val("zhu"));
		ab = txn.get_bucket_names();
		
		mustela::Bucket large_bucket = txn.get_bucket(mustela::Val("large"));
		mustela::Cursor cur = large_bucket.get_cursor();
		mustela::Cursor before_first = cur;
		mustela::Cursor end = cur;
		end.end();
//		large_bucket.put(mustela::Val(), mustela::Val("aha"), false);
//		large_bucket.put(mustela::Val(std::string(db.max_key_size(), 0)), mustela::Val("oho"), false);
//		large_bucket.put(mustela::Val(std::string(db.max_key_size(), char(0xFF))), mustela::Val("uhu"), false);
		large_bucket.put(mustela::Val("000"), mustela::Val("000"), false);
		large_bucket.put(mustela::Val("111"), mustela::Val("111"), false);
		large_bucket.put(mustela::Val("222"), mustela::Val("222"), false);
		std::cerr << "forward " << std::endl;
		for(cur.before_first(); cur != end; cur.next()){
			mustela::Val c_key, c_value;
			bool res = cur.get(&c_key, &c_value);
			std::cerr << res << " " << c_key.to_string() << std::endl;
		}
		std::cerr << "backwards " << std::endl;
		for(cur.end(); cur != before_first; cur.prev()){
			mustela::Val c_key, c_value;
			bool res = cur.get(&c_key, &c_value);
			std::cerr << res << " " << c_key.to_string() << std::endl;
		}
		ab = txn.get_bucket_names();
		txn.check_database([](int progress){
			std::cout << "Checking... " << progress << "%" << std::endl;
		}, true);
		txn.commit();
		txn.check_database([](int progress){
			std::cout << "Checking... " << progress << "%" << std::endl;
		}, true);
//		return 0;
	}
	interactive_test();
	/*    txn.put(mustela::Val("A"), mustela::Val("AVAL"), true);
	 txn.put(mustela::Val("B"), mustela::Val("BVAL"), true);
	 txn.put(mustela::Val("C"), mustela::Val("CVAL"), true);
	 
	 txn.commit();
	 
	 const bool insert = true;
	 //    std::random_device r;
	 std::default_random_engine e;//{r()};
	 const int items_counter = 1000000;
	 std::uniform_int_distribution<int> dist(0, items_counter - 1);
	 int lucky_counter = 0;
	 //    for(int i = items_counter - 200; i-- > 0; ){
	 for(int i = 0; i != 200; ++i){
	 //        std::cout << std::endl << "print_db i=" << i << std::endl << std::endl;
	 //        std::string json = txn.print_db();
	 //        std::cout << json << std::endl;
	 int j = dist(e);
	 std::string key = std::to_string(j);
	 std::string val = "value" + std::to_string(j);
	 
	 mustela::Val got;
	 if( insert ){
	 if( txn.put(mustela::Val(key), mustela::Val(val), true) )
	 {
	 //                mirror[key] = val;
	 continue;
	 }
	 }else{
	 bool was = txn.get(mustela::Val(key), got);
	 lucky_counter += was ? 1 : 0;
	 if( was && !txn.del(mustela::Val(key), true) ){
	 std::cout << "del failed" << std::endl;
	 //            mirror.erase(key);
	 continue;
	 }
	 if( txn.get(mustela::Val(key), got) )
	 std::cout << "after del key is still there" << std::endl;
	 
	 }
	 }
	 txn.del(mustela::Val("A"), true);
	 txn.del(mustela::Val("B"), true);
	 txn.del(mustela::Val("C"), true);
	 std::cout << "Mirror lucky_counter=" << lucky_counter << std::endl;
	 for(auto && ma : mirror){
	 mustela::Val value;
	 bool result = txn.get(mustela::Val(ma.first), value);
	 if( ma.second != value.to_string())
	 std::cout << "Bad " << ma.first << ":" << ma.second << " in db result=" << int(result) << " value=" << value.to_string() << std::endl;
	 }
	 //if( !insert)
	 {
	 std::string json = txn.print_db();
	 std::cout << json << std::endl;
	 }
	 std::cout << txn.get_stats() << std::endl;
	 txn.commit();
	 std::cout << "Page" << std::endl;
	 for(int i = 0; i != pa->item_count; ++i){
	 Val value;
	 Val key = pa->get_item_kv(page_size, i, value);
	 std::cout << key.to_string() << ":" << value.to_string() << std::endl;
	 }
	 txn.put(mustela::Val("A"), mustela::Val("AVAL"), true);
	 txn.put(mustela::Val("B"), mustela::Val("BVAL"), true);
	 txn.put(mustela::Val("C"), mustela::Val("CVAL"), true);*/
	return 0;
}
