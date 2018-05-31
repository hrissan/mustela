// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <stdlib.h>
#include <map>
#include <random>
#include "mustela.hpp"

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
	std::cout << "Meta page size is " << sizeof(mustela::MetaPage) << " Max Key Size is " << db.max_key_size() << std::endl;
	mustela::TX txn(db);
	mustela::Val main_bucket_name("main");
	
	{
//		mustela::Bucket meta_bucket(txn, mustela::Val());
		std::string json = txn.print_meta_db();
		std::cout << "Meta table: " << json << std::endl;
	}
	
	for(auto && tt : txn.get_bucket_names() )
		std::cout << "Table: " << tt.to_string() << std::endl;
	
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
	
	const int items_counter = 100;
	std::default_random_engine e;//{r()};
	std::uniform_int_distribution<int> dist(0, items_counter - 1);
	//    for(int i = 0; i != items_counter * 4; ++i){
	//        int j = dist(e);
	//    }
	std::vector<std::string> cmds{"ar", "db", "t", "t", "t", "t", "ar", "dr", "ar", "dr", "ar", "dr", "ar", "dr", "dr", "dr", "dr", "dr", "a", "d", "a"};
	while(true){
//		mustela::Bucket meta_bucket(txn, mustela::Val());
		mustela::Bucket main_bucket(txn, main_bucket_name);
		{
			mirror.clear();
//			mustela::Bucket main_bucket(txn, main_bucket_name);
//			std::string json = main_bucket.print_db();
//			std::cout << "Main table: " << json << std::endl;
			mustela::Cursor cur(main_bucket);
			mustela::Val c_key, c_value;
			for (cur.first(); cur.get(c_key, c_value); cur.next()) {
				if (!mirror.insert(std::make_pair(c_key.to_string(), Mirror(c_value.to_string(), cur))).second)
					std::cout << "BAD mirror insert " << c_key.to_string() << std::endl;
//				else
//					std::cout << c_key.to_string() << std::endl;
			}
			std::map<std::string, Mirror> mirror2;
			for (cur.last(); cur.get(c_key, c_value); cur.prev()) {
				if (!mirror2.insert(std::make_pair(c_key.to_string(), Mirror(c_value.to_string(), cur))).second)
					std::cout << "BAD mirror2 insert " << c_key.to_string() << std::endl;
//				else
//					std::cout << c_key.to_string() << std::endl;
			}
			if( mirror != mirror2 )
				std::cout << "Inconsistent forward/backward iteration " << mirror.size() << " " << mirror2.size() << std::endl;
		}
		//        std::string json = txn.print_db();
		//        std::cout << json << std::endl;
		std::cout << txn.get_meta_stats() << std::endl;
		std::cout << main_bucket.get_stats() << std::endl;
		std::cout << "q - quit, p - print, a - add 1M values, d - delete 1M values, ar - add 1M random values, dr - delete 1M random values, ab - add 1M values backwards, db - delete 1M values backwards, f - print free list" << std::endl;
		std::string input;
		if( !cmds.empty()){
			input = cmds.at(0);
			std::cout << input << std::endl;
			cmds.erase(cmds.begin());
		}else
			getline(std::cin, input);
		if( input == "q")
			break;
		if( input == "p"){
			std::string json = txn.print_meta_db();
			std::cout << "Meta table: " << json << std::endl;
			json = main_bucket.print_db();
			std::cout << "Main table: " << json << std::endl;
			continue;
		}
		if( input == "f"){
			std::cout << "Free table: " << std::endl;
			txn.print_free_list();
			continue;
		}
		bool new_range = input.find("n") != std::string::npos;
		bool add = input.find("a") != std::string::npos;
		bool ran = input.find("r") != std::string::npos;
		bool back = input.find("b") != std::string::npos;
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
			int j = ran ? dist(e) : back ? items_counter - 1 - i : i;
			if( new_range )
				j += items_counter;
		  	std::string key = std::string(6 - std::to_string(j).length(), '0') + std::to_string(j);
			//std::string key = std::to_string(j) + std::string(4, 'A');
			std::string val = "value" + std::to_string(j);// + std::string(j % 128, '*');
			mustela::Val got;
			if( (!add && i == 82 && j == 82)){ //  || (add && i == 25 && j == 4) || (!add && i == 997 && j == 997)  || (!add && i == j && j >= 990)
				got.size = 0;
				std::string json = main_bucket.print_db();
				std::cout << "Main table: " << json << std::endl;
			}
			bool in_db = main_bucket.get(mustela::Val(key), got) && got.to_string() == val;
			auto mit = mirror.find(key);
			bool in_mirror = mit != mirror.end() && mit->second.value == val;
			if( in_db != in_mirror )
				std::cout << "BAD get" << std::endl;
			if( add ){
				if( !main_bucket.put(mustela::Val(key), mustela::Val(val), false) )
					std::cout << "BAD put" << std::endl;
				mustela::Cursor cur(main_bucket);
				if( !cur.seek(mustela::Val(key)) )
					std::cout << "BAD seek" << std::endl;
				mirror.insert(std::make_pair(key, Mirror(val, cur)));
			}else{
				if( !main_bucket.del(mustela::Val(key), false) )
					std::cout << "BAD del" << std::endl;
				mirror.erase(key);
			}
			if( (!add && i == 82 && j == 82)){ //  || (add && i == 25 && j == 4) || (!add && i == 997 && j == 997)  || (!add && i == j && j >= 990)
				got.size = 0;
				std::string json = main_bucket.print_db();
				std::cout << "Main table: " << json << std::endl;
			}
		for(auto && ma : mirror){
			mustela::Val value, c_key, c_value;
			bool result = main_bucket.get(mustela::Val(ma.first), value);
			bool c_result = ma.second.cursor.get(c_key, c_value);
			if( !result || !c_result || ma.second.value != value.to_string() || c_key.to_string() != ma.first || c_value.to_string() != ma.second.value ){
				std::cout << "Bad check ma=" << ma.first << std::endl;
				std::string json = main_bucket.print_db();
				std::cout << "Main table: " << json << std::endl;
//				result = main_bucket.get(mustela::Val(ma.first), value);
			}
		}
		}
		txn.commit();
	}
}

int main(int argc, char * argv[]){
	
	mustela::FreeList::test();
	mustela::test_data_pages();
	{
		mustela::DB db("test.mustella");
		mustela::TX txn(db);
		mustela::Bucket main_bucket(txn, mustela::Val());
		txn.commit();
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
