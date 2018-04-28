// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <stdlib.h>
#include <map>
#include <random>
#include "mustela.hpp"

void interactive_test(){
//	    mustela::DB db("/Users/hrissan/Documents/devbox/mustela/bin/test.mustella");
//	    mustela::DB db("/Users/user/Desktop/devbox/mustela/bin/test.mustella");
	mustela::DB db("test.mustella");
	mustela::TX txn(db);
	mustela::Val main_table("main");
	txn.create_table(main_table);
	std::map<std::string, std::string> mirror;
	// TODO - load mirror from db
	const int items_counter = 1000;
	std::default_random_engine e;//{r()};
	std::uniform_int_distribution<int> dist(0, items_counter - 1);
	//    for(int i = 0; i != items_counter * 4; ++i){
	//        int j = dist(e);
	//    }
	std::vector<std::string> cmds{"ar", "db", "t", "t", "t", "t", "ar", "dr", "ar", "dr", "ar", "dr", "ar", "dr", "dr", "dr", "dr", "dr", "a", "d", "a"};
	while(true){
		//        std::string json = txn.print_db();
		//        std::cout << json << std::endl;
		std::cout << txn.get_stats() << std::endl;
		std::cout << txn.get_stats(main_table) << std::endl;
		std::cout << "q - quit, p - print, a - add 1M values, d - delete 1M values, ar - add 1M random values, dr - delete 1M random values, ab - add 1M values backwards, db - delete 1M values backwards\n";
		std::string input;
		if( !cmds.empty()){
			input = cmds.at(0);
			cmds.erase(cmds.begin());
		}else
			getline(std::cin, input);
		if( input == "q")
			break;
		if( input == "p"){
			std::string json = txn.print_db();
			std::cout << json << std::endl;
			continue;
		}
		bool new_range = input.find("n") != std::string::npos;
		bool add = input.find("a") != std::string::npos;
		bool ran = input.find("r") != std::string::npos;
		bool back = input.find("b") != std::string::npos;
		std::cout << "add=" << int(add) << " ran=" << int(ran) << " back=" << int(back) << std::endl;
		if( input == "t"){
			int j = dist(e);
			std::string key = std::to_string(j);
			std::string val = "value" + std::to_string(j);// + std::string(j % 512, '*');
			if( !txn.put(main_table, mustela::Val(key), mustela::Val(val), true) )
				std::cout << "BAD put" << std::endl;
			mirror[key] = val;
			txn.commit();
			continue;
		}
		for(int i = 0; i != items_counter; ++i){
			int j = ran ? dist(e) : back ? items_counter - 1 - i : i;
			if( new_range )
				j += items_counter;
			std::string key = std::to_string(j);
			std::string val = "value" + std::to_string(j);// + std::string(j % 512, '*');
			mustela::Val got;
			if( (i == 4 && j == 584) || (i == 5 && j == 402) || (i == 549 && j == 70) || (i == 550 && j == 295) ){
				got.size = 0;
				//                    std::string json = txn.print_db(main_table);
				//                    std::cout << json << std::endl;
			}
			bool in_db = txn.get(main_table, mustela::Val(key), got) && got.to_string() == val;
			auto mit = mirror.find(key);
			bool in_mirror = mit != mirror.end() && mit->second == val;
			if( in_db != in_mirror )
				std::cout << "BAD get" << std::endl;
			if( add ){
				if( !txn.put(main_table, mustela::Val(key), mustela::Val(val), false) )
					std::cout << "BAD put" << std::endl;
				mirror[key] = val;
			}else{
				if( !txn.del(main_table, mustela::Val(key), false) )
					std::cout << "BAD del" << std::endl;
				mirror.erase(key);
			}
		}
		for(auto && ma : mirror){
			mustela::Val value;
			bool result = txn.get(main_table, mustela::Val(ma.first), value);
			if( !result || ma.second != value.to_string()){
				std::cout << "Bad check ma=" << ma.first << std::endl;
				result = txn.get(main_table, mustela::Val(ma.first), value);
			}
		}
		txn.commit();
	}
}

int main(int argc, char * argv[]){
	
	mustela::FreeList::test();
	mustela::test_data_pages();
	
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
