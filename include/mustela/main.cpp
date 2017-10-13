// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <stdlib.h>
#include <map>
#include <random>
#include "mustela.hpp"

void interactive_test(){
    mustela::DB db("/Users/hrissan/Documents/devbox/mustela/bin/test.mustella");
    mustela::TX txn(db);
    std::map<std::string, std::string> mirror;
    const int items_counter = 100000;
    std::default_random_engine e;//{r()};
    std::uniform_int_distribution<int> dist(0, items_counter - 1);
    while(true){
        std::cout << txn.get_stats() << std::endl;
        std::cout << "q - quit, a - add 1M values, d - delete 1M values, ar - add 1M random values, dr - delete 1M random values, ab - add 1M values backwards, db - delete 1M values backwards\n";
        std::string input;
        getline(std::cin, input);
        if( input == "q")
            break;
        bool add = input.find("a") != std::string::npos;
        bool ran = input.find("r") != std::string::npos;
        bool back = input.find("b") != std::string::npos;
        std::cout << "add=" << int(add) << " ran=" << int(ran) << " back=" << int(back) << std::endl;
            for(int i = 0; i != items_counter; ++i){
                int j = ran ? dist(e) : back ? items_counter - 1 - i : i;
                std::string key = std::to_string(j);
                std::string val = "value" + std::to_string(j);
                mustela::Val got;
                bool in_db = txn.get(mustela::Val(key), got) && got.to_string() == val;
                auto mit = mirror.find(key);
                bool in_mirror = mit != mirror.end() && mit->second == val;
                if( in_db != in_mirror )
                    std::cout << "BAD get" << std::endl;
                if( add ){
                    if( !txn.put(mustela::Val(key), mustela::Val(val), false) )
                        std::cout << "BAD put" << std::endl;
                    mirror[key] = val;
                }else{
                    if( !txn.del(mustela::Val(key), false) )
                        std::cout << "BAD del" << std::endl;
                    mirror.erase(key);
                }
            }
        for(auto && ma : mirror){
            mustela::Val value;
            bool result = txn.get(mustela::Val(ma.first), value);
            if( !result || ma.second != value.to_string())
                std::cout << "Bad check" << std::endl;
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
