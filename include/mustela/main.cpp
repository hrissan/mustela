// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <stdlib.h>
#include <map>
#include <random>
#include "mustela.hpp"

int main(int argc, char * argv[]){
    
    mustela::test_data_pages();
    
    mustela::DB db("/Users/hrissan/Documents/devbox/mustela/bin/test.mustella");
//    mustela::DB db("test.mustella");
    mustela::TX txn(db);
/*    txn.put(mustela::Val("A"), mustela::Val("AVAL"), true);
    txn.put(mustela::Val("B"), mustela::Val("BVAL"), true);
    txn.put(mustela::Val("C"), mustela::Val("CVAL"), true);

    txn.commit();*/

    const bool insert = false;
    std::map<std::string, std::string> mirror;
    std::default_random_engine e;
    std::uniform_int_distribution<int> dist(0, 999999);
    for(int i = 1000000; i-- > 0; ){
//    for(int i = 0; i != 10000000; ++i){
//        std::cout << std::endl << "print_db i=" << i << std::endl << std::endl;
//        std::string json = txn.print_db();
//        std::cout << json << std::endl;
        int j = i;//dist(e);
        std::string key = std::to_string(j);
        std::string val = "value" + std::to_string(j);

        mustela::Val got;
        if( insert ){
            if( txn.put(mustela::Val(key), mustela::Val(val), true) )
            {
                mirror[key] = val;
                continue;
            }
        }else{
            bool was = txn.get(mustela::Val(key), got);
            if( was && !txn.del(mustela::Val(key), true) ){
                std::cout << "del failed" << std::endl;
                //            mirror.erase(key);
                continue;
            }
            if( txn.get(mustela::Val(key), got) )
                std::cout << "after del key is still there" << std::endl;
        }
    }
/*    txn.del(mustela::Val("A"), true);
    txn.del(mustela::Val("B"), true);
    txn.del(mustela::Val("C"), true);*/
    std::cout << "Mirror" << std::endl;
    for(auto && ma : mirror){
        mustela::Val value;
        bool result = txn.get(mustela::Val(ma.first), value);
        if( ma.second != value.to_string())
            std::cout << "Bad " << ma.first << ":" << ma.second << " in db result=" << int(result) << " value=" << value.to_string() << std::endl;
    }
    if( !insert){
        std::string json = txn.print_db();
        std::cout << json << std::endl;
    }
    txn.commit();
/*    std::cout << "Page" << std::endl;
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
