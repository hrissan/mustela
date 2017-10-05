// Good visualizer for B-trees. print_db outputs in appropriate format :)
// http://ysangkok.github.io/js-clrs-btree/btree.html

#include <iostream>
#include <stdlib.h>
#include <map>
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

    std::map<std::string, std::string> mirror;
    for(int i = 0; i != 1000000; ++i){
        //std::cout << std::endl << "print_db i=" << i << std::endl << std::endl;
        //std::string json = txn.print_db();
        //std::cout << json << std::endl;
//        char keybuf[32];
//        char valbuf[32];
//        sprintf(keybuf, "%d", i);
//        sprintf(valbuf, "value%d", i);
        std::string key = std::to_string(i);
        std::string val = "value" + std::to_string(i);
//        if( !txn.put(mustela::Val(key), mustela::Val(val), true) )
//        mustela::Val got;
//        if( !txn.get(mustela::Val(key), got) )
//        {
            //mirror[key] = val;
//            continue;
//        }
        if( !txn.del(mustela::Val(key), true) ){
//            mirror.erase(key);
            continue;
        }
    }
/*    txn.del(mustela::Val("A"), true);
    txn.del(mustela::Val("B"), true);
    txn.del(mustela::Val("C"), true);
    std::cout << "Mirror" << std::endl;
    for(auto && ma : mirror){
        mustela::Val value;
        bool result = txn.get(mustela::Val(ma.first), value);
        std::cout << ma.first << ":" << ma.second << " in db result=" << int(result) << " value=" << value.to_string() << std::endl;
    }*/
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
