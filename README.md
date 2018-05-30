# mustela
Mustela


# testing

- page size related
- file size related
- reader gets pristine
- cursors are valid
- reopen db
- variable number of stuff in tx

# api

- creating buckets in ctor is wrong
- tx rollback how?

#9  0x00005555555594ea in mustela::ass (expr=false, what="Some TX still exist while in DB::~DB") at /home/user/dev/mustela/include/mustela/utils.hpp:71
#10 0x0000555555562a92 in mustela::DB::~DB (this=0x7fffffffdb00, __in_chrg=<optimized out>) at /home/user/dev/mustela/include/mustela/db.cpp:63
