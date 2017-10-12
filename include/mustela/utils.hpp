#pragma once

#include <cstdint>
#include <string>

namespace mustela {

    template<class T>
void unpack_uint_be(const unsigned char * buf, unsigned si, T & val){
    T result = 0;
    switch(si)
    {
        case 8:
            result = buf[si - 8];
        case 7:
            result = (result << 8) + buf[si - 7];
        case 6:
            result = (result << 8) + buf[si - 6];
        case 5:
            result = (result << 8) + buf[si - 5];
        case 4:
            result = (result << 8) + buf[si - 4];
        case 3:
            result = (result << 8) + buf[si - 3];
        case 2:
            result = (result << 8) + buf[si - 2];
        case 1:
            result = (result << 8) + buf[si - 1];
        case 0:
            break;
    }
/*    for(unsigned i = 0; i != si; ++i ) {
        result <<= 8;
        result += buf[i];
    }*/
    val = result;
}
template<class T>
void unpack_uint_be(const char * buf, unsigned si, T & val){
    return unpack_uint_be((const unsigned char *)buf, si, val);
}
template<class T>
void pack_uint_be(unsigned char * buf, unsigned si, T val){
    for(unsigned i = si; i-- > 0; ) {
        buf[i] = static_cast<unsigned char>(val);
        val >>= 8;
    }
}
template<class T>
void pack_uint_be(char * buf, unsigned si, T val){
    return pack_uint_be((unsigned char *)buf, si, val);
}
unsigned char get_compact_size_sqlite4(uint64_t val);
unsigned char read_u64_sqlite4(uint64_t & val, const void * ptr);
unsigned char write_u64_sqlite4(uint64_t val, void * ptr);
class Exception {
public:
    explicit Exception(const std::string & what)
    {}
};
inline void ass(bool expr, std::string what){
    if( !expr )
        throw Exception(what);
}
struct MVal {
    char * data;
    size_t size;
    MVal(char * data, size_t size):data(data), size(size)
    {}
    char * end()const{ return data + size; }
};
struct Val {
    const char * data;
    size_t size;
    
    explicit Val():data(nullptr), size(0)
    {}
    explicit Val(const char * data, size_t size):data(data), size(size)
    {}
    explicit Val(const char * data):data(data), size(strlen(data))
    {}
    explicit Val(const std::string & str):data(str.data()), size(str.size())
    {}
    Val(const MVal & mval):data(mval.data), size(mval.size) // allow conversion
    {}
    const char * end()const{ return data + size; }
    
    std::string to_string()const{
        return std::string(data, size);
    }
    int compare(const Val & other)const;
    bool operator==(const Val & other)const{
        return compare(other) == 0;
    }
    bool operator!=(const Val & other)const{
        return !operator==(other);
    }
    bool operator<(const Val & other)const{
        return compare(other) < 0;
    }
    size_t encoded_size()const;
};

typedef uint64_t Tid;
typedef uint64_t Pid;
typedef uint16_t PageOffset;

constexpr uint32_t OUR_VERSION = 1;

constexpr uint64_t MAX_MAPPING_SIZE = 1ULL << 34; // 16GB for now
constexpr uint64_t META_MAGIC = 0x58616c657473754d; // MustelaX in binary form
constexpr uint64_t META_MAGIC_ALTENDIAN = 0x4d757374656c6158;
constexpr int NODE_PID_SIZE = 4; // 4 bytes to store page index will result in ~4 billion pages limit, or 16TB max for 4KB pyges
// fixed pid size allows simple logic when replacing page in node index
    struct ValPid {
        Val key;
        Pid pid;
        ValPid():pid(0) {}
        ValPid(Val key, Pid pid):key(key), pid(pid) {}
    };
    struct ValVal {
        Val key;
        Val value;
        ValVal() {}
        ValVal(Val key, Val value):key(key), value(value) {}
    };
}

