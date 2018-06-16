#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <iostream>
#include <vector>
#include "defs.hpp"

namespace mustela {

	template<class T>
	void unpack_uint_be(const unsigned char * buf, size_t si, T & val){
		T result = 0;
		for(size_t i = 0; i != si; ++i)
			result = (result << 8) + buf[i];
		val = result;
	}
	template<class T>
	void pack_uint_be(unsigned char * buf, size_t si, T val){
		for(size_t i = si; i-- > 0; ) {
			buf[i] = static_cast<unsigned char>(val);
			val >>= 8;
		}
	}
	template<class T>
	size_t unpack_uint_le(const unsigned char * buf, size_t si, T & val){
		T result = 0;
		for(size_t i = si; i-- > 0; )
			result = (result << 8) + buf[i];
		val = result;
		return si;
	}
	template<class T>
	size_t unpack_uint_le(const char * buf, size_t si, T & val){
		return unpack_uint_le((const unsigned char *)buf, si, val);
	}
	template<class T>
	size_t pack_uint_le(unsigned char * buf, size_t si, T val){
		for(size_t i = 0; i != si; ++i) {
			buf[i] = static_cast<unsigned char>(val);
			val >>= 8;
		}
		return si;
	}
	template<class T>
	size_t pack_uint_le(char * buf, size_t si, T val){
		return pack_uint_le((unsigned char *)buf, si, val);
	}
	size_t get_compact_size_sqlite4(uint64_t val);
	inline size_t get_max_compact_size_sqlite4() { return 9; }
	size_t read_u64_sqlite4(uint64_t & val, const void * ptr);
	size_t write_u64_sqlite4(uint64_t val, void * ptr);

	uint32_t crc32c(uint32_t crc, const unsigned char *buf, size_t len);
	inline uint32_t crc32c(uint32_t crc, const void *buf, size_t len){
		return crc32c(crc, reinterpret_cast<const unsigned char *>(buf), len);
	}
	struct Random {
		explicit Random(uint64_t random_seed = 0):random_seed(random_seed)
		{}
		uint64_t rand() { // MMIX by Donald Knuth
			random_seed = 6364136223846793005 * random_seed + 1442695040888963407;
			return random_seed;
		}
	private:
		uint64_t random_seed;
	};
	class Exception {
	public:
		explicit Exception(const std::string & what)
		{}
		static void th(const std::string & what){
			std::cout << "throw Exception " << what << std::endl;
			throw Exception(what);
		}
	};
#define ass(expr, what) do{ if(!(expr)) mustela::do_assert(#expr, __FILE__, __LINE__, what); }while(0)
#define ass2(expr, what, expr2) do{ if((expr2) && !(expr)) mustela::do_assert(#expr, __FILE__, __LINE__, what); }while(0)
	inline void do_assert(const char * expr, const char* file, int line, const char * what){
		std::cerr << "ass " << expr << " " << file << ":" << line << " " << what << std::endl;
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
		explicit Val(const unsigned char * data, size_t size):data(reinterpret_cast<const char *>(data)), size(size)
		{}
		explicit Val(const char * data):data(data), size(strlen(data))
		{}
		explicit Val(const std::string & str):data(str.data()), size(str.size())
		{}
		explicit Val(const std::vector<uint8_t> & buf):data(reinterpret_cast<const char*>(buf.data())), size(buf.size())
		{}
		Val(const MVal & mval):data(mval.data), size(mval.size) // allow conversion
		{}
		const char * end()const{ return data + size; }
		const unsigned char * udata()const{ return (const unsigned char *)data; }
		bool empty()const{ return size == 0; }
		
		std::string to_string()const{
			return std::string(data, size);
		}
		int compare(const Val & other)const{
			size_t min_size = size < other.size ? size : other.size;
			int cmp = memcmp(data, other.data, min_size);
			if( cmp != 0 )
				return cmp;
			return int(size) - int(other.size);
		}
		bool operator==(const Val & other)const{
			return size == other.size && memcmp(data, other.data, size) == 0;
		}
		bool operator!=(const Val & other)const{
			return !(*this == other);
		}
		bool operator<(const Val & other)const{
			return compare(other) < 0;
		}
		bool operator<=(const Val & other)const{
			return compare(other) <= 0;
		}
		bool operator>(const Val & other)const{
			return compare(other) > 0;
		}
		bool operator>=(const Val & other)const{
			return compare(other) >= 0;
		}
		bool has_prefix(const Val & prefix)const {
			return size >= prefix.size && memcmp(data, prefix.data, prefix.size) == 0;
		}
		bool has_prefix(const Val & prefix, Val * tail)const {
			if( !has_prefix(prefix) )
				return false;
			*tail = Val(data + prefix.size, size - prefix.size);
			return true;
		}
	};
	
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
	
    template<class T>
    class IntrusiveNode {
    public:
        IntrusiveNode():next(nullptr), prev(nullptr) {}
        // list ops
        void insert_after_this(T * node, IntrusiveNode<T> T::*Link){
            (node->*Link).next = this->next;
            (node->*Link).prev = this;
            if( this->next )
                (this->next->*Link).prev = &(node->*Link);
            this->next = node;
        }
        void unlink(IntrusiveNode<T> T::*Link){
            if( !prev )
                return;
            prev->next = next;
            if( next )
                (next->*Link).prev = prev;
            next = nullptr;
            prev = nullptr;
        }
        // iterator ops
        T * get_current() { return next; }
        IntrusiveNode<T> * get_next(IntrusiveNode<T> T::*Link) { return &(next->*Link); }
        bool is_end()const { return next == nullptr; }
    private:
        T * next;
        IntrusiveNode<T> * prev;

        IntrusiveNode(const IntrusiveNode<T> &)=delete;
        IntrusiveNode &operator=(const IntrusiveNode<T> &)=delete;
    };
}

