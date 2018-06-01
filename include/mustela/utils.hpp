#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <iostream>

namespace mustela {

	// TODO - use LE by default
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

	uint32_t crc32c(uint32_t crc, const unsigned char *buf, size_t len);
	inline uint32_t crc32c(uint32_t crc, const void *buf, size_t len){
		return crc32c(crc, reinterpret_cast<const unsigned char *>(buf), len);
	}

	class Exception {
	public:
		explicit Exception(const std::string & what)
		{}
	};

#define ass(expr, what) do_assert(expr, __FILE__, __LINE__, what)
// do not use std::string for what - will be constructed every time, even when expr is true
	inline void do_assert(bool expr, const char* file, int line, const char * what){
		if( !expr ) {
			std::cerr << file << ":" << line << ": " << what << std::endl;
			throw Exception(what);
		}
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
		Val(const MVal & mval):data(mval.data), size(mval.size) // allow conversion
		{}
		const char * end()const{ return data + size; }
		bool empty()const{ return size == 0; }
		
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
		bool has_prefix(const Val & prefix)const {
			return size >= prefix.size && memcmp(data, prefix.data, prefix.size) == 0;
		}
		bool has_prefix(const Val & prefix, Val & tail)const {
			if( !has_prefix(prefix) )
				return false;
			tail = Val(data + prefix.size, size - prefix.size);
			return true;
		}
	};
	
	typedef uint64_t Tid;
	typedef uint64_t Pid;
	typedef uint16_t PageOffset;
	typedef int16_t PageIndex; // we use -1 to indicate special left value in nodes

	constexpr uint32_t OUR_VERSION = 1;
	
	constexpr uint64_t META_MAGIC = 0x58616c657473754d; // MustelaX in LE
//	constexpr uint64_t META_MAGIC_ALTENDIAN = 0x4d757374656c6158;
	constexpr int META_PAGES_COUNT = 3; // We might end up using 2 like lmdb
	constexpr int NODE_PID_SIZE = 5;
	constexpr size_t MIN_PAGE_SIZE = 128;
	constexpr size_t GOOD_PAGE_SIZE = 4096;
	constexpr size_t MAX_PAGE_SIZE = 1 << 8*sizeof(PageOffset);
	// 4 bytes to store page index will result in ~4 billion pages limit, or 16TB max for 4KB pages
	// TODO - move NODE_PID_SIZE into MetaPage
	constexpr int MAX_DEPTH = 40; // TODO - calculate from NODE_PID_SIZE, use for Cursor::path
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

