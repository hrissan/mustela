#include "utils.hpp"

namespace mustela {
	unsigned char get_compact_size_sqlite4(uint64_t val){
		if (val <= 240)
			return 1;
		if (val <= 2287)
			return 2;
		if (val <= 67823)
			return 3;
		if (val <= 16777215)
			return 4;
		if (val <= 4294967295)
			return 5;
		if (val <= 1099511627775)
			return 6;
		if (val <= 281474976710655)
			return 7;
		if (val <= 72057594037927935)
			return 8;
		return 9;
	}
	unsigned char read_u64_sqlite4(uint64_t & val, const void * vptr){
		const unsigned char * ptr = (const unsigned char * )vptr;
		unsigned char a0 = *ptr;
		if (a0 <= 240) {
			val = a0;
			return 1;
		}
		if(a0 <= 248) {
			unsigned char a1 = *(ptr + 1);
			val = 240 + 256 * (a0 - 241) + a1;
			return 2;
		}
		if(a0 == 249) {
			const unsigned char * buf = ptr + 1;
			val = 2288 + 256 * buf[0] + buf[1];
			return 3;
		}
		const unsigned char * buf = ptr + 1;
		int bytes = 3 + a0 - 250;
		//if( bytes > 4 )
		//    throw std::runtime_error("read_u32_sqlite4 value does not fit");
		unpack_uint_be<uint64_t>(buf, bytes, val);
		return 1 + bytes;
	}
	unsigned char write_u64_sqlite4(uint64_t val, void * vptr){
		unsigned char * ptr = (unsigned char *)vptr;
		if (val <= 240) {
			*ptr = static_cast<unsigned char>(val);
			return 1;
		}
		if (val <= 2287) {
			*ptr = (val - 240)/256 + 241;
			*(ptr + 1) = static_cast<unsigned char>(val - 240);
			return 2;
		}
		if (val <= 67823) {
			*ptr = 249;
			*(ptr + 1) = (val - 2288)/256;
			*(ptr + 2) = static_cast<unsigned char>(val - 2288);
			return 3;
		}
		if (val <= 16777215) {
			*ptr = 250;
			pack_uint_be<uint32_t>(ptr + 1, 3, static_cast<uint32_t>(val));
			return 4;
		}
		if (val <= 4294967295) {
			*ptr = 251;
			pack_uint_be<uint64_t>(ptr + 1, 4, val);
			return 5;
		}
		if (val <= 1099511627775) {
			*ptr = 252;
			pack_uint_be<uint64_t>(ptr + 1, 5, val);
			return 6;
		}
		if (val <= 281474976710655) {
			*ptr = 253;
			pack_uint_be<uint64_t>(ptr + 1, 6, val);
			return 7;
		}
		if (val <= 72057594037927935) {
			*ptr = 254;
			pack_uint_be<uint64_t>(ptr + 1, 7, val);
			return 8;
		}
		*ptr = 255;
		pack_uint_be<uint64_t>(ptr + 1, 8, val);
		return 9;
	}

	// CRC-32C (iSCSI) polynomial in reversed bit order.
    // Castagnoli polynomial (same one as used by the Intel crc32 instruction)
#define POLY 0x82f63b78

// CRC-32 (Ethernet, ZIP, etc.) polynomial in reversed bit order.
// #define POLY 0xedb88320

	uint32_t crc32c(uint32_t crc, const unsigned char *buf, size_t len)
	{
		int k;

		crc = ~crc;
		while (len--) {
			crc ^= *buf++;
			for (k = 0; k < 8; k++)
				crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
		}
		return ~crc;
	}

	int Val::compare(const Val & other)const{
		size_t min_size = std::min(size, other.size);
		int cmp = memcmp(data, other.data, min_size);
		if( cmp != 0 )
			return cmp;
		return int(size) - int(other.size);
	}
//	size_t Val::encoded_size()const{
//		return get_compact_size_sqlite4(size) + size;
//	}
	
}
