#include "lock.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <chrono>

using namespace mustela;

uint32_t ReaderTable::now(){
	timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return uint32_t(ts.tv_sec);
//	return uint32_t(std::chrono::seconds(std::time(NULL)).count());
//	return uint32_t(__rdtsc() >> 32);
//    uint32_t lo, hi;
//    __asm__ __volatile__ (
//      "xorl %%eax, %%eax\n"
//      "cpuid\n"
//      "rdtsc\n"
//      : "=a" (lo), "=d" (hi)
//      :
//      : "%ebx", "%ecx" );
//    return hi;//(uint64_t)hi << 32 | lo;
}

uint64_t pack_rid(uint64_t reader_id, uint32_t deadline){
	return (reader_id << 32) + deadline;
}

void unpack_rid(uint64_t ridead, uint64_t * reader_id, uint32_t * deadline){
	*reader_id = ridead >> 32;
	*deadline = static_cast<uint32_t>(ridead);
}

ReaderTable::ReaderTable():random(now())
{}

ReaderTable::~ReaderTable()
{
	free_mapping();
}

ReaderSlotDesc ReaderTable::create_reader_slot(Tid tid, uint32_t reader_timeout_seconds, os::File & lock_file, size_t granularity)
{
	if(!slots)
		grow_reader_table(lock_file, granularity, false);
	ReaderSlotDesc result;
	result.rid = __sync_fetch_and_add(&header->next_reader, 1);
	result.now = now();
	result.deadline = result.now + reader_timeout_seconds;
	uint64_t ridead = pack_rid(result.rid, result.deadline);
	while(true){
		for(size_t i = 0; i != 2 * slots_count; ++i){ // first pass random, second one - full scan
			size_t shift = static_cast<size_t>(random.rnd());
			size_t s = (i >= slots_count) ? (i - slots_count) : (i + shift) % slots_count;
			uint64_t other_ridead = slots[s].ridead;
			uint64_t other_rid;
			uint32_t other_deadline;
			unpack_rid(other_ridead, &other_rid, &other_deadline);
			if( other_deadline < result.now ){ // also covers empty slot
				if( __sync_bool_compare_and_swap(&slots[s].ridead, other_ridead, ridead) ){ // grabbed slot
					// If we are preempted here and resumed after half an hour, we might write our now very old tid over other reader newer tid
					slots[s].tid = tid;
					// This will not corrupt DB, just prevent reclamation of some pages until that slot is freed or slot deadline passes
					result.slot = s;
	//				std::cerr << "Grabbed slot=" << i << " tid=" << slots[i].tid << " rand=" << result.rand0 << result.rand1 << std::endl;
					return result;
				}
			}
		}
		grow_reader_table(lock_file, granularity, true);
	}
}

bool ReaderTable::update_reader_slot(ReaderSlotDesc & slot, uint32_t now, uint32_t reader_timeout_seconds)
{
	if( slot.slot >= slots_count )
		return false; // logic error
	uint64_t ridead = pack_rid(slot.rid, slot.deadline);
	uint32_t new_deadline = now + reader_timeout_seconds;
	uint64_t next_ridead = pack_rid(slot.rid, new_deadline);
	if( !__sync_bool_compare_and_swap(&slots[slot.slot].ridead, ridead, next_ridead))
		return false;
	slot.now = now;
	slot.deadline = new_deadline;
	return true;
}

void ReaderTable::release_reader_slot(const ReaderSlotDesc & slot){
	if( slot.slot >= slots_count )
		return;
	uint64_t ridead = pack_rid(slot.rid, slot.deadline);
	__sync_bool_compare_and_swap(&slots[slot.slot].ridead, ridead, 0);
}

Tid ReaderTable::find_oldest_tid(Tid writer_tid, os::File & lock_file, size_t granularity)
{
	grow_reader_table(lock_file, granularity, false);
	const auto now_time = now();
	for(size_t i = 0; i != slots_count; ++i) {
		uint64_t other_ridead = (const volatile uint64_t &)(slots[i].ridead);
		uint64_t other_rid;
		uint32_t other_deadline;
		unpack_rid(other_ridead, &other_rid, &other_deadline);
		if( other_deadline >= now_time ){
			Tid tid = (const volatile Tid &)(slots[i].tid);
			writer_tid = std::min(writer_tid, tid);
		}else if(other_ridead != 0){
			if( !__sync_bool_compare_and_swap(&slots[i].ridead, other_ridead, 0) ){
				i -= 1; // failed to release slot, need to check it again
				continue;
			}
		}
	}
	return writer_tid;
}

void ReaderTable::free_mapping(){
	if(mapping){
		munmap(mapping, mapping_size);
		mapping = nullptr;
		mapping_size = 0;
		slots = nullptr;
		slots_count = 0;
		header = nullptr;
	}
}

void ReaderTable::grow_reader_table(os::File & lock_file, size_t granularity, bool grow_file){
	uint64_t old_file_size = lock_file.get_size();
	if(mapping_size != 0 && old_file_size == mapping_size && !grow_file)
		return;
	free_mapping();
	uint64_t file_size = old_file_size;
	if(grow_file || file_size == 0){ // If not, it was resized by another reader, we just create new mapping
		if(granularity < 4096)
			granularity = 4096; // Do not grow too slowly or too fast
		file_size = os::grow_to_granularity(file_size + granularity, granularity);
		lock_file.set_size(file_size);
	}
	mapping = lock_file.mmap(0, file_size, true, true);
	mapping_size = file_size;
	slots = (volatile ReaderSlot *)mapping + 1;
	slots_count = (mapping_size - sizeof(ReaderSlot)) / sizeof(ReaderSlot);
	header = (volatile LockFileHeader *)mapping;
	header->magic = META_MAGIC;
}
