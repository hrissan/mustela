#include "lock.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <chrono>

using namespace mustela;

constexpr uint64_t READ_TX_INTERVAL = 60; // 1 minutes

// TODO - probably move to CAS operations

static uint64_t steady_now(){
	auto now = std::chrono::steady_clock::now();
	auto epoch = now.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    return static_cast<uint64_t>(value.count());
}

uint64_t pack_rid(uint64_t reader_id, uint64_t deadline){
	return (reader_id << 32) + (deadline & 0xFFFFFFFF);
}

void unpack_rid(uint64_t rid, uint64_t * reader_id, uint64_t * deadline){
	*reader_id = rid >> 32;
	*deadline = rid & 0xFFFFFFFF;
}

ReaderTable::ReaderTable()
{}

ReaderTable::~ReaderTable()
{
	if(mapping_size){
		munmap(slots, mapping_size);
		mapping_size = 0;
		slots = nullptr;
	}
}

ReaderSlotDesc ReaderTable::create_reader_slot(Tid tid, os::File & lock_file, size_t granularity)
{
	if(!slots)
		grow_reader_table(lock_file, granularity, false);
	const auto now = steady_now();
	ReaderSlotDesc result;
	result.rid = __sync_fetch_and_add(&slots[0].ridead, 1); // we use slot 0 as reader counter
	result.deadline = now + READ_TX_INTERVAL;
	uint64_t ridead = pack_rid(result.rid, result.deadline);
	while(true){
		auto count = mapping_size/sizeof(ReaderSlot);
		size_t shift = static_cast<size_t>(rand());
		for(size_t i = 0; i < count; ++i){
			size_t s = (i + shift) % count;
			if( s == 0 )
				continue;
			uint64_t other_ridead = (const volatile uint64_t &)(slots[s].ridead);
			uint64_t other_rid;
			uint64_t other_deadline;
			unpack_rid(other_ridead, &other_rid, &other_deadline);
			if( other_deadline < now ){ // also covers empty slot
				if( __sync_bool_compare_and_swap(&slots[s].ridead, other_ridead, ridead) ){ // grabbed slot
					// If we are preempted here and resumed after half an hour, we might write our now very old tid over other reader newer tid
					(volatile uint64_t &)(slots[s].tid) = tid;
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

bool ReaderTable::update_reader_slot(ReaderSlotDesc & slot)
{
	auto now = steady_now();
	auto count = mapping_size/sizeof(ReaderSlot);
	if( slot.slot == 0 || slot.slot >= count )
		return false; // logic error
	uint64_t ridead = pack_rid(slot.rid, slot.deadline);
	uint64_t new_deadline = now + READ_TX_INTERVAL;
	uint64_t next_ridead = pack_rid(slot.rid, new_deadline);
	return __sync_bool_compare_and_swap(&slots[slot.slot].ridead, ridead, next_ridead);
}

void ReaderTable::release_reader_slot(const ReaderSlotDesc & slot){
	auto count = mapping_size/sizeof(ReaderSlot);
	if( slot.slot == 0 || slot.slot >= count )
		return;
	uint64_t ridead = pack_rid(slot.rid, slot.deadline);
	__sync_bool_compare_and_swap(&slots[slot.slot].ridead, ridead, 0);
}

Tid ReaderTable::find_oldest_tid(Tid writer_tid, os::File & lock_file, size_t granularity)
{
	grow_reader_table(lock_file, granularity, false);
	auto now = steady_now();
	auto count = mapping_size/sizeof(ReaderSlot);
	for(size_t i = 1; i < count; ++i) {
		uint64_t other_ridead = (const volatile uint64_t &)(slots[i].ridead);
		uint64_t other_rid;
		uint64_t other_deadline;
		unpack_rid(other_ridead, &other_rid, &other_deadline);
		if( other_deadline >= now ){
			Tid tid = (const volatile Tid &)(slots[i].tid);
			writer_tid = std::min(writer_tid, tid);
		}
	}
	return writer_tid;
}

void ReaderTable::grow_reader_table(os::File & lock_file, size_t granularity, bool grow_file){
	uint64_t old_file_size = lock_file.get_size();
	if(mapping_size != 0 && old_file_size == mapping_size && !grow_file)
		return;
	if(mapping_size){
		munmap(slots, mapping_size);
		mapping_size = 0;
		slots = nullptr;
	}
	uint64_t file_size = old_file_size;
	if(grow_file || file_size == 0){ // If not, it was resized by another reader, we just create new mapping
		file_size = os::grow_to_granularity(file_size + granularity, granularity);
		lock_file.set_size(file_size);
	}
	slots = (ReaderSlot *)lock_file.mmap(0, file_size, true, true);
	mapping_size = file_size;
}
