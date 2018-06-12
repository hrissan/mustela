#include "lock.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <chrono>

using namespace mustela;

constexpr uint64_t READ_TX_INTERVAL = 1000000 * 10 * 60; // 10 minutes

// TODO - probably move to CAS operations

static uint64_t steady_now(){
	auto now = std::chrono::steady_clock::now();
	auto epoch = now.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::microseconds>(epoch);
    return static_cast<uint64_t>(value.count());
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
	auto now = steady_now();
	ReaderSlotDesc result;
	result.rand0 = (uint64_t(rand()) << 32) + uint64_t(rand()); // TODO - better rand
	result.rand1 = (uint64_t(rand()) << 32) + uint64_t(rand()); // TODO - better rand
	while(true){
		auto count = mapping_size/sizeof(ReaderSlot);
		for(size_t i = 0; i != count; ++i){
			if( slots[i].deadline < now ){
				slots[i].deadline = now + READ_TX_INTERVAL;
				slots[i].rand0 = result.rand0;
				slots[i].rand1 = result.rand1;
				slots[i].tid = tid;
				result.slot = i;
//				std::cerr << "Grabbed slot=" << i << " tid=" << slots[i].tid << " rand=" << result.rand0 << result.rand1 << std::endl;
				return result;
			}
		}
		if(mapping_size){
			munmap(slots, mapping_size);
			mapping_size = 0;
			slots = nullptr;
		}
		uint64_t old_file_size = lock_file.get_size();
		uint64_t new_fs = os::grow_to_granularity(old_file_size + granularity, granularity);
		lock_file.set_size(new_fs);
		slots = (ReaderSlot *)lock_file.mmap(0, new_fs, true, true);
		memset((char *)slots + old_file_size, 0, new_fs - old_file_size);
		mapping_size = new_fs;
	}
}

void ReaderTable::update_reader_slot(const ReaderSlotDesc & slot)
{
	auto now = steady_now();
	auto count = mapping_size/sizeof(ReaderSlot);
	if( slot.slot >= count )
		return;
	if(slots[slot.slot].rand0 != slot.rand0 || slots[slot.slot].rand1 != slot.rand1)
		return;
	slots[slot.slot].deadline = now + READ_TX_INTERVAL;
}

void ReaderTable::release_reader_slot(const ReaderSlotDesc & slot){
	auto count = mapping_size/sizeof(ReaderSlot);
	if( slot.slot >= count )
		return;
	if(slots[slot.slot].rand0 != slot.rand0 || slots[slot.slot].rand1 != slot.rand1) {
//		std::cerr << "Stalled slot=" << slot.slot << " tid=" << slots[slot.slot].tid << std::endl;
		return;
	}
//	std::cerr << "Released slot=" << slot.slot << " tid=" << slots[slot.slot].tid << " rand=" << slots[slot.slot].rand0 << slots[slot.slot].rand1 << std::endl;
	slots[slot.slot].deadline = 0;
}

Tid ReaderTable::find_oldest_tid(Tid writer_tid)
{
	auto now = steady_now();
	auto count = mapping_size/sizeof(ReaderSlot);
	for(size_t i = 0; i != count; ++i) {
		if (slots[i].deadline >= now) {
//			std::cerr << "Active slot=" << i << " tid=" << slots[i].tid << " rand=" << slots[i].rand0 << slots[i].rand1 << std::endl;
			writer_tid = std::min(writer_tid, slots[i].tid);
		}
	}
	return writer_tid;
}
