#include "os.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <chrono>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>

using namespace mustela;

// TODO - all syscalls on unix should check for EINTR???

os::File::File(const std::string & file_path, bool read_only):fd(open(file_path.c_str(), (read_only ? O_RDONLY : O_RDWR | O_CREAT), (mode_t)0600)){
	if( fd == -1)
		throw Exception("file open failed for {" + file_path + "}");
}
uint64_t os::File::get_size()const{
	auto was = lseek(fd, 0, SEEK_CUR);
	auto result = lseek(fd, 0, SEEK_END);
	auto was2 = lseek(fd, was, SEEK_SET);
//	if( result > 1000000000)
//		result = 1000000000;
	if( was < 0 || result < 0 || was2 < 0)
		throw Exception("getting file size error");
	return static_cast<uint64_t>(result);
}
void os::File::set_size(uint64_t new_fs){
//	if( new_fs > 1000000000)
//		new_fs = 1000000000;
	int result = 0;
	do{
		result = ftruncate(fd, static_cast<off_t>(new_fs));
 	}while(result < 0 && errno == 4);
	ass(result == 0, "failed to grow db file using ftruncate");
}
char * os::File::mmap(uint64_t offset, uint64_t size, bool read, bool write){
	void * wm = ::mmap(0, size, (read ? PROT_READ : 0) | (write ? PROT_WRITE : 0), MAP_SHARED, fd, static_cast<off_t>(offset));
	if (wm == MAP_FAILED)
		throw Exception("mmap PROT_READ | PROT_WRITE failed");
	return (char *)wm;
}
void os::File::munmap(char * addr, uint64_t size){
	::munmap(addr, size);
}
void os::File::msync(char * addr, uint64_t size){
	::msync(addr, size, MS_SYNC);
}

os::File::~File(){
	close(fd); fd = -1;
}

os::FileLock::FileLock(File & file):fd(file.fd){
	int result = 0;
	do{
 		result = flock(fd, LOCK_EX);
 	}while(result < 0 && errno == 4);
	ass(result == 0, "Failed to exclusively lock file");
}

os::FileLock::~FileLock(){
	int result = 0;
	do{
 		result = flock(fd, LOCK_EX);
 	}while(errno == 4);
	ass(result == 0, "Failed to exclusively lock file");
}

bool mustela::os::file_exists_on_readonly_partition(const std::string & file_path){
	int fd = open(file_path.c_str(), O_RDONLY, 0);
	if( fd == -1 )
		return false;
	struct statvfs buf;
	bool result = (fstatvfs(fd, &buf) != -1) && (buf.f_flag & ST_RDONLY);
	close(fd);
	return result;
}

size_t mustela::os::get_physical_page_size(){
	return static_cast<size_t>(sysconf(_SC_PAGESIZE));
}

size_t mustela::os::get_map_granularity(){
	// on Windows mmapped regions should be aligned to 65536
	return get_physical_page_size();
}
