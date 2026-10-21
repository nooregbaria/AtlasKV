#include "RaftStorage.h"

#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cerrno>
#include <cstdint>
#include <vector>
#include <zlib.h>
#include <filesystem>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include "io_utils.h"
// Persistent state: "RAFT" (4) | term (8) | voted_for (4) | CRC32 (4).
// Integer fields use little-endian encoding; voted_for = -1 means no vote.

RaftStorage::RaftStorage(const std::string& paht):file_path(paht){}

static void append_little_endian(std::vector<unsigned char>& bytes, std::uint64_t value, unsigned byte_count){
    for (unsigned i = 0; i < byte_count; i++)
    {
        unsigned char next_byte = value & 0xff;
        bytes.push_back(next_byte);
        value >>= 8;
    }
    
}

uint64_t read_little_indian (const std::vector<unsigned char>& bytes, size_t offset,  unsigned byte_count){
    uint64_t value = 0;

    for(unsigned  i=0 ; i<byte_count ; i++){
        value |= static_cast<uint64_t>(bytes[offset+i])  << (8*i);
    }

    return value;
}

bool RaftStorage::save(const PersistentRaftState& state){
    std::string  tmp_path = file_path + ".tmp";

    int tmp_fd = open(tmp_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0600);
    if(tmp_fd < 0){
        std::perror("Error Openning Rafe State");
        return false;
    }

    std::vector<unsigned char> bytes{'R','A','F','T'};
    bytes.reserve(20);

    append_little_endian(bytes, state.current_term,8);
    append_little_endian(bytes, static_cast<std::uint32_t>(state.voted_for),4);

    const auto checksum = static_cast<std::uint32_t>(crc32(0L, bytes.data(), static_cast<uInt>(bytes.size())));
    append_little_endian(bytes, checksum, 4);

    bool write_result = write_all(tmp_fd, bytes.data(), bytes.size());
    if(!write_result){
        std::perror("Error writing Raft State");
        close(tmp_fd);
        unlink(tmp_path.c_str());
        return false;
    }

    int sync_result;

    do{
        sync_result = fsync(tmp_fd);
    }while (sync_result < 0 && errno == EINTR);

    bool sync_success = (sync_result == 0);
    bool close_success = (close(tmp_fd)==0);

    if(!sync_success || !close_success){
        unlink(tmp_path.c_str());
        return false;
    }


    // Replace the state only after syncing the temporary file, then sync its directory.
    if(rename(tmp_path.c_str(), file_path.c_str()) != 0 ){
        unlink(tmp_path.c_str());
        return false;
    }

    std::filesystem::path parent = std::filesystem::path(file_path).parent_path();

    if(parent.empty()){
        parent = "."; // A bare filename belongs to the current directory.
    }

    int dir_fd = open(parent.c_str(), O_RDONLY|O_DIRECTORY);
    if(dir_fd < 0){
        return false;
    }

    int result;
    do{
        result = fsync(dir_fd);
    }while ((result < 0 && errno == EINTR));

    bool success = (result==0);

    if(close(dir_fd) != 0){
        success = false;
    }
    
    return success;
}


struct stat info{};
LoadStatus RaftStorage::load(PersistentRaftState& state){
    

    int fd = open(file_path.c_str(), O_RDONLY);

    if(fd<0){
        if(errno == ENOENT){
            return LoadStatus::NotFound;
        }
        return LoadStatus::Error;
    }

    if(fstat(fd, &info) == -1  || info.st_size != 20){
        close(fd);
        return LoadStatus::Error;
    }

    std::vector<unsigned char> bytes(20);
    std::size_t total_read = 0;

    while(total_read < 20){
        ssize_t bytes_read = read(fd, bytes.data() +total_read , bytes.size() - total_read);
        if(bytes_read <=0){
            if(bytes_read == -1 && errno == EINTR){
                continue;
            }
            close(fd);
            return LoadStatus::Error;
        }

        total_read  += bytes_read;
    }
    if(close(fd) != 0){
        return LoadStatus::Error;
    }

    if(std::memcmp(bytes.data(),  "RAFT" , 4) != 0){
        return LoadStatus::Error;
    }
     
    uint64_t term = read_little_indian(bytes, 4,8);
    uint32_t vote = static_cast<std::uint32_t>(read_little_indian(bytes, 12, 4));
    uint32_t crc  = static_cast<std::uint32_t>(read_little_indian(bytes,16, 4));

    if(crc != crc32(0L, bytes.data(), 16)){
        return LoadStatus::Error;
    }

    int32_t sighned_vote;

    if(vote <= INT32_MAX){
        sighned_vote = static_cast<int32_t>(vote);
    }else{
        sighned_vote = static_cast<int32_t>(static_cast<int64_t>(vote)  - (1LL << 32));
    }
    state.current_term = term;
    state.voted_for = sighned_vote;

    return LoadStatus::Success;
}