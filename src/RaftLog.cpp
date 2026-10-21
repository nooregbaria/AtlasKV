#include "RaftLog.h"
#include <sys/fcntl.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <limits>
#include <vector>
#include <zlib.h>
#include <utility>
#include <filesystem>

constexpr uint64_t MAX_COMMAND_SIZE = 16 * 1024 * 1024;

enum class ReadResult {
    Success,
    EndOfFile,
    Error
};

bool sync_parent_directory(const std::string& path) {
    std::filesystem::path parent =
        std::filesystem::path(path).parent_path();

    if (parent.empty()) {
        parent = ".";
    }

    int dir_fd = ::open(parent.c_str(), O_RDONLY);
    if (dir_fd < 0) {
        return false;
    }

    int result;
    do {
        result = ::fsync(dir_fd);
    } while (result < 0 && errno == EINTR);

    bool success = (result == 0);

    if (::close(dir_fd) != 0) {
        success = false;
    }

    return success;
}

static ReadResult read_exact(int fd,unsigned char* buffer,size_t size) {
    size_t total = 0;

    while (total < size) {
        ssize_t n = read(fd, buffer + total, size - total);

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n < 0) {
            return ReadResult::Error;
        }

        if (n == 0) {
            return ReadResult::EndOfFile;
        }

        total += static_cast<size_t>(n);
    }

    return ReadResult::Success;
}

static void append_uint(std::vector<unsigned char>& buffer, uint64_t value, int byte_count){
    for(int i=byte_count-1 ;i >= 0; i--){
        buffer.push_back(  static_cast<unsigned char>((value >> (8 * i)) & 0xFF));
    }
}

static uint64_t read_uint(const unsigned char*  data, int byte_count){
    uint64_t result  = 0 ;
    for(int i = 0 ; i < byte_count ; i++){
        result = (result << 8) | data[i];
    }

    return  result;
}

RaftLog::RaftLog(const std::string& path): file_path(path) {}

uint64_t RaftLog::last_index() const{
    return entries.size();
}

uint64_t RaftLog::last_term() const{
    if(entries.empty()){
        return 0;
    }
    return entries.back().term;
}


// Log record: term (8) | command length (4) | command (N) | CRC32 (4).
// Integer fields are big-endian; the checksum covers the header and command.

bool RaftLog::append(const RaftLogEntry& entry){

    if (!storage_healthy) {
        return false;
    }


    if(entry.command.size() > MAX_COMMAND_SIZE){
        return false;
    }

    RaftLogEntry prepared_entry = entry;
    entries.reserve(entries.size() + 1);

    std::vector<unsigned char> buffer;
    append_uint(buffer, entry.term, 8);
    append_uint(buffer, static_cast<uint32_t>(entry.command.size()), 4);
    buffer.insert(buffer.end(), entry.command.begin(),  entry.command.end());

    if (buffer.size() > std::numeric_limits<uInt>::max()) {
        return false;
    }

    uLong crc  =  crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(buffer.size()));
    append_uint(buffer, static_cast<uint32_t>(crc), 4);

    int fd = open(file_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    
    if(fd < 0){
        storage_healthy = false;
        std::perror("Error Opening Raft Log");
        return false;
    }

    off_t original_size = lseek(fd, 0, SEEK_END);

    if(original_size == -1){
        storage_healthy = false;
        close(fd);
        return false;
    }

    size_t written = 0;
    while (written < buffer.size())
    {
        ssize_t n = write(fd, buffer.data()+written, buffer.size()-written);

        if(n<0 && errno == EINTR){
            continue;
        }

        if(n<=0){
            storage_healthy = false;
            close(fd);
            return false;
        }

        written += static_cast<size_t>(n);
    }

    if(fsync(fd) == -1){
        storage_healthy = false;
        close(fd);
        return false;
    }

    if(close(fd) == -1){
        storage_healthy = false;
        return false;
    }

    if(!sync_parent_directory(file_path)){
        storage_healthy =false;
        return false;
    }
    

    // Expose the entry to consensus only after the file and directory are synced.
    entries.push_back(std::move(prepared_entry));
    return true;
}



bool RaftLog::load(){
    int fd = open(file_path.c_str(), O_RDONLY);

    if(fd<0){
        if(errno == ENOENT){
            return true;
        }

        storage_healthy = false;
        return false;
    }
    std::vector<RaftLogEntry> recovered_entries;

    while (true)
    {
        unsigned char  header[12];

        off_t record_start = lseek(fd,0,SEEK_CUR);

        if(record_start == -1){
            storage_healthy = false;
            close(fd);
            return false;
        }

        ReadResult result = read_exact(fd, header , 12);

        if(result == ReadResult::Error){
            storage_healthy = false;
            close(fd);
            return false;
        }

        if(result ==  ReadResult::EndOfFile){
            off_t position = lseek(fd, 0, SEEK_CUR);

            if (position == record_start)
            {
                break;
            }

            storage_healthy= false;
            close(fd);
            return false;
        }

        uint64_t term = read_uint(header, 8);
        uint64_t commad_length = read_uint(header+8, 4);

        if(commad_length > MAX_COMMAND_SIZE){
            storage_healthy = false;
            close(fd);
            return false;
        }

        std::vector<unsigned char> command(commad_length);

        if(read_exact(fd, command.data(), command.size()) != ReadResult::Success){
            storage_healthy = false;
            close(fd);
            return false;
        }

        unsigned char checksum_bytes[4];
        if(read_exact(fd, checksum_bytes, 4) != ReadResult::Success){
            storage_healthy = false;
            close(fd);
            return false;
        }
        uint32_t  stored_crc = static_cast<uint32_t>(read_uint(checksum_bytes, 4));

        uLong calculated_crc = crc32(0L, Z_NULL, 0);
        calculated_crc = crc32(calculated_crc , reinterpret_cast<const Bytef*>(header), 12);
        // A null buffer resets zlib's CRC, even for a zero-length update.
        // Empty leader no-ops retain the checksum of their header.
        if (!command.empty()) {
            calculated_crc = crc32(calculated_crc, reinterpret_cast<const Bytef*>(command.data()), static_cast<uInt>(command.size()));
        }

        if(static_cast<uint32_t>(calculated_crc) !=stored_crc){
            storage_healthy = false;
            close(fd);
            return false;
        }

        std::string command_string (command.begin(), command.end());
        recovered_entries.push_back(RaftLogEntry{term , command_string});
    }

    if(close(fd) ==-1){
        storage_healthy = false;
        return false;
    }

    entries = std::move(recovered_entries);
    return true;
    
}


bool RaftLog::truncate_from(uint64_t index){

    if(!storage_healthy){
        return false;
    }

    if(index == 0 || index > entries.size() +1){
        return false;
    }

    if(index  == entries.size() + 1){
        return true;
    }

    uint64_t new_size =0;
    for(uint64_t i =0 ;i<index-1 ;i++){
        new_size +=  16 +entries[i].command.size();
    }


    int fd = open(file_path.c_str(), O_WRONLY);
    if(fd<0){
        storage_healthy = false;
        return false;
    }

    if (ftruncate(fd, static_cast<off_t>(new_size)) == -1) {
       storage_healthy = false;
       close(fd);
       return false;
    }

    if (fsync(fd) == -1) {
       storage_healthy = false;
       close(fd);
       return false;
    }

    if (close(fd) == -1) {
       storage_healthy = false;
       return false;
    }

    entries.resize(index-1);
    return true;
}


bool RaftLog::get_entry(uint64_t index, RaftLogEntry& result) const{
    if(index == 0 || index > entries.size()){
        return false;
    }

    result.command = entries[index-1].command;
    result.term = entries[index-1].term;
    return true;
}


