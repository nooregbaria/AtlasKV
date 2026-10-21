#include "RaftApplyLog.h"
#include <zlib.h>
#include "io_utils.h"
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <mutex>
#include <fstream>
#include <filesystem>
#include <limits>
#include <utility>

namespace {

bool truncate_and_sync(
    const std::string& path,
    std::uint64_t valid_bytes
) {
    int fd;
    do {
        fd = ::open(path.c_str(), O_WRONLY);
    } while (fd < 0 && errno == EINTR);

    if (fd < 0) {
        return false;
    }

    bool success = true;

    if (::ftruncate(fd, static_cast<off_t>(valid_bytes)) != 0) {
        success = false;
    }

    if (success) {
        int result;
        do {
            result = ::fsync(fd);
        } while (result < 0 && errno == EINTR);

        if (result != 0) {
            success = false;
        }
    }

    if (::close(fd) != 0) {
        success = false;
    }

    return success;
}

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

} // namespace

RaftApplyLog::RaftApplyLog(const std::string& path)
    : log_path(path) {}

std::uint64_t RaftApplyLog::get_last_index() const {
    std::lock_guard<std::mutex> lock(mutex_log);
    return last_index;
}

bool RaftApplyLog::append(std::uint64_t index, const std::string& command) {
    std::lock_guard<std::mutex> lock(mutex_log);
    
    if(recovery_required){
        return false;
    }
     
    if (last_index == std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }

    if (index != last_index + 1) {
        return false;
    }

    // The command length must fit in a 32-bit field.
    if (command.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    std::vector<char> record;
    std::uint32_t command_size = static_cast<std::uint32_t>(command.size());
    for(int i = 7 ; i >= 0 ; i--){
        record.push_back(static_cast<char>( (index >> (i*8)) & 0xFF ));
    }

    for(int i = 3 ; i >= 0; i--){
        record.push_back(static_cast<char>(  (command_size >> (i*8)) & 0xFF));
    }

    record.insert(record.end(), command.begin() , command.end());

    if(record.size() > std::numeric_limits<uInt>::max()){
        return false;
    }

    std::uint32_t checksum = static_cast<std::uint32_t>( crc32(0L, reinterpret_cast<const Bytef*>(record.data()),static_cast<uInt>(record.size()) ));
    for(int i = 3 ; i >= 0; i--){
        record.push_back(static_cast<char>(  (checksum >> (i*8)) & 0xFF));
    }
    
    int fd;
    do{
        fd =:: open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND  , 0666);
    }while (fd < 0 && errno == EINTR);

    if(fd < 0){
        std::perror("Error opening Raft application log");
        return false;
    }
    
    recovery_required = true;

    bool success = write_all(fd, record.data(), record.size());
    if(!success){
        std::perror("Error writing Raft application log");
    }else{
        int result;
        do{
            result =:: fsync(fd);
        }while(result < 0 && errno == EINTR);

        if(result < 0){
            std::perror("Error syncing Raft application log");
            success = false;
        }
    }

    if(close(fd) != 0){
        std::perror("Error closing Raft application log");
        success = false;
    }

    if(success){
        success = sync_parent_directory(log_path);
    }

    if(success){
        last_index = index;
        recovery_required = false;
    }

    return success;
}

//┌────────────┬──────────────┬──────────────┬──────────┐
//│ Raft index │ Command size │ Command      │ CRC32    │
//│ 8 bytes    │ 4 bytes      │ N bytes      │ 4 bytes  │
//└────────────┴──────────────┴──────────────┴──────────┘

bool RaftApplyLog::recover(std::vector<AppliedRaftEntry>& entries) {
    std::lock_guard<std::mutex> lock(mutex_log);

    // Never expose partially recovered entries.
    std::vector<AppliedRaftEntry> recovered_entries;

    std::ifstream file(log_path, std::ios::binary | std::ios::ate);

    if (!file.is_open()) {
        std::error_code ec;
        bool exists = std::filesystem::exists(log_path, ec);

        if (!ec && !exists && last_index == 0 && !recovery_required) {
            entries.clear();
            return true; // Brand-new journal.
        }

        recovery_required = true;
        return false;
    }

    recovery_required = true;
    std::streamoff remaining = file.tellg();
    if (remaining < 0 || !file.seekg(0, std::ios::beg)) {
        return false;
    }

    const auto decode_big_endian = [](const unsigned char* bytes, std::size_t size) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < size; ++i) {
            value = (value << 8) | bytes[i];
        }
        return value;
    };

    std::uint64_t valid_bytes = 0;
    std::uint64_t recovered_index = 0;
    while (remaining > 0) {
        unsigned char header[12];
        // A shorter tail cannot contain even an empty record; retain the valid prefix.
        if (remaining < 16) {
            file.close();

        if (!truncate_and_sync(log_path, valid_bytes)) {
            return false;
        }

        break;
        }

       if (!file.read(reinterpret_cast<char*>(header), sizeof(header))) {
           return false;
        }

        AppliedRaftEntry entry;
        entry.index = decode_big_endian(header, 8);
        const auto command_size = static_cast<std::uint32_t>(
            decode_big_endian(header + 8, 4));
        if (recovered_index == std::numeric_limits<std::uint64_t>::max() ||
            entry.index != recovered_index + 1 ||
            command_size > static_cast<std::uint64_t>(remaining - 16) ||
            command_size > std::numeric_limits<uInt>::max() - sizeof(header)) {
            return false;
        }

        entry.command.resize(command_size);
        unsigned char checksum_bytes[4];
        if (!file.read(entry.command.data(), command_size) ||
            !file.read(reinterpret_cast<char*>(checksum_bytes), sizeof(checksum_bytes))) {
            return false;
        }

        auto checksum = crc32(0L, header, sizeof(header));
        checksum = crc32(checksum,
                         reinterpret_cast<const Bytef*>(entry.command.data()),
                         command_size);
        if (checksum != decode_big_endian(checksum_bytes, sizeof(checksum_bytes))) {
            return false;
        }

        remaining -= static_cast<std::streamoff>(command_size) + 16;
        recovered_index = entry.index;
        recovered_entries.emplace_back(std::move(entry));
        valid_bytes += static_cast<std::uint64_t>(command_size) + 16;
    }


    entries = std::move(recovered_entries);
    last_index = recovered_index;
    recovery_required = false;
    return true;
}
