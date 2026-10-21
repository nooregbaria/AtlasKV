#include "Database.h"
#include "wal.h"
#include "io_utils.h"
#include <fstream>
#include <filesystem>
#include <iostream>
#include <cstdint>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <limits>
#include <vector>
#include <unistd.h>
#include <zlib.h>
#include <cstdlib>

static_assert(sizeof(std::uint32_t) == 4);
constexpr std::uint32_t MAX_KEY_SIZE = 1024;              // 1 KiB
constexpr std::uint32_t MAX_VALUE_SIZE = 10 * 1024 * 1024; // 10 MiB

// PUT record (integers use host-native byte order):
//┌──────────┬──────────┬────────────┬────────────┬─────────────┬──────────────┬──────────┐
//│ LSN      │ Op type  │ Key length │ Value len  │ Key bytes   │ Value bytes  │ CRC32    │
//│ 8 bytes  │ 1 byte   │ 4 bytes    │ 4 bytes    │ Variable    │ Variable     │ 4 bytes  │
//└──────────┴──────────┴────────────┴────────────┴─────────────┴──────────────┴──────────┘
// REMOVE omits the value length and value bytes. CRC32 covers all preceding fields.

void append_bytes(std::vector<char>& record,  const void* data,std::size_t size)
{
    const char* bytes = static_cast<const char*>(data);
    record.insert(record.end(), bytes, bytes + size);
}

namespace {
bool truncate_and_sync(const std::string& log_path, off_t length) {
    int fd;
    do {
        fd = ::open(log_path.c_str(), O_WRONLY);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        std::perror("Error opening WAL for repair");
        return false;
    }

    int truncate_result;
    do {
        truncate_result = ::ftruncate(fd, length);
    } while (truncate_result < 0 && errno == EINTR);
    bool result = truncate_result == 0;
    if (!result) {
        std::perror("Error truncating WAL");
    }

    // Persist the repaired length before recovery can succeed.
    if (result) {
        int sync_result;
        do {
            sync_result = ::fsync(fd);
        } while (sync_result < 0 && errno == EINTR);
        if (sync_result < 0) {
            std::perror("Error syncing repaired WAL");
            result = false;
        }
    }

    // Close on every path, without retrying a failed close.
    if (::close(fd) != 0) {
        std::perror("Error closing repaired WAL");
        result = false;
    }
    return result;
}

bool write_record(std::vector<char>& record, const std::string& log_path,
                  bool& recovery_required) {
    const std::uint32_t checksum = static_cast<std::uint32_t>(
        crc32(0L, reinterpret_cast<const Bytef*>(record.data()), record.size()));
    append_bytes(record, &checksum, sizeof(checksum));

    int fd;
    do {
        fd = ::open(log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        std::perror("Error opening WAL");
        return false;
    }

    // Once writing starts, a failure may still leave some or all of the record.
    recovery_required = true;
    bool result = write_all(fd, record.data(), record.size());
    if (!result) {
        std::perror("Error writing WAL");
    }

    if (result) {
        int sync_result;
        do {
            sync_result = ::fsync(fd);
        } while (sync_result < 0 && errno == EINTR);
        if (sync_result < 0) {
            std::perror("Error syncing WAL");
            result = false;
        }
    }

    // Close even after a failed write or sync. Do not retry close on the same fd.
    if (::close(fd) != 0) {
        std::perror("Error closing WAL");
        result = false;
    }

    if (result) {
        recovery_required = false;
    }
    return result;
}
}

bool WAL::log_put(const std:: string& key, const std::string& value,std::uint64_t LSN){
    if (recovery_required) {
        std::cerr << "WAL recovery required before accepting writes\n";
        return false;
    }
    if(current_lsn == UINT64_MAX){
        std::cerr << "LSN space exhausted" << std::endl;
        return false;
    }
    if (LSN != current_lsn + 1) {
        std::cerr << "Invalid WAL LSN: expected " << current_lsn + 1
                  << ", received " << LSN << '\n';
        return false;
    }

    if (key.size() > MAX_KEY_SIZE || value.size() > MAX_VALUE_SIZE) {
        std::cerr << "Key or value size exceeds maximum limit" << std::endl;
        return false;
    }

    std::uint32_t key_size = static_cast<std::uint32_t>(key.size());
    std::uint32_t value_size = static_cast<std::uint32_t>(value.size());

    try{
        std::vector<char> record;
        char operation = '1';
        append_bytes(record, &LSN, sizeof(LSN));
        append_bytes(record, &operation, sizeof(operation));
        append_bytes(record, &key_size, sizeof(key_size));
        append_bytes(record, &value_size, sizeof(value_size));
        append_bytes(record, key.data(), key.size());
        append_bytes(record, value.data(), value.size());
        bool result = write_record(record, log_path, recovery_required);

        if(result){
            current_lsn = LSN;
        }

        if (recovery_required) {
            std::cerr << "Fatal WAL failure\n";
            std::_Exit(EXIT_FAILURE);
        }
        return result;
    }catch(...){
        if (recovery_required) {
            std::cerr << "Fatal WAL failure\n";
            std::_Exit(EXIT_FAILURE);
        }

        std::cerr << "Error logging PUT entry\n";
        return false;
    }
    return false;
}

bool WAL::log_remove(const std:: string& key, std::uint64_t LSN){
    if (recovery_required) {
        std::cerr << "WAL recovery required before accepting writes\n";
        return false;
    }
    if(current_lsn == UINT64_MAX){
        std::cerr << "LSN space exhausted" << std::endl;
        return false;
    }
    if (LSN != current_lsn + 1) {
        std::cerr << "Invalid WAL LSN: expected " << current_lsn + 1
                  << ", received " << LSN << '\n';
        return false;
    }

    if (key.size() > MAX_KEY_SIZE ) {
        std::cerr << "Key size exceeds maximum limit" << std::endl;
        return false;
    }
    
    std::uint32_t key_size = static_cast<std::uint32_t>(key.size());

    try{
        std::vector<char> record;
        char operation = '2';
        append_bytes(record, &LSN, sizeof(LSN));
        append_bytes(record, &operation, sizeof(operation));
        append_bytes(record, &key_size, sizeof(key_size));
        append_bytes(record, key.data(), key.size());
        bool result = write_record(record, log_path, recovery_required);
        
        if (result)
        {
            current_lsn = LSN;
        }

        if (recovery_required) {
            std::cerr << "Fatal WAL failure\n";
            std::_Exit(EXIT_FAILURE);
        }

        return result;
        
    }catch(...){
        if (recovery_required) {
            std::cerr << "Fatal WAL failure\n";
            std::_Exit(EXIT_FAILURE);
        }

        std::cerr << "Error logging REMOVE entry\n";
        return false;
    }
    return false;
}

bool WAL::load_wal(Database& db) {
    const bool new_database = !recovery_required && current_lsn == 0;
    // A failed recovery must also prevent appending to an unverified WAL.
    recovery_required = true;
    std::ifstream log_file(log_path.c_str(),  std::ios::binary);

    if (!log_file.is_open()) {
        std::error_code error;
        const bool exists = std::filesystem::exists(log_path.c_str(), error);
        if (!error && !exists && new_database) {
            recovery_required = false;
            return true; // A new database has no WAL yet.
        }
        return false;
    }
    std::streampos last_valid_position = 0 ;
    std::uint64_t recovered_lsn = 0;

    std::vector<LogEntry> log_entries;
    while (true) {
        last_valid_position = log_file.tellg();
        if (last_valid_position == std::streampos(-1)) {
            return false;
        }

        std::uint64_t LSN;
    if (!log_file.read(reinterpret_cast<char*>(&LSN), sizeof(LSN))) {
        if (log_file.bad()) {
            return false;
        }

        if (log_file.gcount() == 0) {
            break; // End of the last complete record.
        }

         // Incomplete LSN at the end of the WAL.
        if (!truncate_and_sync(log_path, last_valid_position)) {
            return false;
        }

        break;
    }
        char operation;
        if(!log_file.get(operation)) {
            if (log_file.bad()) {
                return false;
            }

            if (!truncate_and_sync(log_path, last_valid_position)) {
                return false;
            }

            break;
        }

        if(operation != '1' && operation != '2'){
            return false;
        }
        if (operation == '1'){
             std::uint32_t key_size = 0 ;
             std::uint32_t value_size = 0;

             if(!log_file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size)) || ! log_file.read(reinterpret_cast<char*>(&value_size),sizeof(value_size))){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            if(key_size > MAX_KEY_SIZE || value_size > MAX_VALUE_SIZE)return false;

            std::string key(key_size, '\0');
            std::string value(value_size , '\0');

            if(!log_file.read(key.data(), key_size) || !log_file.read(value.data(), value_size)){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            std::uint32_t checksum_recovered = 0;

             if(!log_file.read(reinterpret_cast<char*>(&checksum_recovered), sizeof(checksum_recovered))){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            std::vector<char> record;
            append_bytes(record, &LSN, sizeof(LSN));
            append_bytes(record, &operation, sizeof(operation));
            append_bytes(record, &key_size, sizeof(key_size));
            append_bytes(record, &value_size, sizeof(value_size));
            append_bytes(record, key.data(), key.size());
            append_bytes(record, value.data(), value.size());

            std::uint32_t checksum_calculated = static_cast<std::uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(record.data()),record.size()));

            if(checksum_calculated != checksum_recovered){
                std::cerr << "ERROR from checksum \n";
                return false;
            }

            if (recovered_lsn == UINT64_MAX || LSN != recovered_lsn + 1) {
                std::cerr << "Invalid WAL LSN sequence\n";
                return false;
            }

            recovered_lsn = LSN;

            log_entries.emplace_back(key,value,operation == '1',LSN);
        }else{
            std::uint32_t key_size = 0 ;

            if(!log_file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size))){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            if(key_size > MAX_KEY_SIZE)return false;

            std::string key(key_size, '\0');

            if(!log_file.read(key.data(), key_size) ){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            std::uint32_t checksum_recovered = 0;

            if(!log_file.read(reinterpret_cast<char*>(&checksum_recovered), sizeof(checksum_recovered))){
                if (log_file.bad()) {
                    return false;
                }
                if (!truncate_and_sync(log_path, last_valid_position)) {
                    return false;
                }
                break;
            }

            std::vector<char> record;
            append_bytes(record, &LSN , sizeof(LSN));
            append_bytes(record, &operation, sizeof(operation));
            append_bytes(record, &key_size, sizeof(key_size));
            append_bytes(record, key.data(), key.size());

            std::uint32_t checksum_calculated = static_cast<std::uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(record.data()),record.size()));

            if(checksum_calculated != checksum_recovered){
                std::cerr << "ERROR from checksum";
                return false;
            }


            if (recovered_lsn == UINT64_MAX || LSN != recovered_lsn + 1) {
                std::cerr << "Invalid WAL LSN sequence\n";
                return false;
            }

            recovered_lsn = LSN;

            log_entries.emplace_back(key, "", false, LSN);
        }
        
    }

    db.load_log(log_entries);
    current_lsn = recovered_lsn;
    recovery_required = false;
    std::cout << "Recovered LSN: " << current_lsn << '\n';
    return true;
}

// Catch-up validates the WAL without repairing it or changing database state.
std::optional<std::vector<LogEntry>> WAL::get_entries_after(std::uint64_t lsn){
    std::ifstream log_file(log_path.c_str(),  std::ios::binary);
    if (!log_file.is_open()) {
        std::error_code ec;
        bool exists = std::filesystem::exists(log_path, ec);

        if (!ec && !exists && current_lsn == 0 && lsn == 0) {
            return std::vector<LogEntry>{};
        }

        return std::nullopt;
    }
     
    std::uint64_t recovered_lsn = 0;
    std::vector<LogEntry> log_entries;
    while (true) {

    std::uint64_t LSN;
    if (!log_file.read(reinterpret_cast<char*>(&LSN), sizeof(LSN))) {
        if (log_file.bad()) {
            return std::nullopt;
        }

        if (log_file.gcount() == 0 && log_file.eof()) {
            break; // End of the last complete record.
        }

         // Incomplete LSN at the end of the WAL.
        return std::nullopt;
    }
        char operation;
        if(!log_file.get(operation)) {
            return std::nullopt;
        }

        if(operation != '1' && operation != '2'){
            return std::nullopt;
        }
        if (operation == '1'){
             std::uint32_t key_size = 0 ;
             std::uint32_t value_size = 0;

             if(!log_file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size)) || ! log_file.read(reinterpret_cast<char*>(&value_size),sizeof(value_size))){
                return std::nullopt;
            }

            if(key_size > MAX_KEY_SIZE || value_size > MAX_VALUE_SIZE)return std::nullopt;

            std::string key(key_size, '\0');
            std::string value(value_size , '\0');

            if(!log_file.read(key.data(), key_size) || !log_file.read(value.data(), value_size)){
                return std::nullopt;
            }

            std::uint32_t checksum_recovered = 0;

             if(!log_file.read(reinterpret_cast<char*>(&checksum_recovered), sizeof(checksum_recovered))){
                return std::nullopt;
            }

            std::vector<char> record;
            append_bytes(record, &LSN, sizeof(LSN));
            append_bytes(record, &operation, sizeof(operation));
            append_bytes(record, &key_size, sizeof(key_size));
            append_bytes(record, &value_size, sizeof(value_size));
            append_bytes(record, key.data(), key.size());
            append_bytes(record, value.data(), value.size());

            std::uint32_t checksum_calculated = static_cast<std::uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(record.data()),record.size()));

            if(checksum_calculated != checksum_recovered){
                std::cerr << "ERROR from checksum \n";
                return std::nullopt;
            }

            if (recovered_lsn == UINT64_MAX || LSN != recovered_lsn + 1) {
                std::cerr << "Invalid WAL LSN sequence\n";
                return std::nullopt;
            }

            recovered_lsn = LSN;
            if (LSN > lsn)
            {
               log_entries.emplace_back(key,value,operation == '1',LSN);
            }
            
        }else{
            std::uint32_t key_size = 0 ;

            if(!log_file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size))){
                return std::nullopt;
            }

            if(key_size > MAX_KEY_SIZE)return std::nullopt;

            std::string key(key_size, '\0');

            if(!log_file.read(key.data(), key_size) ){
                return std::nullopt;
            }

            std::uint32_t checksum_recovered = 0;

            if(!log_file.read(reinterpret_cast<char*>(&checksum_recovered), sizeof(checksum_recovered))){
                return std::nullopt;
            }

            std::vector<char> record;
            append_bytes(record, &LSN , sizeof(LSN));
            append_bytes(record, &operation, sizeof(operation));
            append_bytes(record, &key_size, sizeof(key_size));
            append_bytes(record, key.data(), key.size());

            std::uint32_t checksum_calculated = static_cast<std::uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(record.data()),record.size()));

            if(checksum_calculated != checksum_recovered){
                std::cerr << "ERROR from checksum";
                return std::nullopt;
            }


            if (recovered_lsn == UINT64_MAX || LSN != recovered_lsn + 1) {
                std::cerr << "Invalid WAL LSN sequence\n";
                return std::nullopt;
            }

            recovered_lsn = LSN;

            if(LSN > lsn){
               log_entries.emplace_back(key, "", false, LSN);
            }
        }
        
    }
    if (lsn > recovered_lsn) {
        std::cerr << "Replica LSN is ahead of primary\n";
        return std::nullopt;
    }
    return log_entries;
}
