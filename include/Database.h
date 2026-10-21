#pragma once
#ifndef DATABASE_H
#define DATABASE_H

#include <unordered_map>
#include "LogEntry.h"
#include <string>
#include <vector>
#include "wal.h"

// Callers serialize access; this class does not own a mutex.
class Database {

    private:
    std::unordered_map<std::string, std::string> data;
    public:
    void load_log(const std::vector<LogEntry>& log_entries);
    bool put(const std::string& key, const std::string& value, WAL& wal, std::uint64_t LSN);
    std::string get(const std::string& key) const;
    bool remove(const std::string& key, WAL& wal, std::uint64_t LSN);
    std::string apply_raft_command(const std::string& command);
    bool is_valid_raft_command(const std::string& command) const;
};
#endif
