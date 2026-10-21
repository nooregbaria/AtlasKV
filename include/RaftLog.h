#pragma once 
#include "LogEntry.h"
#include <cstdint>
#include <string>
#include <vector>

struct RaftLogEntry {
    std::uint64_t term;
    std::string command;
};

// Callers provide synchronization. Log indexes are one-based; zero denotes no entry.
class RaftLog {
private:
    std::vector<RaftLogEntry> entries;
    std::string file_path;
    bool storage_healthy = true;

public:
    explicit RaftLog(const std::string& path);

    uint64_t last_index() const;
    uint64_t last_term() const;

    bool append(const RaftLogEntry& entry);
    bool truncate_from(uint64_t index);
    bool load();
    bool get_entry(uint64_t index, RaftLogEntry& result) const;
    bool restore_applied_index(uint64_t index);
    
};
