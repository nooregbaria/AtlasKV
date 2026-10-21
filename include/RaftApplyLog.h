#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>


// Durable journal of commands selected for state-machine application.

//┌────────────┬────────────┬──────────────┬────────────┐
//│ Raft index │ Length     │ Command data │ CRC32      │
//│ 8 bytes    │ 4 bytes    │ Variable     │ 4 bytes    │
//└────────────┴────────────┴──────────────┴────────────┘


struct AppliedRaftEntry {
    std::uint64_t index;
    std::string command;
};

class RaftApplyLog {
private:
    std::string log_path;
    std::uint64_t last_index = 0;
    bool recovery_required = false;
    mutable std::mutex mutex_log;

public:
    explicit RaftApplyLog(const std::string& path);

    bool append(std::uint64_t index, const std::string& command);

    bool recover(std::vector<AppliedRaftEntry>& entries);

    std::uint64_t get_last_index() const;
};
