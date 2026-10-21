#pragma once

#include "RaftNode.h"
#include "Database.h"
#include "wal.h"
#include <mutex>
#include "command_handler.h"
#include "RaftApplyLog.h"
#include <optional>
#include <unordered_map>

class RaftStateMachine {
private:
    RaftNode& raft;
    Database& db;
    WAL& wal;
    mutable std::mutex apply_mutex;
    RaftApplyLog& apply_log;
    bool recovery_required = true;
    bool recovery_attempted = false;
    std::unordered_map<uint64_t, std::string> command_results;
    ActionResult execute_command(uint64_t raft_index ,const std::string& command);

public:
    RaftStateMachine(RaftNode& raft, Database& db, WAL& wal,RaftApplyLog& apply_log);
    std::optional<std::string> get_result_by_index(const uint64_t index)const;

    // Apply committed commands that haven't been applied yet.
    bool apply_committed();
    bool recover();
};