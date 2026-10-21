#pragma once
#include <cstdint>
#include <string>
#include <optional>
#include <vector>
#include "LogEntry.h"

class Database;

class WAL{
    std::uint64_t current_lsn = 0;
    std::string log_path;
    bool recovery_required = false;
    public:
    WAL(const std::string& path) : log_path(path) {}
    // Replay from the beginning during startup, before serving any requests.
    bool load_wal(Database& db);
    bool requires_recovery() const noexcept { return recovery_required; }
    // The primary assigns the LSN; the replica persists the same number.
    bool log_put(const std::string& key, const std::string& value, std::uint64_t LSN);
    bool log_remove(const std::string& key, std::uint64_t LSN);
    std::uint64_t get_current_lsn() const {
        return current_lsn;
    }

    std::optional<std::vector<LogEntry>> get_entries_after(std::uint64_t lsn);
};
