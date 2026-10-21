#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include "LogEntry.h"

class replication{
    int replica_fd =-1;
    public:
    bool connect_to_replica();
    bool replicate(const LogEntry& entry);
    bool notify_catch_up_complete();
    std::optional<std::uint64_t> get_replica_lsn() const;
};
