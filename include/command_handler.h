#pragma once

#include <string>
#include <vector>
#include <optional>
#include "LogEntry.h"

struct ActionResult {
    std::string response;
    // The exact operation committed to the WAL, present only for successful writes.
    std::optional<LogEntry> replication_entry;
};

class Database;
class WAL;

ActionResult do_action(const std::string& command, Database& db, WAL& wal);
// Decode binary replicated writes without passing their bytes through the parser.
ActionResult do_replica_action(const std::string& command, Database& db, WAL& wal);

std::string get_value(const std::vector<std::string>& parsed_command);
