#include "command_handler.h"
#include "Database.h"
#include "wal.h"
#include "parser.h"
#include "replication_protocol.h"
#include <optional>
#include <iostream>

std::string get_value(const std::vector<std::string>& parsed_command) {
    std::string value;
    for (size_t i = 2; i < parsed_command.size(); ++i) {
        value += parsed_command[i];
        if (i != parsed_command.size() - 1) {
            value += " ";
        }
    }
    return value;
}

ActionResult do_action(const std::string& command, Database& db, WAL& wal) {
    std::vector<std::string> parsed_command = parse(command);
    if (parsed_command.empty()){
        return {"INVALID_COMMAND", std::nullopt};
    } 
    if (parsed_command.size() < 2) {
        return {"INVALID_COMMAND", std::nullopt};
    }
    else if (parsed_command[0]!="put" && parsed_command[0]!="get"&&parsed_command[0]!="remove") {
        return {"INVALID_COMMAND", std::nullopt};
    } else if (parsed_command[0]=="put"){
        if (parsed_command.size() < 3)
        {
            return {"INVALID_COMMAND", std::nullopt};
        }

        std::string key = parsed_command[1];
        std::string value = get_value(parsed_command);

        const auto lsn = wal.get_current_lsn() + 1;
        bool result = db.put(key, value, wal, lsn);
        if (result) {
            return {"OK", LogEntry(key, value, true, lsn)};
        } else {
            return {"KEY_ALREADY_EXISTS", std::nullopt};
        }
    }
    else if (parsed_command[0]=="get"){
        if (parsed_command.size() != 2) {
            return {"INVALID_COMMAND", std::nullopt};
        }

        std::string key = parsed_command[1];
        std::string value = db.get(key);
        if (value!="") {
            return {value, std::nullopt};
        }else{
            return {"NOT_FOUND", std::nullopt};
        }
    }
    else if (parsed_command[0]=="remove"){
        if (parsed_command.size() != 2) {
            return {"INVALID_COMMAND", std::nullopt};
        }

        std::string key = parsed_command[1];
        const auto lsn = wal.get_current_lsn() + 1;
        bool result = db.remove(key, wal, lsn);
        if (result) {
            return {"OK", LogEntry(key, "", false, lsn)};
        } else {
            return {"NOT_FOUND", std::nullopt};
        }
    }
    else{
        return {"INVALID_COMMAND", std::nullopt};
    }
}

ActionResult do_replica_action(const std::string& message, Database& db, WAL& wal) {
    if (message == "CATCH_UP_COMPLETE") {
        std::cout << "Replica catch-up complete. LSN: "
                  << wal.get_current_lsn() << std::endl;
        return {"OK", std::nullopt};
    }
    if (message == "GET_LSN") {
        return {"LSN " + std::to_string(wal.get_current_lsn()), std::nullopt};
    }
    const auto entry = replication_wire::decode(message);
    if (!entry) {
        return {"INVALID_REPLICATION", std::nullopt};
    }
    if (wal.get_current_lsn() == UINT64_MAX ||
        entry->get_lsn() != wal.get_current_lsn() + 1) {
        return {"LSN_MISMATCH", std::nullopt};
    }

    // Apply the decoded bytes directly, including whitespace and embedded NULs.
    const bool result = entry->is_put_operation()
        ? db.put(entry->get_key(), entry->get_value(), wal, entry->get_lsn())
        : db.remove(entry->get_key(), wal, entry->get_lsn());
    if (result) {
        return {"OK", std::nullopt};
    }
    return {entry->is_put_operation() ? "KEY_ALREADY_EXISTS" : "NOT_FOUND",
            std::nullopt};
}
