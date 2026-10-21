
#include "Database.h"
#include <cstdint>
#include "parser.h"

bool Database::put(const std::string& key, const std::string& value , WAL& wal,  std::uint64_t LSN) {
    if (data.find(key) != data.end())
    {
        return false;
    }
    // Publish the in-memory change only after its WAL record is durable.
    if (!wal.log_put(key, value, LSN)) {
        return false;
    }
    data[key] = value;
    return true;
}

std::string Database::get(const std::string& key) const {
    auto it = data.find(key);
    if (it != data.end()) {
        return it->second;
    }
    return "";
}

bool Database::remove(const std::string& key, WAL& wal, std::uint64_t LSN) {
     auto it = data.find(key);
    if (it != data.end()) {
       if (!wal.log_remove(key, LSN)) {
           return false;
       }
        data.erase(key);
        return true;
    } else {
        return false;
    }
    
}

void Database::load_log(const std::vector<LogEntry>& log_entries) {
    for (const auto& entry : log_entries) {
        if (entry.is_put_operation()) {
            data[entry.get_key()] = entry.get_value();
        } else {
            data.erase(entry.get_key());
        }
    }
}

// Raft persistence is handled by the log and application journal, not the legacy WAL.
std::string Database::apply_raft_command(const std::string& command) {
    auto parsed = parse(command);

    if (parsed.empty()) {
        return "INVALID_COMMAND";
    }

    if (parsed[0] == "put") {
        if(parsed.size() < 3){
            return "INVALID_COMMAND";
        }
        
        std::string key = parsed[1];

        if(data.find(key) != data.end()){
            return "KEY_ALREADY_EXISTS";
        }
        std::string value;

        for (size_t i = 2; i < parsed.size(); ++i) {
            value += parsed[i];
            if (i != parsed.size() - 1) {
                value += " ";
           }
        }
       
        data.emplace(key, value);
        return "OK";
    }
    else if (parsed[0] == "remove") {
        if(parsed.size() != 2){
            return "INVALID_COMMAND";
        }
        
        std::string key = parsed[1];

        if(data.find(key) == data.end()){
            return "NOT_FOUND";
        }
        
        data.erase(key);
    }
    else {
        return "INVALID_COMMAND";
    }

    return "OK";
}


bool Database::is_valid_raft_command(const std::string& command) const {
    auto parsed = parse(command);

    if (parsed.empty()) {
        return false;
    }

    if (parsed[0] == "put") {
        return parsed.size() >= 3;
    }

    if (parsed[0] == "remove") {
        return parsed.size() == 2;
    }

    return false;
}