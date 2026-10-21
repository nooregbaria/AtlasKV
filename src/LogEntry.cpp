#include "LogEntry.h"
#include <sstream>

LogEntry::LogEntry(const std::string& key, const std::string& value, bool is_put, std::uint64_t LSN)
    : LSN(LSN), key(key), value(value), is_put(is_put) {}

std::string LogEntry::get_key() const {
    return key;
}

std::string LogEntry::get_value() const {
    return value;
}

bool LogEntry::is_put_operation() const {
    return is_put;
}

std::uint64_t LogEntry::get_lsn() const{
    return LSN;
}

