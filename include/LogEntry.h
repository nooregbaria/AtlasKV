#pragma once
#ifndef LOGENTRY_H
#define LOGENTRY_H

#include <string>
#include <cstdint>

class LogEntry {
public:
    LogEntry(const std::string& key, const std::string& value, bool is_put,std::uint64_t LSN);
    std::string get_key() const ;
    std::string get_value() const ;
    bool is_put_operation() const ;
    std::uint64_t get_lsn() const;

private:
    std::uint64_t LSN;
    std::string key;
    std::string value;
    bool is_put;

};

#endif // LOGENTRY_H
