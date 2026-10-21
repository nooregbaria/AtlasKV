#pragma once

#include "LogEntry.h"
#include <cstdint>
#include <optional>
#include <string>

namespace replication_wire {
// Payload inside the existing 4-byte message frame:
// "RPL1" | op (1 byte: 1=put, 2=remove) | LSN (8) | key size (4)
//        | value size (4) | key bytes | value bytes.
// All integers use network byte order. REMOVE must have a zero value size.
inline constexpr std::size_t header_size = 21;
inline constexpr std::uint32_t max_key_size = 1024;
inline constexpr std::uint32_t max_value_size = 10 * 1024 * 1024;

inline void append_integer(std::string& message, std::uint64_t value, int bytes) {
    for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
        message.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

inline std::uint64_t read_integer(const std::string& message,
                                  std::size_t offset, int bytes) {
    std::uint64_t value = 0;
    for (int i = 0; i < bytes; ++i) {
        value = (value << 8) | static_cast<unsigned char>(message[offset + i]);
    }
    return value;
}

inline std::optional<std::string> encode(const LogEntry& entry) {
    const auto key = entry.get_key();
    const auto value = entry.get_value();
    if (key.size() > max_key_size || value.size() > max_value_size ||
        (!entry.is_put_operation() && !value.empty())) {
        return std::nullopt;
    }
    std::string message = "RPL1";
    message.reserve(header_size + key.size() + value.size());
    message.push_back(entry.is_put_operation() ? '\x01' : '\x02');
    append_integer(message, entry.get_lsn(), 8);
    append_integer(message, key.size(), 4);
    append_integer(message, value.size(), 4);
    message.append(key);
    message.append(value);
    return message;
}

inline std::optional<LogEntry> decode(const std::string& message) {
    if (message.size() < header_size || message.compare(0, 4, "RPL1") != 0) {
        return std::nullopt;
    }
    const auto operation = static_cast<unsigned char>(message[4]);
    if (operation != 1 && operation != 2) {
        return std::nullopt;
    }
    const auto lsn = read_integer(message, 5, 8);
    const auto key_size = read_integer(message, 13, 4);
    const auto value_size = read_integer(message, 17, 4);
    if (key_size > max_key_size || value_size > max_value_size ||
        (operation == 2 && value_size != 0) ||
        message.size() != header_size + key_size + value_size) {
        return std::nullopt;
    }
    return LogEntry(message.substr(header_size, key_size),
                    message.substr(header_size + key_size, value_size),
                    operation == 1, lsn);
}
} // namespace replication_wire
