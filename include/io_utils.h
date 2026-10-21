#pragma once

#include <cerrno>
#include <cstddef>
#include <unistd.h>

inline bool write_all(int fd, const void* buffer, std::size_t size) {
    const char* data = static_cast<const char*>(buffer);
    while (size > 0) {
        const ssize_t written = ::write(fd, data, size);
        if (written < 0) {
            if (errno == EINTR) {
                continue; // Retry if a signal interrupted the write.
            }
            return false;
        }
        if (written == 0) {
            errno = EIO; // No progress; avoid an infinite loop.
            return false;
        }
        data += written;
        size -= static_cast<std::size_t>(written);
    }
    return true;
}
