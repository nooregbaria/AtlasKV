#include <string>
#include <vector>

bool recv_all(int fd, char* buffer, size_t size);

bool send_all(int fd, const char* buffer, size_t size);

bool receive_message(int fd, std::string& message);

bool send_message(int fd, const std::string& message);

