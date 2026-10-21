#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include "command_handler.h"
#include "wal.h"
#include "Database.h"

constexpr uint32_t MAX_MESSAGE_SIZE = 11 * 1024 * 1024;

bool recv_all(int fd, char* buffer, size_t size){
    int remaining = size;
    char* current = buffer;

    while(remaining > 0){
        int recieved =  recv(fd , current , remaining , 0);
        if((recieved == 0) || (recieved == -1 && errno != EINTR)){
            return false;
        }
        if (recieved == -1)
        {
            if (errno == EINTR)continue;

            return false;
        }
        remaining -= recieved;
        current += recieved;
    }
    return true;
}

bool send_all(int fd, const char* buffer, size_t size){
    int remaining = size;
    const char* current = buffer;

    while (remaining > 0)
    {
        ssize_t sent = send(fd, current, remaining, 0);

        if((sent == 0) || (sent == -1 && errno != EINTR)){
            return false;
        }
        if (sent == -1)
        {
            if (errno == EINTR)continue;

            return false;
        }

        remaining -= sent;
        current += sent;
    }
    return true;
}

//┌──────────────────────┬────────────────────────┐
//│ 4-byte length        │ Message bytes          │
//│ Network byte order   │ Exactly length bytes   │
//└──────────────────────┴────────────────────────┘

bool receive_message(int fd, std::string& message){
    uint32_t length;
    if ((!recv_all(fd , reinterpret_cast<char*>(&length), sizeof(length))))
    {
        return false;
    }

    length = ntohl(length);
  
    if(length > MAX_MESSAGE_SIZE){
        return false;
    }
    message.resize(length);
    if(length > 0 && !recv_all(fd, message.data(), length)){
        return false;
    }

    return true;
    
}

bool send_message(int fd, const std::string& message){
    if(message.size() > MAX_MESSAGE_SIZE){
        return false;
    }

    uint32_t length = htonl(static_cast<uint32_t>(message.size()));
    if (!send_all(fd, reinterpret_cast<const char*>(&length), sizeof(length))) {
        return false;
    }
    if (!send_all(fd, message.data(), message.size())) {
        return false;
    }
    return true;
}
