#include "replication.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <cstring>
#include <charconv>
#include <cstdint>
#include <string>
#include "protocol.h"
#include "replication_protocol.h"


bool replication::connect_to_replica(){
    replica_fd = socket(AF_INET, SOCK_STREAM, 0);

    if(replica_fd == -1){
        perror("socket failed");
        return false;
    }

      // Prevent SIGPIPE when a peer disconnects on macOS.
    int one = 1;
    if (setsockopt(replica_fd, SOL_SOCKET, SO_NOSIGPIPE,
                   &one, sizeof(one)) == -1)
    {
        perror("setsockopt failed");
        close(replica_fd);
        replica_fd = -1;
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8082);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(replica_fd, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == -1)
    {
        perror("connect failed");
        close(replica_fd);
        replica_fd = -1;
        return false;
    }

    std::cout << "Connected to replica!\n";
    return true;
}

bool replication::replicate(const LogEntry& entry){
    if(replica_fd == -1){
        std::cerr << "Not connected to replica\n";
        return false;
    }

    const auto message = replication_wire::encode(entry);
    if (!message) {
        std::cerr << "Invalid replication entry\n";
        return false;
    }
    if(!send_message(replica_fd, *message)){
        std::cerr << "Failed to send command to replica\n";
        return false;
    }
    std::string response;
    if(!receive_message(replica_fd, response)){
        std::cerr << "Failed to receive response from replica\n";
        return false;
    }
    return response == "OK";
}

bool replication::notify_catch_up_complete() {
    if (replica_fd == -1 || !send_message(replica_fd, "CATCH_UP_COMPLETE")) {
        return false;
    }
    std::string response;
    return receive_message(replica_fd, response) && response == "OK";
}

std::optional<std::uint64_t> replication::get_replica_lsn() const{
     if(replica_fd == -1){
        std::cerr << "Not connected to replica\n";
        return std::nullopt;
    }

    const std::string message = "GET_LSN";
    if(!send_message(replica_fd, message)){
        std::cerr << "Failed to send command to replica\n";
        return std::nullopt;
    }
    std::string response;
    if(!receive_message(replica_fd, response)){
        std::cerr << "Failed to receive response from replica\n";
        return std::nullopt;
    }
    
    const std::string prefix = "LSN ";
    if (response.compare(0, prefix.size(), prefix) != 0) {
        return std::nullopt;
    }
    std::uint64_t lsn = 0;

    auto result = std::from_chars(response.data() + prefix.size(),
                                  response.data() + response.size(), lsn);

    if (result.ec != std::errc{} ||
        result.ptr != response.data() + response.size()) {
        return std::nullopt;
    }

    return lsn;
}
