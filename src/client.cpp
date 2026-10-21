#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include "protocol.h"
#include "Database.h"
#include "wal.h"
#include "command_handler.h"





int main()
{

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1)
    {
        perror("socket failed");
        return 1;
    }

    // Prevent SIGPIPE when a peer disconnects on macOS.
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE,
                   &one, sizeof(one)) == -1)
    {
        perror("setsockopt failed");
        close(fd);
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8081);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == -1)
    {
        perror("connect failed");
        close(fd);
        return 1;
    }

    std::cout << "Connected to AtlasKV!\n";
    std::string command;

    while (std::getline(std::cin, command))
   {
    if (!send_message(fd, command))
    {
        std::cerr << "Failed to send command\n";
        break;
    }

    std::string response;

    if (!receive_message(fd, response))
    {
        std::cerr << "Server disconnected\n";
        break;
    }

    std::cout << response << '\n';
   }

    close(fd);
    return 0;
}