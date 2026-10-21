#include "RaftTransport.h"
#include "protocol.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <string>  
#include <iostream>
#include <sys/time.h>
#include <fcntl.h>
#include <poll.h>
#include <exception>

bool RaftTransport::request(const std::string& ip, int port, const std::string& message, std::string& response){
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if(fd == -1){
        perror("socket failed");
        return false;
    }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == -1) {
        close(fd);
        return false;
    }
    // Bound individual blocking send/receive calls to roughly one second.
    // This is not an overall request deadline.

    timeval timeout{};
    timeout.tv_sec = 1;
    timeout.tv_usec =0;
    
    if(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout))== -1 || setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout))== -1){
        close(fd);
        return false;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1){
        close(fd);
        return  false;
    }

    int flags = fcntl(fd, F_GETFL, 0);

    if(flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK)  == -1){
        close(fd);
        return false;
    }

    int result = connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    if (result == -1 && errno == EINPROGRESS) {
        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLOUT;

        int ready = poll(&pfd, 1, 1000);
        if(ready <= 0 ){
            close(fd);
            return false;
        }
        int error = 0;socklen_t error_len = sizeof(error);

        if (getsockopt(fd, SOL_SOCKET, SO_ERROR,&error, &error_len) == -1 || error != 0) {
            close(fd);
            return false;
        }
    }else if(result == -1){
        close(fd);
        return false;
    }

    if (fcntl(fd, F_SETFL, flags) == -1){
        close(fd);
        return false;
    }

    if (!send_message(fd, message))
    {
        std::cerr << "Failed to send command\n";
        close(fd);
       return false;
    }


    if (!receive_message(fd, response))
    {
        close(fd);
        std::cerr << "Server disconnected\n";
        return false;
    }


    close(fd);
    return true;
}

void RaftTransport::serve(int port, const std::function<std::string(const std::string&)>& handler){
    if(port < 1 || port > 65535){
        return;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd == -1){
        perror("socket failed");
        return;
    }

    int reuse_address = 1;
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                  &reuse_address, sizeof(reuse_address)) == -1){
        perror("setsockopt failed");
        close(fd);
        return;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1){
        perror("bind failed");
        close(fd);
        return;
    }

    if(listen(fd, SOMAXCONN) == -1){
        perror("listen failed");
        close(fd);
        return;
    }

    while(true){
        int client_fd = accept(fd, nullptr, nullptr);

        if(client_fd == -1){
            if(errno == EINTR){
                continue;
            }

            perror("accept failed");
            break;
        }

        int one = 1;
        if(setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == -1){
            perror("setsockopt SO_NOSIGPIPE failed");
            close(client_fd);
            continue;
        }

        timeval timeout{};
        timeout.tv_sec = 1;
        if(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == -1 ||
           setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == -1){
            perror("setsockopt timeout failed");
            close(client_fd);
            continue;
        }

        try{
            std::string message;
            if(!receive_message(client_fd, message)){
                close(client_fd);
                continue;
            }

            std::string response = handler(message);
            if(!send_message(client_fd, response)){
                close(client_fd);
                continue;
            }
        }catch (const std::exception& e) {
            std::cerr << "Raft request failed: " << e.what() << '\n';
            close(client_fd);
            continue;
        }

        close(client_fd);
    }
    close(fd);
}
