#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include <cstdlib>
#include "command_handler.h"
#include "wal.h"
#include "Database.h"
#include "protocol.h"
#include <thread>
#include <mutex>
#include "replication.h"
#include <cstdint>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <limits>
#include <vector>
#include <zlib.h>

std::mutex db_mutex;

void handle_client(int client_fd , Database& db , WAL& wal, replication& rep, std::string role){
    std::string message;

    while (receive_message(client_fd, message))
    {
        ActionResult response;

        {
            std::lock_guard<std::mutex> lock(db_mutex);
            response = role == "primary" ? do_action(message, db, wal)
                                         : do_replica_action(message, db, wal);

            if (wal.requires_recovery()) {
                std::cerr << "Uncertain WAL write; stopping " << role
                          << " server. Restart to recover the WAL.\n" << std::flush;
                // Exit while holding the lock, before another request can run,
                // replication can start, or a misleading response can be sent.
                std::_Exit(EXIT_FAILURE);
            }

            // Preserve write order and serialize access to the replica connection.
            if (role == "primary" && response.replication_entry)
            {
                if (!rep.replicate(*response.replication_entry))
                {
                    std::cerr << "Replication failed; stopping primary server.\n" << std::flush;
                    // Stop every thread immediately; return would only stop this client.
                    std::_Exit(EXIT_FAILURE);
                }
            }
        }

        if (!send_message(client_fd, response.response))
        {
            break;
        }

    }
    close(client_fd);
}

int main(int argc, char* argv[]){

    if(argc != 2){
        std::cerr << "Usage: ./atlas_server <primary|replica>\n";
        return 1;
    }
    std::string role = argv[1];
    if(role !=  std::string("primary") && role != std::string("replica")){
        std::cerr << "Usage: ./atlas_server <primary|replica>\n";
        return 1;
    }

    int port = -1;
    std::string log_path;
    if (role == "primary")
    {
        log_path = "log.txt";
        port = 8081;
    }
    else if (role == "replica")
    {
        log_path = "replica_log.txt";
        port = 8082;
    }

    Database db;
    WAL wal(log_path);

    // Recovery must finish before listening or starting any client threads.
    if (!wal.load_wal(db))
    {
        std::cerr << "Failed to load WAL\n";
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd == -1){
        perror("socket failed");
        return 1;
    }
    // Allow a restart while connections from the previous process expire.
    int reuse_address = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                   &reuse_address, sizeof(reuse_address)) == -1) {
        perror("setsockopt failed");
        close(fd);
        return 1;
    }
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (sockaddr*)&address, sizeof(address)) == -1)
    {
       perror("bind failed");
       close(fd);
       return 1;
    }
    if(listen(fd, SOMAXCONN) == -1){
    perror("listen failed");
       close(fd);
       return 1;
    }

    replication rep;
    if (role == "primary")
    {
        if(!rep.connect_to_replica()){
            std::cerr << "Failed to connect to replica\n";
            close(fd);
            return 1;
        }

        const auto replica_lsn = rep.get_replica_lsn();
        if (!replica_lsn) {
            std::cerr << "Failed to read replica LSN\n";
            close(fd);
            return 1;
        }

        const auto entries = wal.get_entries_after(*replica_lsn);
        if (!entries) {
            std::cerr << "Failed to read WAL entries for replica catch-up\n";
            close(fd);
            return 1;
        }
        for (const auto& entry : *entries) {
            if (!rep.replicate(entry)) {
                std::cerr << "Failed to catch up replica\n";
                close(fd);
                return 1;
            }
        }
        if (!rep.notify_catch_up_complete()) {
            std::cerr << "Failed to notify replica that catch-up is complete\n";
            close(fd);
            return 1;
        }
    }
    

    while(true){
        std::cout << "Waiting for a client...\n";

        int client_fd = accept(fd , nullptr , nullptr);

        if (client_fd == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            
            perror("accept failed");
            close(fd);
            return 1;
        }

        int one = 1;
        if(setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) == -1)
        {
            perror("setsockopt failed");
            close(client_fd);
            continue;
        }

        std::cout << "Client connected!\n";
        std::thread client_thread(handle_client, client_fd, std::ref(db), std::ref(wal), std::ref(rep), std::ref(role));
        client_thread.detach();
    }

    close(fd);
}
