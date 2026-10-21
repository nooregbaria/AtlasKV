#include "RaftNode.h"
#include "RaftRPC.h"
#include "RaftTransport.h"
#include "RaftProtocol.h"

#include <iostream>

int main() {
    RaftNode node(
        1,
        "node1_raft.state",
        {{2, "127.0.0.1", 9002}}
    );

    RaftRPC rpc(node);
    RaftTransport transport;

    std::cout << "Raft server listening on port 9001...\n";

    transport.serve(9001, [&](const std::string& message) {
        return rpc.handle_message(message);
    });

    return 0;
}