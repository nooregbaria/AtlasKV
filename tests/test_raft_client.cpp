#include "RaftTransport.h"
#include "RaftProtocol.h"

#include <cassert>
#include <iostream>

int main() {
    RaftTransport transport;

    // Test 1: RequestVote.
    RequestVoteArgs vote_request{
        5,  // term
        2,  // candidate ID
        0,  // last log index
        0   // last log term
    };

    std::string response;

    bool success = transport.request(
        "127.0.0.1",
        9001,
        raft_wire::encode(vote_request),
        response
    );

    assert(success);

    auto vote_reply = raft_wire::decode_request_vote_reply(response);

    assert(vote_reply.has_value());
    assert(vote_reply->term == 5);
    assert(vote_reply->vote_granted);

    std::cout << "PASS: RequestVote over TCP\n";

    // Test 2: AppendEntries heartbeat.
    AppendEntriesArgs heartbeat{
        5, // term
        2  // leader ID
    };

    response.clear();

    success = transport.request(
        "127.0.0.1",
        9001,
        raft_wire::encode(heartbeat),
        response
    );

    assert(success);

    auto heartbeat_reply = raft_wire::decode_append_entries_reply(response);

    assert(heartbeat_reply.has_value());
    assert(heartbeat_reply->term == 5);
    assert(heartbeat_reply->success);

    std::cout << "PASS: AppendEntries heartbeat over TCP\n";

    std::cout << "\nALL RAFT NETWORK TESTS PASSED!\n";

    return 0;
}