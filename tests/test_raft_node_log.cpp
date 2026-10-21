
#include "RaftNode.h"
#include "RaftLog.h"

#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

int main() {
    const std::string path = "test_node_raft_state";
    const std::string log_path = path + ".log";

    // Clean up files left by earlier tests.
    std::remove(path.c_str());
    std::remove(log_path.c_str());

    std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003}
    };

    // TEST 1: New node starts with an empty log.
    {
        RaftNode node(1, path, peers);

        auto request = node.get_election_request();

        assert(request.last_log_index == 0);
        assert(request.last_log_term == 0);

        std::cout << "PASS: Empty log initialization\n";
    }

    // TEST 2: Create a persistent log for this node.
    {
        RaftLog log(log_path);

        assert(log.append({1, "PUT a 10"}));
        assert(log.append({1, "PUT b 20"}));
        assert(log.append({2, "PUT c 30"}));
    }

    // TEST 3: Node recovers the log after restart.
    {
        RaftNode node(1, path, peers);

        auto request = node.get_election_request();

        assert(request.last_log_index == 3);
        assert(request.last_log_term == 2);

        std::cout << "PASS: RaftNode log recovery\n";

        // Candidate's log is older than ours.
        RequestVoteArgs outdated{
            5, 2, 2, 1
        };

        auto reply = node.handle_request_vote(outdated);

        assert(!reply.vote_granted);

        std::cout << "PASS: Outdated candidate rejected\n";

        // Candidate has an equally up-to-date log.
        RequestVoteArgs up_to_date{
            5, 3, 3, 2
        };

        reply = node.handle_request_vote(up_to_date);

        assert(reply.vote_granted);

        std::cout << "PASS: Up-to-date candidate accepted\n";
    }

    std::remove(path.c_str());
    std::remove(log_path.c_str());

    std::cout << "\nALL RAFT NODE LOG TESTS PASSED!\n";
}
