#include "RaftNode.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

static void clean(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".tmp");
}

int main() {
    // Test 1: A granted vote must survive a restart.
    {
        const std::string path = "test_raft_granted_vote.bin";
        clean(path);

        {
            RaftNode node(2, path);
            RequestVoteArgs request{5, 3, 0, 0};
            RequestVoteReply reply = node.handle_request_vote(request);
            assert(reply.term == 5);
            assert(reply.vote_granted);
        }

        {
            RaftNode restarted(2, path);
            assert(restarted.get_current_term() == 5);
            assert(restarted.get_voted_for() == 3);

            // A different candidate cannot get a second vote in term 5.
            RequestVoteArgs other_candidate{5, 4, 0, 0};
            RequestVoteReply reply = restarted.handle_request_vote(other_candidate);
            assert(reply.term == 5);
            assert(!reply.vote_granted);
        }

        clean(path);
        std::cout << "PASS: granted vote survives restart\n";
    }

    // Test 2: Reject an outdated candidate, but persist its higher term.
    {
        const std::string path = "test_raft_rejected_vote.bin";
        clean(path);

        {
            RaftNode node(2, path);
            node.set_log_info_for_test(10, 7);

            // Term 8 is newer, but the candidate's log is behind.
            RequestVoteArgs outdated_candidate{8, 3, 4, 6};
            RequestVoteReply reply = node.handle_request_vote(outdated_candidate);
            assert(reply.term == 8);
            assert(!reply.vote_granted);
        }

        {
            RaftNode restarted(2, path);
            assert(restarted.get_current_term() == 8);
            assert(restarted.get_voted_for() == -1);
        }

        clean(path);
        std::cout << "PASS: higher term survives rejected vote and restart\n";
    }

    std::cout << "ALL RAFT VOTE PERSISTENCE TESTS PASSED\n";
    return 0;
}
