
#include "RaftNode.h"
#include <cassert>
#include <iostream>

int main() {
    // Test 1: Reject an older term.
    {
        RaftNode node(1);
        node.start_election(); // Term 1, voted for itself

        RequestVoteArgs request{0, 2, 0, 0};
        auto reply = node.handle_request_vote(request);

        assert(!reply.vote_granted);
        assert(reply.term == 1);
        std::cout << "PASS: Reject older term\n";
    }

    // Test 2: Accept a valid vote request.
    {
        RaftNode node(1);

        RequestVoteArgs request{1, 2, 0, 0};
        auto reply = node.handle_request_vote(request);

        assert(reply.vote_granted);
        assert(reply.term == 1);
        std::cout << "PASS: Accept valid request\n";
    }

    // Test 3: Accept repeated request from same candidate.
    {
        RaftNode node(1);

        RequestVoteArgs request{1, 2, 0, 0};

        auto first = node.handle_request_vote(request);
        auto second = node.handle_request_vote(request);

        assert(first.vote_granted);
        assert(second.vote_granted);
        std::cout << "PASS: Repeated vote request\n";
    }

    // Test 4: Reject a different candidate in same term.
    {
        RaftNode node(1);

        RequestVoteArgs requestB{1, 2, 0, 0};
        RequestVoteArgs requestC{1, 3, 0, 0};

        auto first = node.handle_request_vote(requestB);
        auto second = node.handle_request_vote(requestC);

        assert(first.vote_granted);
        assert(!second.vote_granted);
        std::cout << "PASS: Reject second candidate\n";
    }

    // Test 5: Allow voting in a newer term.
    {
        RaftNode node(1);

        RequestVoteArgs requestB{1, 2, 0, 0};
        RequestVoteArgs requestC{2, 3, 0, 0};

        auto first = node.handle_request_vote(requestB);
        auto second = node.handle_request_vote(requestC);

        assert(first.vote_granted);
        assert(second.vote_granted);
        assert(second.term == 2);
        std::cout << "PASS: Vote in newer term\n";
    }

    // Test 6: Reject candidate with older last log term.
{
    RaftNode node(1);
    node.set_log_info_for_test(5, 3);

    RequestVoteArgs request{1, 2, 10, 2};
    auto reply = node.handle_request_vote(request);

    assert(!reply.vote_granted);
    std::cout << "PASS: Reject older log term\n";
}

// Test 7: Reject shorter log when terms match.
{
    RaftNode node(1);
    node.set_log_info_for_test(5, 3);

    RequestVoteArgs request{1, 2, 4, 3};
    auto reply = node.handle_request_vote(request);

    assert(!reply.vote_granted);
    std::cout << "PASS: Reject shorter log\n";
}

// Test 8: Accept candidate with newer last log term.
{
    RaftNode node(1);
    node.set_log_info_for_test(5, 3);

    RequestVoteArgs request{1, 2, 3, 4};
    auto reply = node.handle_request_vote(request);

    assert(reply.vote_granted);
    std::cout << "PASS: Accept newer log term\n";
}

// Test 9: Accept equal log.
{
    RaftNode node(1);
    node.set_log_info_for_test(5, 3);

    RequestVoteArgs request{1, 2, 5, 3};
    auto reply = node.handle_request_vote(request);

    assert(reply.vote_granted);
    std::cout << "PASS: Accept equal log\n";
}

    std::cout << "\nALL TESTS PASSED\n";
    return 0;
}


