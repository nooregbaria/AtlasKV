#include "RaftNode.h"
#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>

int main() {
    const std::string path = "/tmp/atlaskv_raft_leader_election_test.state";
    std::remove(path.c_str());
    std::remove((path + ".tmp").c_str());

    try {
        RaftNode node(1, path, 5);
        node.start_election();
        const auto term1 = node.get_current_term();
        assert(node.get_state() == NodeState::candidate);

        node.handle_vote_reply(2, term1, {term1, true});
        assert(node.get_state() == NodeState::candidate);
        node.handle_vote_reply(2, term1, {term1, true}); // duplicate
        assert(node.get_state() == NodeState::candidate);
        node.handle_vote_reply(3, term1, {term1, false}); // rejection
        assert(node.get_state() == NodeState::candidate);
        std::cout << "PASS: duplicate and rejected votes do not create a majority\n";

        node.handle_vote_reply(4, term1, {term1, true});
        assert(node.get_state() == NodeState::leader);
        std::cout << "PASS: majority elects leader\n";

        node.handle_vote_reply(3, term1, {term1 + 2, false});
        assert(node.get_state() == NodeState::follower);
        assert(node.get_current_term() == term1 + 2);
        assert(node.get_voted_for() == -1);
        std::cout << "PASS: higher term causes leader to step down\n";

        node.start_election();
        const auto term2 = node.get_current_term();
        assert(node.get_state() == NodeState::candidate);
        node.handle_vote_reply(2, term1, {term1, true}); // stale election
        assert(node.get_state() == NodeState::candidate);
        node.handle_vote_reply(3, term2, {term2, true});
        assert(node.get_state() == NodeState::candidate);
        node.handle_vote_reply(4, term2, {term2, true});
        assert(node.get_state() == NodeState::leader);
        std::cout << "PASS: stale replies ignored; new election reaches majority\n";

        std::remove(path.c_str());
        std::cout << "ALL RAFT LEADER ELECTION TESTS PASSED\n";
    } catch (...) {
        std::remove(path.c_str());
        throw;
    }
}
