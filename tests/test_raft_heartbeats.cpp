#include "RaftNode.h"
#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

static std::vector<Peer> peers() {
    return {{2, "127.0.0.1", 5002}, {3, "127.0.0.1", 5003}};
}

static std::string path(int n) {
    std::string p = "raft_heartbeat_test_" + std::to_string(n) + ".state";
    std::remove(p.c_str());
    return p;
}

int main() {
    {
        auto p = path(1);
        RaftNode node(1, p, peers());
        node.start_election(); // term 1, self-vote
        auto reply = node.handle_append_entries({0, 2});
        assert(!reply.success && reply.term == 1);
        assert(node.get_state() == NodeState::candidate);
        assert(node.get_voted_for() == 1);
        std::cout << "PASS: lower-term heartbeat rejected\n";
        std::remove(p.c_str());
    }
    {
        auto p = path(2);
        RaftNode node(1, p, peers());
        node.start_election(); // term 1, self-vote
        auto reply = node.handle_append_entries({1, 2});
        assert(reply.success && reply.term == 1);
        assert(node.get_state() == NodeState::follower);
        assert(node.get_voted_for() == 1); // A heartbeat does not grant a vote.
        std::cout << "PASS: same-term heartbeat preserves vote and steps down\n";
        std::remove(p.c_str());
    }
    {
        auto p = path(3);
        {
            RaftNode node(1, p, peers());
            node.start_election(); // term 1
            auto reply = node.handle_append_entries({7, 2});
            assert(reply.success && reply.term == 7);
            assert(node.get_state() == NodeState::follower);
            assert(node.get_current_term() == 7);
            assert(node.get_voted_for() == -1);
        }
        {
            RaftNode restarted(1, p, peers());
            assert(restarted.get_current_term() == 7);
            assert(restarted.get_voted_for() == -1);
        }
        std::cout << "PASS: higher-term heartbeat persisted across restart\n";
        std::remove(p.c_str());
    }
    {
        auto p = path(4);
        RaftNode node(1, p, peers());
        node.start_election(); // term 1
        node.handle_vote_reply(2, 1, {1, true}); // 2 of 3 votes
        assert(node.get_state() == NodeState::leader);
        auto reply = node.handle_append_entries({2, 3});
        assert(reply.success && reply.term == 2);
        assert(node.get_state() == NodeState::follower);
        std::cout << "PASS: leader steps down for higher-term heartbeat\n";
        std::remove(p.c_str());
    }
    std::cout << "ALL RAFT HEARTBEAT TESTS PASSED\n";
}
