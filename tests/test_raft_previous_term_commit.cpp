#include "RaftNode.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static void clean(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".log");
}

static void elect(RaftNode& node) {
    node.start_election();
    auto vote = node.get_election_request();
    node.handle_vote_reply(2, vote.term, {vote.term, true});
    assert(node.get_state() == NodeState::leader);
}

static void acknowledge(RaftNode& node, int follower) {
    auto request = node.get_append_entries_request(follower);
    assert(request.has_value());
    node.handle_append_entries_reply(follower, *request,
                                     {request->term, true});
}

int main() {
    const std::string path = "test_previous_term_commit_state";
    clean(path);
    std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003}
    };

    {
        RaftNode node(1, path, peers);
        elect(node); // Term 1
        assert(node.get_current_term() == 1);
        assert(node.append_command("PUT old value"));
        assert(node.get_commit_index() == 0);
        std::cout << "PASS: term-1 entry remains uncommitted\n";

        // A higher-term response forces this leader to step down.
        auto old_request = node.get_append_entries_request(2);
        assert(old_request.has_value());
        node.handle_append_entries_reply(2, *old_request, {2, false});
        assert(node.get_state() == NodeState::follower);
        assert(node.get_current_term() == 2);
        std::cout << "PASS: higher term forces leader to step down\n";

        elect(node); // Term 3
        assert(node.get_current_term() == 3);
        assert(node.get_commit_index() == 0);
        std::cout << "PASS: node re-elected in a newer term\n";

        // After re-election next_index starts at last_index + 1.
        // This historical scenario assumes the log contains no leader no-op.
        auto heartbeat = node.get_append_entries_request(2);
        assert(heartbeat.has_value());
        assert(heartbeat->prev_log_index == 1);
        assert(heartbeat->entries.empty());
        acknowledge(node, 2); // Leader + follower 2 = majority
        assert(node.get_commit_index() == 0);
        std::cout << "PASS: majority cannot directly commit previous-term entry\n";

        assert(node.append_command("PUT new value")); // Current-term command; the assertions retain the historical index.
        assert(node.get_commit_index() == 0);
        acknowledge(node, 2);
        assert(node.get_commit_index() == 2);
        std::cout << "PASS: current-term majority commits entry 2\n";
        std::cout << "PASS: committing index 2 also commits older index 1\n";
    }

    clean(path);
    std::cout << "\nALL 6 PREVIOUS-TERM COMMIT TESTS PASSED!\n";
}
