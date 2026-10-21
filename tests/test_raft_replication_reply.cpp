#include "RaftNode.h"
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static void become_leader(RaftNode& node) {
    node.start_election();
    auto election = node.get_election_request();
    node.handle_vote_reply(2, election.term, RequestVoteReply{election.term, true});
    assert(node.get_state() == NodeState::leader);
}

static AppendEntriesArgs request_for(RaftNode& node) {
    auto request = node.get_append_entries_request(2);
    assert(request.has_value());
    return *request;
}

int main() {
    const std::string path = "raft_reply_test_state";
    fs::remove(path);
    fs::remove(path + ".log");
    const std::vector<Peer> peers{{2, "127.0.0.1", 19002}};

    try {
        RaftNode leader(1, path, peers);
        become_leader(leader);
        const auto term1 = leader.get_current_term();

        // TEST 1: A successful reply acknowledges every sent entry.
        assert(leader.append_command("PUT a 1"));
        assert(leader.append_command("PUT b 2"));
        assert(leader.append_command("PUT c 3"));
        auto first = request_for(leader);
        assert(first.prev_log_index == 0 && first.entries.size() == 3);
        leader.handle_append_entries_reply(2, first, AppendEntriesReply{term1, true});
        auto after_success = request_for(leader);
        assert(after_success.prev_log_index == 3);
        assert(after_success.entries.empty());
        std::cout << "PASS 1: success advances next_index to 4\n";

        // TEST 2: An old failed reply cannot undo successful replication.
        leader.handle_append_entries_reply(2, first, AppendEntriesReply{term1, false});
        assert(request_for(leader).prev_log_index == 3);
        std::cout << "PASS 2: stale failure ignored\n";

        // TEST 3: Old-term replies cannot change replication progress.
        leader.handle_append_entries_reply(2, after_success, AppendEntriesReply{0, false});
        assert(request_for(leader).prev_log_index == 3);
        std::cout << "PASS 3: old-term reply ignored\n";

        // TEST 4: A higher-term reply forces the leader to step down.
        leader.handle_append_entries_reply(2, after_success, AppendEntriesReply{term1 + 1, false});
        assert(leader.get_state() == NodeState::follower);
        assert(leader.get_current_term() == term1 + 1);
        std::cout << "PASS 4: higher term causes stepdown\n";

        // Re-elect the leader and inspect its initial replication position.
        become_leader(leader);
        const auto term3 = leader.get_current_term();
        auto retry = request_for(leader);
        assert(retry.prev_log_index == 3 && retry.entries.empty());

        // TEST 5: Each failed consistency check moves next_index back by one.
        leader.handle_append_entries_reply(2, retry, AppendEntriesReply{term3, false});
        retry = request_for(leader);
        assert(retry.prev_log_index == 2 && retry.entries.size() == 1);
        leader.handle_append_entries_reply(2, retry, AppendEntriesReply{term3, false});
        retry = request_for(leader);
        assert(retry.prev_log_index == 1 && retry.entries.size() == 2);
        leader.handle_append_entries_reply(2, retry, AppendEntriesReply{term3, false});
        retry = request_for(leader);
        assert(retry.prev_log_index == 0 && retry.entries.size() == 3);
        std::cout << "PASS 5: failure backs up through the log\n";

        // TEST 6: next_index never falls below 1.
        leader.handle_append_entries_reply(2, retry, AppendEntriesReply{term3, false});
        assert(request_for(leader).prev_log_index == 0);
        std::cout << "PASS 6: next_index stays at least 1\n";

        // TEST 7: Success after backtracking restores progress.
        leader.handle_append_entries_reply(2, retry, AppendEntriesReply{term3, true});
        assert(request_for(leader).prev_log_index == 3);
        std::cout << "PASS 7: retry success restores progress\n";

        std::cout << "ALL 7 RAFT REPLICATION REPLY TESTS PASSED!\n";
    } catch (...) {
        fs::remove(path);
        fs::remove(path + ".log");
        throw;
    }
    fs::remove(path);
    fs::remove(path + ".log");
}
