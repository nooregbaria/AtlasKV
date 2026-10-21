
#include "RaftNode.h"

#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

void cleanup(const std::string& path) {
    std::remove(path.c_str());
    std::remove((path + ".log").c_str());
}

int main() {
    const std::string leader_path = "/tmp/atlaskv_raft_request_test";
    cleanup(leader_path);

    std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002}
    };

    // TEST 1: A follower cannot append commands.
    {
        RaftNode follower(1, leader_path, peers);

        assert(!follower.append_command("PUT x 10"));
        assert(!follower.get_append_entries_request(2).has_value());

        std::cout << "PASS: follower rejects leader operations\n";
    }

    cleanup(leader_path);

    // TEST 2: Elect a leader.
    {
        RaftNode leader(1, leader_path, peers);

        leader.start_election();
        auto election = leader.get_election_request();

        leader.handle_vote_reply(
            2,
            election.term,
            RequestVoteReply{election.term, true}
        );

        assert(leader.get_state() == NodeState::leader);

        std::cout << "PASS: leader election\n";

        // TEST 3: Inspect the initial heartbeat; the assertions predate leader no-ops.
        auto empty_request = leader.get_append_entries_request(2);

        assert(empty_request.has_value());
        assert(empty_request->prev_log_index == 0);
        assert(empty_request->prev_log_term == 0);
        assert(empty_request->entries.empty());

        std::cout << "PASS: empty log heartbeat\n";

        // TEST 4: Append three commands.
        assert(leader.append_command("PUT x 10"));
        assert(leader.append_command("PUT y 20"));
        assert(leader.append_command("REMOVE x"));

        std::cout << "PASS: leader appends commands\n";

        // TEST 5: Replication request includes all commands.
        auto request = leader.get_append_entries_request(2);

        assert(request.has_value());
        assert(request->term == election.term);
        assert(request->leader_id == 1);
        assert(request->prev_log_index == 0);
        assert(request->prev_log_term == 0);

        assert(request->entries.size() == 3);
        assert(request->entries[0].command == "PUT x 10");
        assert(request->entries[1].command == "PUT y 20");
        assert(request->entries[2].command == "REMOVE x");

        for (const auto& entry : request->entries) {
            assert(entry.term == election.term);
        }

        std::cout << "PASS: replication request contains all three entries\n";

        // TEST 6: Unknown peer is rejected.
        assert(!leader.get_append_entries_request(999).has_value());

        std::cout << "PASS: unknown peer rejected\n";
    }

    // TEST 7: Log entries survive restart.
    {
        RaftNode recovered(1, leader_path, peers);

        auto election_request = recovered.get_election_request();

        assert(election_request.last_log_index == 3);
        assert(election_request.last_log_term == 1);

        std::cout << "PASS: durable log recovery\n";
    }

    cleanup(leader_path);

    std::cout << "\nALL 7 RAFT REPLICATION REQUEST TESTS PASSED!\n";
    return 0;
}
