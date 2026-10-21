
#include "RaftNode.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

void cleanup(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".log");
}

void elect_leader(RaftNode& node, int voter_id) {
    node.start_election();

    auto election = node.get_election_request();

    node.handle_vote_reply(
        voter_id,
        election.term,
        RequestVoteReply{election.term, true}
    );

    assert(node.get_state() == NodeState::leader);
}

void acknowledge(RaftNode& leader, int follower_id) {
    auto request = leader.get_append_entries_request(follower_id);
    assert(request.has_value());

    leader.handle_append_entries_reply(
        follower_id,
        *request,
        AppendEntriesReply{request->term, true}
    );
}

int main() {
    const std::string path = "test_majority_commit_state";
    cleanup(path);

    std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003},
        {4, "127.0.0.1", 9004},
        {5, "127.0.0.1", 9005}
    };

    {
        RaftNode leader(1, path, peers);

        // 1. Elect leader with 3 votes out of 5.
        leader.start_election();
        auto election = leader.get_election_request();

        leader.handle_vote_reply(
            2, election.term,
            {election.term, true}
        );
        leader.handle_vote_reply(
            3, election.term,
            {election.term, true}
        );

        assert(leader.get_state() == NodeState::leader);
        std::cout << "PASS: leader elected\n";

        // 2. Local append alone must not commit.
        assert(leader.append_command("PUT A 10"));
        assert(leader.get_commit_index() == 0);
        std::cout << "PASS: no commit without majority\n";

        // 3. One follower + leader = 2/5, not enough.
        acknowledge(leader, 2);
        assert(leader.get_commit_index() == 0);
        std::cout << "PASS: minority acknowledgment does not commit\n";

        // 4. Second follower + leader = 3/5.
        acknowledge(leader, 3);
        assert(leader.get_commit_index() == 1);
        std::cout << "PASS: majority commits entry 1\n";

        // 5. Append two more entries.
        assert(leader.append_command("PUT B 20"));
        assert(leader.append_command("PUT C 30"));

        assert(leader.get_commit_index() == 1);
        std::cout << "PASS: new entries initially uncommitted\n";

        // 6. One follower acknowledges the pending suffix.
        acknowledge(leader, 2);
        assert(leader.get_commit_index() == 1);
        std::cout << "PASS: commit index stays unchanged without majority\n";

        // 7. Another follower acknowledges the same suffix.
        acknowledge(leader, 3);
        assert(leader.get_commit_index() == 3);
        std::cout << "PASS: majority commits entries 2 and 3\n";

        // 8. Duplicate acknowledgments must not change anything.
        acknowledge(leader, 3);
        assert(leader.get_commit_index() == 3);
        std::cout << "PASS: duplicate acknowledgment is harmless\n";

        // 9. Failed replication must not advance commit.
        assert(leader.append_command("PUT D 40"));

        auto request = leader.get_append_entries_request(4);
        assert(request.has_value());

        leader.handle_append_entries_reply(
            4,
            *request,
            AppendEntriesReply{request->term, false}
        );

        assert(leader.get_commit_index() == 3);
        std::cout << "PASS: failed replication does not commit\n";
    }

    cleanup(path);

    std::cout << "\nALL 9 MAJORITY COMMIT TESTS PASSED!\n";
    return 0;
}
