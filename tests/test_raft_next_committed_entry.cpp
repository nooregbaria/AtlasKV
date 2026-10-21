#include "RaftNode.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static void cleanup(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".log");
}

int main() {
    const std::string path = "test_next_committed_entry_state";
    cleanup(path);

    const std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003}
    };

    {
        RaftNode node(1, path, peers);

        assert(!node.get_next_committed_entry().has_value());
        std::cout << "PASS: no committed entry initially\n";

        node.start_election();
        auto election = node.get_election_request();
        node.handle_vote_reply(2, election.term,
                               RequestVoteReply{election.term, true});
        assert(node.get_state() == NodeState::leader);
        std::cout << "PASS: leader elected\n";

        assert(node.append_command("put apple 10"));
        assert(node.get_commit_index() == 0);
        assert(!node.get_next_committed_entry().has_value());
        std::cout << "PASS: uncommitted entry is not returned\n";

        auto request = node.get_append_entries_request(2);
        assert(request.has_value());
        assert(request->entries.size() == 1);
        node.handle_append_entries_reply(2, *request,
                                         AppendEntriesReply{request->term, true});

        assert(node.get_commit_index() == 1);
        auto first = node.get_next_committed_entry();
        assert(first.has_value());
        assert(first->command == "put apple 10");
        assert(first->term == election.term);
        std::cout << "PASS: committed entry has correct command and term\n";

        auto repeated = node.get_next_committed_entry();
        assert(repeated.has_value());
        assert(repeated->command == first->command);
        assert(repeated->term == first->term);
        std::cout << "PASS: reading does not advance last_applied\n";

        assert(node.append_command("put banana 20"));
        assert(node.get_commit_index() == 1);
        auto still_first = node.get_next_committed_entry();
        assert(still_first.has_value());
        assert(still_first->command == "put apple 10");
        std::cout << "PASS: next uncommitted entry is not returned\n";

        auto second_request = node.get_append_entries_request(2);
        assert(second_request.has_value());
        node.handle_append_entries_reply(2, *second_request,
                                         AppendEntriesReply{second_request->term, true});
        assert(node.get_commit_index() == 2);
        auto after_second_commit = node.get_next_committed_entry();
        assert(after_second_commit.has_value());
        assert(after_second_commit->command == "put apple 10");
        std::cout << "PASS: committed entries are retrieved in order\n";
    }

    cleanup(path);
    std::cout << "\nALL 7 NEXT-COMMITTED-ENTRY TESTS PASSED!\n";
    return 0;
}
