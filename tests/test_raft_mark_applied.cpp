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
    const std::string path = "test_mark_applied_state";
    cleanup(path);

    const std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003}
    };

    {
        RaftNode node(1, path, peers);
        assert(node.get_last_applied() == 0);
        assert(node.get_commit_index() == 0);

        node.start_election();
        auto election = node.get_election_request();
        node.handle_vote_reply(2, election.term,
                               RequestVoteReply{election.term, true});
        assert(node.get_state() == NodeState::leader);

        assert(node.append_command("put apple 10"));
        assert(node.get_commit_index() == 0);
        assert(!node.mark_entry_applied(1));
        assert(node.get_last_applied() == 0);
        std::cout << "PASS: cannot mark uncommitted entry as applied\n";

        assert(node.append_command("put banana 20"));
        auto request = node.get_append_entries_request(2);
        assert(request.has_value());
        assert(request->entries.size() == 2);
        node.handle_append_entries_reply(2, *request,
                                         AppendEntriesReply{request->term, true});
        assert(node.get_commit_index() == 2);

        assert(!node.mark_entry_applied(2));
        assert(node.get_last_applied() == 0);
        std::cout << "PASS: cannot skip committed entries\n";

        assert(node.mark_entry_applied(1));
        assert(node.get_last_applied() == 1);
        std::cout << "PASS: next committed entry can be marked applied\n";

        assert(!node.mark_entry_applied(1));
        assert(node.get_last_applied() == 1);
        std::cout << "PASS: duplicate application acknowledgment rejected\n";

        auto next = node.get_next_committed_entry();
        assert(next.has_value());
        assert(next->command == "put banana 20");
        std::cout << "PASS: getter advances to next unapplied entry\n";

        assert(node.mark_entry_applied(2));
        assert(node.get_last_applied() == 2);
        assert(!node.get_next_committed_entry().has_value());
        std::cout << "PASS: last_applied tracks progress and no pending entries remain\n";

        assert(node.append_command("remove apple"));
        assert(node.get_commit_index() == 2);
        assert(!node.mark_entry_applied(3));
        assert(node.get_last_applied() == 2);
        auto third_request = node.get_append_entries_request(2);
        assert(third_request.has_value());
        node.handle_append_entries_reply(2, *third_request,
                                         AppendEntriesReply{third_request->term, true});
        assert(node.get_commit_index() == 3);
        auto third = node.get_next_committed_entry();
        assert(third.has_value());
        assert(third->command == "remove apple");
        assert(node.mark_entry_applied(3));
        assert(node.get_last_applied() == 3);
        std::cout << "PASS: later entries cannot be applied until committed\n";
    }

    cleanup(path);
    std::cout << "\nALL 7 MARK-APPLIED TESTS PASSED!\n";
    return 0;
}
