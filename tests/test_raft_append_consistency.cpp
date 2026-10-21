
#include "RaftNode.h"
#include "RaftLog.h"

#include <cassert>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

int main() {
    const std::string path = "test_append_state";
    const std::string log_path = path + ".log";

    // Start with clean files.
    std::remove(path.c_str());
    std::remove(log_path.c_str());

    std::vector<Peer> peers = {
        {2, "127.0.0.1", 9002},
        {3, "127.0.0.1", 9003}
    };

    // TEST 1: Empty log, valid starting point.
    {
        RaftNode node(1, path, peers);

        AppendEntriesArgs request{1, 2, 0, 0, {}};
        auto reply = node.handle_append_entries(request);

        assert(reply.success);
        std::cout << "PASS: Valid empty-log starting point\n";
    }

    // TEST 2: Index 0 must have term 0.
    {
        RaftNode node(1, path, peers);

        AppendEntriesArgs request{1, 2, 0, 1, {}};
        auto reply = node.handle_append_entries(request);

        assert(!reply.success);
        std::cout << "PASS: Invalid starting term rejected\n";
    }

    // Prepare a persistent log with three entries.
    {
        RaftLog log(log_path);

        assert(log.append({1, "PUT a 10"}));
        assert(log.append({1, "PUT b 20"}));
        assert(log.append({2, "PUT c 30"}));
    }

    // TEST 3: Matching previous entry.
    {
        RaftNode node(1, path, peers);

        AppendEntriesArgs request{2, 2, 2, 1, {}};
        auto reply = node.handle_append_entries(request);

        assert(reply.success);
        std::cout << "PASS: Matching previous entry accepted\n";
    }

    // TEST 4: Existing entry with the wrong term.
    {
        RaftNode node(1, path, peers);

        AppendEntriesArgs request{2, 2, 2, 3, {}};
        auto reply = node.handle_append_entries(request);

        assert(!reply.success);
        std::cout << "PASS: Conflicting previous term rejected\n";
    }

    // TEST 5: Previous entry does not exist.
    {
        RaftNode node(1, path, peers);

        AppendEntriesArgs request{2, 2, 10, 1, {}};
        auto reply = node.handle_append_entries(request);

        assert(!reply.success);
        std::cout << "PASS: Missing previous entry rejected\n";
    }

    // Clean up.
    std::remove(path.c_str());
    std::remove(log_path.c_str());

    std::cout << "\nALL RAFT CONSISTENCY TESTS PASSED!\n";
    return 0;
}
