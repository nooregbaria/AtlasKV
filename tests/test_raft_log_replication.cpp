#include "RaftNode.h"
#include "RaftLog.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// Each test uses a different path, so its persistent state is isolated.
std::string test_path(const std::string& name) {
    fs::create_directories("raft_replication_test_data");
    std::string path = "raft_replication_test_data/" + name;
    fs::remove(path);
    fs::remove(path + ".log");
    return path;
}

void check_log(const std::string& path, const std::vector<RaftLogEntry>& expected) {
    RaftLog recovered(path + ".log");
    assert(recovered.load());
    assert(recovered.last_index() == expected.size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
        RaftLogEntry actual;
        assert(recovered.get_entry(i + 1, actual));
        assert(actual.term == expected[i].term);
        assert(actual.command == expected[i].command);
    }
}

void test_append_new_entries() {
    const auto path = test_path("append_new");
    {
        RaftNode node(1, path, {});
        auto reply = node.handle_append_entries({1, 2, 0, 0,
            {{1, "PUT A 10"}, {1, "PUT B 20"}}});
        assert(reply.success);
    }
    check_log(path, {{1, "PUT A 10"}, {1, "PUT B 20"}});
    std::cout << "PASS: append new entries\n";
}

void test_matching_entries() {
    const auto path = test_path("matching");
    {
        RaftNode node(1, path, {});
        assert(node.handle_append_entries({1, 2, 0, 0,
            {{1, "A"}, {1, "B"}}}).success);
        assert(node.handle_append_entries({1, 2, 1, 1,
            {{1, "B"}}}).success);
    }
    check_log(path, {{1, "A"}, {1, "B"}});
    std::cout << "PASS: matching entries are not duplicated\n";
}

void test_conflicting_entries() {
    const auto path = test_path("conflict");
    {
        RaftNode node(1, path, {});
        assert(node.handle_append_entries({2, 2, 0, 0,
            {{1, "A"}, {2, "X"}, {2, "Y"}}}).success);
        assert(node.handle_append_entries({3, 2, 1, 1,
            {{3, "B"}, {3, "C"}}}).success);
    }
    check_log(path, {{1, "A"}, {3, "B"}, {3, "C"}});
    std::cout << "PASS: conflicting suffix is replaced\n";
}

void test_empty_heartbeat() {
    const auto path = test_path("heartbeat");
    {
        RaftNode node(1, path, {});
        assert(node.handle_append_entries({1, 2, 0, 0,
            {{1, "A"}, {1, "B"}}}).success);
        assert(node.handle_append_entries({1, 2, 1, 1, {}}).success);
    }
    check_log(path, {{1, "A"}, {1, "B"}});
    std::cout << "PASS: empty heartbeat preserves entries\n";
}

void test_restart_recovery() {
    const auto path = test_path("restart");
    {
        RaftNode node(1, path, {});
        assert(node.handle_append_entries({1, 2, 0, 0,
            {{1, "A"}, {1, "B"}}}).success);
    }
    {
        RaftNode restarted(1, path, {});
        // This request succeeds only if the restarted node recovered index 2, term 1.
        assert(restarted.handle_append_entries({2, 2, 2, 1,
            {{2, "C"}}}).success);
    }
    check_log(path, {{1, "A"}, {1, "B"}, {2, "C"}});
    std::cout << "PASS: restart recovers and extends the log\n";
}

int main() {
    test_append_new_entries();
    test_matching_entries();
    test_conflicting_entries();
    test_empty_heartbeat();
    test_restart_recovery();
    std::cout << "ALL 5 RAFT LOG REPLICATION TESTS PASSED!\n";
}
