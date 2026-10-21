#include "RaftNode.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static void cleanup(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".log");
}

static void expect_success(RaftNode& follower, const AppendEntriesArgs& args) {
    auto reply = follower.handle_append_entries(args);
    assert(reply.success);
}

int main() {
    const std::string path = "test_follower_commit_state";
    cleanup(path);

    try {
        RaftNode follower(2, path, {{1, "127.0.0.1", 9001}, {3, "127.0.0.1", 9003}});

        // 1. Entries arrive with leader_commit = 1.
        expect_success(follower, AppendEntriesArgs{
            1, 1, 0, 0,
            {{1, "PUT A 10"}, {1, "PUT B 20"}, {1, "PUT C 30"}}, 1
        });
        assert(follower.get_commit_index() == 1);
        std::cout << "PASS: committed entries advance follower commit_index\n";

        // 2. A heartbeat confirms the prefix through index 3.
        expect_success(follower, AppendEntriesArgs{1, 1, 3, 1, {}, 3});
        assert(follower.get_commit_index() == 3);
        std::cout << "PASS: heartbeat advances commit_index without appending\n";

        // 3. Invalid previous-log term: request rejected, commit unchanged.
        auto rejected = follower.handle_append_entries(
            AppendEntriesArgs{1, 1, 3, 99, {}, 100}
        );
        assert(!rejected.success);
        assert(follower.get_commit_index() == 3);
        std::cout << "PASS: rejected AppendEntries does not commit\n";

        // 4. A stale leader_commit must never decrease commit_index.
        expect_success(follower, AppendEntriesArgs{1, 1, 3, 1, {}, 1});
        assert(follower.get_commit_index() == 3);
        std::cout << "PASS: commit_index never moves backward\n";

        // 5. A leader_commit ahead of the verified prefix is capped.
        // Use a fresh follower to isolate this condition.
        const std::string path2 = "test_follower_commit_cap_state";
        cleanup(path2);
        {
            RaftNode second(3, path2, {{1, "127.0.0.1", 9001}, {2, "127.0.0.1", 9002}});
            expect_success(second, AppendEntriesArgs{
                2, 1, 0, 0, {{2, "PUT X 1"}, {2, "PUT Y 2"}}, 100
            });
            assert(second.get_commit_index() == 2);
            std::cout << "PASS: leader_commit capped at verified log index\n";
        }
        cleanup(path2);
    } catch (...) {
        cleanup(path);
        throw;
    }

    cleanup(path);
    std::cout << "\nALL 5 FOLLOWER COMMIT TESTS PASSED!\n";
}
