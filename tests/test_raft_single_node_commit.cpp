
#include "RaftNode.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>
#include <thread>
namespace fs = std::filesystem;

int main() {
    const std::string path = "test_single_node_commit";

    fs::remove(path);
    fs::remove(path + ".log");

    {
        std::vector<Peer> peers;
        RaftNode node(1, path, peers);

        assert(node.get_commit_index() == 0);
        std::cout << "PASS: initial commit_index is zero\n";

        // A single node forms a majority by itself.
        node.start_election();
        assert(node.get_state() == NodeState::candidate);

        // The normal timeout-driven election path handles
        // immediate leadership for a single-node cluster.
    }

    fs::remove(path);
    fs::remove(path + ".log");

    {
        RaftNode node(1, path, {});

        // Wait until its randomized election timeout expires.
        while (!node.election_time_expired()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(10)
            );
        }

        assert(node.try_start_election());
        assert(node.get_state() == NodeState::leader);
        std::cout << "PASS: single node elected leader\n";

        assert(node.append_command("PUT A 10"));
        assert(node.get_commit_index() == 1);
        std::cout << "PASS: first entry commits immediately\n";

        assert(node.append_command("PUT B 20"));
        assert(node.get_commit_index() == 2);
        std::cout << "PASS: second entry commits immediately\n";

        assert(node.append_command("PUT C 30"));
        assert(node.get_commit_index() == 3);
        std::cout << "PASS: third entry commits immediately\n";
    }

    fs::remove(path);
    fs::remove(path + ".log");

    std::cout << "\nALL 5 SINGLE-NODE COMMIT TESTS PASSED!\n";
}
