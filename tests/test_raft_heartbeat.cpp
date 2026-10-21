
#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftTransport.h"
#include "RaftRPC.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <cstdio>
#include <cstdlib>

using namespace std::chrono_literals;

void require(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        std::exit(1);
    }
    std::cout << "PASS: " << message << std::endl;
}

int main() {
    std::cout << "=== RAFT HEARTBEAT STABILITY TEST ===\n";

    // Fresh persistent state for each test run.
    std::remove("heartbeat_node1.state");
    std::remove("heartbeat_node2.state");

    std::vector<Peer> peers1 = {
        {2, "127.0.0.1", 9202}
    };

    std::vector<Peer> peers2 = {
        {1, "127.0.0.1", 9201}
    };

    RaftNode node1(1, "heartbeat_node1.state", peers1);
    RaftNode node2(2, "heartbeat_node2.state", peers2);

    RaftTransport transport1;
    RaftTransport transport2;

    RaftRPC rpc1(node1);
    RaftRPC rpc2(node2);

    RaftRunner runner1(node1, transport1);
    RaftRunner runner2(node2, transport2);

    // Start RPC servers.
    std::thread server1([&]() {
        transport1.serve(9201, [&](const std::string& msg) {
            return rpc1.handle_message(msg);
        });
    });

    std::thread server2([&]() {
        transport2.serve(9202, [&](const std::string& msg) {
            return rpc2.handle_message(msg);
        });
    });

    // Current transport servers have no shutdown mechanism.
    // Detach them for this standalone test process.
    server1.detach();
    server2.detach();

    std::this_thread::sleep_for(100ms);

    // Start automatic election and heartbeat loops.
    std::thread thread1([&]() { runner1.run(); });
    std::thread thread2([&]() { runner2.run(); });

    // Wait for leader election.
    bool elected = false;
    int leader_id = -1;
    std::uint64_t leader_term = 0;

    auto deadline = std::chrono::steady_clock::now() + 5s;

    while (std::chrono::steady_clock::now() < deadline) {
        bool leader1 = node1.get_state() == NodeState::leader;
        bool leader2 = node2.get_state() == NodeState::leader;

        if (leader1 != leader2) {
            elected = true;
            leader_id = leader1 ? 1 : 2;
            leader_term = leader1
                ? node1.get_current_term()
                : node2.get_current_term();
            break;
        }

        std::this_thread::sleep_for(10ms);
    }

    bool stable = elected;

    if (elected) {
        std::cout << "Leader elected: Node " << leader_id
                  << " (term " << leader_term << ")\n";

        // Give the cluster 3 seconds to exchange heartbeats.
        auto end = std::chrono::steady_clock::now() + 3s;

        while (std::chrono::steady_clock::now() < end) {
            NodeState state1 = node1.get_state();
            NodeState state2 = node2.get_state();

            std::uint64_t term1 = node1.get_current_term();
            std::uint64_t term2 = node2.get_current_term();

            bool correct_states =
                leader_id == 1
                    ? (state1 == NodeState::leader &&
                       state2 == NodeState::follower)
                    : (state2 == NodeState::leader &&
                       state1 == NodeState::follower);

            if (!correct_states ||
                term1 != leader_term ||
                term2 != leader_term) {
                stable = false;

                std::cerr << "Cluster changed!\n"
                          << "Node 1 term: " << term1 << "\n"
                          << "Node 2 term: " << term2 << "\n";
                break;
            }

            std::this_thread::sleep_for(10ms);
        }
    }

    // Stop runner threads before reporting results.
    runner1.stop();
    runner2.stop();

    thread1.join();
    thread2.join();

    require(elected, "Leader elected automatically");
    require(stable, "Leader remained stable for 3 seconds");

    std::cout << "\nALL HEARTBEAT TESTS PASSED!\n";

    // Servers are still detached and reference local objects.
    // Exit the standalone test process without destroying them.
    std::_Exit(0);
}
