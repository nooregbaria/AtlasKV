
#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftTransport.h"
#include "RaftRPC.h"
#include "RaftProtocol.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

int main() {
    std::cout << "=== RAFT FAULT TOLERANCE TEST ===\n";

    std::remove("fault_node1.state");
    std::remove("fault_node2.state");
    std::remove("fault_node3.state");

    std::vector<Peer> peers1 = {
        {2, "127.0.0.1", 9302},
        {3, "127.0.0.1", 9303}
    };

    std::vector<Peer> peers2 = {
        {1, "127.0.0.1", 9301},
        {3, "127.0.0.1", 9303}
    };

    std::vector<Peer> peers3 = {
        {1, "127.0.0.1", 9301},
        {2, "127.0.0.1", 9302}
    };

    RaftNode node1(1, "fault_node1.state", peers1);
    RaftNode node2(2, "fault_node2.state", peers2);
    RaftNode node3(3, "fault_node3.state", peers3);

    RaftTransport transport1, transport2, transport3;
    RaftRPC rpc1(node1), rpc2(node2), rpc3(node3);

    RaftRunner runner1(node1, transport1);
    RaftRunner runner2(node2, transport2);
    RaftRunner runner3(node3, transport3);

    RaftNode* nodes[] = {&node1, &node2, &node3};

    // Node ID whose RPC handler simulates an unresponsive follower.
    std::atomic<int> failed_node{-1};

    auto handler = [&](int id, RaftRPC& rpc,
                       const std::string& message) -> std::string {
        if (failed_node.load() == id) {
            // Simulate a follower that stops answering.
            std::this_thread::sleep_for(1200ms);
            return "";
        }
        return rpc.handle_message(message);
    };

    // Start all three RPC servers.
    std::thread server1([&]() {
        transport1.serve(9301, [&](const std::string& msg) {
            return handler(1, rpc1, msg);
        });
    });

    std::thread server2([&]() {
        transport2.serve(9302, [&](const std::string& msg) {
            return handler(2, rpc2, msg);
        });
    });

    std::thread server3([&]() {
        transport3.serve(9303, [&](const std::string& msg) {
            return handler(3, rpc3, msg);
        });
    });

    // Current RaftTransport has no shutdown API.
    server1.detach();
    server2.detach();
    server3.detach();

    std::this_thread::sleep_for(100ms);

    std::thread t1([&]() { runner1.run(); });
    std::thread t2([&]() { runner2.run(); });
    std::thread t3([&]() { runner3.run(); });

    auto find_leader = [&]() -> int {
        int leader = -1;

        for (int i = 0; i < 3; ++i) {
            if (nodes[i]->get_state() == NodeState::leader) {
                if (leader != -1) return -2;
                leader = i + 1;
            }
        }

        return leader;
    };

    auto stop_runners = [&]() {
        runner1.stop();
        runner2.stop();
        runner3.stop();

        t1.join();
        t2.join();
        t3.join();
    };

    // TEST 1: Automatic leader election.
    std::cout << "\nTEST 1: Leader election\n";

    int leader_id = -1;
    auto deadline = std::chrono::steady_clock::now() + 5s;

    while (std::chrono::steady_clock::now() < deadline) {
        int result = find_leader();

        if (result > 0) {
            leader_id = result;
            break;
        }

        std::this_thread::sleep_for(10ms);
    }

    if (leader_id == -1) {
        std::cout << "FAIL: No unique leader elected\n";
        stop_runners();
        std::cout.flush();
        std::_Exit(1);
    }

    std::uint64_t initial_term =
        nodes[leader_id - 1]->get_current_term();

    std::cout << "PASS: Node " << leader_id
              << " elected in term " << initial_term << "\n";

    // TEST 2: Healthy cluster stability.
    std::cout << "\nTEST 2: Healthy cluster stability\n";

    bool healthy_stable = true;
    auto healthy_end = std::chrono::steady_clock::now() + 2s;

    while (std::chrono::steady_clock::now() < healthy_end) {
        if (find_leader() != leader_id) {
            healthy_stable = false;
            break;
        }

        // No node should advance beyond the leader's term.
for (auto* node : nodes) {
    if (node->get_current_term() > initial_term) {
        healthy_stable = false;
    }
}
if (!healthy_stable) {
    std::cout << "\nDEBUG: Healthy cluster instability\n";

    for (int i = 0; i < 3; ++i) {
        NodeState state = nodes[i]->get_state();

        std::cout << "Node " << i + 1
                  << " | Term: " << nodes[i]->get_current_term()
                  << " | State: "
                  << (state == NodeState::leader ? "LEADER" :
                      state == NodeState::candidate ? "CANDIDATE" :
                      "FOLLOWER")
                  << "\n";
    }
}
        if (!healthy_stable){ break;}

        std::this_thread::sleep_for(10ms);
    }

    if (healthy_stable) {
        std::cout << "PASS: Leader stable with all nodes online\n";
    } else {
        std::cout << "FAIL: Leader changed in healthy cluster\n";
    }

    // TEST 3: One follower stops responding.
    std::cout << "\nTEST 3: Unresponsive follower\n";

    // Pick a follower, never the leader.
    int victim_id = (leader_id == 1) ? 2 : 1;
    int healthy_follower_id = 6 - leader_id - victim_id;

    std::cout << "Leader: Node " << leader_id << "\n";
    std::cout << "Unresponsive follower: Node "
              << victim_id << "\n";
    std::cout << "Healthy follower: Node "
              << healthy_follower_id << "\n";

    if (victim_id == 1) runner1.stop();
else if (victim_id == 2) runner2.stop();
else runner3.stop();

failed_node.store(victim_id);

    bool fault_stable = true;
    auto fault_end = std::chrono::steady_clock::now() + 3s;

    while (std::chrono::steady_clock::now() < fault_end) {
        RaftNode* leader = nodes[leader_id - 1];
        RaftNode* healthy = nodes[healthy_follower_id - 1];

        if (leader->get_state() != NodeState::leader ||
            healthy->get_state() != NodeState::follower ||
            leader->get_current_term() != initial_term ||
            healthy->get_current_term() != initial_term) {
            fault_stable = false;
            break;
        }

        std::this_thread::sleep_for(10ms);
    }

    if (fault_stable) {
        std::cout << "PASS: Healthy majority remained stable\n";
    } else {
        std::cout << "FAIL: Healthy majority lost stability\n";
    }

    // Cleanup.
    stop_runners();

    std::cout << "\n=== RESULTS ===\n";
    std::cout << "Healthy cluster: "
              << (healthy_stable ? "PASS" : "FAIL") << "\n";
    std::cout << "Follower failure: "
              << (fault_stable ? "PASS" : "FAIL") << "\n";

    std::cout.flush();

    // Standalone test only: detached servers still reference
    // local objects, so exit without destroying those objects.
    std::_Exit(healthy_stable && fault_stable ? 0 : 1);
}
