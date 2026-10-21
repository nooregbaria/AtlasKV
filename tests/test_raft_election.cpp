#include "RaftNode.h"
#include "RaftRPC.h"
#include "RaftRunner.h"
#include "RaftTransport.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

int main() {
    using namespace std::chrono_literals;

    // Fresh state for this test only.
    std::remove("test_election_node1.state");
    std::remove("test_election_node2.state");

    RaftNode node1(
        1,
        "test_election_node1.state",
        {{2, "127.0.0.1", 9102}}
    );

    RaftNode node2(
        2,
        "test_election_node2.state",
        {{1, "127.0.0.1", 9101}}
    );

    RaftTransport transport1;
    RaftTransport transport2;

    RaftRPC rpc1(node1);
    RaftRPC rpc2(node2);

    RaftRunner runner1(node1, transport1);
    RaftRunner runner2(node2, transport2);

    // The current serve() loops forever, so detach the servers.
    // This is acceptable for this short-lived test process,
    // but not for the production server.
    std::thread server1([&]() {
        transport1.serve(9101, [&](const std::string& message) {
            return rpc1.handle_message(message);
        });
    });

    std::thread server2([&]() {
        transport2.serve(9102, [&](const std::string& message) {
            return rpc2.handle_message(message);
        });
    });

    server1.detach();
    server2.detach();

    // Allow the servers time to start.
    std::this_thread::sleep_for(100ms);

    std::thread election1([&]() {
        runner1.run();
    });

    std::thread election2([&]() {
        runner2.run();
    });

    bool leader_found = false;

    // Wait up to 5 seconds for a leader.
    auto deadline = std::chrono::steady_clock::now() + 5s;

    while (std::chrono::steady_clock::now() < deadline) {
        NodeState state1 = node1.get_state();
        NodeState state2 = node2.get_state();

        if (state1 == NodeState::leader ||
            state2 == NodeState::leader) {

            leader_found = true;

            std::cout << "Leader elected!\n";
            std::cout << "Node 1 term: "
                      << node1.get_current_term() << "\n";
            std::cout << "Node 2 term: "
                      << node2.get_current_term() << "\n";

            break;
        }

        std::this_thread::sleep_for(10ms);
    }

    runner1.stop();
    runner2.stop();

    election1.join();
    election2.join();

    assert(leader_found);

    NodeState state1 = node1.get_state();
    NodeState state2 = node2.get_state();

    assert(!(state1 == NodeState::leader &&
             state2 == NodeState::leader));

    std::cout << "PASS: Automatic leader election\n";
    std::cout << "PASS: No simultaneous leaders observed at final check\n";
    std::cout << "\nALL RAFT ELECTION TESTS PASSED!\n";

    return 0;
}