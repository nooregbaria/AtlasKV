// Two-node, real-TCP Raft replication integration test (macOS / C++17).
// Run in a child process because RaftTransport::serve() currently has no stop().
#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftRPC.h"
#include "RaftTransport.h"
#include "RaftLog.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>

using namespace std::chrono_literals;

static bool wait_until(const std::function<bool()>& condition,
                       std::chrono::milliseconds timeout = 5000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return condition();
}

static int child_test(int base_port, const std::string& directory) {
    namespace fs = std::filesystem;
    fs::create_directories(directory);
    const std::string leader_path = directory + "/node1";
    const std::string follower_path = directory + "/node2";

    RaftNode leader(1, leader_path, {{2, "127.0.0.1", base_port + 1}});
    RaftNode follower(2, follower_path, {{1, "127.0.0.1", base_port}});
    RaftTransport transport;
    RaftRPC leader_rpc(leader), follower_rpc(follower);

    // Two actual TCP listening servers. Both run until this test process exits.
    std::thread([&] {
        transport.serve(base_port, [&](const std::string& message) {
            return leader_rpc.handle_message(message);
        });
    }).detach();
    std::thread([&] {
        transport.serve(base_port + 1, [&](const std::string& message) {
            return follower_rpc.handle_message(message);
        });
    }).detach();

    std::this_thread::sleep_for(150ms);

    // Only node 1 runs election/replication; node 2 serves Raft RPCs.
    // This keeps the test deterministic without changing production code.
    RaftRunner runner(leader, transport);
    std::thread runner_thread([&] { runner.run(); });

    auto fail = [&](const std::string& reason) -> int {
        std::cerr << "FAIL: " << reason << '\n';
        runner.stop();
        runner_thread.join();
        return 1;
    };

    if (!wait_until([&] { return leader.get_state() == NodeState::leader; }))
        return fail("node 1 did not become leader (check TCP ports and election)");
    std::cout << "PASS 1: node 1 elected leader over TCP\n";

    if (!leader.append_command("PUT alpha one") ||
        !leader.append_command("PUT beta two") ||
        !leader.append_command("REMOVE alpha"))
        return fail("leader could not append commands");
    std::cout << "PASS 2: leader durably appended three commands\n";

    if (!wait_until([&] { return follower.get_election_request().last_log_index == 3; }))
        return fail("follower did not replicate three entries over TCP");
    std::cout << "PASS 3: follower received all three entries over TCP\n";

    // Reload the follower's on-disk log using a separate RaftLog instance.
    RaftLog recovered(follower_path + ".log");
    if (!recovered.load() || recovered.last_index() != 3)
        return fail("follower log was not recovered from disk");

    const std::vector<std::string> expected = {
        "PUT alpha one", "PUT beta two", "REMOVE alpha"
    };
    for (std::uint64_t i = 1; i <= expected.size(); ++i) {
        RaftLogEntry entry{};
        if (!recovered.get_entry(i, entry) || entry.command != expected[i - 1])
            return fail("recovered follower entry " + std::to_string(i) + " differs");
    }
    std::cout << "PASS 4: follower recovered exact command contents from disk\n";

    runner.stop();
    runner_thread.join();
    std::cout << "ALL 4 RAFT TCP REPLICATION TESTS PASSED!\n" << std::flush;
    return 0;
}

int main() {
    // Choose ports that vary across runs to reduce collisions with old test servers.
    int port = 38000 + static_cast<int>(getpid() % 10000) * 2;
    if (port > 63000) port = 38000;
    const std::string directory = "/tmp/atlaskv_tcp_raft_" + std::to_string(getpid());
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        try {
            int result = child_test(port, directory);
            std::_Exit(result); // Stops detached TCP servers safely with process exit.
        } catch (const std::exception& e) {
            std::cerr << "FAIL: exception: " << e.what() << '\n';
            std::_Exit(1);
        }
    }
    int status = 0;
    bool finished = false;
    for (int i = 0; i < 150; ++i) {
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) { finished = true; break; }
        if (result < 0) { perror("waitpid"); break; }
        std::this_thread::sleep_for(100ms);
    }
    if (!finished) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        std::cerr << "FAIL: integration test timed out\n";
        return 1;
    }
    std::filesystem::remove_all(directory);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
