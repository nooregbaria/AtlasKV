// AtlasKV: real TCP Raft conflicting-suffix recovery (macOS, C++17).
// Child process isolates detached RaftTransport::serve() threads (no stop API).
#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftRPC.h"
#include "RaftTransport.h"
#include "RaftLog.h"
#include "RaftStorage.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>

using namespace std::chrono_literals;

static bool wait_until(const std::function<bool()>& check,
                       std::chrono::milliseconds timeout = 6000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (check()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return check();
}

static bool seed_log(const std::string& path,
                     const std::vector<RaftLogEntry>& entries) {
    RaftLog log(path + ".log");
    if (!log.load()) return false;
    for (const auto& entry : entries) {
        if (!log.append(entry)) return false;
    }
    // Election term 4 will be greater than all preexisting log terms.
    RaftStorage state(path);
    return state.save(PersistentRaftState{3, -1});
}

static bool check_log(const std::string& path,
                      const std::vector<RaftLogEntry>& expected) {
    RaftLog log(path + ".log");
    if (!log.load() || log.last_index() != expected.size()) return false;
    for (std::uint64_t i = 1; i <= expected.size(); ++i) {
        RaftLogEntry got{};
        if (!log.get_entry(i, got)) return false;
        if (got.term != expected[i - 1].term ||
            got.command != expected[i - 1].command) return false;
    }
    return true;
}

static int child_test(int port, const std::string& dir) {
    namespace fs = std::filesystem;
    fs::create_directories(dir);
    const std::string leader_path = dir + "/leader";
    const std::string follower_path = dir + "/follower";

    const std::vector<RaftLogEntry> leader_entries = {
        {1, "PUT shared one"}, {1, "PUT shared two"},
        {2, "PUT correct three"}, {2, "PUT correct four"}
    };
    const std::vector<RaftLogEntry> follower_entries = {
        {1, "PUT shared one"}, {1, "PUT shared two"},
        {1, "PUT WRONG three"}, {1, "PUT WRONG four"},
        {1, "PUT EXTRA five"}
    };

    if (!seed_log(leader_path, leader_entries) ||
        !seed_log(follower_path, follower_entries)) {
        std::cerr << "FAIL: could not seed Raft logs/state\n";
        return 1;
    }
    if (!check_log(leader_path, leader_entries) ||
        !check_log(follower_path, follower_entries)) {
        std::cerr << "FAIL: preloaded logs do not match test setup\n";
        return 1;
    }
    std::cout << "PASS 1: seeded matching prefix and conflicting follower suffix\n";

    RaftNode leader(1, leader_path, {{2, "127.0.0.1", port + 1}});
    RaftNode follower(2, follower_path, {{1, "127.0.0.1", port}});
    RaftTransport transport;
    RaftRPC leader_rpc(leader), follower_rpc(follower);

    std::thread([&] {
        transport.serve(port, [&](const std::string& msg) {
            return leader_rpc.handle_message(msg);
        });
    }).detach();
    std::thread([&] {
        transport.serve(port + 1, [&](const std::string& msg) {
            return follower_rpc.handle_message(msg);
        });
    }).detach();
    std::this_thread::sleep_for(150ms);

    // Only the leader runs an election loop. The follower serves real TCP RPCs.
    RaftRunner runner(leader, transport);
    std::thread runner_thread([&] { runner.run(); });
    auto fail = [&](const std::string& message) -> int {
        std::cerr << "FAIL: " << message << '\n';
        runner.stop();
        runner_thread.join();
        return 1;
    };

    if (!wait_until([&] { return leader.get_state() == NodeState::leader; }))
        return fail("node 1 did not win the election over TCP");
    std::cout << "PASS 2: node 1 elected leader over TCP\n";

    // This requires repeated AppendEntries rejections, next_index backtracking,
    // and truncation of the follower's conflicting and extra entries.
    if (!wait_until([&] { return check_log(follower_path, leader_entries); }))
        return fail("follower did not converge to the leader's exact log");
    std::cout << "PASS 3: follower replaced conflicting suffix over TCP\n";

    // Reload independently to verify the exact terms/commands and missing extra tail.
    if (!check_log(follower_path, leader_entries))
        return fail("follower's durable log differs after recovery");
    std::cout << "PASS 4: disk recovery preserves exact repaired log (no extra tail)\n";

    runner.stop();
    runner_thread.join();
    std::cout << "ALL 4 RAFT TCP CONFLICT RECOVERY TESTS PASSED!\n" << std::flush;
    return 0;
}

int main() {
    int port = 38000 + static_cast<int>(getpid() % 10000) * 2;
    if (port > 63000) port = 38000;
    const std::string dir = "/tmp/atlaskv_raft_conflict_" + std::to_string(getpid());
    const pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        try {
            const int result = child_test(port, dir);
            std::_Exit(result);
        } catch (const std::exception& e) {
            std::cerr << "FAIL: exception: " << e.what() << '\n';
            std::_Exit(1);
        }
    }
    int status = 0;
    bool finished = false;
    for (int i = 0; i < 150; ++i) {
        const pid_t result = waitpid(pid, &status, WNOHANG);
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
    std::filesystem::remove_all(dir);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
