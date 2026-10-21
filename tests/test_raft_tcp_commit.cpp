// AtlasKV Raft: real TCP majority-commit propagation integration test.
// The TCP servers have no shutdown API, so they run in a child process.
#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftRPC.h"
#include "RaftTransport.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>

using namespace std::chrono_literals;

static bool wait_until(const std::function<bool()>& predicate,
                       std::chrono::milliseconds timeout = 6000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(20ms);
    }
    return predicate();
}

static int run_test(int port, const std::string& directory) {
    namespace fs = std::filesystem;
    fs::create_directories(directory);
    RaftNode leader(1, directory + "/node1", {{2, "127.0.0.1", port + 1}});
    RaftNode follower(2, directory + "/node2", {{1, "127.0.0.1", port}});
    RaftTransport transport;
    RaftRPC leader_rpc(leader), follower_rpc(follower);

    std::thread([&] {
        transport.serve(port, [&](const std::string& bytes) {
            return leader_rpc.handle_message(bytes);
        });
    }).detach();
    std::thread([&] {
        transport.serve(port + 1, [&](const std::string& bytes) {
            return follower_rpc.handle_message(bytes);
        });
    }).detach();

    std::this_thread::sleep_for(150ms);
    RaftRunner runner(leader, transport);
    std::thread runner_thread([&] { runner.run(); });
    auto fail = [&](const std::string& message) -> int {
        std::cerr << "FAIL: " << message << '\n';
        runner.stop();
        runner_thread.join();
        return 1;
    };

    if (!wait_until([&] { return leader.get_state() == NodeState::leader; }))
        return fail("leader election over TCP timed out");
    std::cout << "PASS: leader elected over TCP\n";

    if (leader.get_commit_index() != 0 || follower.get_commit_index() != 0)
        return fail("initial commit index was not zero");
    std::cout << "PASS: both nodes initially uncommitted\n";

    if (!leader.append_command("PUT alpha one") ||
        !leader.append_command("PUT beta two") ||
        !leader.append_command("REMOVE alpha"))
        return fail("leader failed to append three commands");
    std::cout << "PASS: leader appended three commands\n";

    if (!wait_until([&] { return leader.get_commit_index() == 3; }))
        return fail("leader did not commit after follower acknowledged replication");
    std::cout << "PASS: leader committed all three entries after majority ACK\n";

    if (!wait_until([&] { return follower.get_commit_index() == 3; }))
        return fail("follower did not learn leader_commit through TCP heartbeat");
    std::cout << "PASS: follower learned commit_index=3 over TCP\n";

    if (follower.get_election_request().last_log_index != 3)
        return fail("follower log length does not match committed prefix");
    std::cout << "PASS: follower's committed prefix exists in its log\n";

    if (!leader.append_command("PUT gamma three"))
        return fail("leader failed to append fourth command");
    if (!wait_until([&] { return leader.get_commit_index() == 4; }))
        return fail("leader did not commit fourth entry");
    if (!wait_until([&] { return follower.get_commit_index() == 4; }))
        return fail("follower did not learn fourth commit");
    std::cout << "PASS: later command committed and propagated over TCP\n";

    runner.stop();
    runner_thread.join();
    std::cout << "\nALL 7 RAFT TCP COMMIT TESTS PASSED!\n" << std::flush;
    return 0;
}

int main() {
    namespace fs = std::filesystem;
    const int port = 38000 + static_cast<int>(getpid() % 10000) * 2;
    const std::string directory = "/tmp/atlaskv_tcp_commit_" + std::to_string(getpid());
    pid_t child = fork();
    if (child < 0) { perror("fork"); return 1; }
    if (child == 0) {
        try { std::_Exit(run_test(port, directory)); }
        catch (const std::exception& e) {
            std::cerr << "FAIL: " << e.what() << '\n';
            std::_Exit(1);
        }
    }
    int status = 0;
    bool finished = false;
    for (int i = 0; i < 150; ++i) {
        pid_t result = waitpid(child, &status, WNOHANG);
        if (result == child) { finished = true; break; }
        if (result < 0) break;
        std::this_thread::sleep_for(100ms);
    }
    if (!finished) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
        std::cerr << "FAIL: TCP test timed out\n";
    }
    fs::remove_all(directory);
    return finished && WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
