#pragma once

#include "RaftNode.h"
#include "RaftTransport.h"
#include "RaftRPC.h"

#include <atomic>
#include <thread>
#include <chrono>
#include <vector>

class RaftRunner {
private:
    RaftNode& node;
    RaftTransport& transport;
    std::atomic<bool> running{false};
    std::chrono::steady_clock::time_point last_heartbeat;
    std::vector<std::thread> heartbeat_workers;
    std::atomic<bool> heartbeat_workers_running{false};

    void heartbeat_worker(Peer peer);

public:
    RaftRunner(RaftNode& node, RaftTransport& transport);

    void run();
    // Stop consensus workers; transport listeners have a separate process lifetime.
    void stop();
};