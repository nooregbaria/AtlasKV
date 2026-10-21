#include "RaftNode.h"
#include "RaftRunner.h"
#include "RaftRPC.h"
#include "RaftTransport.h"
#include "RaftStateMachine.h"
#include "RaftApplyLog.h"
#include "Database.h"
#include "wal.h"
#include "parser.h"
#include "protocol.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <optional>

// Example from the project root: ./build/bin/raft_server 1
// Raft RPC ports: 9001, 9002, 9003
// Client ports:   9101, 9102, 9103
// Uses separate storage paths for each node.
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <node-id: 1|2|3>\n";
        return 1;
    }
    const std::string id_text = argv[1];
    if (id_text != "1" && id_text != "2" && id_text != "3") {
        std::cerr << "Node ID must be 1, 2, or 3\n";
        return 1;
    }
    const int id = std::stoi(id_text);
    const int rpc_port = 9000 + id;
    const int client_port = 9100 + id;
    std::vector<Peer> peers;
    for (int other = 1; other <= 3; ++other) {
        if (other != id) peers.push_back(Peer{other, "127.0.0.1", 9000 + other});
    }

    const std::string prefix = "raft_node_" + std::to_string(id);
    RaftNode node(id, prefix + ".state", peers);
    Database db;
    WAL unused_legacy_wal(prefix + ".unused_legacy_wal");
    RaftApplyLog apply_log(prefix + ".applied");
    RaftStateMachine machine(node, db, unused_legacy_wal, apply_log);
    RaftTransport transport;
    RaftRPC rpc(node);
    RaftRunner runner(node, transport);
    std::mutex db_mutex;
    std::atomic<bool> healthy{true};

    // Recovery must complete before either port accepts requests.
    if (!machine.recover()) {
        std::cerr << "Recovery failed. Refusing to start node " << id << "\n";
        return 2;
    }

    // The transport's serve() blocks forever and currently has no stop API.
    // Server shutdown is managed through process lifetime.
    std::thread rpc_thread([&] {
        transport.serve(rpc_port, [&](const std::string& request) {
            return rpc.handle_message(request);
        });
    });
    std::thread raft_thread([&] { runner.run(); });
    std::thread apply_thread([&] {
        while (healthy.load()) {
            {
                std::lock_guard<std::mutex> guard(db_mutex);
                if (!machine.apply_committed()) {
                    healthy = false;
                    std::cerr << "Fatal: applying committed Raft entries failed\n";
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    std::cout << "Node " << id << ": Raft RPC " << rpc_port
              << ", client " << client_port << std::endl;

    transport.serve(client_port, [&](const std::string& command) -> std::string {
        if (!healthy.load()) return "ERROR RECOVERY_REQUIRED";
        const auto tokens = parse(command);
        if (tokens.empty()) return "ERROR EMPTY_COMMAND";

        if (tokens[0] == "get") {
            if (tokens.size() != 2) return "ERROR BAD_COMMAND";

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

            std::optional<uint64_t> round_id;
            while (std::chrono::steady_clock::now() < deadline)
            {
                if(!healthy.load()){
                     return "ERROR RECOVERY_REQUIRED";
                }

                if(node.get_state() != NodeState::leader){
                    return "NOT_LEADER";
                }

                round_id = node.begin_read_round();
                if(round_id.has_value()){
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            if(!round_id.has_value()){
                 return "TIMEOUT";
            }

            // Confirm leadership with a fresh majority before serving the captured index.
            std::optional<uint64_t> read_index;
            while(std::chrono::steady_clock::now() < deadline){
                if(!healthy.load() || node.get_state() != NodeState::leader){
                    break;
                }

                read_index = node.get_confirmed_read_index(*round_id);
                if(read_index.has_value()){
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            if(!read_index.has_value()){
                node.finish_read_round(*round_id);
                return "TIMEOUT_OR_LEADERSHIP_CHANGED";
            }

            while (std::chrono::steady_clock::now() < deadline)
            {
                if(!healthy.load() || node.get_state() != NodeState::leader){
                    node.finish_read_round(*round_id);
                    return "ERROR_LEADERSHIP_CHANGED_OR_UNHEALTHY";
                }

                if (node.get_last_applied() >= *read_index){
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            if(node.get_last_applied() < *read_index){
                node.finish_read_round(*round_id);
                return "TIMEOUT";
            }
            

            
            std::string result ;
            bool valid = false;

            {
                std::lock_guard<std::mutex> guard(db_mutex);
                // Match the apply thread's lock order: database first, then node.
                valid = healthy.load() && node.read_if_valid(*round_id, *read_index, [&]{
                    result = db.get(tokens[1]);
                });

            }

            node.finish_read_round(*round_id);
            if(!valid){
                return "ERROR_LEADERSHIP_CHANGED_OR_UNHEALTHY";
            }

            return result.empty() ? "NOT_FOUND" : result;
        }

        if (!db.is_valid_raft_command(command)) return "ERROR BAD_COMMAND";
        // Capture the appended index and term atomically under the node mutex.
        auto appended = node.append_command_with_index(command);
        if (!appended) return "NOT_LEADER";

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            if (!healthy.load()) return "ERROR RECOVERY_REQUIRED";
            RaftLogEntry stored;
            if (!node.get_log_entry(appended->index, stored) ||
                stored.term != appended->term || stored.command != command) {
                return "ERROR LEADERSHIP_CHANGED";
            }
            if (node.get_last_applied() >= appended->index) {
                auto  result = machine.get_result_by_index(appended->index);

                if (result.has_value()){
                    return *result;
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        // TIMEOUT is ambiguous: the write may commit later. Don't retry
        // non-idempotent operations blindly without request IDs/deduplication.
        return "TIMEOUT UNKNOWN_OUTCOME";
    });

    healthy = false;
    runner.stop();
    if (apply_thread.joinable()) apply_thread.join();
    if (raft_thread.joinable()) raft_thread.join();
    // RPC serve() has no stop mechanism, so joining here would hang.
    // A production version needs a stoppable transport and orderly shutdown.
    rpc_thread.detach();
    return 0;
}
