#include "RaftRunner.h"
#include "RaftProtocol.h"

#include <chrono>
#include <thread>
#include <vector>

RaftRunner::RaftRunner(RaftNode& node, RaftTransport& transport)
    : node(node), transport(transport) {}

void RaftRunner::run() {

    heartbeat_workers_running = true;

    // Each peer has a worker so an unresponsive follower does not block the others.
    for(const Peer& peer : node.get_peers()){
        heartbeat_workers.emplace_back(&RaftRunner::heartbeat_worker, this,peer);
    }

    last_heartbeat = std::chrono::steady_clock::now();
    running = true;

    while (running) {
        if (node.try_start_election()) {

            RequestVoteArgs request = node.get_election_request();

            std::string message = raft_wire::encode(request);

            for(const Peer& peer : node.get_peers()){
                std::string response;

                if (node.get_state() != NodeState::candidate || node.get_current_term() != request.term) {
                    break;
                }

                bool success = transport.request(peer.ip, peer.port, message, response);

                if(!success){
                    continue;
                }

                auto reply = raft_wire::decode_request_vote_reply(response);

                if(!reply.has_value()){
                    continue;
                }

                node.handle_vote_reply(peer.id, request.term, *reply);
            }
        }


        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    heartbeat_workers_running = false;

    for(std::thread& worker : heartbeat_workers){
        if(worker.joinable()){
            worker.join();
        }
    }

    heartbeat_workers.clear();
}

void RaftRunner::stop() {
    running = false;
}

void RaftRunner::heartbeat_worker(Peer peer) {
    while (heartbeat_workers_running) {
        auto request = node.get_append_entries_request(peer.id);

        if(request.has_value()){
            
            std::string response ;
            auto round_id  = node.get_pending_read_round_id(request->term);
            if(round_id.has_value()){
                request->read_round_id  = *round_id;
            }
            
            bool success = transport.request(peer.ip, peer.port,raft_wire::encode(*request),response);

            if(success){
                auto reply = raft_wire::decode_append_entries_reply(response);
                if(reply.has_value()){
                    node.handle_append_entries_reply(peer.id, *request, *reply);

                    // Count only replies that echo this request's round and term.
                    if(request->read_round_id != 0 &&  request->read_round_id == reply->read_round_id && request->term == reply->term){
                        node.acknowledge_read_round(request->read_round_id, request->term, peer.id);
                    }
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}