#include "RaftRPC.h"
#include "RaftProtocol.h"

RaftRPC::RaftRPC(RaftNode& node) : node(node) {}

std::string RaftRPC::handle_message(const std::string& message) {
    if (message.empty()) {
        return "";
    }

    auto type = static_cast<raft_wire::MessageType>(static_cast<uint8_t>(message[0]));

    switch (type) {
        case raft_wire::MessageType::RequestVote: {
            std::optional<RequestVoteArgs> result =  raft_wire::decode_request_vote(message);
            if(!result.has_value()){
                return "";
            }
            RequestVoteReply reply = node.handle_request_vote(result.value());
            return raft_wire::encode(reply);
        }

        case raft_wire::MessageType::AppendEntries: {
            std::optional<AppendEntriesArgs> result =  raft_wire::decode_append_entries(message);
            if(!result.has_value()){
                return "";
            }
            AppendEntriesReply reply = node.handle_append_entries(result.value());
            return raft_wire::encode(reply);
        }

        default:
            return "";
    }

    return "";
}