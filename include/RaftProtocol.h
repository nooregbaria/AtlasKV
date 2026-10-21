#pragma once 

#include <string>
#include <optional>
#include <cstdint>
#include "RaftNode.h"

// RequestVote and RequestVoteReply exchange election votes.
// AppendEntries carries log entries or an empty heartbeat; AppendEntriesReply
// reports acceptance and echoes the read-round ID for quorum confirmation.

namespace raft_wire{
    enum class MessageType : uint8_t{
        RequestVote = 1,
        RequestVoteReply = 2,
        AppendEntries = 3,
        AppendEntriesReply = 4
    };

    std::string encode(const RequestVoteArgs& args);
    std::optional<RequestVoteArgs> decode_request_vote(const std::string& bytes);

    std::string encode(const RequestVoteReply& reply);
    std::optional<RequestVoteReply> decode_request_vote_reply(const std::string& bytes);

    std::string encode(const AppendEntriesArgs& args);
    std::optional<AppendEntriesArgs> decode_append_entries(const std::string& bytes);

    std::string encode(const AppendEntriesReply& reply);
    std::optional<AppendEntriesReply> decode_append_entries_reply(const std::string& bytes);
}