// Historical wire-size assertions are retained unchanged; see docs/TESTING.md.

#include "RaftProtocol.h"

#include <cassert>
#include <climits>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>

void test_request_vote() {
    RequestVoteArgs original{5, 2, 100, 4};

    std::string bytes = raft_wire::encode(original);
    assert(bytes.size() == 29);

    auto decoded = raft_wire::decode_request_vote(bytes);

    assert(decoded.has_value());
    assert(decoded->term == original.term);
    assert(decoded->candidate_id == original.candidate_id);
    assert(decoded->last_log_index == original.last_log_index);
    assert(decoded->last_log_term == original.last_log_term);

    std::cout << "PASS: RequestVote round-trip\n";
}

void test_request_vote_reply() {
    for (bool granted : {false, true}) {
        RequestVoteReply original{5, granted};

        std::string bytes = raft_wire::encode(original);
        assert(bytes.size() == 10);

        auto decoded = raft_wire::decode_request_vote_reply(bytes);

        assert(decoded.has_value());
        assert(decoded->term == original.term);
        assert(decoded->vote_granted == original.vote_granted);
    }

    std::cout << "PASS: RequestVoteReply round-trip\n";
}

void test_append_entries() {
    AppendEntriesArgs original{5, 2};

    std::string bytes = raft_wire::encode(original);
    assert(bytes.size() == 13);

    auto decoded = raft_wire::decode_append_entries(bytes);

    assert(decoded.has_value());
    assert(decoded->term == original.term);
    assert(decoded->leader_id == original.leader_id);

    std::cout << "PASS: AppendEntries round-trip\n";
}

void test_append_entries_reply() {
    for (bool success : {false, true}) {
        AppendEntriesReply original{5, success};

        std::string bytes = raft_wire::encode(original);
        assert(bytes.size() == 10);

        auto decoded = raft_wire::decode_append_entries_reply(bytes);

        assert(decoded.has_value());
        assert(decoded->term == original.term);
        assert(decoded->success == original.success);
    }

    std::cout << "PASS: AppendEntriesReply round-trip\n";
}

void test_truncated_messages() {
    auto vote = raft_wire::encode(RequestVoteArgs{5, 2, 100, 4});
    auto vote_reply = raft_wire::encode(RequestVoteReply{5, true});
    auto heartbeat = raft_wire::encode(AppendEntriesArgs{5, 2});
    auto heartbeat_reply =
        raft_wire::encode(AppendEntriesReply{5, true});

    for (size_t n = 0; n < vote.size(); ++n) {
        assert(!raft_wire::decode_request_vote(
            vote.substr(0, n)).has_value());
    }

    for (size_t n = 0; n < vote_reply.size(); ++n) {
        assert(!raft_wire::decode_request_vote_reply(
            vote_reply.substr(0, n)).has_value());
    }

    for (size_t n = 0; n < heartbeat.size(); ++n) {
        assert(!raft_wire::decode_append_entries(
            heartbeat.substr(0, n)).has_value());
    }

    for (size_t n = 0; n < heartbeat_reply.size(); ++n) {
        assert(!raft_wire::decode_append_entries_reply(
            heartbeat_reply.substr(0, n)).has_value());
    }

    std::cout << "PASS: Truncated messages rejected\n";
}

void test_extra_bytes() {
    auto vote = raft_wire::encode(RequestVoteArgs{5, 2, 100, 4});
    auto vote_reply = raft_wire::encode(RequestVoteReply{5, true});
    auto heartbeat = raft_wire::encode(AppendEntriesArgs{5, 2});
    auto heartbeat_reply =
        raft_wire::encode(AppendEntriesReply{5, true});

    vote.push_back('\0');
    vote_reply.push_back('\0');
    heartbeat.push_back('\0');
    heartbeat_reply.push_back('\0');

    assert(!raft_wire::decode_request_vote(vote).has_value());
    assert(!raft_wire::decode_request_vote_reply(vote_reply).has_value());
    assert(!raft_wire::decode_append_entries(heartbeat).has_value());
    assert(!raft_wire::decode_append_entries_reply(
        heartbeat_reply).has_value());

    std::cout << "PASS: Extra bytes rejected\n";
}

void test_wrong_message_types() {
    auto vote = raft_wire::encode(RequestVoteArgs{5, 2, 100, 4});
    auto vote_reply = raft_wire::encode(RequestVoteReply{5, true});
    auto heartbeat = raft_wire::encode(AppendEntriesArgs{5, 2});
    auto heartbeat_reply =
        raft_wire::encode(AppendEntriesReply{5, true});

    vote[0] = static_cast<char>(99);
    vote_reply[0] = static_cast<char>(99);
    heartbeat[0] = static_cast<char>(99);
    heartbeat_reply[0] = static_cast<char>(99);

    assert(!raft_wire::decode_request_vote(vote).has_value());
    assert(!raft_wire::decode_request_vote_reply(vote_reply).has_value());
    assert(!raft_wire::decode_append_entries(heartbeat).has_value());
    assert(!raft_wire::decode_append_entries_reply(
        heartbeat_reply).has_value());

    std::cout << "PASS: Wrong message types rejected\n";
}

void test_invalid_boolean_values() {
    auto vote_reply =
        raft_wire::encode(RequestVoteReply{5, true});

    vote_reply[9] = static_cast<char>(2);

    assert(!raft_wire::decode_request_vote_reply(
        vote_reply).has_value());

    auto heartbeat_reply =
        raft_wire::encode(AppendEntriesReply{5, true});

    heartbeat_reply[9] = static_cast<char>(255);

    assert(!raft_wire::decode_append_entries_reply(
        heartbeat_reply).has_value());

    std::cout << "PASS: Invalid boolean values rejected\n";
}

void test_invalid_node_ids() {
    auto vote = raft_wire::encode(RequestVoteArgs{5, 2, 100, 4});
    auto heartbeat = raft_wire::encode(AppendEntriesArgs{5, 2});

    // Overwrite ID fields with UINT32_MAX.
    for (size_t i = 9; i < 13; ++i) {
        vote[i] = static_cast<char>(0xFF);
        heartbeat[i] = static_cast<char>(0xFF);
    }

    assert(!raft_wire::decode_request_vote(vote).has_value());
    assert(!raft_wire::decode_append_entries(heartbeat).has_value());

    std::cout << "PASS: Out-of-range node IDs rejected\n";
}

void test_boundary_values() {
    RequestVoteArgs vote{
        std::numeric_limits<uint64_t>::max(),
        INT_MAX,
        std::numeric_limits<uint64_t>::max(),
        std::numeric_limits<uint64_t>::max()
    };

    auto decoded_vote =
        raft_wire::decode_request_vote(raft_wire::encode(vote));

    assert(decoded_vote.has_value());
    assert(decoded_vote->term == vote.term);
    assert(decoded_vote->candidate_id == vote.candidate_id);
    assert(decoded_vote->last_log_index == vote.last_log_index);
    assert(decoded_vote->last_log_term == vote.last_log_term);

    AppendEntriesArgs heartbeat{
        std::numeric_limits<uint64_t>::max(),
        INT_MAX
    };

    auto decoded_heartbeat =
        raft_wire::decode_append_entries(
            raft_wire::encode(heartbeat));

    assert(decoded_heartbeat.has_value());
    assert(decoded_heartbeat->term == heartbeat.term);
    assert(decoded_heartbeat->leader_id == heartbeat.leader_id);

    std::cout << "PASS: Boundary values\n";
}

void test_big_endian_encoding() {
    RequestVoteArgs vote{
        0x0102030405060708ULL,
        0x01020304,
        0,
        0
    };

    std::string bytes = raft_wire::encode(vote);

    assert(static_cast<uint8_t>(bytes[0]) == 1);

    // Term must be encoded most-significant byte first.
    for (int i = 0; i < 8; ++i) {
        assert(static_cast<uint8_t>(bytes[1 + i]) ==
               static_cast<uint8_t>(i + 1));
    }

    // Candidate ID must also be big-endian.
    for (int i = 0; i < 4; ++i) {
        assert(static_cast<uint8_t>(bytes[9 + i]) ==
               static_cast<uint8_t>(i + 1));
    }

    std::cout << "PASS: Big-endian encoding\n";
}

int main() {
    test_request_vote();
    test_request_vote_reply();
    test_append_entries();
    test_append_entries_reply();
    test_truncated_messages();
    test_extra_bytes();
    test_wrong_message_types();
    test_invalid_boolean_values();
    test_invalid_node_ids();
    test_boundary_values();
    test_big_endian_encoding();

    std::cout << "\nALL 11 RAFT PROTOCOL TESTS PASSED!\n";
    return 0;
}
