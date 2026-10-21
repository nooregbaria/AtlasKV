// Historical wire-size assertions are retained unchanged; see docs/TESTING.md.
#include "RaftProtocol.h"
#include <cassert>
#include <iostream>
#include <string>

void equal_requests(const AppendEntriesArgs& a, const AppendEntriesArgs& b) {
    assert(a.term == b.term && a.leader_id == b.leader_id);
    assert(a.prev_log_index == b.prev_log_index && a.prev_log_term == b.prev_log_term);
    assert(a.entries.size() == b.entries.size());
    for (size_t i = 0; i < a.entries.size(); ++i) {
        assert(a.entries[i].term == b.entries[i].term);
        assert(a.entries[i].command == b.entries[i].command);
    }
}

int main() {
    AppendEntriesArgs many{7, 2, 5, 4, {{4, "PUT A 10"}, {7, "PUT B 20"}, {7, "REMOVE A"}}};
    auto decoded = raft_wire::decode_append_entries(raft_wire::encode(many));
    assert(decoded); equal_requests(many, *decoded);
    std::cout << "PASS: multiple entries round-trip\n";

    AppendEntriesArgs heartbeat{3, 1, 0, 0, {}};
    auto encoded_heartbeat = raft_wire::encode(heartbeat);
    assert(encoded_heartbeat.size() == 33);
    decoded = raft_wire::decode_append_entries(encoded_heartbeat);
    assert(decoded); equal_requests(heartbeat, *decoded);
    std::cout << "PASS: empty heartbeat\n";

    AppendEntriesArgs binary{9, 3, 1, 2, {{9, "PUT key hello world\n"}, {9, std::string("a\0b\0c", 5)}, {9, ""}}};
    decoded = raft_wire::decode_append_entries(raft_wire::encode(binary));
    assert(decoded); equal_requests(binary, *decoded);
    std::cout << "PASS: binary commands and empty command\n";

    auto encoded = raft_wire::encode(many);
    for (size_t n = 0; n < encoded.size(); ++n) {
        assert(!raft_wire::decode_append_entries(encoded.substr(0, n)));
    }
    std::cout << "PASS: truncated messages rejected\n";

    auto extra = encoded; extra.push_back('X');
    assert(!raft_wire::decode_append_entries(extra));
    std::cout << "PASS: extra bytes rejected\n";

    auto wrong_type = encoded; wrong_type[0] = 99;
    assert(!raft_wire::decode_append_entries(wrong_type));
    auto wrong_count = encoded; wrong_count[29] = 0x7f;
    assert(!raft_wire::decode_append_entries(wrong_count));
    auto wrong_length = encoded; wrong_length[41] = 0x7f;
    assert(!raft_wire::decode_append_entries(wrong_length));
    std::cout << "PASS: malformed type and lengths rejected\n";

    std::cout << "ALL 6 RAFT PROTOCOL TESTS PASSED!\n";
}
