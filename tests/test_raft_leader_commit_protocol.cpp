// Historical wire-size assertions are retained unchanged; see docs/TESTING.md.
#include "RaftProtocol.h"
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

int main() {
    // 1: A heartbeat carries the leader's committed index.
    AppendEntriesArgs heartbeat{4, 1, 0, 0, {}, 12};
    auto heartbeat_bytes = raft_wire::encode(heartbeat);
    auto decoded_heartbeat = raft_wire::decode_append_entries(heartbeat_bytes);
    assert(heartbeat_bytes.size() == 41);
    assert(decoded_heartbeat && decoded_heartbeat->entries.empty());
    assert(decoded_heartbeat->leader_commit == 12);
    assert(decoded_heartbeat->term == 4 && decoded_heartbeat->leader_id == 1);
    std::cout << "PASS: heartbeat round-trip (41 bytes)\n";

    // 2: Multiple entries carry a nonzero commit index.
    AppendEntriesArgs request{7, 2, 3, 6, {{7, "PUT A 10"}, {7, "PUT B 20"}}, 4};
    auto bytes = raft_wire::encode(request);
    auto decoded = raft_wire::decode_append_entries(bytes);
    assert(decoded && decoded->term == 7 && decoded->leader_id == 2);
    assert(decoded->prev_log_index == 3 && decoded->prev_log_term == 6);
    assert(decoded->entries.size() == 2);
    assert(decoded->entries[0].term == 7 && decoded->entries[0].command == "PUT A 10");
    assert(decoded->entries[1].term == 7 && decoded->entries[1].command == "PUT B 20");
    assert(decoded->leader_commit == 4);
    std::cout << "PASS: multiple entries and leader_commit round-trip\n";

    // 3: Commands containing zero bytes must survive encoding.
    AppendEntriesArgs binary{9, 1, 0, 0, {{9, std::string("A\0B", 3)}}, 1};
    auto decoded_binary = raft_wire::decode_append_entries(raft_wire::encode(binary));
    assert(decoded_binary && decoded_binary->entries.size() == 1);
    assert(decoded_binary->entries[0].command == std::string("A\0B", 3));
    assert(decoded_binary->leader_commit == 1);
    std::cout << "PASS: binary command round-trip\n";

    // 4: Every truncated prefix of a valid request must be rejected.
    for (size_t n = 0; n < bytes.size(); ++n) {
        assert(!raft_wire::decode_append_entries(bytes.substr(0, n)));
    }
    std::cout << "PASS: truncated messages rejected\n";

    // 5: Extra trailing bytes must be rejected.
    assert(!raft_wire::decode_append_entries(bytes + std::string(1, '\0')));
    std::cout << "PASS: trailing bytes rejected\n";

    // 6: Reject an entry count that cannot fit before the commit index and read-round ID.
    std::string malformed = heartbeat_bytes;
    malformed[29] = 0;
    malformed[30] = 0;
    malformed[31] = 0;
    malformed[32] = 1; // Claim one entry without providing its bytes.
    assert(!raft_wire::decode_append_entries(malformed));
    std::cout << "PASS: malformed entry count rejected\n";

    std::cout << "\nALL 6 LEADER COMMIT PROTOCOL TESTS PASSED!\n";
}
