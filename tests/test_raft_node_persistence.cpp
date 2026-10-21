#include "RaftNode.h"
#include "RaftStorage.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::string path = "test_node_raft.bin";
    std::filesystem::remove(path);
    std::filesystem::remove(path + ".tmp");

    {
        RaftNode node(2, path);
        node.start_election();
    } // Simulate a restart by destroying the first node.

    {
        RaftNode restarted(2, path);

        // Confirm the state was written to disk.
        RaftStorage storage(path);
        PersistentRaftState recovered{};
        assert(storage.load(recovered) == LoadStatus::Success);
        assert(recovered.current_term == 1);
        assert(recovered.voted_for == 2);

        // Confirm the restarted node actually loaded that state.
        assert(restarted.get_current_term() == 1);
        assert(restarted.get_voted_for() == 2);
    }

    std::filesystem::remove(path);
    std::filesystem::remove(path + ".tmp");
    std::cout << "PASS: RaftNode election state survives restart\n";
}
