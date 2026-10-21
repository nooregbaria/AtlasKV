#include "RaftLog.h"
#include <cassert>
#include <cstdio>
#include <iostream>
#include <unistd.h>

int main() {
    const std::string path = "test_raft.log";

    std::remove(path.c_str());

    // First instance: write entries.
    {
        RaftLog log(path);

        assert(log.append({1, ""})); // Leader no-op must survive recovery.
        assert(log.append({1, "PUT name Noor"}));
        assert(log.append({1, "PUT age 23"}));
        assert(log.append({2, "REMOVE name"}));

        assert(log.last_index() == 4);
        assert(log.last_term() == 2);
    }

    // Simulate restart: create a new object.
    {
        RaftLog log(path);

        assert(log.load());

        assert(log.last_index() == 4);
        assert(log.last_term() == 2);
        RaftLogEntry entry;
        assert(log.get_entry(1, entry));
        assert(entry.term == 1 && entry.command.empty());
        assert(log.get_entry(2, entry));
        assert(entry.command == "PUT name Noor");
    }

    // Corrupt the first byte of the log.
{
    FILE* file = fopen(path.c_str(), "r+b");
    assert(file != nullptr);

    unsigned char corrupted_byte = 0xFF;

    assert(fwrite(&corrupted_byte, 1, 1, file) == 1);
    assert(fclose(file) == 0);
}

// Recovery must reject the corrupted log.
{
    RaftLog log(path);
    assert(!log.load());
}

std::cout << "PASS: Corrupted log rejected\n";

{
    const std::string truncated_path = "test_truncated.log";
    std::remove(truncated_path.c_str());

    {
        RaftLog log(truncated_path);
        assert(log.append({1, "PUT name Noor"}));
        assert(log.append({2, "PUT age 23"}));
    }

    // Remove the last 3 bytes to simulate an interrupted write.
    FILE* file = fopen(truncated_path.c_str(), "r+b");
    assert(file != nullptr);

    assert(fseek(file, -3, SEEK_END) == 0);
    long new_size = ftell(file);
    assert(new_size >= 0);
    assert(fclose(file) == 0);

    assert(truncate(truncated_path.c_str(), new_size) == 0);

    {
        RaftLog log(truncated_path);
        assert(!log.load());
    }

    std::remove(truncated_path.c_str());
    std::cout << "PASS: Incomplete record rejected\n";
}

    {
    const std::string path = "test_truncate.log";
    std::remove(path.c_str());

    {
        RaftLog log(path);

        assert(log.append({1, "PUT a 10"}));
        assert(log.append({1, "PUT b 20"}));
        assert(log.append({2, "PUT c 30"}));
        assert(log.append({2, "PUT d 40"}));

        assert(log.truncate_from(3));

        assert(log.last_index() == 2);
        assert(log.last_term() == 1);
    }

    // Verify truncation survives restart.
    {
        RaftLog log(path);

        assert(log.load());
        assert(log.last_index() == 2);
        assert(log.last_term() == 1);
    }

    std::remove(path.c_str());

    std::cout << "PASS: RaftLog truncation test\n";
}

    std::cout << "PASS: RaftLog recovery test\n";
}
