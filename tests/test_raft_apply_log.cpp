
#include "RaftApplyLog.h"

#include <cassert>
#include <cstdio>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

void reset_file(const std::string& path) {
    std::remove(path.c_str());
}

void test_empty_journal() {
    const std::string path = "test_empty.log";
    reset_file(path);

    RaftApplyLog log(path);
    std::vector<AppliedRaftEntry> entries;

    assert(log.recover(entries));
    assert(entries.empty());
    assert(log.get_last_index() == 0);

    std::cout << "PASS: Empty journal\n";
    reset_file(path);
}

void test_normal_recovery() {
    const std::string path = "test_normal.log";
    reset_file(path);

    {
        RaftApplyLog log(path);

        assert(log.append(1, "put apple 10"));
        assert(log.append(2, "put apple 20"));
        assert(log.append(3, "remove apple"));
        assert(log.get_last_index() == 3);
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(entries.size() == 3);

        assert(entries[0].index == 1);
        assert(entries[0].command == "put apple 10");
        assert(entries[1].index == 2);
        assert(entries[1].command == "put apple 20");
        assert(entries[2].index == 3);
        assert(entries[2].command == "remove apple");

        assert(log.get_last_index() == 3);
    }

    std::cout << "PASS: Normal recovery\n";
    reset_file(path);
}

void test_duplicate_and_skipped_indices() {
    const std::string path = "test_indices.log";
    reset_file(path);

    {
        RaftApplyLog log(path);

        assert(!log.append(2, "put a 10"));
        assert(log.append(1, "put a 10"));
        assert(!log.append(1, "put b 20"));
        assert(!log.append(3, "put c 30"));
        assert(log.append(2, "put b 20"));

        assert(log.get_last_index() == 2);
    }

    std::cout << "PASS: Duplicate and skipped indices\n";
    reset_file(path);
}

void test_corrupted_crc() {
    const std::string path = "test_crc.log";
    reset_file(path);

    {
        RaftApplyLog log(path);
        assert(log.append(1, "put apple 10"));
    }

    {
        std::fstream file(
            path, std::ios::binary |
                  std::ios::in | std::ios::out
        );

        assert(file.is_open());
        file.seekp(12);

        char corrupted = 'X';
        file.write(&corrupted, 1);
        assert(file.good());
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(!log.recover(entries));
        assert(entries.empty());
    }

    std::cout << "PASS: Corrupted CRC32 rejected\n";
    reset_file(path);
}

void test_incomplete_tail_repair() {
    const std::string path = "test_tail.log";
    reset_file(path);

    {
        RaftApplyLog log(path);
        assert(log.append(1, "put apple 10"));
        assert(log.append(2, "put banana 20"));
    }

    const auto original_size = fs::file_size(path);

    {
        std::ofstream file(path, std::ios::binary | std::ios::app);
        assert(file.is_open());

        const char partial[] = {0, 0, 0, 0, 0};
        file.write(partial, sizeof(partial));
        assert(file.good());
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(entries.size() == 2);
        assert(log.get_last_index() == 2);
        assert(fs::file_size(path) == original_size);

        assert(log.append(3, "remove apple"));
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(entries.size() == 3);
        assert(entries[2].command == "remove apple");
    }

    std::cout << "PASS: Incomplete tail repaired\n";
    reset_file(path);
}

void test_incomplete_command_rejected() {
    const std::string path = "test_incomplete.log";
    reset_file(path);

    {
        RaftApplyLog log(path);
        assert(log.append(1, "put apple 10"));
    }

    {
        std::ofstream file(path, std::ios::binary | std::ios::app);
        assert(file.is_open());

        // Header claims 10 command bytes, but only 5 exist.
        const unsigned char partial[] = {
            0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 10,
            'p', 'u', 't', ' ', 'x'
        };

        file.write(
            reinterpret_cast<const char*>(partial),
            sizeof(partial)
        );
        assert(file.good());
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(!log.recover(entries));
        assert(entries.empty());
    }

    std::cout << "PASS: Incomplete command rejected\n";
    reset_file(path);
}

void test_append_after_recovery() {
    const std::string path = "test_resume.log";
    reset_file(path);

    {
        RaftApplyLog log(path);
        assert(log.append(1, "put a 10"));
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(log.get_last_index() == 1);

        assert(log.append(2, "put b 20"));
        assert(log.append(3, "remove a"));
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(entries.size() == 3);
        assert(log.get_last_index() == 3);
    }

    std::cout << "PASS: Append after recovery\n";
    reset_file(path);
}

void test_binary_command() {
    const std::string path = "test_binary.log";
    reset_file(path);

    const std::string command("ab\0cd\nxy", 8);

    {
        RaftApplyLog log(path);
        assert(log.append(1, command));
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(log.recover(entries));
        assert(entries.size() == 1);
        assert(entries[0].command == command);
    }

    std::cout << "PASS: Binary command round-trip\n";
    reset_file(path);
}

void test_corrupted_index() {
    const std::string path = "test_bad_index.log";
    reset_file(path);

    {
        RaftApplyLog log(path);
        assert(log.append(1, "put a 10"));
    }

    {
        std::fstream file(
            path, std::ios::binary |
                  std::ios::in | std::ios::out
        );

        assert(file.is_open());

        // Change index 1 to index 2 without updating CRC.
        file.seekp(7);
        const char changed = 2;
        file.write(&changed, 1);
        assert(file.good());
    }

    {
        RaftApplyLog log(path);
        std::vector<AppliedRaftEntry> entries;

        assert(!log.recover(entries));
        assert(entries.empty());
    }

    std::cout << "PASS: Corrupted index rejected\n";
    reset_file(path);
}

int main() {
    test_empty_journal();
    test_normal_recovery();
    test_duplicate_and_skipped_indices();
    test_corrupted_crc();
    test_incomplete_tail_repair();
    test_incomplete_command_rejected();
    test_append_after_recovery();
    test_binary_command();
    test_corrupted_index();

    std::cout << "\nALL 9 RAFT APPLICATION JOURNAL TESTS PASSED!\n";
    return 0;
}
