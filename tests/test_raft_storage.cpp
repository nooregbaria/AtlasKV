#include "RaftStorage.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static void clean(const std::string& path) {
    fs::remove(path);
    fs::remove(path + ".tmp");
}

static std::vector<unsigned char> read_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

static void write_bytes(const std::string& path, const std::vector<unsigned char>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(out.good());
}

static void expect_error_unchanged(RaftStorage& storage) {
    PersistentRaftState state{999, 42};
    assert(storage.load(state) == LoadStatus::Error);
    assert(state.current_term == 999);
    assert(state.voted_for == 42);
}

int main() {
    const std::string path = "test_raft_state.bin";
    clean(path);
    RaftStorage storage(path);

    // Missing file: NotFound, and output is unchanged.
    {
        PersistentRaftState state{999, 42};
        assert(storage.load(state) == LoadStatus::NotFound);
        assert(state.current_term == 999 && state.voted_for == 42);
        std::cout << "PASS: missing file\n";
    }

    // Normal save/load, including -1 (no vote).
    {
        assert(storage.save(PersistentRaftState{5, -1}));
        assert(fs::file_size(path) == 20);
        assert(!fs::exists(path + ".tmp"));
        PersistentRaftState state{0, 0};
        assert(storage.load(state) == LoadStatus::Success);
        assert(state.current_term == 5 && state.voted_for == -1);
        std::cout << "PASS: save/load with no vote\n";
    }

    // Overwrite state and verify that the newest term/vote are recovered.
    {
        assert(storage.save(PersistentRaftState{17, 2}));
        PersistentRaftState state{0, -1};
        assert(storage.load(state) == LoadStatus::Success);
        assert(state.current_term == 17 && state.voted_for == 2);
        std::cout << "PASS: overwrite and reload\n";
    }

    const auto valid = read_bytes(path);
    assert(valid.size() == 20);

    // Corrupt magic.
    {
        auto bytes = valid;
        bytes[0] ^= 0x01;
        write_bytes(path, bytes);
        expect_error_unchanged(storage);
        std::cout << "PASS: invalid magic rejected\n";
    }

    // Corrupt payload, leaving stored CRC unchanged.
    {
        auto bytes = valid;
        bytes[4] ^= 0x01;
        write_bytes(path, bytes);
        expect_error_unchanged(storage);
        std::cout << "PASS: invalid checksum rejected\n";
    }

    // Truncated record.
    {
        auto bytes = valid;
        bytes.resize(12);
        write_bytes(path, bytes);
        expect_error_unchanged(storage);
        std::cout << "PASS: truncated file rejected\n";
    }

    // Extra bytes after the 20-byte record.
    {
        auto bytes = valid;
        bytes.push_back(0xAB);
        write_bytes(path, bytes);
        expect_error_unchanged(storage);
        std::cout << "PASS: extra bytes rejected\n";
    }

    clean(path);
    std::cout << "ALL RAFT STORAGE TESTS PASSED\n";
}
