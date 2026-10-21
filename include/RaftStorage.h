#pragma once 

#include <cstdint>
#include <string>

enum class LoadStatus{NotFound, Success, Error} ;

struct PersistentRaftState  {
    std::uint64_t current_term;
    std::int32_t voted_for;
};

class RaftStorage {
    std::string file_path;

    public:
    explicit RaftStorage (const std::string& path);
    bool save(const PersistentRaftState& state);
    LoadStatus load(PersistentRaftState& state);
};