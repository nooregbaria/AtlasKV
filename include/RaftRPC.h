#pragma once

#include <string>
#include "RaftNode.h"

class RaftRPC {
private:
    RaftNode& node;

public:
    explicit RaftRPC(RaftNode& node);

    std::string handle_message(const std::string& message);
};