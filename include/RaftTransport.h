#pragma once
#include <functional>
#include <string>

class RaftTransport{
    public:
    bool request(const std::string& ip, int port, const std::string& message, std::string& response);
    // Serve one request per accepted connection; the listener runs until process exit.
    void serve(int port, const std::function<std::string(const std::string&)>& handler);
};
