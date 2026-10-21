#include "protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {
constexpr int PORT = 8081;
constexpr int CLIENTS = 20;

int connect_to_server() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

#ifdef SO_NOSIGPIPE
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0) {
        close(fd);
        return -1;
    }
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

bool request(int fd, const std::string& command, std::string& response) {
    return send_message(fd, command) && receive_message(fd, response);
}

bool run_request(const std::string& command, std::string& response) {
    int fd = connect_to_server();
    if (fd < 0) return false;
    bool ok = request(fd, command, response);
    close(fd);
    return ok;
}

// Each worker connects first, then waits until all workers are ready.
// This ensures the requests start together rather than one at a time.
bool concurrent_put_test(const std::string& key) {
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<int> ok_count{0};
    std::atomic<int> exists_count{0};
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    threads.reserve(CLIENTS);

    for (int i = 0; i < CLIENTS; ++i) {
        threads.emplace_back([&, i] {
            int fd = connect_to_server();
            if (fd < 0) {
                ++errors;
                ++ready;
                return;
            }
            ++ready;
            while (!go.load()) std::this_thread::yield();

            std::string response;
            if (!request(fd, "put " + key + " value" + std::to_string(i), response)) {
                ++errors;
            } else if (response == "OK") {
                ++ok_count;
            } else if (response == "KEY_ALREADY_EXISTS") {
                ++exists_count;
            } else {
                ++errors;
            }
            close(fd);
        });
    }

    while (ready.load() != CLIENTS) std::this_thread::yield();
    go = true;
    for (auto& t : threads) t.join();

    const bool passed = ok_count == 1 && exists_count == CLIENTS - 1 && errors == 0;
    std::cout << (passed ? "PASS" : "FAIL") << " concurrent PUT: "
              << ok_count << " OK, " << exists_count << " KEY_ALREADY_EXISTS, "
              << errors << " errors\n";
    return passed;
}

bool concurrent_get_remove_test(const std::string& key) {
    std::string response;
    if (!run_request("put " + key + " hello", response) || response != "OK") {
        std::cout << "FAIL GET/REMOVE setup\n";
        return false;
    }

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::string get_result, remove_result;
    bool get_ok = false, remove_ok = false;

    auto worker = [&](bool is_get) {
        int fd = connect_to_server();
        if (fd < 0) {
            ++ready;
            return;
        }
        ++ready;
        while (!go.load()) std::this_thread::yield();
        std::string result;
        bool ok = request(fd, std::string(is_get ? "get " : "remove ") + key, result);
        if (is_get) {
            get_result = result;
            get_ok = ok;
        } else {
            remove_result = result;
            remove_ok = ok;
        }
        close(fd);
    };

    std::thread reader(worker, true);
    std::thread remover(worker, false);
    while (ready.load() != 2) std::this_thread::yield();
    go = true;
    reader.join();
    remover.join();

    std::string after;
    bool final_check = run_request("get " + key, after) && after == "NOT_FOUND";
    bool passed = get_ok && remove_ok && remove_result == "OK" &&
                  (get_result == "hello" || get_result == "NOT_FOUND") && final_check;

    std::cout << (passed ? "PASS" : "FAIL") << " concurrent GET/REMOVE: GET="
              << (get_ok ? get_result : "<network error>") << ", REMOVE="
              << (remove_ok ? remove_result : "<network error>")
              << ", final GET=" << after << "\n";
    return passed;
}

bool disconnect_test(const std::string& key) {
    int fd = connect_to_server();
    if (fd < 0) {
        std::cout << "FAIL disconnect: could not connect\n";
        return false;
    }
    close(fd); // Disconnect without sending a command.

    std::string response;
    bool passed = run_request("put " + key + " alive", response) && response == "OK";
    std::cout << (passed ? "PASS" : "FAIL") << " disconnect: server still responds\n";
    return passed;
}
} // namespace

int main() {
    // Unique keys across test runs, including when WAL persistence is enabled.
    const std::string suffix = std::to_string(getpid()) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    if (int fd = connect_to_server(); fd < 0) {
        std::cerr << "Cannot connect to AtlasKV on 127.0.0.1:8081. Start the server first.\n";
        return 1;
    } else {
        close(fd);
    }

    bool a = concurrent_put_test("stress_put_" + suffix);
    bool b = concurrent_get_remove_test("stress_race_" + suffix);
    bool c = disconnect_test("stress_disconnect_" + suffix);

    std::cout << "\n" << (a && b && c ? "ALL TESTS PASSED" : "SOME TESTS FAILED") << '\n';
    return a && b && c ? 0 : 1;
}
