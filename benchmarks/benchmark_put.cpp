#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <algorithm>
#include "protocol.h"
#include <cmath>
#include <thread>
#include <mutex>

constexpr int NUM_CLIENTS = 5;
constexpr int NUM_REQUESTS = 100; // PUT requests per client.
std::mutex output_mutex;

struct ClientStats
{
    int successful_requests = 0;
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;
};

int run_client(int client_id, const std::string& run_id, ClientStats& stats)
{
    const std::string key_prefix = "bench_put_" + run_id + "_" +
                                   std::to_string(client_id) + "_";

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd == -1)
    {
        perror("socket failed");
        return 1;
    }
    // Prevent SIGPIPE when a peer disconnects on macOS.
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE,
                   &one, sizeof(one)) == -1)
    {
        perror("setsockopt failed");
        close(fd);
        return 1;
    }

        sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(8081);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) == -1)
    {
        perror("connect failed");
        close(fd);
        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(output_mutex);
        std::cout << "Client " << client_id << " connected to AtlasKV!\n";
    }

    std::string response;
    if (!send_message(fd, "put benchmark_key hello") ||
        !receive_message(fd, response))
    {
        std::cerr << "Benchmark setup failed: could not exchange messages with the server\n";
        close(fd);
        return 1;
    }
    if (response != "OK" && response != "KEY_ALREADY_EXISTS")
    {
        std::cerr << "Benchmark setup failed: unexpected PUT response: " << response << '\n';
        close(fd);
        return 1;
    }

    // An existing key may have persisted from an earlier benchmark run.
    if (!send_message(fd, "get benchmark_key") ||
        !receive_message(fd, response))
    {
        std::cerr << "Benchmark setup failed: could not read benchmark_key\n";
        close(fd);
        return 1;
    }
    if (response != "hello")
    {
        std::cerr << "Benchmark setup failed: expected hello, received: " << response << '\n';
        close(fd);
        return 1;
    }

    std::vector<double> latencies;

    latencies.reserve(NUM_REQUESTS);
    int successful_requests = 0;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < NUM_REQUESTS; i++)
    {
        std::string key = key_prefix + std::to_string(i);
        std::string request = "put " + key + " hello";
        auto request_start = std::chrono::steady_clock::now();
        std::string response;
        if (!send_message(fd, request) ||
            !receive_message(fd, response))
        {
            std::cerr << "Benchmark failed: could not exchange messages with the server\n";
            close(fd);
            return 1;
        }
        auto request_end = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> latency = request_end - request_start;
        latencies.push_back(latency.count());
        if (response != "OK")
        {
            std::cerr << "Benchmark failed: expected OK, received: " << response << " for key: " << key << '\n';
            close(fd);
            return 1;
        }
        ++successful_requests;
    }
     auto end = std::chrono::steady_clock::now();
    stats = {successful_requests, start, end};

    double sum = 0;

    for (double latency : latencies)
    {
        sum += latency;
    }

    double average_latency = sum / latencies.size();
    auto percentile = [&](double p) {
        size_t index = static_cast<size_t>(std::ceil(p * latencies.size())) - 1;
        return latencies[index];
    };

    std::sort(latencies.begin(), latencies.end());
    double p50 = percentile(0.50);
    double p95 = percentile(0.95);
    double p99 = percentile(0.99);

    {
    std::lock_guard<std::mutex> lock(output_mutex);
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nClient " << client_id << ":\n";
    std::cout << "Average latency: " << average_latency << " ms\n";
    std::cout << "P50 latency: " << p50 << " ms\n";
    std::cout << "P95 latency: " << p95 << " ms\n";
    std::cout << "P99 latency: " << p99 << " ms\n";

    std::chrono::duration<double> elapsed = end - start;
    double total_seconds = elapsed.count();

    double throughput = NUM_REQUESTS / total_seconds;
    std::cout << "PUT requests: " << NUM_REQUESTS << '\n';
    std::cout << "Successful requests: " << successful_requests << '\n';
    std::cout << "Total time: " << total_seconds << " seconds\n";
    std::cout << "Throughput: " << throughput << " requests/sec\n";
    }

    for(int i=0 ; i<NUM_REQUESTS; ++i)
    {
    std::string key = key_prefix + std::to_string(i);
    std::string request = "remove " + key;
    if (!send_message(fd, request) ||
        !receive_message(fd, response))
    {
        std::cerr << "Cleanup stopped at " << key << "\n";
        close(fd);
        return 1;
    }
   }  

    close(fd);
    return 0;

}

int main()
{
    // Give every run its own keys, including after an interrupted benchmark.
    const std::string run_id = std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());
    std::vector<int> results(NUM_CLIENTS, 1);
    std::vector<ClientStats> stats(NUM_CLIENTS);
    std::vector<std::thread> clients;
    clients.reserve(NUM_CLIENTS);

    for (int i = 0; i < NUM_CLIENTS; ++i)
    {
        clients.emplace_back([&, i] {
            results[i] = run_client(i, run_id, stats[i]);
        });
    }

    for (auto& client : clients)
    {
        client.join();
    }

    for (int i = 0; i < NUM_CLIENTS; ++i)
    {
        if (results[i] != 0)
        {
            std::cerr << "Benchmark failed for client " << i << '\n';
            return 1;
        }
    }

    // Measure from the first client's PUT loop start to the last one's end.
    auto first_start = stats.front().start;
    auto last_end = stats.front().end;
    int total_successful_requests = 0;
    for (const auto& client_stats : stats)
    {
        first_start = std::min(first_start, client_stats.start);
        last_end = std::max(last_end, client_stats.end);
        total_successful_requests += client_stats.successful_requests;
    }
    const std::chrono::duration<double> elapsed = last_end - first_start;
    const double combined_throughput = total_successful_requests / elapsed.count();
    std::cout << "\nTotal successful PUT requests: " << total_successful_requests << '\n';
    std::cout << "Combined throughput: " << combined_throughput << " requests/sec\n";
    return 0;
}
