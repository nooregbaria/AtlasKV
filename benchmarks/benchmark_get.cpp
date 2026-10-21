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

constexpr int NUM_REQUESTS = 10000; // GET requests per client.

int run_client(int client_id)
{

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

    std::cout << "Connected to AtlasKV!\n";

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
        auto request_start = std::chrono::steady_clock::now();
        std::string response;
        if (!send_message(fd, "get benchmark_key") ||
            !receive_message(fd, response))
        {
            std::cerr << "Benchmark failed: could not exchange messages with the server\n";
            close(fd);
            return 1;
        }
        auto request_end = std::chrono::steady_clock::now();
        std::chrono::duration<double, std::milli> latency = request_end - request_start;
        latencies.push_back(latency.count());
        if (response != "hello")
        {
            std::cerr << "Benchmark failed: expected hello, received: " << response << '\n';
            close(fd);
            return 1;
        }
        ++successful_requests;
    }
     auto end = std::chrono::steady_clock::now();

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
    std::cout << std::fixed << std::setprecision(2);

    std::sort(latencies.begin(), latencies.end());
    double p50 = percentile(0.50);
    double p95 = percentile(0.95);
    double p99 = percentile(0.99);

    std::cout << "Average latency: " << average_latency << " ms\n";
    std::cout << "P50 latency: " << p50 << " ms\n";
    std::cout << "P95 latency: " << p95 << " ms\n";
    std::cout << "P99 latency: " << p99 << " ms\n";

    std::chrono::duration<double> elapsed = end - start;
    double total_seconds = elapsed.count();

    double throughput = NUM_REQUESTS / total_seconds;
    std::cout << "GET requests: " << NUM_REQUESTS << '\n';
    std::cout << "Successful requests: " << successful_requests << '\n';
    std::cout << "Total time: " << total_seconds << " seconds\n";
    std::cout << "Throughput: " << throughput << " requests/sec\n";

    close(fd);
    return 0;

}



int main()
{
    constexpr int NUM_CLIENTS = 10;
    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;

    for(int i = 0; i < NUM_CLIENTS; ++i)
    {
        threads.emplace_back(run_client, i);
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();

    std::chrono::duration<double> elapsed = end_time - start_time;
    double total_requests = NUM_CLIENTS * NUM_REQUESTS;
    double combined_throughput = total_requests / elapsed.count();

    std::cout << "\nCombined throughput: " << combined_throughput << " requests/sec\n";


    return 0;
}
