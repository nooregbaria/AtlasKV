# AtlasKV — Performance Benchmarks

> Historical measurements: the tables below preserve the original recorded results.
> They describe the primary/replica server, not the Raft server. The current PUT
> benchmark uses 100 requests per client; GET and REMOVE use 10,000. Build the
> current sources with `make benchmarks`. Raw results remain in
> [`benchmarks/results/`](../benchmarks/results/).

## 1. Overview

This document presents performance measurements for AtlasKV, a persistent, multithreaded key-value store implemented in C++17.

The benchmarks evaluate three operations:

- **GET:** Retrieve a value associated with a key.
- **PUT:** Insert a new key-value pair.
- **REMOVE:** Delete an existing key.

The goal is to measure throughput, request latency, and the effect of increasing the number of concurrent clients.

## 2. Test Environment

- **Language:** C++17
- **Platform:** macOS
- **Network:** TCP over localhost (`127.0.0.1`)
- **Server architecture:** One thread per client connection
- **Synchronization:** Global mutex protecting database operations
- **Storage:** In-memory key-value store with a persistent write-ahead log (WAL)
- **Durability:** WAL writes synchronized using `fsync()`

All results were collected locally. They should not be interpreted as production or distributed-network performance.

## 3. Benchmark Methodology

Each operation was tested with three concurrency configurations:

- 1 concurrent client
- 5 concurrent clients
- 10 concurrent clients

Each client executed **10,000 requests** per run. Three runs were recorded for each configuration.

The benchmark measured:

- **Throughput:** Total successful requests divided by elapsed time, expressed in requests per second.
- **Average latency:** Mean request round-trip time.
- **P50 latency:** The median request latency.
- **P95 latency:** The latency at or below which approximately 95% of requests completed.
- **P99 latency:** The latency at or below which approximately 99% of requests completed.

For throughput comparisons, the **median of three runs** is reported to reduce the influence of unusually fast or slow measurements.

PUT requests used distinct keys, and REMOVE requests targeted existing keys. All reported benchmark requests completed successfully.

**Methodology note:** The current REMOVE benchmark prepares keys before timing,
waits for all clients to finish setup, and measures from the earliest REMOVE loop
start to the latest finish. The historical measurements below have not been rerun.

## 4. Results

### 4.1 Throughput

The table below reports median aggregate throughput across the three recorded runs.

| Operation | 1 Client | 5 Clients | 10 Clients |
|---|---:|---:|---:|
| GET | 14,424 req/s | 30,546 req/s | 40,599 req/s |
| PUT | 1,887 req/s | 3,082 req/s | 3,365 req/s |
| REMOVE | 2,916 req/s | 3,660 req/s | 4,078 req/s |

**Data note:** The second and third 10-client GET outputs provided for this report were identical. The reported median therefore remains provisional until the third run is verified.

### 4.2 Individual Throughput Measurements

| Operation | Clients | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|---:|
| GET | 1 | 18,780.75 | 14,423.92 | 9,688.59 |
| GET | 5 | 44,510.35 | 30,545.85 | 29,675.80 |
| GET | 10 | 48,111.80 | 40,599.30 | 40,599.30* |
| PUT | 1 | 2,435.83 | 1,761.42 | 1,887.04 |
| PUT | 5 | 3,082.09 | 3,226.22 | 2,931.03 |
| PUT | 10 | 3,446.09 | 3,364.96 | 2,763.48 |
| REMOVE | 1 | 2,916.38 | 2,171.97 | 3,056.32 |
| REMOVE | 5 | 3,631.72 | 3,659.86 | 4,334.74 |
| REMOVE | 10 | 4,077.89 | 3,884.42 | 4,331.45 |

*The third 10-client GET result duplicates the second result in the supplied output.

All throughput values are in requests per second.

## 5. Performance Analysis

### 5.1 Read Performance

GET achieved substantially higher throughput than PUT and REMOVE.

At 10 concurrent clients, median GET throughput was approximately **40,599 requests per second**, compared with approximately **3,365 PUT requests per second** and **4,078 REMOVE requests per second**.

GET operates on the in-memory database without requiring a WAL write. This is consistent with its lower observed latency and higher throughput.

### 5.2 Write Performance

PUT and REMOVE both require persistent WAL updates.

Their throughput increased when additional clients were introduced, but the improvement was not proportional to the number of clients.

For example, increasing PUT concurrency from 5 to 10 clients improved median aggregate throughput from approximately **3,082 to 3,365 requests per second**, an increase of about 9%.

This suggests diminishing returns from additional write concurrency.

### 5.3 Synchronization and Persistence

AtlasKV uses a global mutex to protect database operations. Write operations also synchronize their WAL records to persistent storage.

These design choices may limit the amount of useful parallelism available to concurrent writes.

However, the benchmarks do not isolate the cost of mutex contention from the cost of WAL synchronization. Profiling would be required to identify the dominant bottleneck.

### 5.4 Tail Latency

Request latency generally increased with concurrency, particularly for PUT and REMOVE.

For example, in one 10-client PUT run, individual clients reported P99 latencies of approximately **15–17 ms**. In the third 10-client PUT run, reported P99 latencies increased to approximately **25–29 ms**.

These measurements show that some requests experienced significantly higher latency than the median request.

### 5.5 Variability Between Runs

Performance varied between repeated runs of the same configuration.

For example, single-client GET throughput ranged from approximately **9,689 to 18,781 requests per second**.

Potential contributors include system load, scheduling, storage activity, and differences in database state. The current measurements do not establish which factors caused the variation.

For this reason, the results should be interpreted as an initial performance characterization rather than fixed performance guarantees.

## 6. Limitations

The benchmark results have several limitations:

1. All requests were issued over localhost rather than a real network.
2. The tests were conducted on one development machine.
3. The benchmark configurations used different total numbers of requests because each client performed 10,000 operations.
4. The effect of WAL growth and database size was not independently measured.
5. The benchmarks do not isolate mutex contention, filesystem synchronization, or networking overhead.
6. The third 10-client GET run requires verification.
7. REMOVE setup timing requires confirmation from the benchmark implementation.

## 7. Conclusion

The benchmarks demonstrate that AtlasKV supports successful concurrent GET, PUT, and REMOVE operations.

GET achieved the highest throughput and scaled most effectively with additional clients. PUT and REMOVE also benefited from concurrency, but their improvements were smaller, consistent with the additional synchronization and persistence work required by writes.

The results establish a baseline for evaluating future architectural changes, including replication and other distributed-system features.
