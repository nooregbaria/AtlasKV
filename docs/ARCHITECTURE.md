# Component guide

This guide describes the current implementation; it does not propose protocol or
behavior changes.

## Source map

Headers in `include/` retain their original names. Implementations are in `src/`.

| Area | Files | Responsibility |
| --- | --- | --- |
| Database and commands | `Database`, `parser`, `command_handler` | Key-value operations, text parsing, command results |
| Legacy persistence | `wal`, `LogEntry`, `io_utils.h` | WAL records, replay, sequence numbers, complete writes |
| Framing and replication | `protocol`, `replication`, `replication_protocol.h` | Length-prefixed messages and binary primary/replica replication |
| Raft consensus | `RaftNode`, `RaftRunner` | Terms, votes, election timers, log replication, commit indexes, read rounds |
| Raft networking | `RaftProtocol`, `RaftRPC`, `RaftTransport` | Wire encoding, RPC dispatch, TCP request/response transport |
| Raft persistence | `RaftStorage`, `RaftLog` | Term/vote state and durable consensus log |
| State-machine application | `RaftStateMachine`, `RaftApplyLog` | Journal committed commands, apply them in order, replay on restart |
| Entry points | `main.cpp`, `server.cpp`, `client.cpp`, `raft_server.cpp` | Local CLI, primary/replica server, client, Raft server |

## Raft writes and reads

A leader appends a command durably, then replicates it to peers. A majority can
advance the commit index through an entry from the current term. Newly elected
leaders append an empty no-op entry. Committed commands enter the application
journal before they change the in-memory database; the applied index advances in
log order. No-ops advance that index without changing user data.

A GET starts a read round after a current-term entry has committed. The round
captures a commit index and requires a fresh majority of same-term responses
that echo its ID. The server waits for application through that index and checks
leadership under the node mutex while reading the database. A timeout does not
establish that a write can never commit later.

## Concurrency and recovery

`RaftNode` serializes consensus state with its mutex. `RaftRunner` uses a separate
replication worker per peer. The server protects the database during both reads
and application. TCP listeners use process lifetime for shutdown; stopping the
runner does not stop a transport listener.

Startup recovers persistent state before exposing RPC or client ports. The
application journal must be contiguous and agree with the Raft log before replay
changes the database. The legacy WAL validates checksums and sequence numbers
before publishing recovered state, and repairs certain incomplete tails.
The Raft log rejects incomplete or corrupt records.

## Encodings

| Data | Encoding |
| --- | --- |
| TCP frame length | Four-byte big-endian unsigned length |
| Raft RPC integers | Big-endian; AppendEntries includes a read-round ID |
| Raft log and application journal | Big-endian fields with CRC32 |
| Raft term/vote state | `RAFT` magic, little-endian fields, CRC32 |
| Legacy WAL | Host-native integer byte order, CRC32 |
| Primary/replica messages | `RPL1` binary payload with big-endian fields |

These formats are distinct and unchanged by the folder reorganization. The
precise Raft wire layouts are documented alongside the encoder in
[`src/RaftProtocol.cpp`](../src/RaftProtocol.cpp).
