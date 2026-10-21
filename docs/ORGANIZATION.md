# Organization and preservation report

Completed 2026-10-22. The final Raft integration suite passed all 11 tests after
reorganization. No application logic, public interfaces, wire formats, persistence
formats, or existing test assertions were changed.

## Final structure

```text
AtlasKV/
├── README.md
├── Makefile
├── .gitignore
├── .vscode/                 Editor settings and build/test tasks
├── src/                     20 C++ implementation files
├── include/                 18 headers, with original public names
├── tests/                   31 C++ sources and six Python suites
├── benchmarks/
│   ├── benchmark_*.cpp      Three benchmark programs
│   └── results/             Existing recorded measurements
├── docs/
│   ├── ARCHITECTURE.md
│   ├── BENCHMARK.md
│   ├── REPLICATION.md
│   ├── TESTING.md
│   ├── ORGANIZATION.md
│   └── file-moves.json
├── scripts/
│   ├── run_tests.py
│   └── verify_organization.py
├── archive/phases_progress/ Original phase ZIP archives
└── build/                   Generated artifacts, ignored by Git
    ├── bin/                 Four application executables
    ├── obj/                 Objects and dependency files
    ├── tests/               C++ test executables
    ├── benchmarks/          Benchmark executables
    ├── previous/            Preserved original binaries and debug symbols
    ├── test-results/        Isolated run directories and logs
    └── baseline-check/      Independent checks of historical failures
```

Existing root-level `.state` files, `log.txt`, `replica_log.txt`,
`corruption_test/`, `recovery_test/`, `raft_replication_test_data/`, and the existing
`tests/phase6_test_*` data files remain in place with identical contents. These
are intentional exceptions to source organization: moving or rewriting runtime
state was excluded from the task. Existing benchmark result files are unchanged.

## Comments and documentation

Reviewed all 72 C++ source/header files, including tests and benchmarks. Files
with clear comments or self-explanatory code did not receive unnecessary prose.

- Corrected grammar, punctuation, terminology, and the directory typo in storage comments.
- Removed syntax narration, a commented-out accessor, and references to an accompanying patch.
- Corrected Raft wire-layout documentation to include the existing read-round ID.
- Clarified term/vote durability, current-term commit rules, leader no-ops, conflict
  replacement, stale-reply handling, and quorum-confirmed reads.
- Documented database/node lock ordering, per-peer workers, process-lifetime
  listeners, journal-before-application ordering, and validated recovery.
- Clarified the separate byte orders used by persistent formats, including the
  legacy WAL's distinct PUT and REMOVE layouts.
- Retained meaningful packet diagrams, checksum explanations, and failure semantics.
- Marked historical protocol expectations accurately without changing assertions.
- Added a root README, component guide, build/test instructions, and this report.
- Preserved benchmark measurements while clarifying that they are historical and
  that current PUT request counts differ from the recorded experiment.

Runtime error strings and misspelled identifiers were deliberately preserved:
they are executable code or interfaces, not comments.

## Paths and build configuration

Local includes now use header basenames resolved through `-Iinclude`; obsolete
`./` and `../` header prefixes were removed. Header names and include order are
unchanged. The Makefile uses C++17, pthreads, and system zlib, tracks header
dependencies, and supplies a separate target for every original entry point.
The historical test filename containing a space is retained with an explicit rule.

VS Code builds applications through Make and offers separate whole-project build
and final-test tasks. IntelliSense points at `include/`. Existing Python suites
were changed only for executable locations and their usage/path documentation.
The new runner supplies companion servers and isolated storage without changing
the existing tests. It reports missing binaries and incomplete suite execution
as failures rather than silently omitting them.

No external packages, application features, algorithms, or protocol changes were
introduced. Existing outputs were moved intact into `build/previous/`, while new
builds write separate executables to `build/bin/`.

## Build and test results

- All four applications and all three benchmarks build.
- 27 of 31 C++ test programs build. Four historical tests reference older interfaces.
- All six Python suites pass, including **11/11 final integration tests**.
- The complete runner reports 19 passing cases, 13 runtime failures, and four
  missing builds. The network server and client form one case.
- Every legacy failure was independently reproduced against the original sources:
  all four compilation failures and all 13 runtime failure signatures match.
- No application logic was changed to address those failures. Full details and
  reproduction commands are in [TESTING.md](TESTING.md).

## Preservation checks

The baseline contains all 122 original files. Every file is still accounted for.
The automated comparison verified:

- Identical executable tokens across all 72 C++ files, allowing only include paths
  and comments to differ.
- Identical Python test ASTs across all six original suites, allowing only the
  documented executable-path expressions and usage documentation to differ.
- Identical SHA-256 hashes for all 38 preserved non-documentation/configuration
  files, including runtime state, measurement results, archives, and old binaries.

The snapshot used for verification is stored outside the project and is not a
runtime dependency. The move manifest is [file-moves.json](file-moves.json).

## Complete list of moved files

Files already in `tests/` and `benchmarks/` retained their paths. Directory moves
are expanded below so every moved original file is listed.

| Original path | New path |
| --- | --- |
| `BENCHMARK.md` | `docs/BENCHMARK.md` |
| `Database.cpp` | `src/Database.cpp` |
| `Database.h` | `include/Database.h` |
| `LogEntry.cpp` | `src/LogEntry.cpp` |
| `LogEntry.h` | `include/LogEntry.h` |
| `REPLICATION.md` | `docs/REPLICATION.md` |
| `RaftApplyLog.cpp` | `src/RaftApplyLog.cpp` |
| `RaftApplyLog.h` | `include/RaftApplyLog.h` |
| `RaftLog.cpp` | `src/RaftLog.cpp` |
| `RaftLog.h` | `include/RaftLog.h` |
| `RaftNode.cpp` | `src/RaftNode.cpp` |
| `RaftNode.h` | `include/RaftNode.h` |
| `RaftProtocol.cpp` | `src/RaftProtocol.cpp` |
| `RaftProtocol.h` | `include/RaftProtocol.h` |
| `RaftRPC.cpp` | `src/RaftRPC.cpp` |
| `RaftRPC.h` | `include/RaftRPC.h` |
| `RaftRunner.cpp` | `src/RaftRunner.cpp` |
| `RaftRunner.h` | `include/RaftRunner.h` |
| `RaftStateMachine.cpp` | `src/RaftStateMachine.cpp` |
| `RaftStateMachine.h` | `include/RaftStateMachine.h` |
| `RaftStorage.cpp` | `src/RaftStorage.cpp` |
| `RaftStorage.h` | `include/RaftStorage.h` |
| `RaftTransport.cpp` | `src/RaftTransport.cpp` |
| `RaftTransport.h` | `include/RaftTransport.h` |
| `atlas_client` | `build/previous/atlas_client` |
| `atlas_server` | `build/previous/atlas_server` |
| `client.cpp` | `src/client.cpp` |
| `command_handler.cpp` | `src/command_handler.cpp` |
| `command_handler.h` | `include/command_handler.h` |
| `database.dSYM/Contents/Info.plist` | `build/previous/database.dSYM/Contents/Info.plist` |
| `database.dSYM/Contents/Resources/DWARF/database` | `build/previous/database.dSYM/Contents/Resources/DWARF/database` |
| `database.dSYM/Contents/Resources/Relocations/x86_64/database.yml` | `build/previous/database.dSYM/Contents/Resources/Relocations/x86_64/database.yml` |
| `io_utils.h` | `include/io_utils.h` |
| `main.cpp` | `src/main.cpp` |
| `parser.cpp` | `src/parser.cpp` |
| `parser.h` | `include/parser.h` |
| `phases_progress/phase1.zip` | `archive/phases_progress/phase1.zip` |
| `phases_progress/phase2.zip` | `archive/phases_progress/phase2.zip` |
| `phases_progress/phase4.zip` | `archive/phases_progress/phase4.zip` |
| `phases_progress/phase5.zip` | `archive/phases_progress/phase5.zip` |
| `protocol.cpp` | `src/protocol.cpp` |
| `protocol.h` | `include/protocol.h` |
| `raft_server` | `build/previous/raft_server` |
| `raft_server.cpp` | `src/raft_server.cpp` |
| `replication.cpp` | `src/replication.cpp` |
| `replication.h` | `include/replication.h` |
| `replication_protocol.h` | `include/replication_protocol.h` |
| `server.cpp` | `src/server.cpp` |
| `test_atlaskv_final.py` | `tests/test_atlaskv_final.py` |
| `test_atlaskv_raft.py` | `tests/test_atlaskv_raft.py` |
| `wal.cpp` | `src/wal.cpp` |
| `wal.h` | `include/wal.h` |
