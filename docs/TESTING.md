# Building and testing AtlasKV

Verified on macOS with Apple Clang, C++17, system zlib, and Python 3 on
2026-10-22. Assertions remain enabled; the build does not define `NDEBUG`.

## Commands

Run these from the project root:

```sh
make -j4 apps          # database, atlas_server, atlas_client, raft_server
make benchmarks       # GET, PUT, and REMOVE benchmark executables
make -k -j4 all        # Attempt every existing C++ target, including legacy tests
make test-final       # The final 11-case Raft integration suite
python3 scripts/run_tests.py
```

`make test` builds all targets before invoking the runner. It currently stops at
the four known legacy compilation failures below. To inspect every runnable test
without hiding those failures, use `make -k -j4 all` followed by the runner, which
also records missing executables as failures and returns a nonzero exit status.

A single suite can be selected explicitly:

```sh
python3 scripts/run_tests.py --only test_atlaskv_final
python3 tests/test_atlaskv_final.py --binary ./build/bin/raft_server
```

The Makefile uses `-Iinclude`, C++17, pthreads, and zlib. It builds a static archive
of the shared implementation objects, then links application, test, and benchmark
entry points separately. Dependency files rebuild targets after header changes.
Override `CXX`, `CXXFLAGS`, `CPPFLAGS`, or `LDFLAGS` through Make as needed; retain
C++17 and assertions when reproducing these test results.

The existing `tests/test_raft_protocol copy.cpp` filename is preserved. Its binary
is `build/tests/test_raft_protocol_copy`.

## Storage and server lifetimes

The runner executes tests sequentially to avoid fixed-port collisions. Each case
has a fresh working directory under `build/test-results/run-*/`; Python suites
that already use temporary directories keep their original behavior. Some C++
tests also retain their own `/tmp` paths. Test assertions, timeouts inside the
existing tests, and expectations are unchanged.

`test_raft_network` is a long-running companion server for `test_raft_client`;
the runner starts and stops them together. The concurrency test receives a fresh
primary/replica pair. Child process groups are cleaned up on completion or a
180-second orchestration timeout. Existing persisted project state is not used.

Run `test_phase6.py` through the runner, or from a disposable working directory:
its original logic manages WAL files relative to the current directory. Its
server path now resolves to this project's `build/bin/atlas_server`.

## Results

| Verification | Result |
| --- | --- |
| Four applications | All build successfully |
| Three benchmarks | All build successfully; performance measurements were not rerun |
| C++ test programs | 27 of 31 build; four have pre-existing interface mismatches |
| Final Raft integration suite | **11 PASS, 0 FAIL** |
| Earlier Raft integration suite | **10 PASS, 0 FAIL** |
| Python suites | All six pass |
| Complete runner | 19 passing cases, 13 runtime failures, four unavailable builds |
| Baseline comparison | All 17 legacy failures reproduced before reorganization |

The runner has 36 cases: 30 C++ cases (the network server/client form one case)
and six Python suites. A passing Python case may contain multiple assertions or
subtests. These counts must not be confused with the final suite's 11 tests.

The final suite covers startup/election; command outcomes; malformed commands;
follower rejection; ordered writes; repeated quorum-confirmed reads; concurrent
writes; leader failover; node recovery; quorum loss/recovery; and full-cluster
restart.

## Legacy compilation failures

These fail with the original sources as well as the reorganized tree:

| Test | Existing mismatch |
| --- | --- |
| `test_raft.cpp` | Calls the old one-argument constructor and removed `set_log_info_for_test` helper |
| `test_raft_vote_persistence.cpp` | Calls the old two-argument constructor and removed test helper |
| `test_raft_node_persistence.cpp` | Calls the old two-argument constructor |
| `test_raft_leader_election.cpp` | Passes an integer cluster size where the current constructor requires a peer vector |

## Legacy runtime failures

These exact failures were reproduced against a separately compiled, untouched
baseline. No application logic or test assertions were changed to resolve them.

| Test | First failure |
| --- | --- |
| `test_raft_leader_commit_protocol` | Expects a 41-byte heartbeat; current format includes a read-round ID |
| `test_raft_protocol_copy` | Expects the earlier 33-byte heartbeat |
| `test_raft_protocol` | Expects the earlier 13-byte AppendEntries message |
| `test_raft_majority_commit` | Expects commit index 1 after the first client command |
| `test_raft_mark_applied` | Expects commit index 2 after two client commands |
| `test_raft_next_committed_entry` | Expects commit index 1 after the first client command |
| `test_raft_previous_term_commit` | Expects previous-log index 1 after re-election |
| `test_raft_replication_reply` | Expects previous-log index 0 with exactly three entries |
| `test_raft_replication_request` | Expects previous-log index 0 immediately after election |
| `test_raft_single_node_commit` | Expects commit index 1 after the first client command |
| `test_raft_tcp_commit` | Waits for indexes that omit the elected leader's no-op |
| `test_raft_tcp_conflict_recovery` | Compares the recovered log to a history without the new leader's no-op |
| `test_raft_tcp_replication` | Expects exactly three log entries after three client commands |

The current leader appends a no-op when elected, so client-command counts and log
indexes differ in these historical scenarios. The current AppendEntries minimum
is 49 bytes, and its reply is 18 bytes. The implementation and these assertions
were both preserved exactly; updating historical tests is outside this cleanup.

## Verification artifacts

- `build/organization-build.log`: full build output, including compilation failures.
- `build/test-results/run-8d98p01s/summary.json`: complete organized-tree run.
- `build/test-results/run-8d98p01s/test_atlaskv_final/output.log`: all 11 final tests.
- `build/baseline-build-failures.json`: original-source compilation diagnostics.
- `build/baseline-check/summary.json`: original-source runtime failure comparisons.
- `build/organization-integrity.txt`: token, Python AST, and file-hash verification.

These generated artifacts are local and ignored by Git. The results above remain
in the documentation for reviewers. To compare another pre-organization snapshot:

```sh
python3 scripts/verify_organization.py /path/to/baseline
```

The snapshot must include `baseline-manifest.json` with SHA-256 hashes. The checker
accounts for every original file using `docs/file-moves.json`, compares all C++
tokens after removing comments and normalizing include paths, compares Python test
ASTs allowing only the documented executable-path changes, and hashes preserved
data and build artifacts.
