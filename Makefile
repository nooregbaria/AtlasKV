# AtlasKV: C++17 builds for macOS with the system zlib library.
ifeq ($(origin CXX), default)
CXX := clang++
endif
CPPFLAGS += -Iinclude
CXXFLAGS ?= -std=c++17 -g -pthread
LDLIBS += -lz -pthread
PYTHON ?= python3

BIN_DIR := build/bin
OBJ_DIR := build/obj
LIBRARY := build/libatlaskv.a
APP_SOURCES := src/main.cpp src/server.cpp src/client.cpp src/raft_server.cpp
CORE_SOURCES := $(filter-out $(APP_SOURCES),$(wildcard src/*.cpp))
CORE_OBJECTS := $(patsubst src/%.cpp,$(OBJ_DIR)/%.o,$(CORE_SOURCES))
APPS := database atlas_server atlas_client raft_server
BENCHMARKS := benchmark_get benchmark_put benchmark_remove
# The historical source with a space in its name has an explicit rule below.
TEST_NAMES := concurrency_test test_raft test_raft_append_consistency test_raft_apply_log test_raft_client test_raft_election test_raft_fault_tolerance test_raft_follower_commit test_raft_heartbeat test_raft_heartbeats test_raft_leader_commit_protocol test_raft_leader_election test_raft_log test_raft_log_replication test_raft_majority_commit test_raft_mark_applied test_raft_network test_raft_next_committed_entry test_raft_node_log test_raft_node_persistence test_raft_previous_term_commit test_raft_protocol test_raft_replication_reply test_raft_replication_request test_raft_single_node_commit test_raft_storage test_raft_tcp_commit test_raft_tcp_conflict_recovery test_raft_tcp_replication test_raft_vote_persistence
TEST_BINS := $(addprefix build/tests/,$(TEST_NAMES)) build/tests/test_raft_protocol_copy

.PHONY: all apps tests benchmarks test test-final help
all: apps tests benchmarks
apps: $(addprefix $(BIN_DIR)/,$(APPS))
tests: $(TEST_BINS)
benchmarks: $(addprefix build/benchmarks/,$(BENCHMARKS))

$(LIBRARY): $(CORE_OBJECTS)
	$(AR) rcs $@ $^

$(OBJ_DIR)/%.o: src/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

$(BIN_DIR)/database: $(OBJ_DIR)/main.o $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_DIR)/atlas_server: $(OBJ_DIR)/server.o $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_DIR)/atlas_client: $(OBJ_DIR)/client.o $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BIN_DIR)/raft_server: $(OBJ_DIR)/raft_server.o $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

build/tests/%: tests/%.cpp $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -MF $@.d $(LDFLAGS) $< $(LIBRARY) $(LDLIBS) -o $@

build/tests/test_raft_protocol_copy: tests/test_raft_protocol\ copy.cpp $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -MF $@.d $(LDFLAGS) "$<" $(LIBRARY) $(LDLIBS) -o $@

build/benchmarks/%: benchmarks/%.cpp $(LIBRARY)
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -MF $@.d $(LDFLAGS) $< $(LIBRARY) $(LDLIBS) -o $@

test: all
	$(PYTHON) scripts/run_tests.py

test-final: apps
	$(PYTHON) tests/test_atlaskv_final.py --binary ./build/bin/raft_server

help:
	@echo 'make -j4 apps       Build the four application executables'
	@echo 'make -k -j4 all     Attempt every application, test, and benchmark build'
	@echo 'make test-final    Run the 11-case final Raft integration suite'
	@echo 'make test          Build everything and run the existing test suites'
	@echo 'python3 scripts/run_tests.py  Run available tests and report missing builds'

-include $(wildcard $(OBJ_DIR)/*.d build/tests/*.d build/benchmarks/*.d)
