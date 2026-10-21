#pragma once

#include <cstdint>
#include <chrono>
#include <random>
#include <unordered_set>
#include <vector>
#include <string>
#include <mutex>
#include "LogEntry.h"
#include "RaftStorage.h"
#include "RaftLog.h"
#include <unordered_map>
#include <optional>
#include <functional>

// Followers accept replication and vote requests; candidates seek a majority.
// The leader coordinates replication, committed writes, and quorum-confirmed reads.
enum class NodeState{
    follower , candidate , leader
};


// A read captures a committed index and requires acknowledgements in its term.
struct ReadRound {
    uint64_t id;
    uint64_t term;
    uint64_t commit_index;
    std::unordered_set<int> acknowledged_peers;
};

struct RequestVoteArgs {
    std::uint64_t term;
    int candidate_id;
    std::uint64_t last_log_index;
    std::uint64_t last_log_term;

};

struct  RequestVoteReply
{
    std::uint64_t term;
    bool vote_granted;
};

struct Peer{
    int  id ;
    std::string ip;
    int port;
};

struct AppendEntriesArgs{
    std::uint64_t term;
    int leader_id;
    std::uint64_t prev_log_index;
    std::uint64_t prev_log_term;
    std::vector<RaftLogEntry> entries;
    uint64_t leader_commit = 0;
    uint64_t  read_round_id =0 ;
};

struct AppendEntriesReply{
    std::uint64_t term;
    bool success;
    uint64_t read_round_id =0;
};

struct AppendResult {
    uint64_t index;
    uint64_t term;
};


class RaftNode
{
private:
    int node_id;
    NodeState state ;
    std::uint64_t current_term ;
    int voted_for ;
    std::chrono::steady_clock::time_point election_timer_start;
    std::chrono::milliseconds election_timeout;
    std::mt19937 gen{std::random_device{}()};
    RaftStorage storage;
    int cluster_size;
    int votes_recieved;
    std::unordered_set<int> voters;
    std::vector<Peer> peers;
    void reset_election_timer();
    mutable std::mutex mutex_node;
    // Committed entries are safe to execute; applied entries have reached the database.
    std::uint64_t commit_index = 0 ; // Highest log index known to be committed.
    std::uint64_t last_applied = 0 ;// Highest committed index applied to the database.
    RaftLog log;
    std::unordered_map<int, uint64_t> next_index;
    std::unordered_map<int, uint64_t> match_index;
    std::unordered_map<uint64_t,ReadRound> rounds;
    uint64_t next_read_round_id = 1;
    // Both helpers require mutex_node; replica counts include the leader's local entry.
    int count_replicas(uint64_t index) const;
    void advance_commit_index();
    
public:
   explicit RaftNode(int id, const std::string& storage_path, const std::vector<Peer>& peers);
   bool election_time_expired();
   void start_election();

   RequestVoteReply handle_request_vote(const RequestVoteArgs&request);
   void handle_vote_reply(int peer_id, std::uint64_t election_term, const RequestVoteReply& reply);
   AppendEntriesReply handle_append_entries(const AppendEntriesArgs& request);
   std::vector<Peer> get_peers() const;
   RequestVoteArgs get_election_request() const;
   bool try_start_election();
   void handle_append_entries_reply(int peer_id, const AppendEntriesArgs& request , const AppendEntriesReply& reply);
   std::optional<AppendEntriesArgs> get_append_entries_request(int peer_id) const;
   bool append_command(const std::string& command);
   uint64_t get_commit_index() const;
   std::optional<RaftLogEntry> get_next_committed_entry() const;
   bool mark_entry_applied(uint64_t index);
   std::optional<AppendResult> append_command_with_index(const std::string& command);
   std::optional<uint64_t> begin_read_round();
   void  acknowledge_read_round(uint64_t round_id,uint64_t term,int peer_id);
   std::optional<uint64_t> get_confirmed_read_index(uint64_t round_id);
   std::optional<uint64_t> get_pending_read_round_id(uint64_t request_term) const;
   void finish_read_round(uint64_t round_id);
   bool is_read_round_valid(uint64_t round_id, uint64_t read_index) const;
   bool read_if_valid(uint64_t round_id,uint64_t read_index, const std::function<void()>& read_action) const;

   // Synchronized accessors for runtime coordination and tests.
   std::uint64_t get_current_term() const;
   int get_voted_for() const;
   NodeState get_state() const;
   uint64_t get_last_applied() const;
   bool get_log_entry(uint64_t index, RaftLogEntry& entry) const;
   bool restore_applied_index(uint64_t index);
};
