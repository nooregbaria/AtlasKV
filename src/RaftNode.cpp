#include "RaftNode.h"
#include <stdexcept>
#include <algorithm>


RaftNode::RaftNode(int id ,const std::string& storage_path, const  std::vector<Peer>&  peers):node_id(id), state(NodeState::follower), current_term(0), voted_for(-1), storage(storage_path), votes_recieved(0), peers(peers), log(storage_path + ".log"){
    cluster_size = peers.size()+1;
    if(!log.load()){
        throw std::runtime_error("Failed to recover raft log");
    }

    PersistentRaftState saved_state {};
    LoadStatus result = storage.load(saved_state);
    if(result == LoadStatus::Error){
        throw std::runtime_error("Failedto load Raft state");
    }else if (result == LoadStatus::Success)
    {
        current_term = saved_state.current_term;
        voted_for = saved_state.voted_for;
    }
    
    reset_election_timer();
}
 
void RaftNode::reset_election_timer(){
    election_timer_start = std::chrono::steady_clock::now();

    std::uniform_int_distribution<int> dist(150, 300);
    election_timeout = std::chrono::milliseconds(dist(gen));
}

bool RaftNode::election_time_expired(){
    std::lock_guard<std::mutex> lock(mutex_node);

    if(state  == NodeState::leader){
        return false;
    }
    auto elapsed = std::chrono::steady_clock::now() - election_timer_start;

    return elapsed >= election_timeout;
}

void RaftNode::start_election(){
    std::lock_guard<std::mutex> lock(mutex_node);
    state = NodeState::candidate;
    rounds.clear();
    current_term++;
    voted_for = node_id;

    PersistentRaftState new_state{current_term, voted_for};
    if(!storage.save(new_state)){
        throw std::runtime_error("Failed tp presist Raft election state");
    }

    voters.clear();
    voters.insert(node_id);
    votes_recieved = 1;
    
    reset_election_timer();
}

RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& request){

    std::lock_guard<std::mutex> lock(mutex_node);
    bool term_changed = request.term > current_term;
    if(request.term < current_term){
        return RequestVoteReply{current_term , false};
    }

    if(request.term > current_term){
        current_term = request.term;
        state = NodeState::follower;
        rounds.clear();
        voted_for = -1;
    }

    // Compare the last term first, then the index, before granting a vote.
    bool log_up_to_date =
    (request.last_log_term > log.last_term()) ||
    (request.last_log_term == log.last_term() &&
     request.last_log_index >= log.last_index());
    bool can_vote = voted_for == -1 || voted_for == request.candidate_id;

    bool vote_guaranteed = can_vote && log_up_to_date;

    if(vote_guaranteed){
        voted_for = request.candidate_id;
    }
    
    // Persist the term and vote before replying, including rejected higher-term requests.
    if(term_changed || vote_guaranteed){
        PersistentRaftState new_state{current_term, voted_for};

        if(!storage.save(new_state)){
            throw std::runtime_error("Failed to presist Raft state");
        }
    }

    if(vote_guaranteed){
        reset_election_timer();
    }

    return RequestVoteReply{current_term, vote_guaranteed};
}

 void RaftNode::handle_vote_reply(int peer_id, std::uint64_t election_term, const RequestVoteReply& reply){

    std::lock_guard<std::mutex> lock(mutex_node);
    bool known_peer = false;

    for(const auto& peer : peers){
        if(peer.id == peer_id){
            known_peer = true;
            break;
        }
    }

    if(!known_peer){
        return;
    }

    if(reply.term > current_term){
        current_term = reply.term;
        voted_for = -1;
        PersistentRaftState new_state{current_term, voted_for};

        if(!storage.save(new_state)){
            throw std::runtime_error("Failed to presist Raft state");
        }

        state = NodeState::follower;
        rounds.clear();
        return ;
    }

    if(state != NodeState::candidate){
        return;
    }

    if(election_term != current_term || reply.term != current_term){
        return;
    }

    if(reply.vote_granted && voters.insert(peer_id).second){
       votes_recieved++;
    }

    if(votes_recieved >= cluster_size/2 +1){
        state = NodeState::leader;
        
        next_index.clear();
        match_index.clear();

        // A current-term no-op establishes a commit point for subsequent reads.
        RaftLogEntry NoOp{current_term, ""};
        if(!log.append(NoOp)){
            throw std::runtime_error("Failed to append leader no-op");
        }
        advance_commit_index();

        for(const Peer& peer : peers){
            next_index[peer.id] = log.last_index() + 1;
            match_index[peer.id] = 0;
        }
    }
 }

AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& request){
    std::lock_guard<std::mutex> lock(mutex_node);

    if(request.term < current_term){
        return AppendEntriesReply{current_term , false, request.read_round_id};
    }

    if(request.term > current_term){
        current_term = request.term;
        voted_for = -1;

        PersistentRaftState new_state{current_term,voted_for};
        if(!storage.save(new_state)){
            throw std::runtime_error("Failed to presist Raft state");
        }
    }

    state = NodeState::follower;
    rounds.clear();
    reset_election_timer();

    RaftLogEntry result;
    if(request.prev_log_index == 0){
        if(request.prev_log_term !=0){
            return AppendEntriesReply{current_term, false,  request.read_round_id};
        }
    }else{
        if(!log.get_entry(request.prev_log_index, result) || request.prev_log_term != result.term){
            return AppendEntriesReply{current_term, false, request.read_round_id};
        }
    }


    // Retain the matching prefix and replace only an uncommitted conflicting suffix.
    size_t i = 0;
    for(; i<request.entries.size();i++){
        uint64_t log_index = request.prev_log_index + i + 1;
        RaftLogEntry local_entry ;
        if(!log.get_entry(log_index, local_entry)){
            break;
        }
        if(local_entry.term != request.entries[i].term){
            break;  
        }
    }

    if(i<request.entries.size()){
        uint64_t log_index  = request.prev_log_index +1+i;
        RaftLogEntry local_entry;
        if(log.get_entry(log_index, local_entry)){
            if(log_index <= commit_index){
                return AppendEntriesReply{current_term, false, request.read_round_id};
            }

            if(!log.truncate_from(log_index)){
                throw std::runtime_error("Failed to turnicate Raft log");
            }
        }


    }

    for(; i <request.entries.size(); i++){
        if(!log.append(request.entries[i])){
            throw std::runtime_error("Failed to append Raft log entry");
        }
    }
    uint64_t verified_index =request.prev_log_index + request.entries.size();

    // This request confirms only its own prefix, even if the local log is longer.
    commit_index = std::max(commit_index , std::min(request.leader_commit, verified_index));

    return  AppendEntriesReply{current_term, true, request.read_round_id};

}

std::vector<Peer> RaftNode::get_peers() const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return peers;
}

RequestVoteArgs RaftNode::get_election_request() const {
    std::lock_guard<std::mutex> lock(mutex_node);

    return RequestVoteArgs{
        current_term,
        node_id,
        log.last_index(),
        log.last_term()
    };
}

bool RaftNode::try_start_election() {
    std::lock_guard<std::mutex> lock(mutex_node);

    if (state == NodeState::leader) {
        return false;
    }

    auto elapsed = std::chrono::steady_clock::now() - election_timer_start;

    if (elapsed < election_timeout) {
        return false;
    }

    state = NodeState::candidate;
    rounds.clear();
    current_term++;
    voted_for = node_id;

    PersistentRaftState new_state{current_term, voted_for};

    if (!storage.save(new_state)) {
        throw std::runtime_error("Failed to persist Raft election state");
    }

    voters.clear();
    voters.insert(node_id);
    votes_recieved = 1;

    reset_election_timer();

    if (votes_recieved >= cluster_size / 2 + 1) {
        state = NodeState::leader;

        next_index.clear();
        match_index.clear();

        // A current-term no-op establishes a commit point for subsequent reads.
        RaftLogEntry NoOp{current_term, ""};
        if(!log.append(NoOp)){
            throw std::runtime_error("Failed to append leader no-op");
        }
        advance_commit_index();

        for(const Peer& peer : peers){
            next_index[peer.id] = log.last_index() + 1;
            match_index[peer.id] = 0;
        }
    }

    return true;
}

void RaftNode::handle_append_entries_reply(int peer_id, const AppendEntriesArgs& request , const AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(mutex_node);


    if(reply.term > current_term){
        PersistentRaftState new_state{reply.term, -1};
        if (!storage.save(new_state)) {
            throw std::runtime_error("Failed to persist higher Raft term"); 
        } 

        current_term = reply.term;
        voted_for = -1;
        state = NodeState::follower;
        rounds.clear();
        reset_election_timer();
        return;
    }

    if(state != NodeState::leader || request.term != current_term || reply.term!= current_term){
        return;
    }

    auto next_it = next_index.find(peer_id);
    auto match_it = match_index.find(peer_id);

    if(next_it == next_index.end() || match_it == match_index.end()){
        return;
    }

    if(request.prev_log_index > log.last_index() || request.entries.size() >log.last_index() - request.prev_log_index){
        return;
    }

    if(reply.success){
        uint64_t replicated_index = request.prev_log_index + request.entries.size();
        match_it->second  = std::max(match_it->second , replicated_index);
        next_it->second = std::max(next_it->second , match_it->second+1);
        advance_commit_index();
        return;
    }

    // A delayed rejection must not undo progress made by a newer reply.
    uint64_t request_next = request.prev_log_index+1;
    if(next_it->second != request_next){
        return;
    }
    uint64_t  min_next = match_it->second +1;
    if(next_it->second > min_next){
        next_it->second -- ;
    }
}

std::optional<AppendEntriesArgs> RaftNode::get_append_entries_request(int peer_id) const{
    std::lock_guard<std::mutex> lock(mutex_node);
    
    if(state != NodeState::leader){
        return std::nullopt;
    }

    if(next_index.find(peer_id) == next_index.end()){
        return std::nullopt;
    }

    uint64_t next = next_index.at(peer_id);

    if(next == 0 || next > log.last_index()+1){
        return std::nullopt;
    }

    uint64_t prev_log_index = next - 1;

    uint64_t prev_log_term = 0 ;
    if(prev_log_index != 0){
        RaftLogEntry entry ;
        if(!log.get_entry(prev_log_index, entry)){
            return std::nullopt;
        }

        prev_log_term = entry.term;
    }
    
    std::vector<RaftLogEntry> entries ;
    for(uint64_t i = next ; i<=log.last_index();i++){
        RaftLogEntry  next_entry;
        if(!(log.get_entry(i, next_entry))){
            return std::nullopt;
        }
        entries.push_back(next_entry);
    }

    AppendEntriesArgs result {current_term, node_id, prev_log_index, prev_log_term, entries, commit_index};
    return result;
}

bool RaftNode::append_command(const std::string& command){
    return  append_command_with_index(command).has_value();

}

int RaftNode::count_replicas(uint64_t index) const{
    int count = 1;
    for(const auto& [peer_id, matched] : match_index){
        if(matched >= index){
            count++;
        }
    }
    return count;
}

void RaftNode::advance_commit_index(){
    if(state != NodeState::leader){
        return;
    }

    for(uint64_t index = log.last_index(); index > commit_index ; index--){
        RaftLogEntry entry;

        if(!log.get_entry(index, entry)){
            continue;
        }

        // Commit older terms indirectly through a majority-replicated current-term entry.
        if(entry.term != current_term){
            continue;
        }

        if(count_replicas(index) >= cluster_size/2 +1){
            commit_index = index;
            return;
        }
    }
}


uint64_t RaftNode::get_commit_index() const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return commit_index;
}

std::optional<RaftLogEntry> RaftNode::get_next_committed_entry() const{
    std::lock_guard<std::mutex> lock(mutex_node);

    if(last_applied >= commit_index){
        return std::nullopt;
    }

    RaftLogEntry entry ;
    if(!log.get_entry(last_applied+1 , entry)){
        return std::nullopt;
    }

    return entry;
}


bool RaftNode::mark_entry_applied(uint64_t index){
    std::lock_guard<std::mutex> lock(mutex_node);

    if(index != last_applied+1){
        return false;
    }

    if(index > commit_index){
        return false;
    }

    last_applied = index;
    return true;
}

std::optional<AppendResult> RaftNode::append_command_with_index( const std::string& command) {
    std::lock_guard<std::mutex> lock(mutex_node);

    if (state != NodeState::leader) {
        return std::nullopt;
    }

    RaftLogEntry entry{current_term, command};

    if (!log.append(entry)) {
        return std::nullopt;
    }

    AppendResult result{log.last_index(),current_term};

    advance_commit_index();

    return result;
}


std::optional<uint64_t> RaftNode::begin_read_round(){
    std::lock_guard<std::mutex> lock(mutex_node);
    if(state != NodeState::leader){
        return std::nullopt;
    }

    if(commit_index == 0){
        return std::nullopt;
    }

    RaftLogEntry entry;
    if(!log.get_entry(commit_index, entry) || entry.term != current_term){
        return std::nullopt;
    }

    ReadRound round;
    round.id = next_read_round_id;
    next_read_round_id++;

    round.term = current_term;
    round.commit_index = commit_index;
    // Self counts once; fresh peer replies must confirm this specific round.
    round.acknowledged_peers.insert(node_id);

    rounds[round.id] = round;
    return round.id;
}

void  RaftNode::acknowledge_read_round(uint64_t round_id,uint64_t term,int peer_id){
    std::lock_guard<std::mutex> lock(mutex_node);
    if(state != NodeState::leader ||  term != current_term){
        return;
    }
    auto round = rounds.find(round_id);
    if(round == rounds.end() || round->second.term != term){
        return;
    }

    for(int  i=0 ;  i<peers.size() ; i++){
        if(peers[i].id == peer_id){
            round->second.acknowledged_peers.insert(peer_id);
            break;
        }
    }
}

std::optional<uint64_t> RaftNode::get_confirmed_read_index(uint64_t round_id){
    std::lock_guard<std::mutex> lock(mutex_node);
    auto round = rounds.find(round_id);
    if(round == rounds.end() || state != NodeState::leader || round->second.term != current_term){
        return std::nullopt;
    }

    if(commit_index == 0){
        return std::nullopt;
    }
    RaftLogEntry entry;

    if(!log.get_entry(commit_index, entry)){
        return std::nullopt;
    }

    if(entry.term != current_term){
        return std::nullopt;
    }

    if(round->second.acknowledged_peers.size() < cluster_size/2 +1){
        return std::nullopt;
    }

    return round->second.commit_index;
}

std::optional<uint64_t> RaftNode::get_pending_read_round_id(uint64_t request_term) const {
    std::lock_guard<std::mutex> lock(mutex_node);

    if (state != NodeState::leader || request_term != current_term) {
        return std::nullopt;
    }

    uint64_t oldest_id = UINT64_MAX;
    bool found = false;

    for (const auto& [id, round] : rounds) {

        // Skip rounds from previous terms.
        if (round.term != current_term) {
            continue;
        }

        // Skip rounds that already have a majority.
        if (round.acknowledged_peers.size() >= cluster_size / 2 + 1) {
            continue;
        }

        // Prioritize the oldest pending read to avoid starvation.
        if (id < oldest_id) {
            oldest_id = id;
            found = true;
        }
    }

    if (!found) {
        return std::nullopt;
    }

    return oldest_id;
}

void RaftNode::finish_read_round(uint64_t round_id) {
    std::lock_guard<std::mutex> lock(mutex_node);
    rounds.erase(round_id);
}

bool RaftNode::is_read_round_valid(
    uint64_t round_id,
    uint64_t read_index
) const {
    std::lock_guard<std::mutex> lock(mutex_node);

    auto it = rounds.find(round_id);

    if (it == rounds.end() ||
        state != NodeState::leader ||
        it->second.term != current_term) {
        return false;
    }

    if (it->second.acknowledged_peers.size() < cluster_size / 2 + 1) {
        return false;
    }

    if (it->second.commit_index != read_index ||
        last_applied < read_index) {
        return false;
    }

    return true;
}

bool RaftNode::read_if_valid(uint64_t round_id,uint64_t read_index,const std::function<void()>& read_action) const {
    std::lock_guard<std::mutex> lock(mutex_node);

    auto it = rounds.find(round_id);

    if (it == rounds.end() ||
        state != NodeState::leader ||
        it->second.term != current_term ||
        it->second.acknowledged_peers.size() < cluster_size / 2 + 1 ||
        it->second.commit_index != read_index ||
        last_applied < read_index) {
        return false;
    }

    // Hold the node mutex through the read so leadership cannot change mid-check.
    read_action();
    return true;
}



std::uint64_t RaftNode::get_current_term() const {
    std::lock_guard<std::mutex> lock(mutex_node);

    return current_term;
}

int RaftNode::get_voted_for() const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return voted_for;
}

NodeState RaftNode::get_state() const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return state;
}

uint64_t RaftNode::get_last_applied() const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return last_applied;
}

bool RaftNode::get_log_entry(uint64_t index, RaftLogEntry& entry) const {
    std::lock_guard<std::mutex> lock(mutex_node);
    return log.get_entry(index, entry);
}

bool RaftNode::restore_applied_index(uint64_t index) {
    std::lock_guard<std::mutex> lock(mutex_node);

    if (last_applied != 0 || commit_index != 0) {
        return false;
    }

    if (index > log.last_index()) {
        return false;
    }

    // The validated application journal supplies the recovered committed prefix.
    last_applied = index;
    commit_index = index;

    return true;
}

