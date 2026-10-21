#include "RaftStateMachine.h"

RaftStateMachine::RaftStateMachine( RaftNode& raft, Database& db, WAL& wal, RaftApplyLog& apply_log) : raft(raft), db(db), wal(wal),  apply_log(apply_log) {}

ActionResult RaftStateMachine::execute_command(uint64_t raft_index , const std::string& command) {
    (void)raft_index;
    (void)command;

    return {"NOT_IMPLEMENTED", std::nullopt};
}

bool RaftStateMachine::apply_committed() {
    std::lock_guard<std::mutex> lock(apply_mutex);
    if (recovery_required) {
        return false;
    }

    while (true) {
        auto entry = raft.get_next_committed_entry();

        if (!entry.has_value()) {
            return true;
        }

        uint64_t index = raft.get_last_applied() + 1;
        // Journal the committed command before changing the in-memory database.
        if(!apply_log.append(index, entry->command)){
            recovery_required = true;
            return false;
        }

        std::string result ;
        // Leader no-ops advance the applied index without changing user data.
        if(entry->command.empty()){
            result =  "OK";
        }else{
            result = db.apply_raft_command(entry->command);
        }
         
        if(result ==  "INVALID_COMMAND"){
            recovery_required = true;
            return false;
        }

        if(!raft.mark_entry_applied(index)){
            recovery_required = true;
            return false;
        }

        command_results[index] = result;

    }
}

bool RaftStateMachine::recover() {
    std::lock_guard<std::mutex> lock(apply_mutex);

    if (recovery_attempted) {
        return false;
    }

    recovery_attempted = true;

    std::vector<AppliedRaftEntry> entries;

    if (!apply_log.recover(entries)) {
        return false;
    }

    // Validate the entire journal against the Raft log before replaying any command.
    uint64_t expected_index = 1;

    for (const auto& entry : entries) {
        if (entry.index != expected_index) {
            return false;
        }

        RaftLogEntry raft_entry;

        if (!raft.get_log_entry(entry.index, raft_entry)) {
            return false;
        }

        if (raft_entry.command != entry.command) {
            return false;
        }

        ++expected_index;
    }

    for(const auto& entry : entries){
        std::string result;
        if(entry.command.empty()){
            result = "OK";
        }else{
            result = db.apply_raft_command(entry.command);
        }

        if(result == "INVALID_COMMAND"){
            return false;
        }

        command_results[entry.index] = result;
    }

    if(!raft.restore_applied_index(entries.size())){
        return false;
    }

    recovery_required = false;
    return true;
}

std::optional<std::string> RaftStateMachine::get_result_by_index( uint64_t index)const{
    std::lock_guard<std::mutex> lock(apply_mutex);
    auto it = command_results.find(index);
    if(it == command_results.end()){
        return std::nullopt;
    }
    return it->second;
}