#include "RaftProtocol.h"
#include <climits>
#include <stdexcept>

// Append an unsigned integer as eight bytes in network byte order.
static void append_u64(std::string& bytes, uint64_t value){
    for(int i=7 ; i>=0 ; i--){
        bytes += static_cast<char>((value >> (i * 8)) & 0xFF);
    }
}

static void append_u32(std::string& bytes, uint32_t value){
    for(int i=3 ; i>=0 ; i--){
        bytes += static_cast<char>((value >> (i * 8)) & 0xFF);
    }
}

// Decode eight bytes and advance the offset; reject incomplete fields.
static bool read_u64(const std::string& bytes, size_t& offset, uint64_t& value){
    if(offset > bytes.size() || bytes.size() - offset < 8){
        return false;
    }

    value = 0;
    for(int i=0; i< 8 ;i++){
        value = (value << 8) | static_cast<uint8_t>(bytes[i+offset]);
    }
    offset += 8;
    return true;
}

static bool read_u32(const std::string& bytes, size_t& offset, uint32_t& value){
    if(offset > bytes.size() || bytes.size() - offset < 4){
        return false;
    }

    value = 0;
    for(int i=0; i< 4 ;i++){
        value = (value << 8) | static_cast<uint8_t>(bytes[i+offset]);
    }
    offset += 4;
    return true;
}


// RequestVote wire format — 29 bytes total
//
// Byte  0       : MessageType (1 = RequestVote) [1 byte]
// Bytes 1–8     : term                        [8 bytes]
// Bytes 9–12    : candidate_id                [4 bytes]
// Bytes 13–20   : last_log_index              [8 bytes]
// Bytes 21–28   : last_log_term               [8 bytes]
//
// All multi-byte integers use big-endian order.

std::string raft_wire::encode(const RequestVoteArgs& args) {
    std::string bytes;

    bytes += static_cast<char>(raft_wire::MessageType::RequestVote);
    append_u64(bytes, args.term);
    append_u32(bytes, static_cast<uint32_t>(args.candidate_id));
    append_u64(bytes, args.last_log_index);
    append_u64(bytes, args.last_log_term);

    return bytes;
}

std::optional<RequestVoteArgs> raft_wire::decode_request_vote(const std::string& bytes){

    if(bytes.size() != 29){
        return std::nullopt;
    }

    if(static_cast<uint8_t>(bytes[0]) != static_cast<uint8_t>(raft_wire::MessageType::RequestVote)){
        return std::nullopt;
    }

    std::size_t offset = 1;
    RequestVoteArgs args;

    if(!read_u64(bytes, offset, args.term)){
        return std::nullopt;
    }

    std::uint32_t id;
    if(!read_u32(bytes, offset, id)){
        return std::nullopt;
    }
    if(id > static_cast<uint32_t>(INT_MAX)){
        return std::nullopt;
    }
    args.candidate_id = static_cast<int>(id);

    if(!read_u64(bytes, offset, args.last_log_index)){
        return std::nullopt;
    }

    if(!read_u64(bytes, offset, args.last_log_term)){
        return std::nullopt;
    }

    return args;
}


// RequestVoteReply — 10 bytes total
//
// Byte 0     : MessageType (2)
// Bytes 1–8  : term          [8 bytes]
// Byte 9     : vote_granted  [1 byte, 0 or 1]

std::string raft_wire::encode(const RequestVoteReply& reply){
    std::string bytes;

    bytes += static_cast<char>(raft_wire::MessageType::RequestVoteReply);
    append_u64(bytes, reply.term);
    bytes += static_cast<char>(reply.vote_granted ? 1 : 0);

    return bytes;
}


std::optional<RequestVoteReply> raft_wire::decode_request_vote_reply(const std::string& bytes){
    if(bytes.size() != 10){
        return std::nullopt;
    }

    if(static_cast<uint8_t>(bytes[0]) != static_cast<uint8_t>(raft_wire::MessageType::RequestVoteReply)){
        return std::nullopt;
    }

    std::size_t offset = 1;
    RequestVoteReply args;

    if(!read_u64(bytes, offset, args.term)){
        return std::nullopt;
    }

    uint8_t vote = static_cast<uint8_t>(bytes[9]);
    if(vote > 1){
        return std::nullopt;
    }
    args.vote_granted = (vote == 1);

    return args;
}


// AppendEntries — 49-byte minimum message, including the read-round ID.
//
// Byte 0       : MessageType       [1 byte]
// Bytes 1–8    : term              [8 bytes]
// Bytes 9–12   : leader_id         [4 bytes]
// Bytes 13–20  : prev_log_index    [8 bytes]
// Bytes 21–28  : prev_log_term     [8 bytes]
// Bytes 29–32  : entries_count     [4 bytes]
//
// For each entry:
//   entry_term                     [8 bytes]
//   command_length                 [4 bytes]
//   command                        [variable]
//
// Final 16 bytes:
//   leader_commit                  [8 bytes]
//   read_round_id                  [8 bytes; zero when no read is pending]

std::string raft_wire::encode(const AppendEntriesArgs& args){
    std::string  bytes;

    bytes += static_cast<char>(raft_wire::MessageType::AppendEntries);
    append_u64(bytes,args.term );
    append_u32(bytes, static_cast<uint32_t>(args.leader_id));
    append_u64(bytes, args.prev_log_index);
    append_u64(bytes, args.prev_log_term);
    if (args.entries.size() > UINT32_MAX) {
        throw std::length_error("Too many Raft log entries");
    }
    append_u32(bytes, static_cast<uint32_t>(args.entries.size()));
    for(size_t i=0; i<args.entries.size();i++){
        append_u64(bytes, args.entries[i].term);
        if (args.entries[i].command.size() > UINT32_MAX) {
           throw std::length_error("Raft command too large");
        }
        append_u32(bytes, static_cast<uint32_t>(args.entries[i].command.size()));
        bytes+= args.entries[i].command;
    }
    append_u64(bytes,  args.leader_commit);
    append_u64(bytes, args.read_round_id);


    return bytes;
}

std::optional<AppendEntriesArgs> raft_wire::decode_append_entries(const std::string& bytes){

    if (bytes.size() < 49) {
       return std::nullopt;
    }

    if(static_cast<uint8_t>(bytes[0]) != static_cast<uint8_t>(raft_wire::MessageType::AppendEntries)){
        return std::nullopt;
    }

    std::size_t offset = 1;
    AppendEntriesArgs args;

    if(!read_u64(bytes, offset, args.term)){
        return std::nullopt;
    }

    std::uint32_t id;
    if(!read_u32(bytes, offset, id)){
        return std::nullopt;
    }
    if(id > static_cast<uint32_t>(INT_MAX)){
        return std::nullopt;
    }
    args.leader_id = static_cast<int>(id);

    if(!read_u64(bytes, offset, args.prev_log_index)){
        return std::nullopt;
    }

    if(!read_u64(bytes, offset, args.prev_log_term)){
        return std::nullopt;
    }

    std::uint32_t entries_num;
    if(!read_u32(bytes, offset, entries_num)){
        return std::nullopt;
    }
    if(entries_num > static_cast<uint32_t>(INT_MAX)){
        return std::nullopt;
    }

    if (bytes.size() - offset < 16) {
       return std::nullopt;
    }

    if (entries_num > (bytes.size() - offset - 16) / 12) {
       return std::nullopt;
    }

    for(uint32_t i =0 ;i< entries_num;i++){
        RaftLogEntry entry{};

        if(!read_u64(bytes, offset, entry.term)){
            return std::nullopt;
        }
        uint32_t commande_size;
        if(!read_u32(bytes, offset, commande_size)){
            return std::nullopt;
        }

        if(offset > bytes.size() || commande_size > bytes.size() - offset){
            return std::nullopt;
        }
        entry.command = bytes.substr(offset, commande_size);
        offset += commande_size;

        args.entries.push_back(entry);
        
    }

    if(!read_u64(bytes, offset , args.leader_commit)){
        return std::nullopt;
    }

    if(!read_u64(bytes, offset , args.read_round_id)){
        return std::nullopt;
    }

    if (offset != bytes.size()) {
       return std::nullopt;
    }

    return args;
}


// AppendEntriesReply — 18 bytes total
//
// Byte 0     : MessageType (4)
// Bytes 1–8  : term       [8 bytes]
// Byte 9     : success       [1 byte, 0 or 1]
// Bytes 10–17: read_round_id [8 bytes, echoed from the request]

std::string raft_wire::encode(const AppendEntriesReply& reply){
    std::string bytes;

    bytes += static_cast<char>(raft_wire::MessageType::AppendEntriesReply);
    append_u64(bytes, reply.term);
    bytes += static_cast<char>(reply.success ? 1 : 0);
    append_u64(bytes, reply.read_round_id);

    return bytes;
}


std::optional<AppendEntriesReply> raft_wire::decode_append_entries_reply(const std::string& bytes){
    if(bytes.size() != 18){
        return std::nullopt;
    }

    if(static_cast<uint8_t>(bytes[0]) != static_cast<uint8_t>(raft_wire::MessageType::AppendEntriesReply)){
        return std::nullopt;
    }

    std::size_t offset = 1;
    AppendEntriesReply args;

    if(!read_u64(bytes, offset, args.term)){
        return std::nullopt;
    }

    uint8_t suc = static_cast<uint8_t>(bytes[9]);
    if(suc > 1){
        return std::nullopt;
    }
    args.success = (suc == 1);
    offset++;


    if(!read_u64(bytes, offset, args.read_round_id)){
        return std::nullopt;
    }

    return args; 
}