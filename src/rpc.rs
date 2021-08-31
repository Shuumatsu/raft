use crate::{LogEntry, ServerId, Term};

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteArgs {
    pub requester_term: Term,
    pub candidate_id: ServerId,
    pub prev_entry_identifier: Option<(Term, usize)>, // (term, index) of last log entry
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteReply {
    pub responser_term: Term,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesArgs {
    pub requester_term: Term,
    pub leader_id: ServerId,
    pub prev_entry_identifier: Option<(Term, usize)>, // (term, index) of log entry immediately preceding new ones
    pub leader_commit: Option<usize>,                 // leader's commit_ndex
    pub entries: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesReply {
    pub responser_term: Term,
    pub success: bool, // true if follower contained entry matching prev_log_term and prev_log_index
}

#[tarpc::service]
pub trait Raft {
    async fn request_vote(args: RequestVoteArgs) -> RequestVoteReply;

    async fn append_entries(args: AppendEntriesArgs) -> AppendEntriesReply;
}
