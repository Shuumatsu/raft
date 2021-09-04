use crate::{Entry, ServerId, Term};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteReq {
    pub requester_term: Term,
    pub requester_id: ServerId,
    pub progress: (Term, usize), // (term, index) of last log entry
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteResp {
    pub responser_term: Term,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesReq {
    pub requester_term: Term,
    pub requester_id: ServerId,
    pub prev_entry_identifier: (Term, usize), // (term, index) of log entry immediately preceding new ones
    pub leader_commit: usize,                 // leader's commit_ndex
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResp {
    pub responser_term: Term,
    pub success: bool, // true if follower contained entry matching prev_log_term and prev_log_index
}

#[tarpc::service]
pub trait Raft {
    async fn request_vote(request: RequestVoteReq) -> RequestVoteResp;

    async fn append_entries(request: AppendEntriesReq) -> AppendEntriesResp;
}
