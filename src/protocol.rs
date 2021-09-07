#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub term: Term,
    pub command: String,
}

pub type Term = usize;

pub type ServerId = usize;

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
    pub requester_id: ServerId,
    pub requester_term: Term,
    pub sequence_number: usize,
    pub prev_entry_index: usize,
    pub prev_entry_term: Term,
    pub leader_commit: usize,
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum AppendEntriesError {
    Conflict { recommended_next_index: usize },
    Outdated,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResp {
    pub responser_term: Term,
    pub result: Result<(), AppendEntriesError>,
}
