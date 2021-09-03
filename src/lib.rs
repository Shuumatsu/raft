#[macro_use]
extern crate scopeguard;
#[macro_use]
extern crate async_recursion;
#[macro_use]
extern crate serde;

use futures::prelude::*;
use scopeguard::guard_on_success;
use std::cmp::{self, Ordering};
use std::time;
use tarpc::context;
use tokio::sync::{mpsc, oneshot};

mod rpc;

use rpc::{AppendEntriesReq, AppendEntriesResp, Raft, RaftClient, RequestVoteReq, RequestVoteResp};

type Term = usize;

type ServerId = usize;

#[derive(Debug)]
pub enum Event {
    Timer(TimerEvent),
    RPC(RPCEvent),
}

#[derive(Debug)]
pub enum TimerEvent {
    ElectionTimeout {
        term: Term,
        timestamp: time::Instant,
    },
    // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
    // 候选人发起投票后，如果在一定时间内没有获得半数以上的投票的话，则视为失败
    VoteTimeout {
        term: Term,
    },
}

#[derive(Debug)]
pub enum RequestVoteEvent {
    Request {
        request: RequestVoteReq,
        reply: oneshot::Sender<RequestVoteResp>,
    },
    Response {
        peer_id: ServerId,
        request: RequestVoteReq,
        response: RequestVoteResp,
    },
}

#[derive(Debug)]
pub enum AppendEntriesEvent {
    // AppendEntries RPC 由领导者调用，用于日志条目的复制，同时也被当做心跳使用
    Request {
        request: AppendEntriesReq,
        reply: oneshot::Sender<AppendEntriesResp>,
    },
    Response {
        peer_id: ServerId,
        request: AppendEntriesReq,
        response: AppendEntriesResp,
    },
}

#[derive(Debug)]
pub enum RPCEvent {
    RequestVote(RequestVoteEvent),
    AppendEntries(AppendEntriesEvent),
}

#[derive(Debug)]
pub enum Role {
    Leader {
        // for each server, index of the next log entry to send to that server
        // initialized to leader last log index + 1
        next_index: Vec<usize>,
        // for each server, number of log entries known to be replicated on server
        match_cnt: Vec<usize>,
    },
    Candidate {
        votes_cnt: usize,
    },
    Follower {
        voted_for: Option<ServerId>,
        timestamp: time::Instant,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Entry {
    term: Term,
    command: usize,
}

#[derive(Debug)]
pub struct PersistentState {
    term: Term,
    id: ServerId,
    log_entries: Vec<Entry>,
}

impl PersistentState {
    pub fn new(id: ServerId) -> Self {
        PersistentState {
            term: 0,
            id,
            log_entries: vec![],
        }
    }

    pub fn progress(&self) -> Option<(Term, usize)> {
        let entry = self.log_entries.last();
        entry.map(|Entry { term, .. }| (*term, self.log_entries.len() - 1))
    }
}

#[derive(Debug)]
pub struct Server {
    persistent_state: PersistentState,
    commit_index: Option<usize>, // 已知已提交的最高的日志条目的索引（初始值为 0，单调递增）
    last_applied: Option<usize>, // 已经被应用到状态机的最高的日志条目的索引（初始值为 0，单调递增）
    peers: Vec<RaftClient>,
    role: Role,
    events_sender: mpsc::UnboundedSender<Event>,
    events_receiver: mpsc::UnboundedReceiver<Event>,
}

impl Server {
    pub fn new(id: usize, peers: Vec<RaftClient>) -> Self {
        let (events_sender, events_receiver) = mpsc::unbounded_channel();
        Server {
            persistent_state: PersistentState::new(id),
            commit_index: None,
            last_applied: None,
            role: Role::Follower {
                voted_for: None,
                timestamp: time::Instant::now(),
            },
            peers,
            events_sender,
            events_receiver,
        }
    }

    pub fn run(mut self) {
        self.start_election_timer();

        tokio::spawn(async move {
            while let Some(event) = self.events_receiver.recv().await {
                match event {
                    Event::RPC(event) => self.react_rpc(event),
                    Event::Timer(event) => self.react_timer(event),
                }
            }
        });
    }

    pub fn start_election_timer(&mut self) {
        let event = Event::Timer(TimerEvent::ElectionTimeout {
            term: self.persistent_state.term,
            timestamp: time::Instant::now(),
        });
        self.events_sender.send(event).unwrap();
    }

    pub fn start_vote_timer(&mut self) {
        let event = Event::Timer(TimerEvent::VoteTimeout {
            term: self.persistent_state.term,
        });
        self.events_sender.send(event).unwrap();
    }

    pub fn start_election(&mut self) {
        self.start_vote_timer();

        match &self.role {
            Role::Follower { .. } => {
                for (peer_id, peer) in self.peers.iter().map(|p| p.clone()).enumerate() {
                    let sender = self.events_sender.clone();

                    let request = RequestVoteReq {
                        requester_term: self.persistent_state.term,
                        requester_id: self.persistent_state.id,
                        prev_entry_identifier: self.persistent_state.progress(),
                    };

                    tokio::spawn(async move {
                        match peer.request_vote(context::current(), request.clone()).await {
                            Ok(response) => {
                                let event =
                                    Event::RPC(RPCEvent::RequestVote(RequestVoteEvent::Response {
                                        request,
                                        response,
                                        peer_id,
                                    }));
                                sender.send(event).unwrap()
                            }
                            _ => {}
                        }
                    });
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn append_entries(&mut self) {
        match &self.role {
            Role::Leader { next_index, .. } => {
                for (peer_id, peer) in self.peers.iter().map(|p| p.clone()).enumerate() {
                    let sender = self.events_sender.clone();

                    let prev_entry_identifier = next_index[peer_id]
                        .checked_sub(1)
                        .map(|idx| (self.persistent_state.log_entries[idx].term, idx));
                    let entries =
                        Vec::from(&self.persistent_state.log_entries[next_index[peer_id]..]);
                    let request = AppendEntriesReq {
                        requester_term: self.persistent_state.term,
                        requester_id: self.persistent_state.id,
                        prev_entry_identifier,
                        leader_commit: self.commit_index,
                        entries,
                    };

                    tokio::spawn(async move {
                        match peer
                            .append_entries(context::current(), request.clone())
                            .await
                        {
                            Ok(response) => {
                                let event = Event::RPC(RPCEvent::AppendEntries(
                                    AppendEntriesEvent::Response {
                                        request,
                                        response,
                                        peer_id,
                                    },
                                ));
                                sender.send(event).unwrap()
                            }
                            _ => {}
                        }
                    });
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn react_timer(&mut self, event: TimerEvent) {
        let term = match &event {
            TimerEvent::ElectionTimeout { term, .. } => *term,
            TimerEvent::VoteTimeout { term, .. } => *term,
        };
        match term.cmp(&self.persistent_state.term) {
            Ordering::Less => {}
            Ordering::Greater => unreachable!(),
            Ordering::Equal => match (&mut self.role, event) {
                // 在同一 term，不存在从 Follower 到 Leader 的转变，不应有 ElectionTimeout 事件
                (Role::Leader { .. }, TimerEvent::ElectionTimeout { .. }) => unreachable!(),
                // 在同一 term，可能先处于 Candidate 后处于 Leader，忽略已过期的定时器
                (Role::Leader { .. }, TimerEvent::VoteTimeout { .. }) => {}

                // 在同一 term，不存在从 Follower 到 Candidate 的转变，不应有 ElectionTimeout 事件
                (Role::Candidate { .. }, TimerEvent::ElectionTimeout { .. }) => unreachable!(),
                // 超时后通过增加当前任期号来开始一轮新的选举
                (Role::Candidate { votes_cnt, .. }, TimerEvent::VoteTimeout { .. }) => {
                    self.persistent_state.term += 1;
                    *votes_cnt = 1;
                    self.start_election();
                }

                (
                    Role::Follower { timestamp, .. },
                    TimerEvent::ElectionTimeout {
                        timestamp: e_timestamp,
                        ..
                    },
                ) => {
                    // 计时器未失效，即自从上次计时后没有新的 RPC 请求
                    if &e_timestamp == timestamp {
                        // 超时后通过增加当前任期号来开始一轮新的选举。
                        self.persistent_state.term += 1;
                        self.role = Role::Candidate { votes_cnt: 1 };
                        self.start_election();
                    } else {
                        self.start_election_timer();
                    }
                }
                // 在同一 term，可能先处于 Candidate 状态后处于 Follower 状态，忽略已过期的计时器
                (Role::Follower { .. }, TimerEvent::VoteTimeout { .. }) => {}
            },
        }
    }

    // #[async_recursion(?Send)]
    pub fn react_rpc(&mut self, event: RPCEvent) {
        let remote_term = match &event {
            RPCEvent::RequestVote(RequestVoteEvent::Request { request, .. }) => {
                request.requester_term
            }
            RPCEvent::RequestVote(RequestVoteEvent::Response { response, .. }) => {
                response.responser_term
            }
            RPCEvent::AppendEntries(AppendEntriesEvent::Request { request, .. }) => {
                request.requester_term
            }
            RPCEvent::AppendEntries(AppendEntriesEvent::Response { response, .. }) => {
                response.responser_term
            }
        };

        match remote_term.cmp(&self.persistent_state.term) {
            Ordering::Less => match event {
                RPCEvent::RequestVote(RequestVoteEvent::Request { reply, .. }) => {
                    let resp = RequestVoteResp {
                        responser_term: self.persistent_state.term,
                        vote_granted: false,
                    };
                    reply.send(resp).unwrap();
                }
                RPCEvent::AppendEntries(AppendEntriesEvent::Request { reply, .. }) => {
                    let resp = AppendEntriesResp {
                        responser_term: self.persistent_state.term,
                        success: false,
                    };
                    reply.send(resp).unwrap();
                }
                _ => {}
            },
            Ordering::Greater => {
                self.persistent_state.term = remote_term;
                self.role = Role::Follower {
                    voted_for: None,
                    timestamp: time::Instant::now(),
                };
                self.react_rpc(event)
            }
            Ordering::Equal => match event {
                RPCEvent::RequestVote(event) => self.react_request_vote(event),
                RPCEvent::AppendEntries(event) => self.react_append_entries(event),
            },
        }
    }

    pub fn react_append_entries(&mut self, event: AppendEntriesEvent) {
        match (event, &mut self.role) {
            // AppendEntries 请求只由 leader 发出，而同一任期内只应该有一个 leader
            (AppendEntriesEvent::Request { .. }, Role::Leader { .. }) => {}
            (
                AppendEntriesEvent::Response {
                    request,
                    response,
                    peer_id,
                },
                Role::Leader {
                    next_index,
                    match_cnt,
                },
            ) => {
                if response.success {
                    next_index[peer_id] += request.entries.len();
                    match_cnt[peer_id] += next_index[peer_id]; // ?

                // 如果过半了
                } else {
                    next_index[peer_id] -= 1;
                }
            }

            // 同期内有其他服务器成为 Leader
            (event @ AppendEntriesEvent::Request { .. }, Role::Candidate { .. }) => {
                self.role = Role::Follower {
                    voted_for: None,
                    timestamp: time::Instant::now(),
                };
                return self.react_append_entries(event);
            }
            // 在同一 term，不存在从 Leader 到 Candidate 的转变，不应曾发起过 AppendEntries 请求
            (AppendEntriesEvent::Response { .. }, Role::Candidate { .. }) => {
                unreachable!()
            }

            (AppendEntriesEvent::Request { request, reply }, Role::Follower { timestamp, .. }) => {
                *timestamp = time::Instant::now();

                // 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
                if let Some((prev_index, prev_term)) = request.prev_entry_identifier {
                    if self.persistent_state.log_entries[prev_index].term != prev_term {
                        let resp = AppendEntriesResp {
                            success: false,
                            responser_term: self.persistent_state.term,
                        };
                        reply.send(resp).unwrap();
                        return;
                    }
                }

                let curr_index = request
                    .prev_entry_identifier
                    .map(|(i, _)| i + 1)
                    .unwrap_or(0);
                self.persistent_state.log_entries.truncate(curr_index);
                for entry in request.entries {
                    self.persistent_state.log_entries.push(entry);
                }
                // 如果领导者的已知已经提交的最高的日志条目的索引 leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引 commitIndex
                if request.leader_commit > self.commit_index {
                    self.commit_index = cmp::min(
                        // 领导者的已知已经提交的最高的日志条目的索引
                        request.leader_commit,
                        // 上一个新条目的索引
                        self.persistent_state.log_entries.len().checked_sub(1),
                    );
                }

                let resp = AppendEntriesResp {
                    success: true,
                    responser_term: self.persistent_state.term,
                };
                reply.send(resp).unwrap();
            }
            // 在同一 term，可能先处于 Leader 后处于 Follower，忽略已过期的回复
            (AppendEntriesEvent::Response { .. }, Role::Follower { .. }) => {}
        }

        if self.commit_index > self.last_applied {
            self.last_applied = self.last_applied.map(|i| i + 1);
            // 并把 log_entries[last_applied] 应用到状态机中
        }
    }

    pub fn react_request_vote(&mut self, event: RequestVoteEvent) {
        match (event, &mut self.role) {
            (RequestVoteEvent::Request { reply, .. }, Role::Leader { .. }) => {
                let resp = RequestVoteResp {
                    responser_term: self.persistent_state.term,
                    vote_granted: false,
                };
                reply.send(resp).unwrap();
            }
            // 在同一 term，可能先处于 Candidate 后处于 Leader，忽略已过期的回复
            (RequestVoteEvent::Response { .. }, Role::Leader { .. }) => {}

            (RequestVoteEvent::Request { reply, .. }, Role::Candidate { .. }) => {
                let resp = RequestVoteResp {
                    responser_term: self.persistent_state.term,
                    vote_granted: false,
                };
                reply.send(resp).unwrap();
            }
            (RequestVoteEvent::Response { response, .. }, Role::Candidate { votes_cnt, .. }) => {
                *votes_cnt += if response.vote_granted { 1 } else { 0 };
                // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
                if *votes_cnt > self.peers.len() / 2 {
                    self.role = Role::Leader {
                        next_index: self.peers.iter().map(|_| 0).collect(),
                        match_cnt: self.peers.iter().map(|_| 0).collect(),
                    };
                    // 然后他会向其他的服务器发送心跳消息来建立自己的权威并且阻止新的领导人的产生。
                }
            }

            (
                RequestVoteEvent::Request { request, reply, .. },
                Role::Follower {
                    voted_for,
                    timestamp,
                    ..
                },
            ) => {
                if voted_for.is_none() {
                    *voted_for = Some(request.requester_id);
                }
                *timestamp = time::Instant::now();

                let resp = RequestVoteResp {
                    responser_term: self.persistent_state.term,
                    vote_granted: *voted_for == Some(request.requester_id),
                };
                reply.send(resp).unwrap();
            }
            // 在同一 term，可能先处于 Candidate 后处于 Follower，忽略已过期的回复
            (RequestVoteEvent::Response { .. }, Role::Follower { .. }) => {}
        }
    }
}

#[derive(Debug)]
struct Service(usize);

#[tarpc::server]
impl Raft for Server {
    async fn request_vote(
        mut self,
        _: context::Context,
        request: RequestVoteReq,
    ) -> RequestVoteResp {
        let (tx, rx) = oneshot::channel();
        let event = RPCEvent::RequestVote(RequestVoteEvent::Request { request, reply: tx });

        self.react_rpc(event);
        rx.await.unwrap()
    }

    async fn append_entries(
        mut self,
        _: context::Context,
        request: AppendEntriesReq,
    ) -> AppendEntriesResp {
        let (tx, rx) = oneshot::channel();
        let event = RPCEvent::AppendEntries(AppendEntriesEvent::Request { request, reply: tx });

        self.react_rpc(event);
        rx.await.unwrap()
    }
}
