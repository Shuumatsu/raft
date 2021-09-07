#![feature(let_chains)]
#[macro_use]
extern crate scopeguard;
#[macro_use]
extern crate async_recursion;
#[macro_use]
extern crate serde;

use futures::prelude::*;
use scopeguard::guard_on_success;
use std::cmp::{self, Ordering};
use std::collections::HashMap;
use tarpc::context;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration, Instant};

mod event_channel;
mod protocol;
mod rpc;

use event_channel::{AppendEntriesEvent, Event, RPCEvent, RequestVoteEvent, TimerEvent};
use protocol::{
    AppendEntriesError, AppendEntriesReq, AppendEntriesResp, Entry, RequestVoteReq,
    RequestVoteResp, ServerId, Term,
};
use rpc::RaftClient;

#[derive(Debug)]
pub enum Role {
    Leader {
        // for each server, index of the next log entry to send to that server
        // initialized to leader last log index + 1
        next_index: HashMap<ServerId, usize>,
        // for each server, number of log entries known to be replicated on server
        match_index: HashMap<ServerId, usize>,
        sequence_number: HashMap<ServerId, usize>,
        replied_sequence_number: HashMap<ServerId, usize>,
    },
    Candidate {
        votes_count: usize,
    },
    Follower {
        voted_for: Option<ServerId>,
        last_sequence_number: usize,
        last_rpc_timestamp: Instant,
    },
}

#[derive(Debug)]
pub struct PersistentState {
    term: Term,
    id: ServerId,
    log_entries: Vec<Entry>,
}

impl PersistentState {
    pub fn new(id: ServerId) -> Self {
        let initial_entry = Entry {
            term: 0,
            command: String::new(),
        };

        PersistentState {
            term: 0,
            id,
            log_entries: vec![initial_entry],
        }
    }

    pub async fn persist(&self) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Server {
    role: Role,
    peers: HashMap<ServerId, RaftClient>,
    persistent_state: PersistentState,
    commit_index: usize, // 已知已提交的最高的日志条目的索引（初始值为 0，单调递增）
    last_applied: usize, // 已经被应用到状态机的最高的日志条目的索引（初始值为 0，单调递增）
    events_sender: event_channel::Sender,
    events_receiver: event_channel::Receiver,
    apply_ch: mpsc::UnboundedSender<Entry>,
}

impl Server {
    pub fn new(
        id: usize,
        peers: HashMap<ServerId, RaftClient>,
        apply_ch: mpsc::UnboundedSender<Entry>,
    ) -> Self {
        let (events_sender, events_receiver) = event_channel::new();
        Server {
            role: Role::Follower {
                voted_for: None,
                last_sequence_number: 0,
                last_rpc_timestamp: Instant::now(),
            },
            peers,
            persistent_state: PersistentState::new(id),
            commit_index: 0,
            last_applied: 0,
            events_sender,
            events_receiver,
            apply_ch,
        }
    }

    pub fn become_leader(&mut self) {
        match self.role {
            Role::Leader { .. } => unreachable!(),
            Role::Candidate { .. } => {
                self.role = Role::Leader {
                    next_index: self
                        .peers
                        .iter()
                        .map(|(&peer_id, _)| (peer_id, self.persistent_state.log_entries.len()))
                        .collect(),
                    match_index: self
                        .peers
                        .iter()
                        .map(|(&peer_id, _)| (peer_id, 0))
                        .collect(),
                    sequence_number: self
                        .peers
                        .iter()
                        .map(|(&peer_id, _)| (peer_id, 0))
                        .collect(),
                    replied_sequence_number: self
                        .peers
                        .iter()
                        .map(|(&peer_id, _)| (peer_id, 0))
                        .collect(),
                };

                // 然后他会向其他的服务器发送心跳消息来建立自己的权威并且阻止新的领导人的产生。
                let event = TimerEvent::HeartBeatTimeout {
                    term: self.persistent_state.term,
                };
                self.events_sender.send_timer_event(event).unwrap();
            }
            Role::Follower { .. } => unreachable!(),
        }
    }

    pub fn become_candidate(&mut self) {
        match self.role {
            Role::Leader { .. } => unreachable!(),
            Role::Candidate { .. } => unreachable!(),
            Role::Follower { .. } => {
                self.persistent_state.term += 1;
                self.role = Role::Candidate { votes_count: 1 };
                self.start_request_vote();
            }
        }
    }

    pub fn become_follower(&mut self, next_term: Term) {
        self.persistent_state.term = next_term;
        self.role = Role::Follower {
            voted_for: None,
            last_sequence_number: 0,
            last_rpc_timestamp: Instant::now(),
        };
        self.start_election_timer();
    }

    pub fn run(mut self) {
        self.start_election_timer();

        tokio::task::spawn(async move {
            while let Some(event) = self.events_receiver.recv().await {
                match event {
                    Event::RPC(event) => self.react_rpc(event),
                    Event::Timer(event) => self.react_timer(event),
                }
            }
        });
    }

    pub fn start_election_timer(&mut self) {
        match &self.role {
            Role::Candidate { .. } => {
                let event = TimerEvent::ElectionTimeout {
                    term: self.persistent_state.term,
                    timestamp: Instant::now(),
                };

                let sender = self.events_sender.clone();
                tokio::task::spawn(async move {
                    sleep(Duration::from_millis(1000)).await;
                    sender.send_timer_event(event).unwrap();
                });
            }
            _ => unreachable!(),
        }
    }

    pub fn start_vote_timer(&mut self) {
        match &self.role {
            Role::Candidate { .. } => {
                let event = TimerEvent::VoteTimeout {
                    term: self.persistent_state.term,
                };

                let sender = self.events_sender.clone();
                tokio::task::spawn(async move {
                    sleep(Duration::from_millis(1000)).await;
                    sender.send_timer_event(event).unwrap();
                });
            }
            _ => unreachable!(),
        }
    }

    pub fn start_heat_beat_timer(&mut self) {
        match &self.role {
            Role::Leader { .. } => {
                let event = TimerEvent::HeartBeatTimeout {
                    term: self.persistent_state.term,
                };

                let sender = self.events_sender.clone();
                tokio::task::spawn(async move {
                    sleep(Duration::from_millis(100)).await;
                    sender.send_timer_event(event).unwrap();
                });
            }
            _ => unreachable!(),
        }
    }

    pub fn start_request_vote(&mut self) {
        self.start_vote_timer();

        match &mut self.role {
            Role::Candidate { .. } => {
                for (&peer_id, peer) in self.peers.iter() {
                    let sender = self.events_sender.clone();
                    let peer = peer.clone();

                    let progress = {
                        let term = self.persistent_state.log_entries.last().unwrap().term;
                        (term, self.persistent_state.log_entries.len() - 1)
                    };
                    let request = RequestVoteReq {
                        requester_term: self.persistent_state.term,
                        requester_id: self.persistent_state.id,
                        progress,
                    };

                    tokio::task::spawn(async move {
                        match peer.request_vote(context::current(), request.clone()).await {
                            Ok(response) => {
                                let event = RequestVoteEvent::Response {
                                    request,
                                    response,
                                    peer_id,
                                };
                                sender.send_request_vote_event(event).unwrap()
                            }
                            _ => {}
                        }
                    });
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn start_append_entries(&mut self) {
        match &mut self.role {
            Role::Leader {
                replied_sequence_number,
                sequence_number,
                next_index,
                ..
            } => {
                for (&peer_id, peer) in self.peers.iter() {
                    let sender = self.events_sender.clone();
                    let peer = peer.clone();

                    match replied_sequence_number[&peer_id].cmp(&sequence_number[&peer_id]) {
                        Ordering::Less => {}
                        Ordering::Equal => {
                            sequence_number.entry(peer_id).and_modify(|s| *s += 1);
                        }
                        Ordering::Greater => unreachable!(),
                    }

                    let (prev_entry_index, prev_entry_term) = {
                        let index = next_index[&peer_id] - 1;
                        (self.persistent_state.log_entries[index].term, index)
                    };
                    let entries =
                        Vec::from(&self.persistent_state.log_entries[next_index[&peer_id]..]);
                    let request = AppendEntriesReq {
                        requester_id: self.persistent_state.id,
                        requester_term: self.persistent_state.term,
                        sequence_number: sequence_number[&peer_id],
                        prev_entry_index,
                        prev_entry_term,
                        leader_commit: self.commit_index,
                        entries,
                    };

                    tokio::task::spawn(async move {
                        if let Ok(response) = peer
                            .append_entries(context::current(), request.clone())
                            .await
                        {
                            let event = AppendEntriesEvent::Response {
                                request,
                                response,
                                peer_id,
                            };
                            sender.send_append_entries_event(event).unwrap();
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
            TimerEvent::HeartBeatTimeout { term, .. } => *term,
        };
        match term.cmp(&self.persistent_state.term) {
            Ordering::Less => {}
            Ordering::Greater => unreachable!(),
            Ordering::Equal => match (event, &mut self.role) {
                // 在同一 term，不存在从 Follower 到 Leader 的转变，不应有 ElectionTimeout 事件
                (TimerEvent::ElectionTimeout { .. }, Role::Leader { .. }) => unreachable!(),

                // 在同一 term，不存在从 Follower 到 Candidate 的转变，不应有 ElectionTimeout 事件
                (TimerEvent::ElectionTimeout { .. }, Role::Candidate { .. }) => unreachable!(),

                (
                    TimerEvent::ElectionTimeout { timestamp, .. },
                    Role::Follower {
                        last_rpc_timestamp, ..
                    },
                ) => {
                    // 计时器未失效，即自从上次计时后没有新的 RPC 请求
                    if &timestamp == last_rpc_timestamp {
                        // 超时后通过增加当前任期号来开始一轮新的选举。
                        self.become_candidate();
                    } else {
                        self.start_election_timer();
                    }
                }

                // 在同一 term，可能先处于 Candidate 后处于 Leader，忽略已过期的定时器
                (TimerEvent::VoteTimeout { .. }, Role::Leader { .. }) => {}

                // 超时后通过增加当前任期号来开始一轮新的选举
                (TimerEvent::VoteTimeout { .. }, Role::Candidate { .. }) => self.become_candidate(),

                // 在同一 term，可能先处于 Candidate 状态后处于 Follower 状态，忽略已过期的计时器
                (TimerEvent::VoteTimeout { .. }, Role::Follower { .. }) => {}

                (TimerEvent::HeartBeatTimeout { .. }, Role::Leader { .. }) => {
                    self.start_append_entries();
                    self.start_heat_beat_timer();
                }

                // 在同一 term，不存在从 Leader 到 Candidate 的转变，不应有 HeartBeatTimeout 事件
                (TimerEvent::HeartBeatTimeout { .. }, Role::Candidate { .. }) => unreachable!(),

                // 在同一 term，不存在从 Leader 到 Follower 的转变，不应有 HeartBeatTimeout 事件
                (TimerEvent::HeartBeatTimeout { .. }, Role::Follower { .. }) => unreachable!(),
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

        match (remote_term.cmp(&self.persistent_state.term), event) {
            (Ordering::Greater, event) => {
                self.become_follower(remote_term);
                self.react_rpc(event)
            }
            (_, RPCEvent::RequestVote(event)) => self.react_request_vote(event),
            (_, RPCEvent::AppendEntries(event)) => self.react_append_entries(event),
        }
    }

    pub fn react_request_vote(&mut self, event: RequestVoteEvent) {
        match (event, &mut self.role) {
            (RequestVoteEvent::Request { request, reply }, _)
                if request.requester_term < self.persistent_state.term =>
            {
                let resp = RequestVoteResp {
                    responser_term: self.persistent_state.term,
                    vote_granted: false,
                };
                reply.send(resp).unwrap();
            }
            (RequestVoteEvent::Response { request, .. }, _)
                if request.requester_term < self.persistent_state.term => {}

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
            (
                RequestVoteEvent::Response { response, .. },
                Role::Candidate {
                    votes_count: votes_cnt,
                    ..
                },
            ) => {
                *votes_cnt += if response.vote_granted { 1 } else { 0 };
                // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
                if *votes_cnt == self.peers.len() / 2 + 1 {
                    self.become_leader();
                }
            }

            (
                RequestVoteEvent::Request { request, reply, .. },
                Role::Follower {
                    voted_for,
                    last_rpc_timestamp,
                    ..
                },
            ) => {
                let progress = {
                    let term = self.persistent_state.log_entries.last().unwrap().term;
                    (term, self.persistent_state.log_entries.len() - 1)
                };
                if request.progress < progress {
                    let resp = RequestVoteResp {
                        responser_term: self.persistent_state.term,
                        vote_granted: false,
                    };
                    reply.send(resp).unwrap();
                } else {
                    *last_rpc_timestamp = Instant::now();
                    let resp = if let Some(_) = voted_for {
                        RequestVoteResp {
                            responser_term: self.persistent_state.term,
                            vote_granted: false,
                        }
                    } else {
                        *voted_for = Some(request.requester_id);
                        RequestVoteResp {
                            responser_term: self.persistent_state.term,
                            vote_granted: true,
                        }
                    };
                    reply.send(resp).unwrap();
                }
            }
            // 在同一 term，可能先处于 Candidate 后处于 Follower，忽略已过期的回复
            (RequestVoteEvent::Response { .. }, Role::Follower { .. }) => {}
        }
    }

    pub fn react_append_entries(&mut self, event: AppendEntriesEvent) {
        match (event, &mut self.role) {
            // 拒绝过期的请求
            (AppendEntriesEvent::Request { request, reply }, _)
                if request.requester_term < self.persistent_state.term =>
            {
                let resp = AppendEntriesResp {
                    responser_term: self.persistent_state.term,
                    result: Err(AppendEntriesError::Outdated),
                };
                reply.send(resp).unwrap();
            }
            // 忽略过期的回复
            (AppendEntriesEvent::Response { request, .. }, _)
                if request.requester_term < self.persistent_state.term => {}

            // AppendEntries 请求只由 leader 发出，而同一任期内只应该有一个 leader
            (AppendEntriesEvent::Request { .. }, Role::Leader { .. }) => unreachable!(),
            (
                AppendEntriesEvent::Response {
                    request,
                    response,
                    peer_id,
                },
                Role::Leader {
                    next_index,
                    match_index,
                    sequence_number,
                    replied_sequence_number,
                },
            ) => {
                if request.sequence_number > replied_sequence_number[&peer_id] {
                    replied_sequence_number.insert(peer_id, request.sequence_number);

                    match response.result {
                        Ok(()) => {
                            next_index
                                .insert(peer_id, request.prev_entry_index + request.entries.len());
                            match_index.insert(peer_id, next_index[&peer_id] - 1);

                            let mut appended_index = self.commit_index;
                            for entry_index in self.commit_index + 1..=match_index[&peer_id] {
                                let cnt = self.peers.keys().fold(1, |accu, peer_id| {
                                    accu + if match_index[peer_id] >= entry_index {
                                        1
                                    } else {
                                        0
                                    }
                                });
                                if cnt > (self.peers.len() + 1) / 2 {
                                    appended_index = entry_index;
                                } else {
                                    break;
                                }
                            }
                            if self.persistent_state.log_entries[appended_index].term
                                == self.persistent_state.term
                            {
                                self.commit_index = appended_index;
                            }
                        }
                        Err(AppendEntriesError::Outdated) => {}
                        Err(AppendEntriesError::Conflict {
                            recommended_next_index,
                        }) => {
                            next_index.insert(peer_id, recommended_next_index);
                        }
                    }
                }
            }

            // 同期内有其他服务器成为 Leader
            (event @ AppendEntriesEvent::Request { .. }, Role::Candidate { .. }) => {
                self.role = Role::Follower {
                    voted_for: None,
                    last_rpc_timestamp: Instant::now(),
                    last_sequence_number: 0,
                };
                self.start_election_timer();

                return self.react_append_entries(event);
            }
            // 在同一 term，不存在从 Leader 到 Candidate 的转变，不应曾发起过 AppendEntries 请求
            (AppendEntriesEvent::Response { .. }, Role::Candidate { .. }) => {
                unreachable!()
            }

            (
                AppendEntriesEvent::Request { request, reply },
                Role::Follower {
                    last_rpc_timestamp,
                    last_sequence_number,
                    ..
                },
            ) => {
                if request.sequence_number <= *last_sequence_number {
                    return;
                }
                *last_sequence_number = request.sequence_number;
                *last_rpc_timestamp = Instant::now();

                // 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上
                if request.prev_entry_index >= self.persistent_state.log_entries.len() {
                    let resp = AppendEntriesResp {
                        responser_term: self.persistent_state.term,
                        result: Err(AppendEntriesError::Conflict {
                            recommended_next_index: self.persistent_state.log_entries.len(),
                        }),
                    };
                    reply.send(resp).unwrap();
                } else if request.prev_entry_term
                    != self.persistent_state.log_entries[request.prev_entry_index].term
                {
                    let resp = AppendEntriesResp {
                        responser_term: self.persistent_state.term,
                        result: Err(AppendEntriesError::Conflict {
                            recommended_next_index: request.prev_entry_index - 1,
                        }),
                    };
                    reply.send(resp).unwrap();
                } else {
                    let curr_index = request.prev_entry_index + 1;
                    self.persistent_state.log_entries.truncate(curr_index);
                    for entry in request.entries {
                        self.persistent_state.log_entries.push(entry);
                    }

                    // 如果领导者的已知已经提交的最高的日志条目的索引 leaderCommit 大于接收者的已知已经提交的最高的日志条目的索引 commitIndex
                    if request.leader_commit > self.commit_index {
                        self.commit_index = cmp::min(
                            // 领导者的已知已经提交的最高的日志条目的索引
                            request.leader_commit,
                            // 上一个新条目的索引
                            self.persistent_state.log_entries.len() - 1,
                        );
                    }

                    let resp = AppendEntriesResp {
                        responser_term: self.persistent_state.term,
                        result: Ok(()),
                    };
                    reply.send(resp).unwrap();
                }
            }

            // 在同一 term，不存在从 Leader 到 Follower 的转变，不应曾发起过 AppendEntries 请求
            (AppendEntriesEvent::Response { .. }, Role::Follower { .. }) => unreachable!(),
        }

        for i in self.last_applied + 1..=self.commit_index {
            // 并把 log_entries[last_applied] 应用到状态机中
            self.apply_ch
                .send(self.persistent_state.log_entries[i].clone())
                .unwrap();
        }
        self.last_applied = self.commit_index;
    }
}
