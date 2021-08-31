#![feature(let_chains)]
#[macro_use]
extern crate async_recursion;
#[macro_use]
extern crate serde;

use futures::{future::Ready, prelude::*};
use std::cmp::{self, Ordering};
use std::time;
use tarpc::context;
use tokio::sync::mpsc;

mod rpc;

use rpc::{AppendEntriesArgs, AppendEntriesReply, Raft, RequestVoteArgs, RequestVoteReply};

type Term = usize;

type ServerId = usize;

#[derive(Debug)]
pub enum Event {
    ElectionTimeout { timestamp: time::Instant },
    // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
    // 候选人发起投票后，如果在一定时间内没有获得半数以上的投票的话，则视为失败
    VoteTimeout, // VoteFailure
    RequestVoteReq { candidate: ServerId },
    RequestVoteResp { ok: bool },
    // AppendEntries RPC 由领导者调用，用于日志条目的复制，同时也被当做心跳使用
    AppendEntriesReq { leader: ServerId },
    AppendEntriesResp { ok: bool },
}

#[derive(Debug)]
pub struct Input {
    term: Term,
    event: Event,
}

#[derive(Debug)]
pub enum Role {
    Leader {
        next_index: Vec<usize>,
        match_index: Vec<usize>,
    },
    Candidate {
        votes_cnt: usize,
    },
    Follower {
        voted_for: Option<ServerId>,
        timestamp: time::Instant,
    },
}

pub type LogEntry = (usize, String);

#[derive(Debug)]
pub struct PersistentState {
    term: Term,
    id: ServerId,
    log_entries: Vec<LogEntry>,
}

impl PersistentState {
    pub fn new(id: ServerId) -> Self {
        PersistentState {
            term: 0,
            id,
            log_entries: vec![],
        }
    }
}

#[derive(Debug)]
pub struct Server {
    persistent_state: PersistentState,
    commit_index: Option<usize>, // 已知已提交的最高的日志条目的索引（初始值为 0，单调递增）
    last_applied: Option<usize>, // 已经被应用到状态机的最高的日志条目的索引（初始值为 0，单调递增）
    system_servers: Vec<usize>,
    role: Role,
}

impl Server {
    pub fn new(id: usize, system_servers: Vec<usize>) -> Self {
        Server {
            persistent_state: PersistentState::new(id),
            commit_index: None,
            last_applied: None,
            role: Role::Follower {
                voted_for: None,
                timestamp: time::Instant::now(),
            },
            system_servers,
        }
    }

    pub async fn send_request_vote(&mut self, peer: usize) {
        unimplemented!()
    }

    pub async fn start_election(&mut self) {
        match self.role {
            Role::Follower { .. } => {
                unimplemented!()
            }
            _ => unreachable!(),
        }
    }

    pub async fn anounce_leadership(&mut self) {
        match self.role {
            Role::Leader { .. } => {
                unimplemented!()
            }
            _ => unreachable!(),
        }
    }

    pub async fn run(&mut self) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Input>();

        while let Some(input) = rx.recv().await {
            self.react(input);
        }
    }

    #[async_recursion(?Send)]
    pub async fn react(&mut self, input: Input) {
        match (input.term.cmp(&self.persistent_state.term), &mut self.role) {
            // 忽略所有已过期的事件
            (Ordering::Less, _) => {}
            (Ordering::Greater, _) => match input.event {
                // 如果接收到的 RPC 请求或响应中，任期号 resp.term > self.term，那么就令 self.term 等于 resp.term，并切换状态为跟随者
                Event::RequestVoteReq { .. }
                | Event::RequestVoteResp { .. }
                | Event::AppendEntriesReq { .. }
                | Event::AppendEntriesResp { .. } => {
                    self.persistent_state.term = input.term;
                    self.role = Role::Follower {
                        voted_for: None,
                        timestamp: time::Instant::now(),
                    };
                    self.react(input).await
                }
                // 计时器事件不可能有更高的 term
                _ => unreachable!(),
            },

            (Ordering::Equal, Role::Leader { .. }) => match input.event {
                Event::RequestVoteReq { .. } => {}
                // 在同一 term，可能先处于 Candidate 后处于 Leader，忽略已过期的回复
                Event::RequestVoteResp { .. } => {}
                // AppendEntries 请求只由 leader 发出，而同一任期内只应该有一个 leader
                Event::AppendEntriesReq { .. } => unreachable!(),
                Event::AppendEntriesResp { .. } => {}
                // 在同一 term，不存在从 Follower 到 Leader 的转变，不应有 ElectionTimeout 事件
                Event::ElectionTimeout { .. } => unreachable!(),
                // 在同一 term，可能先处于 Candidate 后处于 Leader，忽略已过期的定时器
                Event::VoteTimeout { .. } => {}
            },

            (
                Ordering::Equal,
                Role::Candidate {
                    ref mut votes_cnt, ..
                },
            ) => match input.event {
                Event::RequestVoteReq { .. } => {}

                Event::RequestVoteResp { ok } => {
                    *votes_cnt += if ok { 1 } else { 0 };
                    // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
                    if *votes_cnt > self.system_servers.len() / 2 {
                        self.role = Role::Leader {
                            next_index: self.system_servers.iter().map(|_| 0).collect(),
                            match_index: self.system_servers.iter().map(|_| 0).collect(),
                        };
                        // 然后他会向其他的服务器发送心跳消息来建立自己的权威并且阻止新的领导人的产生。
                    }
                }
                // 同期内有其他服务器成为 Leader
                Event::AppendEntriesReq { .. } => {
                    self.role = Role::Follower {
                        voted_for: None,
                        timestamp: time::Instant::now(),
                    };
                    self.react(input).await
                }
                // 在同一 term，不存在从 Leader 到 Candidate 的转变，不应曾发起过 AppendEntries 请求
                Event::AppendEntriesResp { .. } => unreachable!(),
                // 在同一 term，不存在从 Follower 到 Candidate 的转变，不应有 ElectionTimeout 事件
                Event::ElectionTimeout { .. } => unreachable!(),
                // 超时后通过增加当前任期号来开始一轮新的选举
                Event::VoteTimeout { .. } => {
                    self.persistent_state.term += 1;
                    *votes_cnt = 1;
                }
            },

            (
                Ordering::Equal,
                Role::Follower {
                    timestamp,
                    voted_for,
                },
            ) => match input.event {
                Event::RequestVoteReq { candidate } => {
                    if voted_for.is_none() {
                        *voted_for = Some(candidate);
                    }
                    *timestamp = time::Instant::now();
                }
                // 在同一 term，可能先处于 Candidate 后处于 Follower，忽略已过期的回复
                Event::RequestVoteResp { .. } => {}
                Event::AppendEntriesReq { .. } => {
                    *timestamp = time::Instant::now();
                }
                // 在同一 term，可能先处于 Leader 后处于 Follower，忽略已过期的回复
                Event::AppendEntriesResp { .. } => {}
                // 超时后通过增加当前任期号来开始一轮新的选举。
                Event::ElectionTimeout { timestamp } => {
                    // 计时器未失效即自从上次计时后没有新的 RPC 请求
                    if timestamp == timestamp {
                        self.persistent_state.term += 1;
                        self.role = Role::Candidate { votes_cnt: 1 };
                    }
                }
                // 在同一 term，可能先处于 Candidate 状态后处于 Follower 状态，忽略已过期的计时器
                Event::VoteTimeout { .. } => {}
            },
        }
    }
}

#[derive(Debug)]
struct Service(usize);

#[tarpc::server]
impl Raft for Server {
    async fn request_vote(
        mut self,
        ctx: context::Context,
        args: RequestVoteArgs,
    ) -> RequestVoteReply {
        let progress = {
            let log_entries = &self.persistent_state.log_entries;
            let entry = log_entries.last();
            entry.map(|(term, _)| (*term, log_entries.len() - 1))
        };

        match &mut self.role {
            Role::Follower {
                ref mut voted_for, ..
            } if voted_for.is_none() && args.prev_entry_identifier >= progress => {
                *voted_for = Some(args.candidate_id);
                RequestVoteReply {
                    responser_term: self.persistent_state.term,
                    vote_granted: true,
                }
            }
            _ => RequestVoteReply {
                responser_term: self.persistent_state.term,
                vote_granted: false,
            },
        }
    }

    async fn append_entries(
        mut self,
        _: context::Context,
        args: AppendEntriesArgs,
    ) -> AppendEntriesReply {
        if let Some((prev_index, prev_term)) = args.prev_entry_identifier {
            if self.persistent_state.log_entries[prev_index].0 != prev_term {
                return AppendEntriesReply {
                    success: false,
                    responser_term: self.persistent_state.term,
                };
            }
        }

        let curr_index = args.prev_entry_identifier.map(|(i, _)| i + 1).unwrap_or(0);
        self.persistent_state.log_entries.truncate(curr_index);
        for entry in args.entries {
            self.persistent_state.log_entries.push(entry);
        }
        if args.leader_commit > self.commit_index {
            self.commit_index = cmp::min(
                self.persistent_state.log_entries.len().checked_sub(1),
                args.leader_commit,
            );
        }

        AppendEntriesReply {
            success: true,
            responser_term: self.persistent_state.term,
        }
    }
}
