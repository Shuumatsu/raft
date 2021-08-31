use std::cmp::Ordering;
use std::sync::mpsc;
use std::time;

#[derive(Debug)]
pub enum Event {
    ElectionTimeout { timestamp: time::Instant },
    // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
    // 候选人发起投票后，如果在一定时间内没有获得半数以上的投票的话，则视为失败
    VoteTimeout, // VoteFailure
    RequestVoteReq { candidate: usize },
    RequestVoteResp { ok: bool },
    // AppendEntries RPC 由领导者调用，用于日志条目的复制，同时也被当做心跳使用
    AppendEntriesReq { leader: usize },
    AppendEntriesResp { ok: bool },
}

#[derive(Debug)]
pub struct Input {
    term: usize,
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
        voted_for: Option<usize>,
        timestamp: time::Instant,
    },
}

#[derive(Debug)]
pub struct PersistentState {
    term: usize,
    id: usize,
    log: Vec<usize>,
}

impl PersistentState {
    pub fn new(id: usize) -> Self {
        PersistentState {
            term: 0,
            id,
            log: vec![],
        }
    }
}

#[derive(Debug)]
pub struct Server {
    persistent_state: PersistentState,
    commit_index: usize,
    last_applied: usize,
    system_servers: Vec<usize>,
    role: Role,
}

impl Server {
    pub fn new(id: usize, system_servers: Vec<usize>) -> Self {
        Server {
            persistent_state: PersistentState::new(id),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower {
                voted_for: None,
                timestamp: time::Instant::now(),
            },
            system_servers,
        }
    }

    pub fn run(&mut self) {
        unimplemented!()
    }

    pub fn react(&mut self, input: Input) {
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
                    self.react(input)
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
