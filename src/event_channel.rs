use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration, Instant};

use crate::protocol::{
    AppendEntriesReq, AppendEntriesResp, RequestVoteReq, RequestVoteResp, ServerId, Term,
};

#[derive(Debug)]
pub enum Event {
    Timer(TimerEvent),
    RPC(RPCEvent),
}

#[derive(Debug)]
pub enum TimerEvent {
    ElectionTimeout { term: Term, timestamp: Instant },
    // 当一个候选人从整个集群的大多数服务器节点获得了针对同一个任期号的选票，那么他就赢得了这次选举并成为领导人。
    // 候选人发起投票后，如果在一定时间内没有获得半数以上的投票的话，则视为失败
    VoteTimeout { term: Term },
    HeartBeatTimeout { term: Term },
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
pub enum RPCEvent {
    RequestVote(RequestVoteEvent),
    AppendEntries(AppendEntriesEvent),
}

#[derive(Debug, Clone)]
pub struct Sender {
    inner: mpsc::UnboundedSender<Event>,
}

#[derive(Debug)]
pub struct Receiver {
    inner: mpsc::UnboundedReceiver<Event>,
}

pub fn new() -> (Sender, Receiver) {
    let (events_sender, events_receiver) = mpsc::unbounded_channel();
    (
        Sender {
            inner: events_sender,
        },
        Receiver {
            inner: events_receiver,
        },
    )
}

impl Sender {
    pub fn send(&self, event: Event) -> Result<(), SendError<Event>> {
        self.inner.send(event)
    }

    pub fn send_event(&self, event: Event) -> Result<(), SendError<Event>> {
        self.send(event)
    }

    pub fn send_timer_event(&self, event: TimerEvent) -> Result<(), SendError<Event>> {
        self.send_event(Event::Timer(event))
    }

    pub fn send_rpc_event(&self, event: RPCEvent) -> Result<(), SendError<Event>> {
        self.inner.send(Event::RPC(event))
    }

    pub fn send_append_entries_event(
        &self,
        event: AppendEntriesEvent,
    ) -> Result<(), SendError<Event>> {
        self.send_rpc_event(RPCEvent::AppendEntries(event))
    }

    pub fn send_request_vote_event(&self, event: RequestVoteEvent) -> Result<(), SendError<Event>> {
        self.send_rpc_event(RPCEvent::RequestVote(event))
    }
}

impl Receiver {
    pub async fn recv(&mut self) -> Option<Event> {
        self.inner.recv().await
    }
}
