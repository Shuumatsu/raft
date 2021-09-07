use crate::protocol::{AppendEntriesReq, AppendEntriesResp, RequestVoteReq, RequestVoteResp};

#[tarpc::service]
pub trait Raft {
    async fn request_vote(request: RequestVoteReq) -> RequestVoteResp;

    async fn append_entries(request: AppendEntriesReq) -> AppendEntriesResp;
}

// #[derive(Debug)]
// struct Service(usize);

// #[tarpc::server]
// impl Raft for Server {
//     async fn request_vote(
//         mut self,
//         _: context::Context,
//         request: RequestVoteReq,
//     ) -> RequestVoteResp {
//         let (tx, rx) = oneshot::channel();
//         let event = RPCEvent::RequestVote(RequestVoteEvent::Request { request, reply: tx });

//         self.react_rpc(event);
//         rx.await.unwrap()
//     }

//     async fn append_entries(
//         mut self,
//         _: context::Context,
//         request: AppendEntriesReq,
//     ) -> AppendEntriesResp {
//         let (tx, rx) = oneshot::channel();
//         let event = RPCEvent::AppendEntries(AppendEntriesEvent::Request { request, reply: tx });

//         self.react_rpc(event);
//         rx.await.unwrap()
//     }
// }
