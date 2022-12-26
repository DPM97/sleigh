use std::{borrow::BorrowMut, cmp::min, sync::Arc};

use futures::{
    future::{self},
    StreamExt,
};
use tarpc::{
    client, context,
    serde::{Deserialize, Serialize},
    server::{incoming::Incoming, BaseChannel},
    tokio_serde::formats::Json,
};
use tokio::sync::Mutex;

use crate::{
    peer::{Log, PeerState},
    utils::{time_since_epoch, Persist},
};

#[derive(Deserialize, Serialize, Debug)]
pub struct AppendEntriesPayload {
    pub term: u64,
    pub leader_id: Option<String>,
    pub prev_log_index: usize,
    pub prev_log_term: u64,
    pub entries: Vec<Log>,
    pub leader_commit: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RequestVotePayload {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[tarpc::service]
pub trait Peer {
    async fn append_entries(payload: AppendEntriesPayload) -> AppendEntriesResponse;
    async fn request_vote(payload: RequestVotePayload) -> RequestVoteResponse;
}

#[derive(Clone)]
pub struct Receiver {
    state: Arc<Mutex<PeerState>>,
    timer_last_reset: Arc<Mutex<u128>>,
}

impl Receiver {
    async fn reset_timer(&self) {
        **self.timer_last_reset.lock().await.borrow_mut() = time_since_epoch();
    }
}

#[tarpc::server]
impl Peer for Receiver {
    async fn append_entries(
        self,
        _: context::Context,
        payload: AppendEntriesPayload,
    ) -> AppendEntriesResponse {
        self.reset_timer().await;
        let mut state = self.state.lock().await;

        // Leaders send periodic
        // heartbeats (AppendEntries RPCs that carry no log entries)
        if payload.entries.len() == 0 {
            self.reset_timer().await;
        }

        // Reply false if term < currentTerm (§5.1)
        if payload.term < state.persistent.current_term {
            return AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            };
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)
        match state.persistent.logs.get(payload.prev_log_index) {
            Some(l) => {
                if l.term_recieved_by_leader != payload.prev_log_term {
                    return AppendEntriesResponse {
                        term: state.persistent.current_term,
                        success: false,
                    };
                }
            }
            None => {
                return AppendEntriesResponse {
                    term: state.persistent.current_term,
                    success: false,
                }
            }
        };

        // If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)
        'entry_iter: for (i, e) in payload.entries.iter().enumerate() {
            let j = payload.prev_log_index + i;
            match state.persistent.logs.get(j) {
                None => {}
                Some(l) => {
                    if e.term_recieved_by_leader != l.term_recieved_by_leader {
                        state.persistent.logs.drain(j..);
                        break 'entry_iter;
                    }
                }
            }
        }

        // Append any new entries not already in the log
        payload
            .entries
            .into_iter()
            .for_each(|v| state.persistent.logs.push(v));

        // If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if payload.leader_commit > state.volatile.peer.commit_index {
            state.volatile.peer.commit_index =
                min(payload.leader_commit, state.persistent.logs.len());
        }

        // TODO panic if failed write?
        match Persist::write(&state.persistent).await {
            Ok(_) => AppendEntriesResponse {
                term: state.persistent.current_term,
                success: true,
            },
            Err(_) => AppendEntriesResponse {
                term: state.persistent.current_term,
                success: false,
            },
        }
    }

    async fn request_vote(
        self,
        _: context::Context,
        payload: RequestVotePayload,
    ) -> RequestVoteResponse {
        let state = self.state.lock().await;

        // Reply false if term < currentTerm (§5.1)
        if payload.term < state.persistent.current_term {
            return RequestVoteResponse {
                term: state.persistent.current_term,
                vote_granted: false,
            };
        }

        // If votedFor is null or candidateId, and candidate’s log is at
        // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        if state.persistent.voted_for.is_none()
            || state.persistent.voted_for.as_ref().unwrap() == &payload.candidate_id
        {
            // If the logs have last entries with different terms, then
            // the log with the later term is more up-to-date.
            match state.persistent.logs.last() {
                Some(l) => {
                    if l.term_recieved_by_leader < payload.last_log_term {
                        return RequestVoteResponse {
                            term: state.persistent.current_term,
                            vote_granted: true,
                        };
                    } else if l.term_recieved_by_leader > payload.last_log_term {
                        return RequestVoteResponse {
                            term: state.persistent.current_term,
                            vote_granted: false,
                        };
                    }
                }
                _ => {}
            };

            // If the logs end with the same term, then whichever log is longer is
            // more up-to-date.
            if state.persistent.logs.len() <= payload.last_log_index {
                return RequestVoteResponse {
                    term: state.persistent.current_term,
                    vote_granted: true,
                };
            }
        }

        RequestVoteResponse {
            term: state.persistent.current_term,
            vote_granted: false,
        }
    }
}

pub struct Rpc {}

impl Rpc {
    pub async fn serve(
        port: u16,
        state: Arc<Mutex<PeerState>>,
        timer_last_reset: Arc<Mutex<u128>>,
    ) -> anyhow::Result<()> {
        tokio::spawn(
            tarpc::serde_transport::tcp::listen(format!("127.0.0.1:{port}"), Json::default)
                .await?
                .filter_map(|r| future::ready(r.ok()))
                .map(BaseChannel::with_defaults)
                .execute(
                    Receiver {
                        state,
                        timer_last_reset,
                    }
                    .serve(),
                ),
        );

        Ok(())
    }

    pub async fn create_client(host_addr: &str) -> anyhow::Result<PeerClient> {
        let client = PeerClient::new(
            client::Config::default(),
            tarpc::serde_transport::tcp::connect(host_addr, Json::default).await?,
        )
        .spawn();

        Ok(client)
    }
}
