use std::sync::Arc;

use futures::{
    future::{self},
    stream::FuturesUnordered,
    StreamExt,
};
use serde::{Deserialize, Serialize};
use tarpc::context;
use tokio::sync::{Mutex, MutexGuard};

use crate::{
    rpc::{RequestVotePayload, Rpc},
    utils::Timer,
};

#[derive(Debug, Clone)]
pub enum PeerType {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    // string ==> log
    pub logs: Vec<Log>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Get,
    Set,
    Remove,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Log {
    pub command: Command,
    pub term_recieved_by_leader: u64,
}

#[derive(Debug, Clone)]
pub struct VolatilePeerState {
    pub commit_index: usize,
    pub last_applied: u64,
}

#[derive(Debug, Clone)]
pub struct VolatileLeaderState {
    pub next_index: Vec<usize>,
    pub match_index: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct VolatileState {
    pub peer: VolatilePeerState,
    pub leader: Option<VolatileLeaderState>,
}

#[derive(Debug, Clone)]
pub struct PeerState {
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub peer_type: PeerType,
    // pub timer: Timer,
}

impl PeerState {
    pub fn new() -> Self {
        Self {
            persistent: PersistentState {
                current_term: 0,
                voted_for: None,
                logs: vec![],
            },
            volatile: VolatileState {
                peer: VolatilePeerState {
                    commit_index: 0,
                    last_applied: 0,
                },
                leader: None,
            },
            peer_type: PeerType::Follower,
        }
    }
}

pub struct Runtime {
    peer_id: String,
    timer: Timer,
    peers: Vec<String>,
    state: Arc<Mutex<PeerState>>,
}

impl Runtime {
    pub async fn new(
        timer: Timer,
        peer_id: String,
        peers: Vec<String>,
        port: u16,
        last_heartbeat_received: Arc<Mutex<u128>>,
        state: Arc<Mutex<PeerState>>,
    ) -> anyhow::Result<Self> {
        Rpc::serve(port, state.clone(), last_heartbeat_received.clone()).await?;
        Ok(Self {
            timer,
            peer_id,
            peers,
            state,
        })
    }

    async fn request_votes(&self, lock: MutexGuard<'_, PeerState>) {
        let payload = RequestVotePayload {
            term: lock.persistent.current_term,
            candidate_id: self.peer_id.clone(),
            last_log_index: lock.persistent.logs.len(),
            last_log_term: match lock.persistent.logs.last() {
                Some(l) => l.term_recieved_by_leader,
                None => 0,
            },
        };

        // probably want to throw this on a thread so it doesn't block the runtime loop
        let needed_votes = (self.peers.len() / 2) + 1;
        let mut current_votes = 0;

        self.peers
            .iter()
            .map(|peer| {
                let peer = peer.clone();
                let payload = payload.clone();
                async move {
                    // TODO: log errors :)
                    let r = Rpc::create_client(&peer).await;
                    if r.is_err() {
                        return (peer, None);
                    }
                    let r = r.unwrap().request_vote(context::current(), payload).await;
                    if r.is_err() {
                        return (peer, None);
                    }
                    (peer, Some(r.unwrap()))
                }
            })
            .collect::<FuturesUnordered<_>>()
            .take_while(|x| {
                future::ready(match x {
                    (peer, Some(vote)) => {
                        if vote.term == lock.persistent.current_term {
                            if vote.vote_granted {
                                current_votes += 1;
                                println!("{peer} granted vote.");
                                if current_votes >= needed_votes {
                                    false
                                } else {
                                    true
                                }
                            } else {
                                println!("{peer} did not grant vote.");
                                true
                            }
                        } else {
                            if vote.vote_granted {
                                println!("{peer} granted vote for another term.");
                                true
                            } else {
                                println!("{peer} did not grant vote for another term.");
                                true
                            }
                        }
                    }
                    (peer, None) => {
                        println!("Didn't recieve vote from {peer}.");
                        true
                    }
                })
            })
            .collect::<Vec<_>>()
            .await;
    }

    pub async fn beat(&mut self) -> ! {
        loop {
            let peer_type = self.state.lock().await.peer_type.clone();
            match peer_type {
                PeerType::Follower => {
                    self.timer.defer().await;
                    println!("Follower has failed to recieve a heartbeat from the leader. Converting to candidate.");
                    let mut lock = self.state.lock().await;
                    lock.peer_type = PeerType::Candidate;
                    // begin election
                    lock.persistent.current_term += 1;
                    lock.persistent.voted_for = Some(self.peer_id.clone());
                    self.request_votes(lock).await;
                }
                PeerType::Candidate => {
                    println!("hi");
                }
                PeerType::Leader => todo!(),
            }
        }
    }
}
