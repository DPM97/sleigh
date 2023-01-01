use std::{ops::Range, sync::Arc, time::Duration};

use futures::{
    future::{self},
    stream::FuturesUnordered,
    StreamExt,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tarpc::context;
use tokio::{select, sync::Mutex, time::sleep};

use crate::{
    rpc::{AppendEntriesPayload, RequestVotePayload, Rpc},
    utils::{Persist, Timer},
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

impl Default for PeerState {
    fn default() -> Self {
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
    peers: Vec<String>,
    state: Arc<Mutex<PeerState>>,
    last_heartbeat_received: Arc<Mutex<u128>>,
}

impl Runtime {
    pub async fn new(
        peer_id: String,
        peers: Vec<String>,
        port: u16,
        last_heartbeat_received: Arc<Mutex<u128>>,
        state: Arc<Mutex<PeerState>>,
    ) -> anyhow::Result<Self> {
        Rpc::serve(port, state.clone(), last_heartbeat_received.clone()).await?;
        Ok(Self {
            peer_id,
            peers,
            state,
            last_heartbeat_received,
        })
    }

    async fn send_heartbeats(&mut self) {
        println!("Sending heartbeats to peers!");

        let payload = {
            let lock = self.state.lock().await;
            AppendEntriesPayload {
                term: lock.persistent.current_term,
                leader_id: Some(self.peer_id.clone()),
                prev_log_index: lock.persistent.logs.len(),
                prev_log_term: match lock.persistent.logs.last() {
                    None => 0,
                    Some(l) => l.term_recieved_by_leader,
                },
                entries: vec![],
                leader_commit: lock.volatile.peer.commit_index,
            }
        };

        self.peers
            .iter()
            .map(|peer| {
                let peer = peer.clone();
                let payload = payload.clone();
                async move {
                    // TODO: log errors :)
                    loop {
                        let r = Rpc::create_client(&peer).await;
                        if r.is_err() {
                            continue;
                        }
                        if r.unwrap()
                            .append_entries(context::current(), payload.clone())
                            .await
                            .is_err()
                        {
                            continue;
                        }
                        break;
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .take((self.peers.len() - 1) / 2)
            .collect::<Vec<_>>()
            .await;

        println!("Adequate amount of hearbeat responses recieved!");
    }

    async fn self_elect(&mut self) -> bool {
        let payload = {
            let lock = self.state.lock().await;
            RequestVotePayload {
                term: lock.persistent.current_term,
                candidate_id: self.peer_id.clone(),
                last_log_index: lock.persistent.logs.len(),
                last_log_term: match lock.persistent.logs.last() {
                    Some(l) => l.term_recieved_by_leader,
                    None => 0,
                },
            }
        };

        let mut futures_resolved = 0;
        let peer_ct = self.peers.len() - 1;

        let votes_necessary = (peer_ct / 2) + 1;
        let mut votes_granted = 0;

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
                future::ready({
                    futures_resolved += 1;
                    !(match x {
                        (peer, Some(vote)) => {
                            // TODO: short circuit if changed from candiate --> follower?
                            // (term_equality, vote_granted)
                            match (vote.term == payload.term, vote.vote_granted) {
                                (true, true) => {
                                    println!("{peer} granted vote.");
                                    votes_granted += 1;
                                    votes_granted < votes_necessary
                                }
                                (true, false) => {
                                    println!("{peer} did not grant vote.");
                                    true
                                }
                                (false, true) => {
                                    println!("{peer} granted vote for another term.");
                                    true
                                }
                                (false, false) => {
                                    println!("{peer} did not grant vote for another term.");
                                    true
                                }
                            }
                        }
                        (peer, None) => {
                            println!("Didn't recieve vote from {peer}.");
                            true
                        }
                    }) || futures_resolved == peer_ct
                })
            })
            .collect::<Vec<_>>()
            .await;

        votes_granted >= votes_necessary
    }

    fn create_timer(&self, range: Range<u128>) -> Timer {
        Timer::new(range, self.last_heartbeat_received.clone())
    }

    pub async fn beat(&mut self) -> ! {
        loop {
            let peer_type = self.state.lock().await.peer_type.clone();
            match peer_type {
                PeerType::Follower => {
                    // begin election timer (i.e., if this resolves, we should
                    // convert to candidate as there hasn't been leader
                    // communication in a while)
                    self.create_timer(150..300).defer().await;
                    println!("Follower has failed to recieve a heartbeat from the leader. Converting to candidate.");
                    self.state.lock().await.peer_type = PeerType::Candidate;
                }
                PeerType::Candidate => {
                    println!("Transitioned to candidate. Attempting to elect self.");
                    {
                        let mut lock = self.state.lock().await;
                        lock.persistent.current_term += 1;
                        lock.persistent.voted_for = Some(self.peer_id.clone());
                        Persist::write(&lock.persistent)
                            .await
                            .expect("failed to write persistent state");
                    }

                    let mut election_timer = self.create_timer(150..300);
                    let elected = select! {
                        _ = election_timer.defer() => {
                            println!("Election timed out.");
                            false
                        }
                        elected = self.self_elect() => {
                            println!("Election ended. Elected: {elected}");
                            elected
                        }
                    };

                    let mut lock = self.state.lock().await;
                    if elected && matches!(lock.peer_type, PeerType::Candidate) {
                        lock.peer_type = PeerType::Leader;
                    }
                }
                // leaders service peer requests through the rpc server
                // and will send heartbeats here
                PeerType::Leader => {
                    sleep(Duration::from_millis(
                        rand::thread_rng().gen_range(150..300),
                    ))
                    .await;
                    self.send_heartbeats().await;
                }
            }
        }
    }
}
