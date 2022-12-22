use std::{sync::Arc, time::SystemTime};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::utils::Timer;

#[derive(Debug, Clone)]
pub enum PeerType {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub term: u64,
    // u32 ==> peer ID
    pub voted_for: Option<u32>,
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
    pub fn new(/*timer_interval: u128, last_heartbeat_recieved: Arc<Mutex<u128>>*/) -> Self {
        Self {
            persistent: PersistentState {
                term: 0,
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
            // timer: Timer::new(timer_interval, last_heartbeat_recieved),
        }
    }
}

pub struct Runtime {
    timer: Timer,
}

impl Runtime {
    pub fn new(timer_interval: u128, last_heartbeat_recieved: Arc<Mutex<u128>>) -> Self {
        Self {
            timer: Timer::new(timer_interval, last_heartbeat_recieved),
        }
    }

    pub async fn beat(&mut self, _state: Arc<Mutex<PeerState>>) -> ! {
        let mut t = SystemTime::now();

        while self.timer.defer().await {
            println!("deffered {:?}", t.elapsed());
            t = SystemTime::now();
        }

        panic!("timer should not have timed out like this :O");
    }
}
