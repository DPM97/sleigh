use serde::{Deserialize, Serialize};

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
}

impl Default for PeerState {
    fn default() -> Self {
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
        }
    }
}
