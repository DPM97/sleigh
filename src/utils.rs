use std::{
    ops::Range,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::{rngs::ThreadRng, Rng};
use tokio::{sync::Mutex, time::sleep};

use crate::peer::PersistentState;

pub struct Persist;

impl Persist {
    pub async fn write(data: &PersistentState) -> anyhow::Result<()> {
        tokio::fs::write("./db/persistent", bincode::serialize(data)?).await?;
        Ok(())
    }

    pub async fn read() -> anyhow::Result<PersistentState> {
        Ok(bincode::deserialize(
            &tokio::fs::read("./db/persistent").await?,
        )?)
    }
}

#[derive(Debug, Clone)]
pub struct Timer {
    interval_range: Range<u128>,
    last_heartbeat_received: Arc<Mutex<u128>>,
    interval_end: u128,
    _rng: ThreadRng,
}

pub fn time_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

impl Timer {
    pub fn new(interval_range: Range<u128>, last_heartbeat_received: Arc<Mutex<u128>>) -> Self {
        Self {
            last_heartbeat_received,
            interval_range,
            interval_end: Default::default(),
            _rng: ThreadRng::default(),
        }
    }

    pub async fn defer(&mut self) -> bool {
        self.interval_end = time_since_epoch() + self._rng.gen_range(self.interval_range.clone());

        loop {
            let lhr = *self.last_heartbeat_received.lock().await;
            sleep(Duration::from_millis(
                (self.interval_end - time_since_epoch()) as u64,
            ))
            .await;
            let new = *self.last_heartbeat_received.lock().await;
            if new == lhr {
                break;
            }
            self.interval_end = new + self._rng.gen_range(self.interval_range.clone());
        }

        true
    }
}
