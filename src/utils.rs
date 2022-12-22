use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
    interval: u128,
    last_heartbeat_received: Arc<Mutex<u128>>,
    interval_end: u128,
}

pub fn time_since_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

impl Timer {
    pub fn new(interval: u128, last_heartbeat_received: Arc<Mutex<u128>>) -> Self {
        Self {
            last_heartbeat_received,
            interval,
            interval_end: Default::default(),
        }
    }

    pub async fn defer(&mut self) -> bool {
        self.interval_end = time_since_epoch() + self.interval;

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
            self.interval_end = new + self.interval;
        }

        true
    }
}
