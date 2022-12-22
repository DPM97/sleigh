use std::{sync::Arc, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};
use sleigh::{
    peer::{PeerState, Runtime},
    rpc::{AppendEntriesPayload, Rpc},
    utils::time_since_epoch,
};
use tarpc::context;
use tokio::{spawn, sync::Mutex, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let last_heartbeat_received = Arc::new(Mutex::new(0));
    let state = Arc::new(Mutex::new(PeerState::new()));

    Rpc::serve(8080, state.clone(), last_heartbeat_received.clone()).await?;
    let client = Rpc::create_client().await?;

    let _todo = client
        .append_entries(
            context::current(),
            AppendEntriesPayload {
                term: 0,
                leader_id: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        )
        .await?;

    let mut rt = Runtime::new(5000, last_heartbeat_received.clone());

    spawn(async move {
        loop {
            let mut rng: StdRng = SeedableRng::from_entropy();
            let d = rng.gen_range(4000..8000);
            sleep(Duration::from_millis(d)).await;
            *last_heartbeat_received.lock().await = time_since_epoch();
            println!("slept {d}ms");
        }
    });

    rt.beat(state).await;

    Ok(())
}
