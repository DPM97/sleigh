use std::sync::Arc;

use sleigh::{
    peer::PeerState,
    rpc::{AppendEntriesPayload, Rpc},
};
use tarpc::context;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = Arc::new(Mutex::new(PeerState::default()));

    Rpc::serve(8080, state).await?;
    let client = Rpc::create_client().await?;

    println!(
        "{:#?}",
        client
            .append_entries(
                context::current(),
                AppendEntriesPayload {
                    term: 0,
                    leader_id: 0,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0
                }
            )
            .await?
    );
    Ok(())
}
