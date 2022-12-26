use std::sync::Arc;

use sleigh::peer::{PeerState, Runtime};
use tokio::sync::Mutex;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // create peer state
    let last_heartbeat_received = Arc::new(Mutex::new(0));
    let state = Arc::new(Mutex::new(PeerState::default()));

    let cur_host = "127.0.0.1:8080".to_string();
    let hosts = vec![
        cur_host.clone(),
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];

    // create runtime loop
    let mut rt = Runtime::new(cur_host, hosts, args.port, last_heartbeat_received, state).await?;

    /*
    spawn(async move {
        loop {
            let mut rng: StdRng = SeedableRng::from_entropy();
            let d = rng.gen_range(4000..8000);
            sleep(Duration::from_millis(d)).await;
            *last_heartbeat_received.lock().await = time_since_epoch();
            println!("slept {d}ms");
        }
    });
    */

    rt.beat().await;

    Ok(())
}
