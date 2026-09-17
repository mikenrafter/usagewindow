//! uw-daemon: long-running polling/scheduling/delivery process.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        tick.tick().await;
        // Wiring a configured SQLite store and adapter registry belongs to the
        // daemon service configuration phase. The pure tick runners are exposed
        // by the library and are exercised with real/fake implementations there.
        tracing::debug!("uw-daemon poll tick");
    }
}
