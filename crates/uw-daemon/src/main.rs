//! uw-daemon: long-running polling/scheduling/delivery process.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    uw_daemon::run_daemon_loop().await
}
