use clap::Parser;
use uw_cli::{Cli, HttpApiClient, ReadFallback, StoreReader, execute};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let path = std::env::var("UW_DB_PATH").unwrap_or_else(|_| "usagewindow.db".into());
    let direct = StoreReader::open(&path)?;
    println!(
        "{}",
        execute(
            cli,
            ReadFallback {
                api: HttpApiClient,
                direct
            }
        )?
    );
    Ok(())
}
