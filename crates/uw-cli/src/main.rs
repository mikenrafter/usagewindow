use clap::Parser;
use uw_cli::{Cli, HttpApiClient, ReadFallback, StoreReader, execute};

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let path = std::env::var("UW_DB_PATH").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.usagewindow/usagewindow.db")
    });
    let direct = StoreReader::open(&path)?;
    println!(
        "{}",
        execute(
            cli,
            ReadFallback {
                api: HttpApiClient::default(),
                direct
            }
        )?
    );
    Ok(())
}
