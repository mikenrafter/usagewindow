use std::env;
use std::net::SocketAddr;

use uw_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = env::var("UW_DB_PATH").unwrap_or_else(|_| {
        let home = env::var("HOME").unwrap_or_default();
        format!("{home}/.usagewindow/usagewindow.db")
    });
    let address: SocketAddr = env::var("UW_DEV_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7879".into())
        .parse()?;
    let app = uw_web::app(Store::open(&db_path)?);
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("uw-web-dev serving on http://{address} using {db_path}");
    axum::serve(listener, app).await?;
    Ok(())
}
