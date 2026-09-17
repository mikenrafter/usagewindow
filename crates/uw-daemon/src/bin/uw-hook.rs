use uw_daemon::handle_hook;

#[tokio::main]
async fn main() {
    let response = handle_hook(tokio::io::stdin(), |payload| async move {
        tracing::debug!(payload = %payload, "hook payload received");
        Ok(serde_json::json!({}))
    })
    .await;
    println!("{}", response);
}
