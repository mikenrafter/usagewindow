use uw_daemon::handle_hook;

#[tokio::main]
async fn main() {
    let endpoint = uw_client::Discovery::from_environment("UW_DAEMON_URL").resolve();
    let provider = std::env::var("UW_HOOK_PROVIDER").unwrap_or_else(|_| "generic".into());
    let client = match endpoint.async_client(std::time::Duration::from_millis(200)) {
        Ok(client) => client,
        Err(_) => { println!("{{}}"); return; }
    };
    let daemon_url = endpoint.base_url().to_owned();
    let response = handle_hook(tokio::io::stdin(), |payload| async move {
        let response = client
            .post(format!("{daemon_url}/api/hooks"))
            .query(&[("provider", provider)])
            .json(&payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    })
    .await;
    println!("{}", response);
}
