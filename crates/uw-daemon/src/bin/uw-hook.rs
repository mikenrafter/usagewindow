use uw_daemon::handle_hook;

#[tokio::main]
async fn main() {
    let daemon_url =
        std::env::var("UW_DAEMON_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let provider = std::env::var("UW_HOOK_PROVIDER").unwrap_or_else(|_| "generic".into());
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(200))
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            println!("{{}}");
            return;
        }
    };
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
