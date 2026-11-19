use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// why: the published web listener must require login even though local tools need an unauthenticated API.
// proves: CLAIM(local-api-boundary)
#[test]
fn socket_requires_no_token_and_tcp_including_internal_aliases_requires_login() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = dir.path().join("api.sock");
    let endpoint = dir.path().join("endpoint");
    let hash = dir.path().join("password-hash");
    std::fs::write(&hash, "$argon2id$v=19$m=19456,t=2,p=1$MDEyMzQ1Njc4OWFiY2RlZg$PKEqtV6u9NNwpXPbppTEj0jTgckkrrWnVj5oRk0V1a4").unwrap();
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_uw-daemon"))
            .env_clear()
            .env("HOME", dir.path())
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("UW_DB_PATH", dir.path().join("usagewindow.db"))
            .env("UW_SOCKET_PATH", &socket)
            .env("UW_ENDPOINT_FILE", &endpoint)
            .env("UW_LISTEN_ADDR", "127.0.0.1:0")
            .env("UW_WEB_PASSWORD_HASH_FILE", hash)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !endpoint.exists() && Instant::now() < deadline {
        assert!(
            daemon.0.try_wait().unwrap().is_none(),
            "daemon exited before publishing discovery"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let published: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&endpoint).expect("daemon publishes endpoint"))
            .unwrap();
    assert_eq!(published["endpoint"], format!("unix:{}", socket.display()));
    assert_ne!(published["web_url"], "http://127.0.0.1:0");
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let local = reqwest::blocking::Client::builder()
        .unix_socket(socket)
        .build()
        .unwrap();
    assert_eq!(
        local
            .get("http://localhost/api/status")
            .send()
            .unwrap()
            .status(),
        200
    );
    let tcp = reqwest::blocking::Client::new();
    let web = published["web_url"].as_str().unwrap();
    for path in [
        "/api/status",
        "/api/internal/status",
        "/api/internal/sessions/missing",
    ] {
        assert_eq!(
            tcp.get(format!("{web}{path}")).send().unwrap().status(),
            401,
            "{path}"
        );
    }
    for path in [
        "/api/internal/sessions/missing/compact/ask",
        "/api/internal/sessions/missing/resume",
        "/api/hooks",
    ] {
        assert_eq!(
            tcp.post(format!("{web}{path}"))
                .json(&serde_json::json!({}))
                .send()
                .unwrap()
                .status(),
            401,
            "{path}"
        );
    }
}
