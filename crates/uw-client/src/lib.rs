//! Endpoint discovery shared by the CLI, MCP bridge, and best-effort hooks.
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub struct Endpoint(pub String);

impl Endpoint {
    pub fn socket_path(&self) -> Option<&Path> {
        self.0.strip_prefix("unix:").map(Path::new)
    }

    pub fn base_url(&self) -> &str {
        if self.socket_path().is_some() { "http://localhost" } else { self.0.trim_end_matches('/') }
    }

    pub fn blocking_client(&self) -> anyhow::Result<reqwest::blocking::Client> {
        let mut builder = reqwest::blocking::Client::builder().timeout(Duration::from_secs(10));
        if let Some(path) = self.socket_path() {
            anyhow::ensure!(path.is_absolute(), "Unix socket path must be absolute");
            builder = builder.unix_socket(path).no_proxy();
        }
        Ok(builder.build()?)
    }

    pub fn async_client(&self, timeout: Duration) -> anyhow::Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().timeout(timeout);
        if let Some(path) = self.socket_path() {
            anyhow::ensure!(path.is_absolute(), "Unix socket path must be absolute");
            builder = builder.unix_socket(path).no_proxy();
        }
        Ok(builder.build()?)
    }
}

pub struct Discovery {
    pub explicit: Option<String>,
    pub socket_paths: Vec<PathBuf>,
    pub endpoint_files: Vec<PathBuf>,
}

pub fn data_dir() -> PathBuf {
    std::env::var_os("UW_DB_PATH").and_then(|p| PathBuf::from(p).parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".usagewindow"))
}

pub fn default_directories() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        paths.push(PathBuf::from(runtime).join("usagewindow"));
    }
    paths.push(data_dir());
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".usagewindow");
    if !paths.contains(&home) { paths.push(home); }
    paths
}

pub fn default_socket_path() -> PathBuf {
    std::env::var_os("UW_SOCKET_PATH").map(PathBuf::from)
        .unwrap_or_else(|| default_directories()[0].join("api.sock"))
}

impl Discovery {
    /// URL overrides are authoritative. Discovery ignores stale sockets and malformed files.
    pub fn from_environment(primary: &str) -> Self {
        let secondary = if primary == "UW_API_URL" { "UW_DAEMON_URL" } else { "UW_API_URL" };
        let explicit = [primary, secondary].into_iter().find_map(|key| std::env::var(key).ok().filter(|v| !v.is_empty()));
        let directories = default_directories();
        let mut socket_paths = Vec::new();
        if let Some(path) = std::env::var_os("UW_SOCKET_PATH") { socket_paths.push(path.into()); }
        socket_paths.extend(directories.iter().map(|dir| dir.join("api.sock")));
        let mut endpoint_files = Vec::new();
        if let Some(path) = std::env::var_os("UW_ENDPOINT_FILE") { endpoint_files.push(path.into()); }
        endpoint_files.extend(directories.iter().map(|dir| dir.join("endpoint")));
        Self { explicit, socket_paths, endpoint_files }
    }

    pub fn resolve(&self) -> Endpoint {
        if let Some(endpoint) = &self.explicit { return Endpoint(endpoint.clone()); }
        for path in &self.socket_paths {
            if socket_reachable(path) { return Endpoint(format!("unix:{}", path.display())); }
        }
        for path in &self.endpoint_files {
            let Some(endpoint) = std::fs::read(path).ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|value| value.get("endpoint")?.as_str().map(str::to_owned)) else { continue; };
            let endpoint = Endpoint(endpoint);
            if endpoint.socket_path().is_some_and(socket_reachable)
                || endpoint.0.starts_with("http://") || endpoint.0.starts_with("https://") {
                return endpoint;
            }
        }
        Endpoint("http://127.0.0.1:7878".into())
    }
}

/// A nonblocking probe keeps discovery bounded even when a daemon's accept queue is full.
pub fn socket_reachable(path: &Path) -> bool {
    let Ok(socket) = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None) else { return false; };
    if socket.set_nonblocking(true).is_err() { return false; }
    let Ok(address) = socket2::SockAddr::unix(path) else { return false; };
    match socket.connect(&address) {
        Ok(()) => true,
        Err(error) => matches!(error.kind(), std::io::ErrorKind::WouldBlock),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    // why: explicit URLs must win over an unrelated local daemon, including unix: overrides.
    #[test]
    fn explicit_endpoint_wins() {
        for endpoint in ["http://127.0.0.1:9988", "unix:/custom/api.sock"] {
            let discovery = Discovery { explicit: Some(endpoint.into()), socket_paths: vec![], endpoint_files: vec![] };
            assert_eq!(discovery.resolve(), Endpoint(endpoint.into()));
        }
    }

    // why: agents and system services can have different runtime directories; stale sockets must not hide a live daemon.
    #[test]
    fn live_default_socket_precedes_discovery_and_stale_sockets_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let stale = dir.path().join("stale.sock");
        drop(UnixListener::bind(&stale).unwrap());
        let live = dir.path().join("live.sock");
        let _listener = UnixListener::bind(&live).unwrap();
        let file = dir.path().join("endpoint");
        std::fs::write(&file, r#"{"endpoint":"http://127.0.0.1:9999","web_url":"http://127.0.0.1:9999"}"#).unwrap();
        let discovery = Discovery { explicit: None, socket_paths: vec![stale, live.clone()], endpoint_files: vec![file] };
        assert_eq!(discovery.resolve(), Endpoint(format!("unix:{}", live.display())));
    }

    // why: a daemon on a custom socket or ephemeral TCP port must be discoverable without client URL configuration.
    #[test]
    fn discovery_file_precedes_legacy_http_default() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("custom.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        let file = dir.path().join("endpoint");
        std::fs::write(&file, serde_json::json!({"endpoint":format!("unix:{}", socket.display()),"web_url":"http://127.0.0.1:48123"}).to_string()).unwrap();
        let mut discovery = Discovery { explicit: None, socket_paths: vec![], endpoint_files: vec![file] };
        assert_eq!(discovery.resolve(), Endpoint(format!("unix:{}", socket.display())));
        discovery.endpoint_files.clear();
        assert_eq!(discovery.resolve(), Endpoint("http://127.0.0.1:7878".into()));
    }
}
