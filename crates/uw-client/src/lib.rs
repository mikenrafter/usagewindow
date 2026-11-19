use std::path::PathBuf;

#[derive(Debug, PartialEq, Eq)]
pub struct Endpoint(pub String);

pub struct Discovery {
    pub explicit: Option<String>,
    pub socket_paths: Vec<PathBuf>,
    pub endpoint_files: Vec<PathBuf>,
}

impl Discovery {
    pub fn resolve(&self) -> Endpoint {
        Endpoint("http://127.0.0.1:7878".into())
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
