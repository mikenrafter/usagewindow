use anyhow::Context;
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub(crate) struct LocalApi {
    pub listener: tokio::net::UnixListener,
    pub socket_path: PathBuf,
    endpoint_file: PathBuf,
    _lock: File,
}

fn private_directory(path: &Path) -> anyhow::Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0,
        "local API directory {} must be a private directory (mode 0700)", path.display());
    Ok(())
}

impl LocalApi {
    pub fn bind() -> anyhow::Result<Self> {
        Self::bind_paths(uw_client::default_socket_path(),
            std::env::var_os("UW_ENDPOINT_FILE").map(PathBuf::from)
                .unwrap_or_else(|| uw_client::data_dir().join("endpoint")))
    }

    fn bind_paths(socket_path: PathBuf, endpoint_file: PathBuf) -> anyhow::Result<Self> {
        anyhow::ensure!(socket_path.is_absolute() && endpoint_file.is_absolute(), "local API paths must be absolute");
        private_directory(socket_path.parent().context("socket has no parent")?)?;
        private_directory(endpoint_file.parent().context("endpoint file has no parent")?)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .mode(0o600).open(socket_path.with_extension("lock"))?;
        lock.try_lock().context("another daemon owns the local API socket")?;
        if let Ok(metadata) = std::fs::symlink_metadata(&socket_path) {
            anyhow::ensure!(metadata.file_type().is_socket(), "refusing to replace a non-socket at {}", socket_path.display());
            anyhow::ensure!(!uw_client::socket_reachable(&socket_path), "another daemon is listening on {}", socket_path.display());
            std::fs::remove_file(&socket_path)?;
        }
        let listener = tokio::net::UnixListener::bind(&socket_path)?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
        Ok(Self { listener, socket_path, endpoint_file, _lock: lock })
    }

    pub fn publish(&self, web_address: SocketAddr) -> anyhow::Result<()> {
        use std::io::Write;
        let temporary = self.endpoint_file.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        let result = (|| {
            let bytes = serde_json::to_vec(&serde_json::json!({
                "endpoint": format!("unix:{}", self.socket_path.display()),
                "web_url": format!("http://{web_address}")
            }))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &self.endpoint_file)?;
            Ok(())
        })();
        if result.is_err() { let _ = std::fs::remove_file(temporary); }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // why: permissions on a newly bound socket must never expose a pre-chmod access window.
    #[tokio::test]
    async fn refuses_public_socket_parent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(LocalApi::bind_paths(dir.path().join("api.sock"), dir.path().join("endpoint")).is_err());
        assert!(!dir.path().join("api.sock").exists());
    }

    // why: restarting after a crash must work, but a second scheduler must not steal a live daemon's socket.
    #[tokio::test]
    async fn replaces_stale_socket_and_refuses_live_daemon() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("api.sock");
        let endpoint = dir.path().join("endpoint");
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        let first = LocalApi::bind_paths(socket.clone(), endpoint.clone()).unwrap();
        assert!(LocalApi::bind_paths(socket.clone(), endpoint.clone()).is_err());
        drop(first);
        let _restarted = LocalApi::bind_paths(socket, endpoint).unwrap();
    }

    // why: an incorrect socket setting must not destroy a user's file or follow a symlink.
    #[tokio::test]
    async fn refuses_to_replace_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = dir.path().join("api.sock");
        std::fs::write(&socket, "keep me").unwrap();
        assert!(LocalApi::bind_paths(socket.clone(), dir.path().join("endpoint")).is_err());
        assert_eq!(std::fs::read_to_string(socket).unwrap(), "keep me");
    }
}
