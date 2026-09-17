use async_trait::async_trait;
use std::path::Path;
use uw_core::adapter::{AdapterError, AdapterResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessOutput {
    pub status: i32,
    pub stdout: String,
    pub session_id: Option<String>,
}
#[async_trait]
pub trait ProcessSpawner: Send + Sync {
    async fn run(&self, spec: ProcessSpec) -> AdapterResult<ProcessOutput>;
}
pub struct TokioProcessSpawner;
#[async_trait]
impl ProcessSpawner for TokioProcessSpawner {
    async fn run(&self, spec: ProcessSpec) -> AdapterResult<ProcessOutput> {
        let output = tokio::process::Command::new(&spec.program)
            .args(&spec.args)
            .current_dir(Path::new(&spec.cwd))
            .output()
            .await
            .map_err(|e| AdapterError::Transient(e.to_string()))?;
        if !output.status.success() {
            return Err(AdapterError::Other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        Ok(ProcessOutput {
            status: output.status.code().unwrap_or(0),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            session_id: None,
        })
    }
}
