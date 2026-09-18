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

fn extract_session_id(stdout: &str) -> Option<String> {
    fn visit(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Object(object) => {
                for key in ["session_id", "thread_id"] {
                    if let Some(candidate) = object.get(key).and_then(serde_json::Value::as_str)
                        && uuid::Uuid::parse_str(candidate).is_ok()
                    {
                        return Some(candidate.to_owned());
                    }
                }
                object.values().find_map(visit)
            }
            serde_json::Value::Array(values) => values.iter().find_map(visit),
            _ => None,
        }
    }
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .find_map(|value| visit(&value))
}

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
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        Ok(ProcessOutput {
            status: output.status.code().unwrap_or(0),
            session_id: extract_session_id(&stdout),
            stdout,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_only_uuid_session_ids_from_json_or_jsonl() {
        let id = "01a0aecd-181f-7491-90b1-d2cc8beaad3f";
        assert_eq!(
            extract_session_id(&format!(
                r#"{{"type":"thread.started","thread_id":"{id}"}}"#
            )),
            Some(id.into())
        );
        assert_eq!(
            extract_session_id(&format!("noise\n{{\"session_id\":\"{id}\"}}\n")),
            Some(id.into())
        );
        assert_eq!(extract_session_id(r#"{"id":"not-a-session"}"#), None);
    }
}
