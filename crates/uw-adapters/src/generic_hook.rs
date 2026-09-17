use crate::process::{ProcessSpawner, ProcessSpec};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, StatusEvent,
};
use uw_core::model::*;

pub struct GenericHookAdapter {
    command: Option<Vec<String>>,
    spawner: Arc<dyn ProcessSpawner>,
}
impl GenericHookAdapter {
    pub fn new(command: Option<Vec<String>>) -> Self {
        Self {
            command,
            spawner: Arc::new(crate::process::TokioProcessSpawner),
        }
    }
    pub fn with_spawner(mut self, spawner: Arc<dyn ProcessSpawner>) -> Self {
        self.spawner = spawner;
        self
    }
}
#[async_trait]
impl HarnessAdapter for GenericHookAdapter {
    fn provider(&self) -> Provider {
        Provider::Other("generic".into())
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: false,
            headless_resume: false,
            seed_modes: vec![],
        }
    }
    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let command = self.command.as_ref().ok_or(AdapterError::Unsupported)?;
        let (program, args) = command.split_first().ok_or(AdapterError::Unsupported)?;
        let out = self
            .spawner
            .run(ProcessSpec {
                program: program.clone(),
                args: args.to_vec(),
                cwd: ".".into(),
            })
            .await?;
        parse_sample(&out.stdout)
    }
    async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
        Err(AdapterError::Unsupported)
    }
    async fn emit_status(&self, _: &SessionId, _: StatusEvent) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn compact(
        &self,
        _: &SessionSummary,
        _: &CompactionRequest,
    ) -> AdapterResult<DeliveryOutcome> {
        Err(AdapterError::Unsupported)
    }
    async fn resume_session(&self, _: &SessionSummary) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}
fn parse_sample(stdout: &str) -> AdapterResult<UsageSample> {
    let value: Value =
        serde_json::from_str(stdout).map_err(|e| AdapterError::Other(e.to_string()))?;
    let windows_value = value
        .get("windows")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let windows: HashMap<WindowKey, UsageWindowState> =
        serde_json::from_value(windows_value).map_err(|e| AdapterError::Other(e.to_string()))?;
    Ok(UsageSample {
        at: Utc::now(),
        fetched_at: Some(Utc::now()),
        source: UsageSource::ProviderReported,
        provider: Provider::Other("generic".into()),
        account: None,
        windows,
        credits: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uw_core::adapter::{Capabilities, HarnessAdapter};
    #[test]
    fn defaults_are_unsupported() {
        assert_eq!(
            GenericHookAdapter::new(None).capabilities(),
            Capabilities {
                can_trigger_compaction: false,
                can_advise_mid_turn: false,
                can_inject_at_session_start: false,
                can_observe_compaction: false,
                reports_token_counts: false,
                headless_resume: false,
                seed_modes: vec![]
            }
        );
    }
    #[tokio::test]
    async fn no_command_means_unsupported() {
        assert!(matches!(
            GenericHookAdapter::new(None).fetch_usage(None).await,
            Err(AdapterError::Unsupported)
        ));
    }
}
