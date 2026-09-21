use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use uw_core::adapter::{
    AdapterError, AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, StatusEvent,
};
use uw_core::model::*;

#[async_trait]
pub trait CursorUsageTransport: Send + Sync {
    async fn current_period_usage(&self) -> AdapterResult<Value>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CursorAuth {
    Bearer(String),
    Cookie(String),
}

pub struct CursorHttpTransport {
    client: reqwest::Client,
    auth: CursorAuth,
}

impl CursorHttpTransport {
    pub fn new(auth: CursorAuth) -> Self {
        Self {
            client: reqwest::Client::new(),
            auth,
        }
    }
}

#[async_trait]
impl CursorUsageTransport for CursorHttpTransport {
    async fn current_period_usage(&self) -> AdapterResult<Value> {
        let mut request = self
            .client
            .post("https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage")
            .header("Content-Type", "application/json")
            .header("Connect-Protocol-Version", "1")
            .json(&Value::Object(Default::default()));
        request = match &self.auth {
            CursorAuth::Bearer(token) => request.bearer_auth(token),
            CursorAuth::Cookie(cookie) => request.header(reqwest::header::COOKIE, cookie),
        };
        let response = request
            .send()
            .await
            .map_err(|error| AdapterError::Transient(error.to_string()))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(AdapterError::Auth);
        }
        if response.status().is_server_error()
            || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return Err(AdapterError::Transient(format!(
                "HTTP {}",
                response.status()
            )));
        }
        if !response.status().is_success() {
            return Err(AdapterError::Other(format!("HTTP {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|error| AdapterError::Other(error.to_string()))
    }
}

pub struct CursorAdapter {
    transport: Arc<dyn CursorUsageTransport>,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl CursorAdapter {
    pub fn new(transport: Arc<dyn CursorUsageTransport>) -> Self {
        Self {
            transport,
            now: Arc::new(Utc::now),
        }
    }

    pub fn real(auth: CursorAuth) -> Self {
        Self::new(Arc::new(CursorHttpTransport::new(auth)))
    }

    pub fn with_clock(mut self, now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    pub fn capabilities_static() -> Capabilities {
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

    fn parse_sample(&self, value: &Value) -> AdapterResult<UsageSample> {
        let plan_usage = value
            .get("planUsage")
            .ok_or_else(|| AdapterError::Other("missing Cursor planUsage".into()))?;
        let reset = value.get("billingCycleEnd").and_then(parse_epoch_millis);
        let auto = percentage(plan_usage, "autoPercentUsed")?;
        let api = percentage(plan_usage, "apiPercentUsed")?;
        let mut windows = HashMap::new();
        for (name, pct) in [("auto", auto), ("api", api)] {
            windows.insert(
                WindowKey {
                    provider: Provider::Cursor,
                    kind: WindowKind::Custom(name.into()),
                },
                UsageWindowState::new(pct, pct >= 100.0, true, reset, None),
            );
        }
        let at = (self.now)();
        Ok(UsageSample {
            at,
            fetched_at: Some(at),
            source: UsageSource::ProviderReported,
            provider: Provider::Cursor,
            account: None,
            windows,
            credits: None,
        })
    }
}

fn percentage(value: &Value, field: &str) -> AdapterResult<f32> {
    let pct = value
        .get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| AdapterError::Other(format!("missing Cursor {field}")))?
        as f32;
    if pct.is_finite() {
        Ok(pct)
    } else {
        Err(AdapterError::Other(format!("invalid Cursor {field}")))
    }
}

fn parse_epoch_millis(value: &Value) -> Option<DateTime<Utc>> {
    let millis = value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())?;
    Utc.timestamp_millis_opt(millis).single()
}

#[async_trait]
impl HarnessAdapter for CursorAdapter {
    fn provider(&self) -> Provider {
        Provider::Cursor
    }

    fn capabilities(&self) -> Capabilities {
        Self::capabilities_static()
    }

    async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
        let value = self.transport.current_period_usage().await?;
        self.parse_sample(&value)
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

    async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
        Err(AdapterError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uw_core::adapter::HarnessAdapter;

    struct FakeTransport(Value);

    #[async_trait]
    impl CursorUsageTransport for FakeTransport {
        async fn current_period_usage(&self) -> AdapterResult<Value> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn parses_cursor_two_bar_monthly_usage() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({
            "billingCycleEnd": "1771077734000",
            "planUsage": {
                "autoPercentUsed": 12.5,
                "apiPercentUsed": 87.25
            }
        }))));
        let sample = adapter.fetch_usage(None).await.unwrap();
        let reset = Utc.timestamp_millis_opt(1771077734000).single();
        assert_eq!(sample.provider, Provider::Cursor);
        assert_eq!(sample.windows.len(), 2);
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Cursor,
                kind: WindowKind::Custom("auto".into())
            }]
                .pct,
            12.5
        );
        assert_eq!(
            sample.windows[&WindowKey {
                provider: Provider::Cursor,
                kind: WindowKind::Custom("api".into())
            }]
                .pct,
            87.25
        );
        assert_eq!(
            sample
                .windows
                .values()
                .map(|window| window.resets_at)
                .collect::<Vec<_>>(),
            vec![reset, reset]
        );
    }

    #[tokio::test]
    async fn clamps_percentages_and_marks_each_exhausted_bar() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({
            "planUsage": { "autoPercentUsed": 120, "apiPercentUsed": -5 }
        }))));
        let sample = adapter.fetch_usage(None).await.unwrap();
        let auto = &sample.windows[&WindowKey {
            provider: Provider::Cursor,
            kind: WindowKind::Custom("auto".into()),
        }];
        let api = &sample.windows[&WindowKey {
            provider: Provider::Cursor,
            kind: WindowKind::Custom("api".into()),
        }];
        assert_eq!(auto.pct, 100.0);
        assert!(auto.exceeded);
        assert_eq!(api.pct, 0.0);
        assert!(!api.exceeded);
    }

    #[test]
    fn cursor_has_no_destructive_harness_capabilities() {
        assert!(!CursorAdapter::capabilities_static().can_trigger_compaction);
        assert!(!CursorAdapter::capabilities_static().reports_token_counts);
    }
}
