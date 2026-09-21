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

impl CursorAuth {
    /// Returns every candidate credential this environment has, in the order
    /// they should be tried. An explicit env var override is trusted alone; the
    /// IDE and cursor-agent CLI sources are both always collected because they
    /// expire independently (see `discover`).
    pub fn discover_from_environment() -> Vec<Self> {
        if let Ok(token) = std::env::var("UW_CURSOR_ACCESS_TOKEN")
            && !token.is_empty()
        {
            return vec![Self::Bearer(token)];
        }
        if let Ok(cookie) = std::env::var("UW_CURSOR_SESSION_COOKIE")
            && !cookie.is_empty()
        {
            return vec![Self::Cookie(cookie)];
        }
        if let Ok(token) = std::env::var("CURSOR_AUTH_TOKEN")
            && !token.is_empty()
        {
            return vec![Self::Bearer(token)];
        }

        let Some(home) = std::env::var_os("HOME").map(std::path::PathBuf::from) else {
            return Vec::new();
        };
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        let state_db = std::env::var_os("CURSOR_STATE_DB")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| config.join("Cursor/User/globalStorage/state.vscdb"));
        let cli_auth = std::env::var_os("CURSOR_CLI_AUTH_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| config.join("cursor/auth.json"));
        Self::discover(Some(&state_db), Some(&cli_auth))
    }

    /// The Cursor IDE and the standalone `cursor-agent` CLI keep entirely
    /// separate access tokens, and either one can be the stale one: a user
    /// who only runs `cursor-agent` has a long-expired IDE token sitting in
    /// `state.vscdb` from whenever they last opened the IDE, while a user who
    /// mostly lives in the IDE may have let a `cursor-agent login` token lapse.
    /// Preferring the IDE source unconditionally (as pure precedence would)
    /// means a dead IDE session permanently shadows a perfectly good CLI one.
    /// So: collect both discovered tokens, and sort the unexpired ones first
    /// (stable sort keeps the IDE-first tie-break when both are equally
    /// valid/invalid). A token this can't decode is treated as unexpired —
    /// trust it and let the API be the final arbiter.
    pub fn discover(
        state_db: Option<&std::path::Path>,
        cli_auth: Option<&std::path::Path>,
    ) -> Vec<Self> {
        let mut tokens: Vec<String> = [
            state_db.and_then(read_access_token_from_state_db),
            cli_auth.and_then(read_access_token_from_cli_auth),
        ]
        .into_iter()
        .flatten()
        .collect();
        tokens.sort_by_key(|token| jwt_is_expired(token));
        tokens.into_iter().map(Self::Bearer).collect()
    }
}

/// Decodes a JWT's unverified payload segment and checks its `exp` claim
/// against the current time. Cursor's access tokens are JWTs; this never
/// validates a signature and must only be used to pick which already-issued,
/// already-trusted local credential to try first.
fn jwt_is_expired(token: &str) -> bool {
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let mut padded = payload.to_owned();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    let Ok(bytes) = base64_url_decode(&padded) else {
        return false;
    };
    let Ok(claims) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    let Some(exp) = claims.get("exp").and_then(Value::as_i64) else {
        return false;
    };
    exp < Utc::now().timestamp()
}

/// Minimal base64url decoder (RFC 4648 §5) so this file doesn't need a base64
/// crate dependency just to read one JWT claim.
fn base64_url_decode(input: &str) -> Result<Vec<u8>, ()> {
    let mut value: u32 = 0;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    for c in input.bytes() {
        let digit = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => continue,
            _ => return Err(()),
        } as u32;
        value = (value << 6) | digit;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((value >> bits) as u8);
        }
    }
    Ok(out)
}

fn read_access_token_from_state_db(path: &std::path::Path) -> Option<String> {
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    connection
        .query_row(
            "SELECT value FROM ItemTable WHERE key = 'cursorAuth/accessToken'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .filter(|token| !token.is_empty())
}

fn read_access_token_from_cli_auth(path: &std::path::Path) -> Option<String> {
    let value: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    value
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

pub struct CursorHttpTransport {
    client: reqwest::Client,
    /// Tried in order on every call; a candidate that fails with
    /// `AdapterError::Auth` is skipped in favor of the next one, since the IDE
    /// and cursor-agent CLI tokens expire independently and either can be the
    /// stale one. The last candidate's error (or `Auth` if candidates is
    /// empty) is returned when every candidate fails.
    candidates: Vec<CursorAuth>,
}

impl CursorHttpTransport {
    pub fn new(candidates: Vec<CursorAuth>) -> Self {
        Self {
            client: reqwest::Client::new(),
            candidates,
        }
    }

    async fn call_with(&self, auth: &CursorAuth) -> AdapterResult<Value> {
        let mut request = self
            .client
            .post("https://api2.cursor.sh/aiserver.v1.DashboardService/GetCurrentPeriodUsage")
            .header("Content-Type", "application/json")
            .header("Connect-Protocol-Version", "1")
            .json(&Value::Object(Default::default()));
        request = match auth {
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

#[async_trait]
impl CursorUsageTransport for CursorHttpTransport {
    async fn current_period_usage(&self) -> AdapterResult<Value> {
        for (index, auth) in self.candidates.iter().enumerate() {
            match self.call_with(auth).await {
                Ok(value) => return Ok(value),
                Err(AdapterError::Auth) if index + 1 < self.candidates.len() => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AdapterError::Auth)
    }
}

pub struct CursorAdapter {
    transport: Arc<dyn CursorUsageTransport>,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    account: Option<AccountId>,
}

impl CursorAdapter {
    pub fn new(transport: Arc<dyn CursorUsageTransport>) -> Self {
        Self {
            transport,
            now: Arc::new(Utc::now),
            account: None,
        }
    }

    pub fn real(candidates: Vec<CursorAuth>) -> Self {
        Self::new(Arc::new(CursorHttpTransport::new(candidates))).with_account(read_cached_email())
    }

    pub fn with_account(mut self, account: Option<AccountId>) -> Self {
        self.account = account;
        self
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
        let account = account_from_usage(value).or_else(|| self.account.clone());
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
            account,
            windows,
            credits: None,
        })
    }
}

fn account_from_usage(value: &Value) -> Option<AccountId> {
    ["email", "accountEmail", "userEmail"]
        .into_iter()
        .chain(["/user/email", "/account/email"])
        .filter_map(|path| {
            if path.starts_with('/') {
                value.pointer(path).and_then(Value::as_str)
            } else {
                value.get(path).and_then(Value::as_str)
            }
        })
        .find(|value| !value.is_empty())
        .map(|value| AccountId(value.to_owned()))
}

/// Cursor IDE keeps the signed-in email beside its access token in the local
/// VS Code-compatible state database. It is read-only and optional: API-only
/// credentials may not have this file, and the usage response can still carry
/// an email in that case.
fn read_cached_email() -> Option<AccountId> {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from)?;
    let configured = std::env::var_os("CURSOR_STATE_DB").map(std::path::PathBuf::from);
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| home.join(".config"));
    let path = configured.unwrap_or_else(|| xdg.join("Cursor/User/globalStorage/state.vscdb"));
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .ok()?;
    connection
        .query_row(
            "SELECT value FROM ItemTable WHERE key = 'cursorAuth/cachedEmail'",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok()
        .filter(|email| !email.is_empty())
        .map(AccountId)
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
            "email": "cursor@example.com",
            "planUsage": {
                "autoPercentUsed": 12.5,
                "apiPercentUsed": 87.25
            }
        }))));
        let sample = adapter.fetch_usage(None).await.unwrap();
        let reset = Utc.timestamp_millis_opt(1771077734000).single();
        assert_eq!(sample.provider, Provider::Cursor);
        assert_eq!(sample.account, Some(AccountId("cursor@example.com".into())));
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

    #[tokio::test]
    async fn unsupported_stop_detection_is_explicit() {
        let adapter = CursorAdapter::new(Arc::new(FakeTransport(json!({}))));
        assert!(matches!(
            adapter
                .detect_stop(&SessionId("cursor-session".into()))
                .await,
            Err(AdapterError::Unsupported)
        ));
    }

    #[test]
    fn discovers_access_token_from_cursor_ide_state_database() {
        let path =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.vscdb", uuid::Uuid::new_v4()));
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ItemTable (key, value) VALUES ('cursorAuth/accessToken', 'ide-token')",
                [],
            )
            .unwrap();

        assert_eq!(
            CursorAuth::discover(Some(&path), None),
            vec![CursorAuth::Bearer("ide-token".into())]
        );
        drop(connection);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn falls_back_to_cursor_agent_auth_json() {
        let path =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, r#"{"accessToken":"cli-token"}"#).unwrap();

        assert_eq!(
            CursorAuth::discover(None, Some(&path)),
            vec![CursorAuth::Bearer("cli-token".into())]
        );
        let _ = std::fs::remove_file(path);
    }

    fn fake_jwt(exp: i64) -> String {
        fn b64url(bytes: &[u8]) -> String {
            const CHARS: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let mut out = String::new();
            for chunk in bytes.chunks(3) {
                let b0 = chunk[0] as u32;
                let b1 = *chunk.get(1).unwrap_or(&0) as u32;
                let b2 = *chunk.get(2).unwrap_or(&0) as u32;
                let n = (b0 << 16) | (b1 << 8) | b2;
                out.push(CHARS[((n >> 18) & 63) as usize] as char);
                out.push(CHARS[((n >> 12) & 63) as usize] as char);
                if chunk.len() > 1 {
                    out.push(CHARS[((n >> 6) & 63) as usize] as char);
                }
                if chunk.len() > 2 {
                    out.push(CHARS[(n & 63) as usize] as char);
                }
            }
            out
        }
        let payload = b64url(json!({"exp": exp}).to_string().as_bytes());
        format!("header.{payload}.sig")
    }

    #[test]
    fn an_expired_ide_token_does_not_shadow_a_valid_cli_token() {
        let far_future = Utc::now().timestamp() + 3600;
        let long_expired = Utc::now().timestamp() - 3600;

        let state_db =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.vscdb", uuid::Uuid::new_v4()));
        let connection = rusqlite::Connection::open(&state_db).unwrap();
        connection
            .execute(
                "CREATE TABLE ItemTable (key TEXT PRIMARY KEY, value TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO ItemTable (key, value) VALUES ('cursorAuth/accessToken', ?1)",
                [fake_jwt(long_expired)],
            )
            .unwrap();
        drop(connection);

        let cli_auth =
            std::env::temp_dir().join(format!("usagewindow-cursor-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(
            &cli_auth,
            json!({"accessToken": fake_jwt(far_future)}).to_string(),
        )
        .unwrap();

        assert_eq!(
            CursorAuth::discover(Some(&state_db), Some(&cli_auth)),
            vec![
                CursorAuth::Bearer(fake_jwt(far_future)),
                CursorAuth::Bearer(fake_jwt(long_expired)),
            ]
        );

        let _ = std::fs::remove_file(state_db);
        let _ = std::fs::remove_file(cli_auth);
    }
}
