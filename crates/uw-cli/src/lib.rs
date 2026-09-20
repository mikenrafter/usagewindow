use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use std::collections::BTreeMap;
use uw_core::{api::*, model::*};
use uw_store::Store;

#[derive(Debug, Parser)]
#[command(name = "uw", version, about = "usagewindow command line client")]
pub struct Cli {
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Debug, Subcommand)]
pub enum Command {
    Status(StatusArgs),
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    Resume {
        session_id: Option<SessionId>,
        #[arg(long)]
        at: Option<DateTime<Utc>>,
        #[arg(long)]
        message: Option<String>,
        #[command(subcommand)]
        command: Option<ResumeCommand>,
    },
    Compact {
        #[command(subcommand)]
        command: CompactCommand,
    },
    Reseed(ReseedArgs),
    Keepalive {
        #[command(subcommand)]
        command: KeepaliveCommand,
    },
    Thresholds {
        #[command(subcommand)]
        command: ThresholdCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}
#[derive(Debug, Args, Default, Clone)]
pub struct StatusArgs {
    #[arg(long)]
    pub provider: Option<Provider>,
    #[arg(long)]
    pub model: Option<ModelId>,
    #[arg(long)]
    pub account: Option<AccountId>,
}
#[derive(Debug, Subcommand)]
pub enum SessionsCommand {
    List {
        #[arg(long)]
        stopped: bool,
        #[arg(long)]
        harness: Option<Provider>,
    },
    Show {
        session_id: SessionId,
    },
}
#[derive(Debug, Subcommand)]
pub enum ResumeCommand {
    Cancel { session_id: SessionId },
}
#[derive(Debug, Subcommand)]
pub enum CompactCommand {
    Ask {
        session_id: SessionId,
        #[arg(long)]
        reason: Option<String>,
    },
    Status {
        session_id: SessionId,
    },
}
#[derive(Debug, Args)]
pub struct ReseedArgs {
    pub session_id: SessionId,
    #[arg(long)]
    pub model: ModelId,
    #[arg(long)]
    pub dry_run: bool,
}
#[derive(Debug, Subcommand)]
pub enum KeepaliveCommand {
    Enable { session_id: SessionId },
    Disable { session_id: SessionId },
}
#[derive(Debug, Subcommand)]
pub enum ThresholdCommand {
    Get {
        #[arg(long)]
        scope: Option<String>,
    },
    Set {
        scope: String,
        field: String,
        value: String,
    },
}
#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    Status,
    Run,
}

pub trait ApiClient {
    fn status(&mut self, args: &StatusArgs) -> Result<StatusResponse>;
    fn sessions_list(
        &mut self,
        stopped: bool,
        harness: Option<&Provider>,
    ) -> Result<Vec<SessionListItem>>;
    fn sessions_show(&mut self, id: &SessionId) -> Result<SessionDetail>;
    fn resume(&mut self, request: ResumeRequest) -> Result<ResumeResponse>;
    fn cancel_resume(&mut self, request: CancelResumeRequest) -> Result<()>;
    fn compact_ask(&mut self, request: CompactAskRequest) -> Result<CompactStatusResponse>;
    fn compact_status(&mut self, id: &SessionId) -> Result<CompactStatusResponse>;
    fn reseed(
        &mut self,
        id: &SessionId,
        model: &ModelId,
        dry_run: bool,
    ) -> Result<serde_json::Value>;
    fn keepalive(&mut self, id: &SessionId, enabled: bool) -> Result<serde_json::Value>;
    fn thresholds_get(&mut self, request: ThresholdGetRequest) -> Result<ThresholdResponse>;
    fn thresholds_set(&mut self, request: ThresholdSetRequest) -> Result<ThresholdResponse>;
    fn daemon_status(&mut self) -> Result<serde_json::Value>;
    fn daemon_run(&mut self) -> Result<serde_json::Value>;
}

pub trait DirectReader {
    fn status(&mut self, args: &StatusArgs) -> Result<StatusResponse>;
    fn sessions_list(
        &mut self,
        stopped: bool,
        harness: Option<&Provider>,
    ) -> Result<Vec<SessionListItem>>;
    fn sessions_show(&mut self, id: &SessionId) -> Result<SessionDetail>;
    fn thresholds_get(&mut self, request: ThresholdGetRequest) -> Result<ThresholdResponse>;
}

/// Read operations use the daemon first and fall back only on an unreachable/read error.
/// Writes never fall back: the daemon is the owner of destructive state transitions.
pub struct ReadFallback<A, D> {
    pub api: A,
    pub direct: D,
}
impl<A, D> ReadFallback<A, D>
where
    A: ApiClient,
    D: DirectReader,
{
    pub fn status(&mut self, args: &StatusArgs) -> Result<StatusResponse> {
        self.api.status(args).or_else(|_| self.direct.status(args))
    }
    pub fn sessions_list(
        &mut self,
        stopped: bool,
        harness: Option<&Provider>,
    ) -> Result<Vec<SessionListItem>> {
        self.api
            .sessions_list(stopped, harness)
            .or_else(|_| self.direct.sessions_list(stopped, harness))
    }
    pub fn sessions_show(&mut self, id: &SessionId) -> Result<SessionDetail> {
        self.api
            .sessions_show(id)
            .or_else(|_| self.direct.sessions_show(id))
    }
    pub fn thresholds_get(&mut self, req: ThresholdGetRequest) -> Result<ThresholdResponse> {
        self.api
            .thresholds_get(req.clone())
            .or_else(|_| self.direct.thresholds_get(req))
    }
}

pub fn execute<A: ApiClient, D: DirectReader>(
    cli: Cli,
    mut client: ReadFallback<A, D>,
) -> Result<String> {
    let json = cli.json;
    let value = match cli.command {
        Command::Status(args) => serde_json::to_value(client.status(&args)?)?,
        Command::Sessions {
            command: SessionsCommand::List { stopped, harness },
        } => serde_json::to_value(client.sessions_list(stopped, harness.as_ref())?)?,
        Command::Sessions {
            command: SessionsCommand::Show { session_id },
        } => serde_json::to_value(client.sessions_show(&session_id)?)?,
        Command::Resume {
            session_id: Some(session_id),
            at,
            message,
            command: None,
        } => serde_json::to_value(client.api.resume(ResumeRequest {
            session_id,
            at,
            message,
        })?)?,
        Command::Resume {
            command: Some(ResumeCommand::Cancel { session_id }),
            ..
        } => {
            client
                .api
                .cancel_resume(CancelResumeRequest { session_id })?;
            serde_json::json!({"ok":true})
        }
        Command::Resume { .. } => {
            return Err(anyhow!(
                "resume requires a session id or `cancel <session-id>`"
            ));
        }
        Command::Compact {
            command: CompactCommand::Ask { session_id, reason },
        } => serde_json::to_value(
            client
                .api
                .compact_ask(CompactAskRequest { session_id, reason })?,
        )?,
        Command::Compact {
            command: CompactCommand::Status { session_id },
        } => serde_json::to_value(client.api.compact_status(&session_id)?)?,
        Command::Reseed(ReseedArgs {
            session_id,
            model,
            dry_run,
        }) => client.api.reseed(&session_id, &model, dry_run)?,
        Command::Keepalive {
            command: KeepaliveCommand::Enable { session_id },
        } => client.api.keepalive(&session_id, true)?,
        Command::Keepalive {
            command: KeepaliveCommand::Disable { session_id },
        } => client.api.keepalive(&session_id, false)?,
        Command::Thresholds {
            command: ThresholdCommand::Get { scope },
        } => serde_json::to_value(client.thresholds_get(ThresholdGetRequest {
            scope: parse_scope(scope.as_deref())?,
        })?)?,
        Command::Thresholds {
            command:
                ThresholdCommand::Set {
                    scope,
                    field,
                    value,
                },
        } => serde_json::to_value(client.api.thresholds_set(ThresholdSetRequest {
            scope: parse_scope(Some(&scope))?.ok_or_else(|| anyhow!("scope is required"))?,
            field,
            value,
        })?)?,
        Command::Daemon {
            command: DaemonCommand::Status,
        } => client.api.daemon_status()?,
        Command::Daemon {
            command: DaemonCommand::Run,
        } => client.api.daemon_run()?,
    };
    if json {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(human(&value))
    }
}

fn parse_scope(value: Option<&str>) -> Result<Option<ThresholdScope>> {
    let Some(value) = value else { return Ok(None) };
    let mut parts = value.split(':');
    let provider = match parts.next().unwrap_or_default() {
        "claude-code" => Provider::ClaudeCode,
        "codex" => Provider::Codex,
        "cursor" => Provider::Cursor,
        "gemini" => Provider::Gemini,
        other if !other.is_empty() => Provider::Other(other.into()),
        _ => return Err(anyhow!("invalid threshold scope")),
    };
    let model = parts
        .next()
        .filter(|x| !x.is_empty())
        .map(|x| ModelId(x.into()));
    let session = parts
        .next()
        .filter(|x| !x.is_empty())
        .map(|x| SessionId(x.into()));
    Ok(Some(ThresholdScope {
        provider,
        model,
        session,
    }))
}
fn human(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(items) => items.iter().map(human).collect::<Vec<_>>().join("\n"),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, v)| format!("{k}: {}", human(v)))
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Null => "-".into(),
        _ => value.to_string().trim_matches('"').into(),
    }
}

pub struct HttpApiClient {
    base_url: String,
    client: reqwest::blocking::Client,
}
impl HttpApiClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').into(),
            client: reqwest::blocking::Client::builder().build()?,
        })
    }
    fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()?
            .error_for_status()?;
        Ok(response.json()?)
    }
    fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .send()?
            .error_for_status()?;
        Ok(response.json()?)
    }
}
impl Default for HttpApiClient {
    fn default() -> Self {
        Self::new(std::env::var("UW_API_URL").unwrap_or_else(|_| "http://127.0.0.1:7878".into()))
            .expect("valid default daemon URL")
    }
}
impl ApiClient for HttpApiClient {
    fn status(&mut self, args: &StatusArgs) -> Result<StatusResponse> {
        let mut request = self.client.get(format!("{}/api/status", self.base_url));
        let mut query = Vec::new();
        if let Some(provider) = &args.provider {
            query.push((
                "provider",
                serde_json::to_string(provider)?
                    .trim_matches('"')
                    .to_string(),
            ));
        }
        if let Some(model) = &args.model {
            query.push(("model", model.0.clone()));
        }
        if let Some(account) = &args.account {
            query.push(("account", account.0.clone()));
        }
        request = request.query(&query);
        Ok(request.send()?.error_for_status()?.json()?)
    }
    fn sessions_list(
        &mut self,
        stopped: bool,
        harness: Option<&Provider>,
    ) -> Result<Vec<SessionListItem>> {
        let mut query = vec![("stopped", stopped.to_string())];
        if let Some(harness) = harness {
            query.push((
                "harness",
                serde_json::to_string(harness)?
                    .trim_matches('"')
                    .to_string(),
            ));
        }
        let page: SessionsPage = self
            .client
            .get(format!("{}/api/sessions", self.base_url))
            .query(&query)
            .send()?
            .error_for_status()?
            .json()?;
        Ok(page.items)
    }
    fn sessions_show(&mut self, id: &SessionId) -> Result<SessionDetail> {
        self.get(&format!("/api/sessions/{}", id.0))
    }
    fn resume(&mut self, request: ResumeRequest) -> Result<ResumeResponse> {
        self.post(
            &format!("/api/sessions/{}/resume", request.session_id.0),
            &request,
        )
    }
    fn cancel_resume(&mut self, request: CancelResumeRequest) -> Result<()> {
        let _: serde_json::Value = self.post(
            &format!("/api/sessions/{}/resume/cancel", request.session_id.0),
            &request,
        )?;
        Ok(())
    }
    fn compact_ask(&mut self, request: CompactAskRequest) -> Result<CompactStatusResponse> {
        self.post(
            &format!("/api/sessions/{}/compact/ask", request.session_id.0),
            &request,
        )
    }
    fn compact_status(&mut self, id: &SessionId) -> Result<CompactStatusResponse> {
        self.get(&format!("/api/sessions/{}/compact/status", id.0))
    }
    fn reseed(&mut self, _: &SessionId, _: &ModelId, _: bool) -> Result<serde_json::Value> {
        Err(anyhow!("reseed is a Phase 8 operation"))
    }
    fn keepalive(&mut self, id: &SessionId, enabled: bool) -> Result<serde_json::Value> {
        self.post(
            &format!("/api/sessions/{}/keepalive", id.0),
            &serde_json::json!({"enabled": enabled}),
        )
    }
    fn thresholds_get(&mut self, request: ThresholdGetRequest) -> Result<ThresholdResponse> {
        let scope = request.scope.map(|s| {
            format!(
                "{:?}:{}:{}",
                s.provider,
                s.model.map(|m| m.0).unwrap_or_default(),
                s.session.map(|s| s.0).unwrap_or_default()
            )
        });
        let mut query = self.client.get(format!("{}/api/thresholds", self.base_url));
        if let Some(scope) = scope {
            query = query.query(&[("scope", scope)]);
        }
        Ok(query.send()?.error_for_status()?.json()?)
    }
    fn thresholds_set(&mut self, request: ThresholdSetRequest) -> Result<ThresholdResponse> {
        self.post("/api/thresholds", &request)
    }
    fn daemon_status(&mut self) -> Result<serde_json::Value> {
        Err(anyhow!(
            "daemon HTTP API is not available until uw-web Phase 7"
        ))
    }
    fn daemon_run(&mut self) -> Result<serde_json::Value> {
        Err(anyhow!("daemon run is a local process operation"))
    }
}

pub struct StoreReader {
    store: Store,
}
impl StoreReader {
    pub fn open(path: &str) -> Result<Self> {
        Ok(Self {
            store: Store::open(path)?,
        })
    }
}
impl DirectReader for StoreReader {
    fn status(&mut self, args: &StatusArgs) -> Result<StatusResponse> {
        let samples = self.store.all_usage_samples()?;
        let mut latest: BTreeMap<String, UsageSample> = BTreeMap::new();
        for mut s in samples {
            if args.provider.as_ref().is_some_and(|p| p != &s.provider)
                || args
                    .account
                    .as_ref()
                    .is_some_and(|a| Some(a) != s.account.as_ref())
            {
                continue;
            }
            if let Some(model) = args.model.as_ref() {
                s.windows.retain(|key, _| matches!(&key.kind, WindowKind::WeeklyModel(candidate) if candidate == model));
                if s.windows.is_empty() {
                    continue;
                }
            }
            let key = format!("{:?}:{:?}", s.provider, s.account);
            if latest.get(&key).is_none_or(|old| old.at < s.at) {
                latest.insert(key, s);
            }
        }
        let usage = latest
            .into_values()
            .map(|s| {
                let provider = s.provider;
                let account = s.account;
                let windows = s
                    .windows
                    .into_iter()
                    .map(|(key, w)| UsageWindowSummary {
                        window: key.kind,
                        pct: w.pct,
                        resets_at: w.resets_at,
                        exceeded: w.exceeded,
                        // CLI status stays lightweight: burn rate / exhaustion projection and
                        // per-window session counts are HTTP-API-only (see uw-web::status).
                        burn_rate_pct_per_hour: None,
                        active_sessions: 0,
                        depletes_at: None,
                    })
                    .collect();
                ProviderUsageSummary {
                    provider,
                    account,
                    windows,
                }
            })
            .collect();
        Ok(StatusResponse {
            usage,
            last_updated: Utc::now(),
            provider_status: self.store.fetch_statuses()?,
            keepalive_active_count: self.store.keepalive_active_count()?,
        })
    }
    fn sessions_list(
        &mut self,
        stopped: bool,
        harness: Option<&Provider>,
    ) -> Result<Vec<SessionListItem>> {
        self
            .store
            .list_sessions()?
            .into_iter()
            .filter(|s| {
                harness.is_none_or(|h| h == &s.harness) && (stopped || s.stopped_reason.is_none())
            })
            .map(|s| -> Result<SessionListItem> {
                let id = s.id.clone();
                let keepalive = self
                    .store
                    .keepalive_config(&id)
                    .map(|k| k.enabled)
                    .unwrap_or(false);
                let resume_status = self
                    .store
                    .resume_markers_for_session(&id)?
                    .last()
                    .map(|m| m.status.clone());
                let compaction_status = self
                    .store
                    .compaction_requests_for_session(&id)?
                    .last()
                    .map(|r| r.status.clone());
                Ok(SessionListItem {
                    id,
                    harness: s.harness,
                    model: s.model,
                    account: s.account,
                    last_seen: s.last_seen,
                    stopped_reason: s.stopped_reason,
                    resume_status,
                    compaction_status,
                    keepalive,
                    active: false,
                    cached: false,
                })
            })
            .collect::<Result<Vec<_>>>()
    }
    fn sessions_show(&mut self, id: &SessionId) -> Result<SessionDetail> {
        let summary = self.store.read_session(id)?;
        let markers = self.store.resume_markers_for_session(id)?;
        let marker = markers.last().cloned();
        let history = self
            .store
            .all_usage_samples()?
            .into_iter()
            .filter(|s| s.provider == summary.harness && s.account == summary.account)
            .flat_map(|s| {
                s.windows.into_values().map(move |w| SparklinePoint {
                    at: s.at,
                    pct: w.pct,
                })
            })
            .collect();
        Ok(SessionDetail {
            summary,
            history,
            resume_controls: ResumeControls {
                can_resume: marker.is_some(),
                marker,
            },
            compaction_log: self.store.compaction_requests_for_session(id)?,
            reseed_lineage: vec![],
            errors: vec![],
        })
    }
    fn thresholds_get(&mut self, request: ThresholdGetRequest) -> Result<ThresholdResponse> {
        Ok(ThresholdResponse {
            scope: request.scope,
            values: BTreeMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fake {
        calls: Vec<String>,
        fail_reads: bool,
    }
    impl Fake {
        fn new(fail_reads: bool) -> Self {
            Self {
                calls: vec![],
                fail_reads,
            }
        }
    }
    impl ApiClient for Fake {
        fn status(&mut self, _: &StatusArgs) -> Result<StatusResponse> {
            self.calls.push("status".into());
            if self.fail_reads {
                Err(anyhow!("down"))
            } else {
                Ok(StatusResponse {
                    usage: vec![],
                    last_updated: Utc::now(),
                    provider_status: vec![],
                    keepalive_active_count: 0,
                })
            }
        }
        fn sessions_list(&mut self, _: bool, _: Option<&Provider>) -> Result<Vec<SessionListItem>> {
            self.calls.push("list".into());
            Err(anyhow!("down"))
        }
        fn sessions_show(&mut self, _: &SessionId) -> Result<SessionDetail> {
            self.calls.push("show".into());
            Err(anyhow!("down"))
        }
        fn resume(&mut self, r: ResumeRequest) -> Result<ResumeResponse> {
            self.calls.push(format!("resume:{}", r.session_id.0));
            Ok(ResumeResponse { marker: None })
        }
        fn cancel_resume(&mut self, r: CancelResumeRequest) -> Result<()> {
            self.calls.push(format!("cancel:{}", r.session_id.0));
            Ok(())
        }
        fn compact_ask(&mut self, r: CompactAskRequest) -> Result<CompactStatusResponse> {
            self.calls.push(format!("ask:{}", r.session_id.0));
            Ok(CompactStatusResponse { requests: vec![] })
        }
        fn compact_status(&mut self, _: &SessionId) -> Result<CompactStatusResponse> {
            Ok(CompactStatusResponse { requests: vec![] })
        }
        fn reseed(&mut self, _: &SessionId, _: &ModelId, _: bool) -> Result<serde_json::Value> {
            Ok(serde_json::json!({}))
        }
        fn keepalive(&mut self, _: &SessionId, _: bool) -> Result<serde_json::Value> {
            Ok(serde_json::json!({}))
        }
        fn thresholds_get(&mut self, _: ThresholdGetRequest) -> Result<ThresholdResponse> {
            Err(anyhow!("down"))
        }
        fn thresholds_set(&mut self, _: ThresholdSetRequest) -> Result<ThresholdResponse> {
            Ok(ThresholdResponse {
                scope: None,
                values: BTreeMap::new(),
            })
        }
        fn daemon_status(&mut self) -> Result<serde_json::Value> {
            Ok(serde_json::json!({}))
        }
        fn daemon_run(&mut self) -> Result<serde_json::Value> {
            Ok(serde_json::json!({}))
        }
    }
    struct Direct;
    impl DirectReader for Direct {
        fn status(&mut self, _: &StatusArgs) -> Result<StatusResponse> {
            Ok(StatusResponse {
                usage: vec![],
                last_updated: Utc::now(),
                provider_status: vec![],
                keepalive_active_count: 0,
            })
        }
        fn sessions_list(&mut self, _: bool, _: Option<&Provider>) -> Result<Vec<SessionListItem>> {
            Ok(vec![])
        }
        fn sessions_show(&mut self, _: &SessionId) -> Result<SessionDetail> {
            Err(anyhow!("no"))
        }
        fn thresholds_get(&mut self, r: ThresholdGetRequest) -> Result<ThresholdResponse> {
            Ok(ThresholdResponse {
                scope: r.scope,
                values: BTreeMap::new(),
            })
        }
    }
    #[test]
    fn reachable_read_uses_api() {
        let mut f = ReadFallback {
            api: Fake::new(false),
            direct: Direct,
        };
        let _ = f.status(&StatusArgs::default()).unwrap();
        assert_eq!(f.api.calls, vec!["status"]);
    }
    #[test]
    fn failed_read_uses_direct_fallback() {
        let mut f = ReadFallback {
            api: Fake::new(true),
            direct: Direct,
        };
        f.status(&StatusArgs::default()).unwrap();
        assert_eq!(f.api.calls, vec!["status"]);
    }
    #[test]
    fn clap_parses_documented_command_shapes() {
        assert!(
            matches!(Cli::try_parse_from(["uw", "resume", "s", "--at", "2026-01-01T00:00:00Z"]), Ok(Cli { command: Command::Resume { session_id: Some(SessionId(id)), command: None, .. }, .. }) if id == "s")
        );
        assert!(Cli::try_parse_from(["uw", "resume", "cancel", "s", "--json"]).is_ok());
        assert!(
            Cli::try_parse_from(["uw", "compact", "ask", "s", "--reason", "why", "--json"]).is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "uw",
                "thresholds",
                "set",
                "codex",
                "closing_pct",
                "85",
                "--json"
            ])
            .is_ok()
        );
    }

    #[tokio::test]
    async fn http_client_deserializes_status_from_mock_server() {
        let router = axum::Router::new().route(
            "/api/status",
            axum::routing::get(|| async {
                axum::Json(StatusResponse {
                    usage: vec![],
                    last_updated: Utc::now(),
                    provider_status: vec![],
                    keepalive_active_count: 0,
                })
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let result = tokio::task::spawn_blocking(move || {
            HttpApiClient::new(format!("http://{address}"))?.status(&StatusArgs::default())
        })
        .await
        .unwrap()
        .unwrap();
        assert!(result.usage.is_empty());
        task.abort();
    }

    #[test]
    fn refused_http_read_uses_direct_fallback() {
        let mut fallback = ReadFallback {
            api: HttpApiClient::new("http://127.0.0.1:9").unwrap(),
            direct: Direct,
        };
        fallback.status(&StatusArgs::default()).unwrap();
    }
}
