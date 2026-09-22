use argon2::{Argon2, PasswordHash, PasswordVerifier};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use uw_core::{adapter::HarnessAdapter, api::*, compaction::HARD_BOUNDARY_REASON, model::*};
use uw_store::{Store, ThresholdOverride, ThresholdScopeKind};

#[derive(RustEmbed)]
#[folder = "ui/"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    store: Arc<Mutex<Store>>,
    adapters: Arc<HashMap<Provider, Arc<dyn HarnessAdapter>>>,
    auth: Arc<WebAuth>,
}

pub type SharedStore = Arc<Mutex<Store>>;

const SESSION_COOKIE: &str = "uw_session";
const MAX_FAILED_LOGINS: u8 = 4;

struct WebAuth {
    password_hash: Option<String>,
    state: Mutex<AuthState>,
}

struct AuthState {
    failed_logins: u8,
    sessions: std::collections::HashSet<String>,
}

impl WebAuth {
    fn new(password_hash: Option<String>) -> anyhow::Result<Self> {
        if let Some(hash) = &password_hash {
            PasswordHash::new(hash)
                .map_err(|error| anyhow::anyhow!("invalid password verifier: {error:?}"))?;
        }
        Ok(Self {
            password_hash,
            state: Mutex::new(AuthState {
                failed_logins: 0,
                sessions: std::collections::HashSet::new(),
            }),
        })
    }

    fn enabled(&self) -> bool {
        self.password_hash.is_some()
    }

    fn authenticated(&self, headers: &HeaderMap) -> bool {
        if !self.enabled() {
            return true;
        }
        let Some(token) = cookie(headers, SESSION_COOKIE) else {
            return false;
        };
        self.state
            .lock()
            .is_ok_and(|state| state.sessions.contains(token))
    }

    fn login(&self, password: &str) -> LoginResult {
        let valid = self.password_hash.as_ref().is_some_and(|hash| {
            PasswordHash::new(hash).ok().is_some_and(|parsed| {
                Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .is_ok()
            })
        });
        let Ok(mut state) = self.state.lock() else {
            return LoginResult::Locked;
        };
        if !valid {
            state.failed_logins = state.failed_logins.saturating_add(1);
            return if state.failed_logins >= MAX_FAILED_LOGINS {
                LoginResult::Locked
            } else {
                LoginResult::Invalid
            };
        }
        state.failed_logins = 0;
        let token = uuid::Uuid::new_v4().to_string();
        state.sessions.insert(token.clone());
        LoginResult::Authenticated(token)
    }

    fn logout(&self, headers: &HeaderMap) {
        let Some(token) = cookie(headers, SESSION_COOKIE) else {
            return;
        };
        if let Ok(mut state) = self.state.lock() {
            state.sessions.remove(token);
        }
    }
}

enum LoginResult {
    Authenticated(String),
    Invalid,
    Locked,
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&format!("{name}=")))
}

/// For callers with no live adapters to offer (tests, or read-only tooling) — every
/// route works except `/preview`, which needs a real adapter to read a transcript from.
pub fn app(store: Store) -> Router {
    app_with_adapters(store, HashMap::new())
}

pub fn app_with_shared_store(
    store: SharedStore,
    adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
) -> Router {
    app_with_shared_store_and_auth(store, adapters, None).expect("disabled web auth is valid")
}

pub fn app_with_shared_store_and_auth(
    store: SharedStore,
    adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
    password_hash: Option<String>,
) -> anyhow::Result<Router> {
    let auth = Arc::new(WebAuth::new(password_hash)?);
    let protected = Router::new()
        .route("/api/status", get(status))
        .route("/api/sessions", get(sessions).post(create_session))
        .route(
            "/api/sessions/{id}",
            get(session).put(update_session).delete(delete_session),
        )
        .route("/api/sessions/{id}/preview", get(session_preview))
        .route("/api/sessions/{id}/resume", post(resume))
        .route("/api/sessions/{id}/resume/cancel", post(cancel_resume))
        .route("/api/sessions/{id}/compact/ask", post(compact_ask))
        .route("/api/sessions/{id}/compact/cancel", post(cancel_compact))
        .route("/api/sessions/{id}/compact/status", get(compact_status))
        .route("/api/sessions/{id}/keepalive", post(keepalive))
        .route(
            "/api/sessions/{id}/supersede-stop",
            post(supersede_stop),
        )
        .route(
            "/api/provider-status/non-blocking",
            post(set_provider_non_blocking),
        )
        .route("/api/hooks", post(hook_ingress))
        .route("/api/thresholds", get(thresholds_get).post(thresholds_set))
        .route("/api/compactions/recent", get(compactions_recent))
        .route("/api/actions/recent", get(actions_recent))
        .layer(middleware::from_fn_with_state(Arc::clone(&auth), authorize));
    Ok(Router::new()
        .route("/api/auth/status", get(auth_status))
        .route("/api/auth/login", post(auth_login))
        .route("/api/auth/logout", post(auth_logout))
        // These routes are for the loopback-only MCP bridge. The daemon owns
        // the database, so MCP must use the daemon even when web auth is on.
        .route("/api/internal/status", get(status))
        .route("/api/internal/sessions/{id}", get(session))
        .route(
            "/api/internal/sessions/{id}/compact/ask",
            post(compact_ask),
        )
        .merge(protected)
        .fallback(static_asset)
        .with_state(AppState {
            store,
            adapters: Arc::new(adapters),
            auth,
        }))
}

pub fn app_with_adapters(
    store: Store,
    adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
) -> Router {
    app_with_adapters_and_auth(store, adapters, None).expect("disabled web auth is valid")
}

pub fn app_with_adapters_and_auth(
    store: Store,
    adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
    password_hash: Option<String>,
) -> anyhow::Result<Router> {
    app_with_shared_store_and_auth(Arc::new(Mutex::new(store)), adapters, password_hash)
}

async fn authorize(
    State(auth): State<Arc<WebAuth>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    // Harness hook processes call this local endpoint directly and do not have
    // a browser session. The handler only validates and records hook payloads;
    // all model-control UI routes remain behind auth.
    if request.uri().path() == "/api/hooks" || auth.authenticated(request.headers()) {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "authentication required").into_response()
    }
}

async fn auth_status(State(state): State<AppState>, headers: HeaderMap) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "enabled": state.auth.enabled(),
        "authenticated": state.auth.authenticated(&headers),
    }))
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    password: String,
}

async fn auth_login(State(state): State<AppState>, Json(request): Json<LoginRequest>) -> Response {
    if !state.auth.enabled() {
        return StatusCode::NO_CONTENT.into_response();
    }
    match state.auth.login(&request.password) {
        LoginResult::Authenticated(token) => {
            let mut response = Json(serde_json::json!({"authenticated": true})).into_response();
            let value = format!(
                "{SESSION_COOKIE}={token}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=86400"
            );
            if let Ok(value) = HeaderValue::from_str(&value) {
                response.headers_mut().insert(header::SET_COOKIE, value);
            }
            response
        }
        LoginResult::Invalid => (StatusCode::UNAUTHORIZED, "invalid password").into_response(),
        LoginResult::Locked => {
            // A clean exit is intentional: systemd's Restart=on-failure will not
            // restart a manually locked service. An operator must restart it.
            std::process::exit(0);
        }
    }
}

async fn auth_logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    state.auth.logout(&headers);
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "uw_session=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0",
        ),
    );
    response
}

#[derive(Debug, Deserialize)]
struct HookQuery {
    provider: String,
}

async fn hook_ingress(
    State(state): State<AppState>,
    Query(query): Query<HookQuery>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let provider = query
        .provider
        .parse::<Provider>()
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let event = payload
        .get("hook_event_name")
        .or_else(|| payload.get("hookEventName"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing hook event name".into()))?
        .to_owned();
    if !supports_hook_event(&provider, &event) {
        return Err((StatusCode::BAD_REQUEST, "unsupported hook event".into()));
    }
    let id = payload
        .get("session_id")
        .or_else(|| payload.get("sessionId"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing session id".into()))?;
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err((StatusCode::BAD_REQUEST, "invalid session id".into()));
    }
    if provider == Provider::ClaudeCode && uuid::Uuid::parse_str(id).is_err() {
        return Err((StatusCode::BAD_REQUEST, "invalid Claude session id".into()));
    }
    let transcript_path = payload
        .get("transcript_path")
        .or_else(|| payload.get("transcriptPath"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    if provider == Provider::ClaudeCode && transcript_path.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "missing Claude transcript path".into(),
        ));
    }
    if provider == Provider::ClaudeCode
        && transcript_path.as_deref().is_some_and(|path| {
            std::path::Path::new(path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                != Some(id)
        })
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Claude transcript path does not match session id".into(),
        ));
    }
    let id = SessionId(id.to_owned());
    let cwd = payload
        .get("cwd")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(".")
        .to_owned();
    let model = payload
        .get("model")
        .and_then(serde_json::Value::as_str)
        .map(|value| ModelId(value.to_owned()));
    let pid = payload
        .get("pid")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    let now = Utc::now();
    let output_event = event.clone();
    let output_provider = provider.clone();
    let trigger = payload
        .get("trigger")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let messages = read(state, move |store| {
        store.upsert_session(&SessionSummary {
            id: id.clone(),
            harness: provider,
            model,
            account: None,
            first_seen: now,
            last_seen: now,
            cwd,
            state_path: transcript_path,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Interactive,
            pid,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        })?;
        store.record_hook_event(&id, &event)?;
        match event.as_str() {
            "PreCompact" => record_pre_compact(store, &id, trigger, now)?,
            "PostCompact" => {
                let tokens_after = store.read_session(&id)?.last_known_token_count;
                store.complete_compaction_event(&id, tokens_after, now)?;
            }
            _ => {}
        }
        Ok(store.take_hook_messages(&id, &event)?)
    })
    .await?;
    if messages.is_empty()
        || !matches!(
            (output_provider, output_event.as_str()),
            (Provider::ClaudeCode, "Stop" | "SessionStart") | (Provider::Codex, "SessionStart")
        )
    {
        return Ok(Json(serde_json::json!({})));
    }
    Ok(Json(serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": output_event,
            "additionalContext": messages.join("\n")
        }
    })))
}

/// Opens a compaction_events row for a PreCompact hook. `source` is a heuristic:
/// a `CompactionRequest` we sent ourselves (Sending/Sent, created within the last
/// 5 minutes) means this compaction was triggered by us (Inline); otherwise the
/// harness/user triggered it on their own (External) — there's no direct FK
/// linking a `CompactionRequest` to the hook event, so timing is the best signal available.
fn record_pre_compact(
    store: &Store,
    id: &SessionId,
    trigger: Option<String>,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    let session = store.read_session(id)?;
    let inline = store
        .compaction_requests_for_session(id)?
        .into_iter()
        .any(|r| {
            matches!(r.status, CompactionStatus::Sending | CompactionStatus::Sent)
                && (now - r.created_at) <= chrono::Duration::minutes(5)
        });
    let tokens_before = session.last_known_token_count;
    let context_pct_before = match (tokens_before, session.context_window_size) {
        (Some(tokens), Some(window)) if window > 0 => Some(tokens as f32 / window as f32 * 100.0),
        _ => None,
    };
    // Best-effort snapshot: the highest pct across this session's provider/account
    // windows in the most recent sample, since a session can be gated by whichever
    // window is closest to its limit, not necessarily the first one in the map.
    let usage_window_pct_before = store
        .all_usage_samples()?
        .into_iter()
        .filter(|s| s.provider == session.harness && s.account == session.account)
        .max_by_key(|s| s.at)
        .and_then(|s| {
            s.windows
                .values()
                .map(|w| w.pct)
                .fold(None, |acc: Option<f32>, pct| {
                    Some(acc.map_or(pct, |acc| acc.max(pct)))
                })
        });
    store.insert_compaction_event(&CompactionEvent {
        id: uuid::Uuid::new_v4(),
        session_id: id.clone(),
        source: if inline {
            CompactionSource::Inline
        } else {
            CompactionSource::External
        },
        trigger,
        context_pct_before,
        usage_window_pct_before,
        tokens_before,
        tokens_after: None,
        started_at: now,
        completed_at: None,
    })?;
    Ok(())
}

fn supports_hook_event(provider: &Provider, event: &str) -> bool {
    match provider {
        Provider::ClaudeCode => matches!(
            event,
            "SessionStart"
                | "UserPromptSubmit"
                | "PreToolUse"
                | "PostToolUse"
                | "PermissionRequest"
                | "Stop"
                | "SubagentStart"
                | "SubagentStop"
                | "SessionEnd"
                | "Interrupt"
                | "PreCompact"
                | "PostCompact"
        ),
        Provider::Codex => matches!(
            event,
            "SessionStart"
                | "UserPromptSubmit"
                | "PreToolUse"
                | "PostToolUse"
                | "PermissionRequest"
                | "Stop"
                | "SubagentStart"
                | "SubagentStop"
                | "SessionEnd"
                | "Interrupt"
                | "PreCompact"
                | "PostCompact"
        ),
        _ => matches!(
            event,
            "SessionStart" | "UserPromptSubmit" | "PostToolUse" | "Stop" | "SessionEnd"
        ),
    }
}

async fn read<T, F>(state: AppState, operation: F) -> Result<T, (StatusCode, String)>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> anyhow::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let store = state
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("store lock poisoned"))?;
        operation(&store)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

fn session_id(id: String) -> SessionId {
    SessionId(id)
}

#[derive(Debug, Deserialize, Default)]
struct StatusQuery {
    provider: Option<String>,
    model: Option<String>,
    account: Option<String>,
}

async fn status(
    State(state): State<AppState>,
    Query(query): Query<StatusQuery>,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    let provider: Option<Provider> = query.provider.and_then(|p| p.parse().ok());
    let model = query.model.map(ModelId);
    let account = query.account.map(AccountId);
    let value = read(state, move |store| {
        let all_samples = store.all_usage_samples()?;
        let sessions = store.list_sessions()?;
        let mut latest: BTreeMap<String, UsageSample> = BTreeMap::new();
        for mut sample in all_samples.iter().cloned() {
            if provider.as_ref().is_some_and(|p| p != &sample.provider) || account.as_ref().is_some_and(|a| Some(a) != sample.account.as_ref()) { continue; }
            if let Some(model) = &model { sample.windows.retain(|key, _| matches!(&key.kind, WindowKind::WeeklyModel(candidate) if candidate == model)); }
            if sample.windows.is_empty() { continue; }
            let key = format!("{:?}:{:?}", sample.provider, sample.account);
            match latest.get_mut(&key) {
                Some(old) if old.at == sample.at => old.windows.extend(sample.windows),
                Some(old) if old.at < sample.at => { *old = sample; }
                None => { latest.insert(key, sample); }
                _ => {}
            }
        }
        let now = Utc::now();
        let activity_cutoff = now - Duration::minutes(30);
        let session_activity = sessions
            .iter()
            .map(|session| {
                let records = store.token_usage_for_session(&session.id)?;
                let active = session.stopped_reason.is_none()
                    && records.iter().any(|record| {
                        record.at >= activity_cutoff && record.total_tokens > 0
                    });
                let activity = records
                    .into_iter()
                    .filter(|record| record.total_tokens > 0)
                    .map(|record| record.at)
                    .collect::<Vec<_>>();
                Ok((session.id.clone(), active, activity))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let usage = latest.into_values().map(|sample| {
            let provider = sample.provider.clone();
            let account = sample.account.clone();
            let plan = sample.plan.clone();
            let windows = sample.windows.into_iter().map(|(key, window)| {
                let window_key = WindowKey { provider: provider.clone(), kind: key.kind.clone() };
                let blocks = uw_policy::segment_blocks(&all_samples, &window_key, account.as_ref());
                let burn_lookback = uw_policy::burn_rate_display_lookback_for_provider(&provider, &key.kind);
                let burn_rate_pct_per_hour = blocks.last().and_then(|block| uw_policy::burn_rate_pct_per_hour_available(block, burn_lookback));
                let purview = uw_policy::burn_rate_display_lookback_for_provider(&provider, &key.kind);
                let long_window = purview > Duration::hours(2);
                let active_sessions = sessions
                    .iter()
                    .filter(|s| s.harness == provider && s.account == account)
                    .filter(|s| {
                        session_activity.iter().any(|(id, active, activity)| {
                            id == &s.id
                                && if long_window {
                                    let cutoff = now - purview;
                                    activity.iter().any(|at| *at >= cutoff)
                                } else {
                                    *active
                                }
                        })
                    })
                    .count() as u32;
                let depletes_at = burn_rate_pct_per_hour.filter(|rate| *rate > 0.0).map(|rate| {
                    let minutes_remaining = (100.0 - window.pct) / rate * 60.0;
                    now + chrono::Duration::minutes(minutes_remaining.max(0.0) as i64)
                });
                UsageWindowSummary { window: key.kind, pct: window.pct, resets_at: window.resets_at, exceeded: window.exceeded, burn_rate_pct_per_hour, active_sessions, depletes_at }
            }).collect();
            ProviderUsageSummary { provider, account, plan, windows }
        }).collect();
        Ok(StatusResponse {
            usage,
            last_updated: now,
            provider_status: store.fetch_statuses()?,
            keepalive_active_count: store.keepalive_active_count(now)?,
        })
    }).await?;
    Ok(Json(value))
}

const DEFAULT_SESSIONS_LIMIT: u32 = 15;
const MAX_SESSIONS_LIMIT: u32 = 200;

#[derive(Debug, Deserialize, Default)]
struct SessionsQuery {
    inactive: Option<bool>,
    malformed: Option<bool>,
    stopped: Option<bool>,
    harness: Option<String>,
    q: Option<String>,
    offset: Option<u32>,
    limit: Option<u32>,
}

async fn sessions(
    State(state): State<AppState>,
    Query(query): Query<SessionsQuery>,
) -> Result<Json<SessionsPage>, (StatusCode, String)> {
    let harness: Option<Provider> = query.harness.and_then(|value| value.parse().ok());
    let include_inactive = query.inactive.or(query.stopped).unwrap_or(false);
    let include_malformed = query.malformed.unwrap_or(false);
    let offset = query.offset.unwrap_or(0);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_SESSIONS_LIMIT)
        .min(MAX_SESSIONS_LIMIT);
    Ok(Json(
        read(state, move |store| {
            let mut matching = Vec::new();
            let now = Utc::now();
            for s in store.search_sessions(query.q.as_deref())? {
                if !include_malformed && store.session_last_seen_missing(&s.id)? {
                    continue;
                }
                let active = s.stopped_reason.is_none()
                    && store
                        .token_usage_for_session(&s.id)?
                        .into_iter()
                        .any(|record| {
                            record.at >= now - Duration::minutes(30) && record.total_tokens > 0
                        });
                if !harness.as_ref().is_none_or(|h| h == &s.harness)
                    || (!include_inactive && !active)
                {
                    continue;
                }
                matching.push((s, active));
            }
            let total = matching.len() as u32;
            // Filtering (harness/stopped/q) happens above in Rust rather than SQL, so
            // pagination is a plain slice here too — this DB is small/local and doing
            // it in SQL would require duplicating those filters in the query.
            let mut items = Vec::new();
            for (s, active) in matching
                .into_iter()
                .skip(offset as usize)
                .take(limit as usize)
            {
                let resume_status = store
                    .resume_markers_for_session(&s.id)?
                    .last()
                    .map(|m| m.status.clone());
                let compaction_status = store
                    .compaction_requests_for_session(&s.id)?
                    .last()
                    .map(|r| r.status.clone());
                let token_records = store.token_usage_for_session(&s.id)?;
                let keepalive = store
                    .keepalive_config(&s.id)
                    .map(|k| k.enabled)
                    .unwrap_or(false)
                    && uw_core::model::is_keepalive_eligible(&s, &token_records, now);
                let cached = store.session_cache_warm(&s.id, now)?;
                let title = store.resolve_session_title(&s.id)?;
                items.push(SessionListItem {
                    id: s.id,
                    title,
                    harness: s.harness,
                    model: s.model,
                    account: s.account,
                    last_seen: s.last_seen,
                    stopped_reason: s.stopped_reason,
                    resume_status,
                    compaction_status,
                    keepalive,
                    active,
                    cached,
                });
            }
            Ok(SessionsPage {
                items,
                total,
                offset,
                limit,
            })
        })
        .await?,
    ))
}

fn recent_history(mut history: Vec<SparklinePoint>, now: DateTime<Utc>) -> Vec<SparklinePoint> {
    let cutoff = now - Duration::hours(24);
    history.retain(|point| point.at >= cutoff);
    history.sort_by_key(|point| point.at);
    if history.len() > 20 {
        history = history.split_off(history.len() - 20);
    }
    history
}

fn detail(store: &Store, id: &SessionId) -> anyhow::Result<SessionDetail> {
    let summary = store.read_session(id)?;
    let title = store.resolve_session_title(id)?;
    let markers = store.resume_markers_for_session(id)?;
    let marker = markers.last().cloned();
    let history = store
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
    let history = recent_history(history, Utc::now());
    let compaction_log = store.compaction_requests_for_session(id)?;
    let mut errors = compaction_log
        .iter()
        .filter_map(|request| match &request.status {
            CompactionStatus::Failed(reason) if request.reason.contains(HARD_BOUNDARY_REASON) => {
                Some(format!("Blocking compaction failed: {reason}"))
            }
            CompactionStatus::Failed(reason) => Some(format!("Compaction failed: {reason}")),
            _ => None,
        })
        .collect::<Vec<_>>();
    errors.extend(markers.iter().filter_map(|marker| match &marker.status {
        ResumeStatus::Failed(reason) => Some(format!("Resume failed: {reason}")),
        _ => None,
    }));
    Ok(SessionDetail {
        summary,
        title,
        history,
        resume_controls: ResumeControls {
            can_resume: marker.is_some(),
            marker,
        },
        compaction_log,
        reseed_lineage: vec![],
        errors,
    })
}

async fn session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionDetail>, (StatusCode, String)> {
    let id = session_id(id);
    Ok(Json(read(state, move |store| detail(store, &id)).await?))
}

async fn create_session(
    State(state): State<AppState>,
    Json(request): Json<CreateSessionRequest>,
) -> Result<Json<SessionDetail>, (StatusCode, String)> {
    let id = request.id.clone();
    read(state.clone(), move |store| {
        let now = Utc::now();
        Ok(store.insert_session(&SessionSummary {
            id: request.id,
            harness: request.harness,
            model: request.model,
            account: None,
            first_seen: now,
            last_seen: now,
            cwd: request.cwd,
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Interactive,
            pid: None,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        })?)
    })
    .await
    .map_err(|(status, message)| {
        if message.contains("UNIQUE constraint") {
            (StatusCode::CONFLICT, "session already exists".into())
        } else {
            (status, message)
        }
    })?;
    Ok(Json(read(state, move |store| detail(store, &id)).await?))
}

async fn update_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateSessionRequest>,
) -> Result<Json<SessionDetail>, (StatusCode, String)> {
    let id = session_id(id);
    read(state.clone(), {
        let id = id.clone();
        move |store| {
            Ok(store.update_session_editable(
                &id,
                &request.cwd,
                request.model.as_ref(),
                request.account.as_ref(),
            )?)
        }
    })
    .await?;
    Ok(Json(read(state, move |store| detail(store, &id)).await?))
}

async fn delete_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = session_id(id);
    read(state, move |store| Ok(store.delete_session(&id)?)).await?;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn session_preview(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<uw_core::adapter::TurnPreview>>, (StatusCode, String)> {
    let id = session_id(id);
    let session = read(state.clone(), move |store| Ok(store.read_session(&id)?)).await?;
    let Some(adapter) = state.adapters.get(&session.harness) else {
        return Err((
            StatusCode::NOT_FOUND,
            "no live adapter registered for this harness".into(),
        ));
    };
    match adapter.session_preview(&session).await {
        Ok(turns) => Ok(Json(turns)),
        Err(uw_core::adapter::AdapterError::Unsupported) => Err((
            StatusCode::NOT_FOUND,
            "no transcript preview available for this session".into(),
        )),
        Err(error) => Err((StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
    }
}

async fn resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<ResumeRequest>,
) -> Result<Json<ResumeResponse>, (StatusCode, String)> {
    let id = session_id(id);
    Ok(Json(
        read(state, move |store| {
            let marker = ResumeMarker {
                id: uuid::Uuid::new_v4(),
                session_id: id.clone(),
                reason: ResumeReason::ManuallyMarked,
                // Resolved by the daemon's policy tick against the session's
                // window/burn-rate floor — never fired on `request.at` alone,
                // since that would let a manual resume jump ahead of the
                // window boundary. `request.at` is kept as a floor override
                // in `requested_at` and can only push the fire time later.
                resume_at: None,
                requested_at: request.at,
                created_at: Utc::now(),
                status: ResumeStatus::Pending,
                message: request.message.clone(),
            };
            store.insert_resume_marker(&marker)?;
            Ok(ResumeResponse {
                marker: Some(marker),
            })
        })
        .await?,
    ))
}

async fn cancel_resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = session_id(id);
    read(state, move |store| {
        store.cancel_resume_markers(&id)?;
        Ok(())
    })
    .await?;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn cancel_compact(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = session_id(id);
    read(state, move |store| {
        store.cancel_compaction_requests(&id)?;
        Ok(())
    })
    .await?;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn compact_ask(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<CompactAskRequest>,
) -> Result<Json<CompactStatusResponse>, (StatusCode, String)> {
    let id = session_id(id);
    let harness = read(state.clone(), {
        let id = id.clone();
        move |store| Ok(store.read_session(&id)?.harness)
    })
    .await?;
    let supported = state
        .adapters
        .get(&harness)
        .is_some_and(|adapter| adapter.capabilities().can_trigger_compaction);
    if !supported {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("compaction cannot be triggered for harness {harness:?}"),
        ));
    }
    Ok(Json(
        read(state, move |store| {
            let request = CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: id.clone(),
                kind: CompactionKind::AgentRequested,
                prompt: request.reason.clone().unwrap_or_default(),
                reason: request.reason.unwrap_or_else(|| "manual request".into()),
                status: CompactionStatus::Pending,
                created_at: Utc::now(),
            };
            store.insert_compaction_request(&request)?;
            Ok(CompactStatusResponse {
                requests: store.compaction_requests_for_session(&id)?,
            })
        })
        .await?,
    ))
}

async fn compact_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CompactStatusResponse>, (StatusCode, String)> {
    let id = session_id(id);
    Ok(Json(
        read(state, move |store| {
            Ok(CompactStatusResponse {
                requests: store.compaction_requests_for_session(&id)?,
            })
        })
        .await?,
    ))
}

#[derive(Debug, Deserialize)]
struct KeepaliveRequest {
    enabled: bool,
}
async fn keepalive(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<KeepaliveRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = session_id(id);
    if request.enabled {
        let harness = read(state.clone(), {
            let id = id.clone();
            move |store| Ok(store.read_session(&id)?.harness)
        })
        .await?;
        let supported = state
            .adapters
            .get(&harness)
            .is_some_and(|adapter| adapter.capabilities().can_advise_mid_turn);
        if !supported {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "keepalive is not supported for harness {harness:?}: it cannot advise mid-turn"
                ),
            ));
        }
    }
    read(state, move |store| {
        store.set_keepalive(&id, request.enabled)?;
        Ok(())
    })
    .await?;
    Ok(Json(
        serde_json::json!({"ok": true, "enabled": request.enabled}),
    ))
}

async fn supersede_stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<SupersedeStopRequest>,
) -> Result<Json<SessionDetail>, (StatusCode, String)> {
    let id = session_id(id);
    read(state.clone(), {
        let id = id.clone();
        move |store| {
            Ok(store.supersede_stop_reason(&id, request.note.as_deref(), Utc::now())?)
        }
    })
    .await?;
    Ok(Json(read(state, move |store| detail(store, &id)).await?))
}

async fn set_provider_non_blocking(
    State(state): State<AppState>,
    Json(request): Json<SetFetchNonBlockingRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    read(state, move |store| {
        Ok(store.set_fetch_non_blocking(
            &request.provider,
            request.account.as_ref(),
            request.non_blocking,
        )?)
    })
    .await?;
    Ok(Json(serde_json::json!({"ok": true})))
}

#[derive(Debug, Deserialize)]
struct ScopeQuery {
    scope: Option<String>,
}

fn parse_scope(value: Option<&str>) -> Option<ThresholdScope> {
    let mut parts = value?.split(':');
    let provider = parts.next()?.parse().ok()?;
    Some(ThresholdScope {
        provider,
        model: parts
            .next()
            .filter(|v| !v.is_empty())
            .map(|v| ModelId(v.into())),
        session: parts
            .next()
            .filter(|v| !v.is_empty())
            .map(|v| SessionId(v.into())),
    })
}

async fn thresholds_get(
    State(state): State<AppState>,
    Query(query): Query<ScopeQuery>,
) -> Result<Json<ThresholdResponse>, (StatusCode, String)> {
    let scope = parse_scope(query.scope.as_deref());
    let requested = scope.clone();
    Ok(Json(
        read(state, move |store| {
            Ok(ThresholdResponse {
                scope: requested,
                values: store.threshold_values(scope.as_ref())?,
            })
        })
        .await?,
    ))
}

async fn thresholds_set(
    State(state): State<AppState>,
    Json(request): Json<ThresholdSetRequest>,
) -> Result<Json<ThresholdResponse>, (StatusCode, String)> {
    let response_scope = request.scope.clone();
    Ok(Json(
        read(state, move |store| {
            let kind = if request.scope.session.is_some() {
                ThresholdScopeKind::Session
            } else if request.scope.model.is_some() {
                ThresholdScopeKind::Model
            } else {
                ThresholdScopeKind::Provider
            };
            store.set_threshold_override(&ThresholdOverride {
                id: uuid::Uuid::new_v4(),
                scope_kind: kind,
                provider: request.scope.provider.clone(),
                model_value: request.scope.model.clone(),
                session_value: request.scope.session.clone(),
                field: request.field,
                value_json: request.value,
                updated_at: Utc::now(),
            })?;
            Ok(ThresholdResponse {
                scope: Some(response_scope.clone()),
                values: store.threshold_values(Some(&response_scope))?,
            })
        })
        .await?,
    ))
}

const DEFAULT_COMPACTIONS_LIMIT: u32 = 20;
const MAX_COMPACTIONS_LIMIT: u32 = 200;

#[derive(Debug, Deserialize, Default)]
struct CompactionsQuery {
    limit: Option<u32>,
}

async fn compactions_recent(
    State(state): State<AppState>,
    Query(query): Query<CompactionsQuery>,
) -> Result<Json<Vec<CompactionEvent>>, (StatusCode, String)> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_COMPACTIONS_LIMIT)
        .min(MAX_COMPACTIONS_LIMIT);
    Ok(Json(
        read(state, move |store| {
            Ok(store.recent_compaction_events(limit)?)
        })
        .await?,
    ))
}

fn compact_action_status(status: &CompactionStatus) -> (String, Option<String>) {
    match status {
        CompactionStatus::Pending => ("pending".into(), None),
        CompactionStatus::Sending => ("sending".into(), None),
        CompactionStatus::Sent => ("in progress".into(), None),
        CompactionStatus::Failed(reason) => ("blocked".into(), Some(reason.clone())),
        CompactionStatus::Cancelled => ("cancelled".into(), None),
    }
}

fn resume_status(status: &ResumeStatus) -> (String, Option<String>) {
    match status {
        ResumeStatus::Pending => ("queued".into(), None),
        ResumeStatus::Scheduled => ("scheduled".into(), None),
        ResumeStatus::Fired => ("resumed".into(), None),
        ResumeStatus::Cancelled => ("cancelled".into(), None),
        ResumeStatus::Failed(reason) => ("failed".into(), Some(reason.clone())),
    }
}

async fn actions_recent(
    State(state): State<AppState>,
    Query(query): Query<CompactionsQuery>,
) -> Result<Json<Vec<RecentAction>>, (StatusCode, String)> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_COMPACTIONS_LIMIT)
        .min(MAX_COMPACTIONS_LIMIT);
    Ok(Json(
        read(state, move |store| {
            let cutoff = Utc::now() - Duration::hours(24);
            let mut actions = Vec::new();
            for session in store.list_sessions()? {
                for request in store.compaction_requests_for_session(&session.id)? {
                    let (status, error) = compact_action_status(&request.status);
                    if request.created_at < cutoff {
                        continue;
                    }
                    actions.push(RecentAction {
                        session_id: session.id.clone(),
                        kind: "compaction".into(),
                        status,
                        at: request.created_at,
                        detail: Some(request.reason),
                        error,
                    });
                }
                for event in store.compaction_events_for_session(&session.id)? {
                    if event.started_at < cutoff {
                        continue;
                    }
                    actions.push(RecentAction {
                        session_id: session.id.clone(),
                        kind: "compaction".into(),
                        status: if event.completed_at.is_some() {
                            "successful".into()
                        } else {
                            "in progress".into()
                        },
                        at: event.started_at,
                        detail: event.trigger,
                        error: None,
                    });
                }
                for marker in store.resume_markers_for_session(&session.id)? {
                    let (status, error) = resume_status(&marker.status);
                    if marker.created_at < cutoff {
                        continue;
                    }
                    actions.push(RecentAction {
                        session_id: session.id.clone(),
                        kind: "resume".into(),
                        status,
                        at: marker.created_at,
                        detail: marker.message,
                        error,
                    });
                }
                if let Ok(keepalive) = store.keepalive_config(&session.id)
                    && keepalive.enabled
                {
                    if keepalive.last_ping_at.unwrap_or(session.last_seen) < cutoff {
                        continue;
                    }
                    actions.push(RecentAction {
                        session_id: session.id.clone(),
                        kind: "keepalive".into(),
                        status: "active".into(),
                        at: keepalive.last_ping_at.unwrap_or(session.last_seen),
                        detail: None,
                        error: None,
                    });
                }
            }
            actions.sort_by(|a, b| {
                b.at.cmp(&a.at)
                    .then_with(|| a.session_id.0.cmp(&b.session_id.0))
            });
            actions.truncate(limit as usize);
            Ok(actions)
        })
        .await?,
    ))
}

async fn static_asset() -> Response<Body> {
    let Some(asset) = Assets::get("index.html") else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    let html =
        String::from_utf8_lossy(&asset.data).replace("__UW_VERSION__", env!("CARGO_PKG_VERSION"));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(html))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::{PasswordHasher, SaltString};
    use axum::http::Request;
    use chrono::Utc;
    use std::collections::HashMap;
    use tower::ServiceExt;
    use uw_core::adapter::TokenUsageRecord;

    #[test]
    fn recent_history_keeps_only_last_twenty_points_from_past_24_hours() {
        let now = Utc::now();
        let history = (0..25)
            .map(|hours_ago| SparklinePoint {
                at: now - Duration::hours(hours_ago),
                pct: hours_ago as f32,
            })
            .collect::<Vec<_>>();

        let recent = recent_history(history, now);

        assert_eq!(recent.len(), 20);
        assert!(
            recent
                .iter()
                .all(|point| point.at >= now - Duration::hours(24))
        );
        assert!(recent.iter().all(|point| point.pct <= 23.0));
    }

    struct StubAdapter(uw_core::adapter::Capabilities);
    #[async_trait::async_trait]
    impl HarnessAdapter for StubAdapter {
        fn provider(&self) -> Provider {
            Provider::Codex
        }
        fn capabilities(&self) -> uw_core::adapter::Capabilities {
            self.0.clone()
        }
        async fn fetch_usage(
            &self,
            _: Option<&AccountId>,
        ) -> uw_core::adapter::AdapterResult<UsageSample> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn detect_stop(
            &self,
            _: &SessionId,
        ) -> uw_core::adapter::AdapterResult<Option<StopReason>> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn emit_status(
            &self,
            _: &SessionId,
            _: uw_core::adapter::StatusEvent,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn advise(
            &self,
            _: &SessionId,
            _: &str,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Ok(uw_core::adapter::DeliveryOutcome::Delivered)
        }
        async fn compact(
            &self,
            _: &SessionSummary,
            _: &CompactionRequest,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Ok(uw_core::adapter::DeliveryOutcome::Delivered)
        }
        async fn resume_session(
            &self,
            _: &SessionSummary,
            _: Option<&str>,
        ) -> uw_core::adapter::AdapterResult<()> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
    }

    fn session() -> SessionSummary {
        SessionSummary {
            id: SessionId("session-1".into()),
            harness: Provider::Codex,
            model: Some(ModelId("gpt".into())),
            account: None,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            cwd: "/tmp".into(),
            state_path: None,
            context_window_size: None,
            last_known_token_count: None,
            launch_mode: LaunchMode::Headless,
            pid: None,
            stopped_reason: None,
            superseded_stop_reason: None,
            superseded_stop_reason_at: None,
            superseded_stop_reason_note: None,
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        }
    }

    #[tokio::test]
    async fn status_and_sessions_read_seeded_store() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        store
            .insert_compaction_request(&CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: SessionId("session-1".into()),
                kind: CompactionKind::AgentRequested,
                prompt: "/compact".into(),
                reason: "test".into(),
                status: CompactionStatus::Failed("adapter error".into()),
                created_at: Utc::now(),
            })
            .unwrap();
        store
            .insert_usage_sample(&UsageSample {
                at: Utc::now(),
                fetched_at: None,
                source: UsageSource::ProviderReported,
                provider: Provider::Codex,
                account: None,
                plan: None,
                windows: HashMap::from([(
                    WindowKey {
                        provider: Provider::Codex,
                        kind: WindowKind::Rolling { minutes: 300 },
                    },
                    UsageWindowState::new(42.0, false, true, None, None),
                )]),
                credits: None,
            })
            .unwrap();
        let router = app(store);
        let response = router
            .clone()
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: StatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.usage[0].windows[0].pct, 42.0);
        let response = router
            .oneshot(
                Request::get("/api/sessions?stopped=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value.items[0].compaction_status,
            Some(CompactionStatus::Failed("adapter error".into()))
        );
    }

    #[tokio::test]
    async fn shared_store_router_reads_changes_from_owner_connection() {
        let shared = Arc::new(Mutex::new(Store::open_memory().unwrap()));
        let router = app_with_shared_store(shared.clone(), HashMap::new());
        shared.lock().unwrap().insert_session(&session()).unwrap();

        let response = router
            .oneshot(
                Request::get("/api/sessions/session-1")
                    .header("accept", "application/json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("session-1"));
    }

    #[tokio::test]
    async fn configured_auth_protects_api_until_password_login() {
        let hash = Argon2::default()
            .hash_password(
                b"correct horse battery staple",
                &SaltString::encode_b64(b"0123456789abcdef").unwrap(),
            )
            .unwrap()
            .to_string();
        let router =
            app_with_adapters_and_auth(Store::open_memory().unwrap(), HashMap::new(), Some(hash))
                .unwrap();

        let response = router
            .clone()
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = router
            .clone()
            .oneshot(
                Request::get("/api/internal/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .clone()
            .oneshot(
                Request::post("/api/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"password":"correct horse battery staple"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cookie = response.headers().get(header::SET_COOKIE).unwrap().clone();

        let response = router
            .oneshot(
                Request::get("/api/status")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn status_counts_sessions_within_the_burn_window() {
        let store = Store::open_memory().unwrap();
        let mut active = session();
        let mut idle = session();
        idle.id = SessionId("session-2".into());
        let mut outside = session();
        outside.id = SessionId("session-3".into());
        active.harness = Provider::Cursor;
        idle.harness = Provider::Cursor;
        outside.harness = Provider::Cursor;
        store.insert_session(&active).unwrap();
        store.insert_session(&idle).unwrap();
        store.insert_session(&outside).unwrap();
        store
            .insert_token_usage_records(
                &active.id,
                &[TokenUsageRecord {
                    at: Utc::now(),
                    model: active.model.clone(),
                    input_tokens: 100,
                    cached_input_tokens: 0,
                    cache_write_input_tokens: 0,
                    output_tokens: 20,
                    reasoning_output_tokens: 0,
                    total_tokens: 120,
                }],
            )
            .unwrap();
        store
            .insert_token_usage_records(
                &idle.id,
                &[TokenUsageRecord {
                    at: Utc::now() - Duration::hours(1),
                    model: idle.model.clone(),
                    input_tokens: 80,
                    cached_input_tokens: 0,
                    cache_write_input_tokens: 0,
                    output_tokens: 20,
                    reasoning_output_tokens: 0,
                    total_tokens: 100,
                }],
            )
            .unwrap();
        store
            .insert_token_usage_records(
                &outside.id,
                &[TokenUsageRecord {
                    at: Utc::now() - Duration::hours(30),
                    model: outside.model.clone(),
                    input_tokens: 80,
                    cached_input_tokens: 0,
                    cache_write_input_tokens: 0,
                    output_tokens: 20,
                    reasoning_output_tokens: 0,
                    total_tokens: 100,
                }],
            )
            .unwrap();
        store
            .insert_usage_sample(&UsageSample {
                at: Utc::now(),
                fetched_at: None,
                source: UsageSource::ProviderReported,
                provider: Provider::Cursor,
                account: None,
                plan: None,
                windows: HashMap::from([(
                    WindowKey {
                        provider: Provider::Cursor,
                        kind: WindowKind::Rolling { minutes: 300 },
                    },
                    UsageWindowState::new(42.0, false, true, None, None),
                )]),
                credits: None,
            })
            .unwrap();

        let response = app(store)
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: StatusResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(value.usage[0].windows[0].active_sessions, 2);
    }

    #[tokio::test]
    async fn status_merges_windows_from_rows_at_the_same_snapshot() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let at = Utc::now();
        for minutes in [300, 10080] {
            store
                .insert_usage_sample(&UsageSample {
                    at,
                    fetched_at: None,
                    source: UsageSource::ProviderReported,
                    provider: Provider::Codex,
                    account: None,
                    plan: None,
                    windows: HashMap::from([(
                        WindowKey {
                            provider: Provider::Codex,
                            kind: WindowKind::Rolling { minutes },
                        },
                        UsageWindowState::new(minutes as f32 / 100.0, false, true, None, None),
                    )]),
                    credits: None,
                })
                .unwrap();
        }
        let response = app(store)
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: StatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.usage.len(), 1);
        assert_eq!(value.usage[0].windows.len(), 2);
    }

    #[tokio::test]
    async fn root_serves_embedded_index() {
        let response = app(Store::open_memory().unwrap())
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(
            html.contains(&format!("Usagewindow v{}", env!("CARGO_PKG_VERSION"))),
            "expected versioned title in html"
        );
        assert!(!html.contains("__UW_VERSION__"));
    }

    #[tokio::test]
    async fn recent_actions_header_describes_the_24_hour_window() {
        let response = app(Store::open_memory().unwrap())
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);

        assert!(html.contains("latest 20 over 24h"));
        assert!(!html.contains(">latest 20</span>"));
    }

    #[tokio::test]
    async fn hook_ingress_routes_supported_session_events_and_rejects_unknown_events() {
        let router = app(Store::open_memory().unwrap());
        let start = serde_json::json!({
            "hook_event_name": "SessionStart",
            "session_id": "hook-session",
            "cwd": "/work",
            "source": "startup"
        });
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=codex")
                    .header("content-type", "application/json")
                    .body(Body::from(start.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .clone()
            .oneshot(
                Request::get("/api/sessions/hook-session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .oneshot(
                Request::post("/api/hooks?provider=codex")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"hook_event_name":"MadeUp"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn claude_hook_ingress_rejects_transcript_identity_mismatch() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let other = "7fd75ec4-661a-4d52-8308-b65c60f44b85";
        let response = app(Store::open_memory().unwrap())
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "SessionStart",
                            "session_id": id,
                            "source": "compact",
                            "transcript_path": format!("/tmp/{other}.jsonl")
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn claude_hook_ingress_records_verified_transcript_path() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let path = format!("/tmp/{id}.jsonl");
        let router = app(Store::open_memory().unwrap());
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "SessionStart",
                            "session_id": id,
                            "source": "compact",
                            "transcript_path": path
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .oneshot(
                Request::get(format!("/api/sessions/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail: SessionDetail = serde_json::from_slice(&body).unwrap();
        assert_eq!(detail.summary.state_path.as_deref(), Some(path.as_str()));
    }

    #[tokio::test]
    async fn claude_hook_ingress_rejects_missing_transcript_identity() {
        let response = app(Store::open_memory().unwrap())
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "SessionStart",
                            "session_id": "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn precompact_postcompact_hooks_record_inline_and_external_compaction_events() {
        let id = "3507fe61-2d6b-4aae-a0b6-4fe4eec12b40";
        let path = format!("/tmp/{id}.jsonl");
        let store = Store::open_memory().unwrap();
        store
            .insert_session(&{
                let mut s = session();
                s.id = SessionId(id.into());
                s.harness = Provider::ClaudeCode;
                s
            })
            .unwrap();
        // A CompactionRequest we issued ourselves and have already claimed for
        // delivery (Sending), so the upcoming PreCompact should be classified
        // as Inline.
        let request = CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: SessionId(id.into()),
            kind: CompactionKind::AskNearLimit,
            prompt: "compact".into(),
            reason: "near limit".into(),
            status: CompactionStatus::Pending,
            created_at: Utc::now(),
        };
        store.insert_compaction_request(&request).unwrap();
        assert!(store.claim_compaction(request.id).unwrap());
        let router = app(store);

        let precompact = router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "PreCompact",
                            "session_id": id,
                            "source": "compact",
                            "transcript_path": path,
                            "trigger": "manual"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(precompact.status(), StatusCode::OK);

        router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "PostCompact",
                            "session_id": id,
                            "source": "compact",
                            "transcript_path": path
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // A second session with no CompactionRequest history — its PreCompact
        // must be classified as External.
        let other_id = "7fd75ec4-661a-4d52-8308-b65c60f44b85";
        let other_path = format!("/tmp/{other_id}.jsonl");
        router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "SessionStart",
                            "session_id": other_id,
                            "source": "startup",
                            "transcript_path": other_path
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        router
            .clone()
            .oneshot(
                Request::post("/api/hooks?provider=claude-code")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "hook_event_name": "PreCompact",
                            "session_id": other_id,
                            "source": "compact",
                            "transcript_path": other_path,
                            "trigger": "auto"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = router
            .oneshot(
                Request::get("/api/compactions/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let events: Vec<CompactionEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(events.len(), 2);
        let inline = events
            .iter()
            .find(|e| e.session_id == SessionId(id.into()))
            .unwrap();
        assert_eq!(inline.source, CompactionSource::Inline);
        assert_eq!(inline.trigger.as_deref(), Some("manual"));
        assert!(inline.completed_at.is_some());
        let external = events
            .iter()
            .find(|e| e.session_id == SessionId(other_id.into()))
            .unwrap();
        assert_eq!(external.source, CompactionSource::External);
        assert_eq!(external.trigger.as_deref(), Some("auto"));
        assert!(external.completed_at.is_none());
    }

    #[tokio::test]
    async fn resume_compaction_and_threshold_routes_write_store() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([(
            Provider::Codex,
            Arc::new(StubAdapter(uw_core::adapter::Capabilities {
                can_trigger_compaction: true,
                can_advise_mid_turn: false,
                can_inject_at_session_start: true,
                can_observe_compaction: true,
                reports_token_counts: true,
                headless_resume: true,
                seed_modes: vec![],
            })) as Arc<dyn HarnessAdapter>,
        )]);
        let router = app_with_adapters(store, adapters);
        let resume = ResumeRequest {
            session_id: SessionId("session-1".into()),
            at: None,
            message: None,
        };
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/sessions/session-1/resume")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&resume).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ask = CompactAskRequest {
            session_id: SessionId("session-1".into()),
            reason: Some("test".into()),
        };
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/sessions/session-1/compact/ask")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&ask).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&response_body)
        );
        let compact_status: CompactStatusResponse = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(compact_status.requests.last().unwrap().prompt, "test");
        let threshold = ThresholdSetRequest {
            scope: ThresholdScope {
                provider: Provider::Codex,
                model: None,
                session: None,
            },
            field: "closing_pct".into(),
            value: "85".into(),
        };
        let response = router
            .oneshot(
                Request::post("/api/thresholds")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&threshold).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: ThresholdResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.values.get("closing_pct"), Some(&"85".to_string()));
    }

    #[tokio::test]
    async fn compact_cancel_marks_pending_requests_cancelled() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        store
            .insert_compaction_request(&CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: SessionId("session-1".into()),
                kind: CompactionKind::AgentRequested,
                prompt: "compact".into(),
                reason: "manual".into(),
                status: CompactionStatus::Pending,
                created_at: Utc::now(),
            })
            .unwrap();
        let router = app(store);

        let response = router
            .clone()
            .oneshot(
                Request::post("/api/sessions/session-1/compact/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .oneshot(
                Request::get("/api/sessions/session-1/compact/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: CompactStatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.requests[0].status, CompactionStatus::Cancelled);
    }

    fn unsupported_caps() -> uw_core::adapter::Capabilities {
        uw_core::adapter::Capabilities {
            can_trigger_compaction: false,
            can_advise_mid_turn: false,
            can_inject_at_session_start: false,
            can_observe_compaction: false,
            reports_token_counts: false,
            headless_resume: false,
            seed_modes: vec![],
        }
    }

    #[tokio::test]
    async fn compact_ask_rejects_a_harness_that_cannot_trigger_compaction() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([(
            Provider::Codex,
            Arc::new(StubAdapter(unsupported_caps())) as Arc<dyn HarnessAdapter>,
        )]);
        let response = app_with_adapters(store, adapters)
            .oneshot(
                Request::post("/api/sessions/session-1/compact/ask")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&CompactAskRequest {
                            session_id: SessionId("session-1".into()),
                            reason: None,
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn keepalive_rejects_a_harness_that_cannot_advise_mid_turn() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([(
            Provider::Codex,
            Arc::new(StubAdapter(unsupported_caps())) as Arc<dyn HarnessAdapter>,
        )]);
        let response = app_with_adapters(store, adapters)
            .oneshot(
                Request::post("/api/sessions/session-1/keepalive")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"enabled": true})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn keepalive_accepts_a_harness_that_can_advise_mid_turn() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([(
            Provider::Codex,
            Arc::new(StubAdapter(uw_core::adapter::Capabilities {
                can_advise_mid_turn: true,
                ..unsupported_caps()
            })) as Arc<dyn HarnessAdapter>,
        )]);
        let response = app_with_adapters(store, adapters)
            .oneshot(
                Request::post("/api/sessions/session-1/keepalive")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"enabled": true})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn keepalive_disable_does_not_require_capability() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        // No adapters registered at all: disabling keepalive must still succeed since
        // it never needs the mid-turn advise capability.
        let response = app(store)
            .oneshot(
                Request::post("/api/sessions/session-1/keepalive")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"enabled": false})).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn supersede_stop_clears_the_reason_and_records_it_for_audit() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        store
            .update_session_stop(&SessionId("session-1".into()), Some(&StopReason::Crashed))
            .unwrap();
        let response = app(store)
            .oneshot(
                Request::post("/api/sessions/session-1/supersede-stop")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({"note": "stale detection"}))
                            .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: SessionDetail = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body.summary.stopped_reason, None);
        assert_eq!(
            body.summary.superseded_stop_reason,
            Some(StopReason::Crashed)
        );
        assert_eq!(
            body.summary.superseded_stop_reason_note.as_deref(),
            Some("stale detection")
        );
    }

    #[tokio::test]
    async fn set_provider_non_blocking_persists_the_flag() {
        let store = Store::open_memory().unwrap();
        store
            .record_fetch_failure(
                &Provider::Other("t3code".into()),
                None,
                Utc::now(),
                "unsupported",
            )
            .unwrap();
        let response = app(store)
            .oneshot(
                Request::post("/api/provider-status/non-blocking")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&serde_json::json!({
                            "provider": {"Other": "t3code"},
                            "account": null,
                            "non_blocking": true
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sessions_search_filters_by_q() {
        let store = Store::open_memory().unwrap();
        let mut alpha = session();
        alpha.id = SessionId("alpha".into());
        alpha.cwd = "/repos/usagewindow".into();
        store.insert_session(&alpha).unwrap();
        let mut beta = session();
        beta.id = SessionId("beta".into());
        beta.cwd = "/repos/other".into();
        store.insert_session(&beta).unwrap();

        let response = app(store)
            .oneshot(
                Request::get("/api/sessions?q=usagewindow&inactive=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.total, 1);
        assert_eq!(page.items[0].id, SessionId("alpha".into()));
    }

    #[tokio::test]
    async fn sessions_default_to_active_and_report_cache_state() {
        let store = Store::open_memory().unwrap();
        let active = session();
        let mut inactive = session();
        inactive.id = SessionId("inactive".into());
        inactive.last_seen = Utc::now() - chrono::Duration::minutes(31);
        store.insert_session(&active).unwrap();
        store.insert_session(&inactive).unwrap();
        store
            .insert_token_usage_records(
                &active.id,
                &[TokenUsageRecord {
                    at: Utc::now(),
                    model: active.model.clone(),
                    input_tokens: 10,
                    cached_input_tokens: 5,
                    cache_write_input_tokens: 0,
                    output_tokens: 1,
                    reasoning_output_tokens: 0,
                    total_tokens: 11,
                }],
            )
            .unwrap();

        let router = app(store);
        let response = router
            .clone()
            .oneshot(Request::get("/api/sessions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, active.id);
        assert!(page.items[0].active);
        assert!(page.items[0].cached);

        let response = router
            .oneshot(
                Request::get("/api/sessions?inactive=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.total, 2);
    }

    #[tokio::test]
    async fn malformed_sessions_require_explicit_opt_in() {
        let store = Store::open_memory().unwrap();
        let normal = session();
        let mut malformed = session();
        malformed.id = SessionId("malformed".into());
        store.insert_session(&normal).unwrap();
        store.insert_session(&malformed).unwrap();
        store
            .set_session_last_seen_missing(&malformed.id, true)
            .unwrap();

        let router = app(store);
        let response = router
            .clone()
            .oneshot(
                Request::get("/api/sessions?inactive=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.items[0].id, normal.id);

        let response = router
            .oneshot(
                Request::get("/api/sessions?inactive=true&malformed=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.total, 2);
        assert!(page.items.iter().any(|item| item.id == malformed.id));
    }

    #[tokio::test]
    async fn sessions_are_paginated_with_a_sane_default_and_cap() {
        let store = Store::open_memory().unwrap();
        for i in 0..3 {
            let mut s = session();
            s.id = SessionId(format!("session-{i}"));
            store.insert_session(&s).unwrap();
        }
        let router = app(store);
        let response = router
            .clone()
            .oneshot(
                Request::get("/api/sessions?limit=2&inactive=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.total, 3);
        assert_eq!(page.limit, 2);
        assert_eq!(page.offset, 0);

        let response = router
            .oneshot(
                Request::get("/api/sessions?offset=2&limit=2&inactive=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: SessionsPage = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.total, 3);
    }

    #[tokio::test]
    async fn delivered_compaction_stays_in_progress_until_observation_verifies_it() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        store
            .insert_compaction_request(&CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: SessionId("session-1".into()),
                kind: CompactionKind::AgentRequested,
                prompt: "compact".into(),
                reason: "hard quota boundary".into(),
                status: CompactionStatus::Sent,
                created_at: Utc::now(),
            })
            .unwrap();

        let response = app(store)
            .oneshot(
                Request::get("/api/actions/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let actions: Vec<RecentAction> = serde_json::from_slice(&body).unwrap();

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].kind, "compaction");
        assert_eq!(actions[0].status, "in progress");
    }

    #[tokio::test]
    async fn recent_actions_excludes_entries_older_than_24_hours() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let now = Utc::now();
        for (id, created_at) in [
            (uuid::Uuid::new_v4(), now - Duration::hours(25)),
            (uuid::Uuid::new_v4(), now - Duration::hours(1)),
        ] {
            store
                .insert_compaction_request(&CompactionRequest {
                    id,
                    session_id: SessionId("session-1".into()),
                    kind: CompactionKind::AgentRequested,
                    prompt: "compact".into(),
                    reason: "test".into(),
                    status: CompactionStatus::Sent,
                    created_at,
                })
                .unwrap();
        }

        let response = app(store)
            .oneshot(
                Request::get("/api/actions/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let actions: Vec<RecentAction> = serde_json::from_slice(&body).unwrap();
        assert_eq!(actions.len(), 1);
        assert!(actions[0].at >= now - Duration::hours(24));
    }

    #[tokio::test]
    async fn blocking_compaction_failure_is_persisted_and_visible_in_api_views() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        store
            .insert_compaction_request(&CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: SessionId("session-1".into()),
                kind: CompactionKind::AgentRequested,
                prompt: "compact".into(),
                reason: "hard quota boundary".into(),
                status: CompactionStatus::Failed("provider rejected compaction".into()),
                created_at: Utc::now(),
            })
            .unwrap();
        let router = app(store);

        let response = router
            .clone()
            .oneshot(
                Request::get("/api/sessions/session-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail: SessionDetail = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            detail.errors,
            ["Blocking compaction failed: provider rejected compaction"]
        );
        assert!(!detail.resume_controls.can_resume);

        let response = router
            .oneshot(
                Request::get("/api/actions/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let actions: Vec<RecentAction> = serde_json::from_slice(&body).unwrap();
        assert_eq!(actions[0].status, "blocked");
        assert_eq!(
            actions[0].error.as_deref(),
            Some("provider rejected compaction")
        );
    }

    #[test]
    fn failed_blocking_compaction_is_a_blocked_recent_action() {
        assert_eq!(
            compact_action_status(&CompactionStatus::Failed(
                "provider rejected compaction".into()
            )),
            (
                "blocked".into(),
                Some("provider rejected compaction".into())
            )
        );
    }

    #[tokio::test]
    async fn session_crud_create_update_delete_round_trip() {
        let router = app(Store::open_memory().unwrap());

        let create = CreateSessionRequest {
            id: SessionId("crud-session".into()),
            harness: Provider::Codex,
            cwd: "/repos/x".into(),
            model: None,
        };
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&create).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Creating the same id again is a conflict, not a 500.
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/sessions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&create).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let update = UpdateSessionRequest {
            cwd: "/repos/y".into(),
            model: Some(ModelId("gpt-5.6-luna".into())),
            account: None,
        };
        let response = router
            .clone()
            .oneshot(
                Request::put("/api/sessions/crud-session")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&update).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let detail: SessionDetail = serde_json::from_slice(&body).unwrap();
        assert_eq!(detail.summary.cwd, "/repos/y");
        assert_eq!(detail.summary.model, Some(ModelId("gpt-5.6-luna".into())));

        let response = router
            .clone()
            .oneshot(
                Request::delete("/api/sessions/crud-session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = router
            .oneshot(
                Request::get("/api/sessions/crud-session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    struct FakePreviewAdapter {
        turns: Vec<uw_core::adapter::TurnPreview>,
    }
    #[async_trait::async_trait]
    impl HarnessAdapter for FakePreviewAdapter {
        fn provider(&self) -> Provider {
            Provider::Codex
        }
        fn capabilities(&self) -> uw_core::adapter::Capabilities {
            uw_core::adapter::Capabilities {
                can_trigger_compaction: false,
                can_advise_mid_turn: false,
                can_inject_at_session_start: true,
                can_observe_compaction: true,
                reports_token_counts: false,
                headless_resume: true,
                seed_modes: vec![],
            }
        }
        async fn fetch_usage(
            &self,
            _: Option<&AccountId>,
        ) -> uw_core::adapter::AdapterResult<UsageSample> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn detect_stop(
            &self,
            _: &SessionId,
        ) -> uw_core::adapter::AdapterResult<Option<StopReason>> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn emit_status(
            &self,
            _: &SessionId,
            _: uw_core::adapter::StatusEvent,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn advise(
            &self,
            _: &SessionId,
            _: &str,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn compact(
            &self,
            _: &SessionSummary,
            _: &CompactionRequest,
        ) -> uw_core::adapter::AdapterResult<uw_core::adapter::DeliveryOutcome> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn resume_session(
            &self,
            _: &SessionSummary,
            _: Option<&str>,
        ) -> uw_core::adapter::AdapterResult<()> {
            Err(uw_core::adapter::AdapterError::Unsupported)
        }
        async fn session_preview(
            &self,
            _: &SessionSummary,
        ) -> uw_core::adapter::AdapterResult<Vec<uw_core::adapter::TurnPreview>> {
            if self.turns.is_empty() {
                Err(uw_core::adapter::AdapterError::Unsupported)
            } else {
                Ok(self.turns.clone())
            }
        }
    }

    #[tokio::test]
    async fn session_preview_returns_turns_or_404_without_a_live_adapter() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();

        // No adapters registered at all (the plain `app()` used everywhere else) — 404,
        // not a 500, since this is an expected "nothing to preview" case.
        let no_adapter_store = Store::open_memory().unwrap();
        no_adapter_store.insert_session(&session()).unwrap();
        let response = app(no_adapter_store)
            .oneshot(
                Request::get("/api/sessions/session-1/preview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let turns = vec![uw_core::adapter::TurnPreview {
            role: uw_core::adapter::TurnRole::User,
            text: "hello".into(),
        }];
        let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([(
            Provider::Codex,
            Arc::new(FakePreviewAdapter { turns }) as Arc<dyn HarnessAdapter>,
        )]);
        let response = app_with_adapters(store, adapters)
            .oneshot(
                Request::get("/api/sessions/session-1/preview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let preview: Vec<uw_core::adapter::TurnPreview> = serde_json::from_slice(&body).unwrap();
        assert_eq!(preview[0].text, "hello");
    }
}
