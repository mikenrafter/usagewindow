use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::Response,
    routing::{get, post},
};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use uw_core::{adapter::HarnessAdapter, api::*, model::*};
use uw_store::{Store, ThresholdOverride, ThresholdScopeKind};

#[derive(RustEmbed)]
#[folder = "ui/"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    store: Arc<Mutex<Store>>,
    adapters: Arc<HashMap<Provider, Arc<dyn HarnessAdapter>>>,
}

/// For callers with no live adapters to offer (tests, or read-only tooling) — every
/// route works except `/preview`, which needs a real adapter to read a transcript from.
pub fn app(store: Store) -> Router {
    app_with_adapters(store, HashMap::new())
}

pub fn app_with_adapters(
    store: Store,
    adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
) -> Router {
    Router::new()
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
        .route("/api/sessions/{id}/compact/status", get(compact_status))
        .route("/api/sessions/{id}/keepalive", post(keepalive))
        .route("/api/hooks", post(hook_ingress))
        .route("/api/thresholds", get(thresholds_get).post(thresholds_set))
        .fallback(static_asset)
        .with_state(AppState {
            store: Arc::new(Mutex::new(store)),
            adapters: Arc::new(adapters),
        })
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
            resume_marker: None,
            superseded_by: None,
            reseeded_from: None,
        })?;
        store.record_hook_event(&id, &event)?;
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
        let mut latest: BTreeMap<String, UsageSample> = BTreeMap::new();
        for mut sample in store.all_usage_samples()? {
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
        Ok(StatusResponse { usage: latest.into_values().map(|sample| ProviderUsageSummary { provider: sample.provider, account: sample.account, windows: sample.windows.into_iter().map(|(key, window)| UsageWindowSummary { window: key.kind, pct: window.pct, resets_at: window.resets_at, exceeded: window.exceeded }).collect() }).collect(), last_updated: Utc::now() })
    }).await?;
    Ok(Json(value))
}

#[derive(Debug, Deserialize, Default)]
struct SessionsQuery {
    stopped: Option<bool>,
    harness: Option<String>,
    q: Option<String>,
}

async fn sessions(
    State(state): State<AppState>,
    Query(query): Query<SessionsQuery>,
) -> Result<Json<Vec<SessionListItem>>, (StatusCode, String)> {
    let harness: Option<Provider> = query.harness.and_then(|value| value.parse().ok());
    let include_stopped = query.stopped.unwrap_or(false);
    Ok(Json(
        read(state, move |store| {
            let mut items = Vec::new();
            for s in store.search_sessions(query.q.as_deref())? {
                if !harness.as_ref().is_none_or(|h| h == &s.harness)
                    || (!include_stopped && s.stopped_reason.is_some())
                {
                    continue;
                }
                let resume_status = store
                    .resume_markers_for_session(&s.id)?
                    .last()
                    .map(|m| m.status.clone());
                items.push(SessionListItem {
                    id: s.id,
                    harness: s.harness,
                    model: s.model,
                    account: s.account,
                    last_seen: s.last_seen,
                    stopped_reason: s.stopped_reason,
                    resume_status,
                });
            }
            Ok(items)
        })
        .await?,
    ))
}

fn detail(store: &Store, id: &SessionId) -> anyhow::Result<SessionDetail> {
    let summary = store.read_session(id)?;
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
    Ok(SessionDetail {
        summary,
        history,
        resume_controls: ResumeControls {
            can_resume: marker.is_some(),
            marker,
        },
        compaction_log: store.compaction_requests_for_session(id)?,
        reseed_lineage: vec![],
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

async fn compact_ask(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<CompactAskRequest>,
) -> Result<Json<CompactStatusResponse>, (StatusCode, String)> {
    let id = session_id(id);
    Ok(Json(
        read(state, move |store| {
            let request = CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id: id.clone(),
                kind: CompactionKind::AskNearLimit,
                prompt: request
                    .reason
                    .clone()
                    .unwrap_or_else(|| "Please compact the current context.".into()),
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
    read(state, move |store| {
        store.set_keepalive(&id, request.enabled)?;
        Ok(())
    })
    .await?;
    Ok(Json(
        serde_json::json!({"ok": true, "enabled": request.enabled}),
    ))
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

async fn static_asset() -> Response<Body> {
    let Some(asset) = Assets::get("index.html") else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(asset.data.into_owned()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use chrono::Utc;
    use std::collections::HashMap;
    use tower::ServiceExt;

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
            .insert_usage_sample(&UsageSample {
                at: Utc::now(),
                fetched_at: None,
                source: UsageSource::ProviderReported,
                provider: Provider::Codex,
                account: None,
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
        let response = app(store)
            .oneshot(Request::get("/api/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: StatusResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.usage[0].windows[0].pct, 42.0);
        let response = app(Store::open_memory().unwrap())
            .oneshot(Request::get("/api/sessions").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
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
        assert!(String::from_utf8_lossy(&body).contains("usagewindow"));
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
    async fn resume_compaction_and_threshold_routes_write_store() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let router = app(store);
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
        let error_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&error_body)
        );
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
                Request::get("/api/sessions?q=usagewindow")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let items: Vec<SessionListItem> = serde_json::from_slice(&body).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, SessionId("alpha".into()));
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
