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
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use uw_core::{api::*, model::*};
use uw_store::{Store, ThresholdOverride, ThresholdScopeKind};

#[derive(RustEmbed)]
#[folder = "ui/"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    store: Arc<Mutex<Store>>,
}

pub fn app(store: Store) -> Router {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/sessions", get(sessions))
        .route("/api/sessions/{id}", get(session))
        .route("/api/sessions/{id}/resume", post(resume))
        .route("/api/sessions/{id}/resume/cancel", post(cancel_resume))
        .route("/api/sessions/{id}/compact/ask", post(compact_ask))
        .route("/api/sessions/{id}/compact/status", get(compact_status))
        .route("/api/sessions/{id}/keepalive", post(keepalive))
        .route("/api/thresholds", get(thresholds_get).post(thresholds_set))
        .fallback(static_asset)
        .with_state(AppState {
            store: Arc::new(Mutex::new(store)),
        })
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
            for s in store.list_sessions()? {
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
                resume_at: request.at,
                created_at: Utc::now(),
                status: ResumeStatus::Pending,
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
    async fn resume_compaction_and_threshold_routes_write_store() {
        let store = Store::open_memory().unwrap();
        store.insert_session(&session()).unwrap();
        let router = app(store);
        let resume = ResumeRequest {
            session_id: SessionId("session-1".into()),
            at: None,
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
}
