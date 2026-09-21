//! MCP server for usagewindow.
//!
//! HTTP serves the stateless MCP 2026-07-28 protocol. `--stdio` retains the
//! previous newline-delimited JSON-RPC transport for older local clients.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use uw_core::adapter::HarnessAdapter;
use uw_core::api::{ProviderUsageSummary, StatusResponse, UsageWindowSummary};
use uw_core::model::*;
use uw_store::Store;

const PROTOCOL_VERSION: &str = "2026-07-28";
const SERVER_INFO_KEY: &str = "io.modelcontextprotocol/serverInfo";
const PROTOCOL_VERSION_KEY: &str = "io.modelcontextprotocol/protocolVersion";
const CLIENT_INFO_KEY: &str = "io.modelcontextprotocol/clientInfo";
const CLIENT_CAPABILITIES_KEY: &str = "io.modelcontextprotocol/clientCapabilities";
const CALLER_SESSION_ID_KEY: &str = "com.usagewindow/sessionId";

pub struct Deps {
    pub store: Arc<Mutex<Store>>,
    pub adapters: HashMap<Provider, Arc<dyn HarnessAdapter>>,
}

fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc":"2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

fn result(id: Value, value: Value) -> Value {
    json!({"jsonrpc":"2.0", "id": id, "result": value})
}

fn tool_result(value: Value) -> Value {
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    json!({"resultType":"complete","content":[{"type":"text","text":text}],"structuredContent":value})
}

fn tool_definitions() -> Value {
    json!({"tools":[
        {"name":"get_usage","description":"Read the latest usage-window state.","inputSchema":{"type":"object","properties":{"provider":{"type":"string"},"account":{"type":"string"}}}},
        {"name":"get_resume_state","description":"Read resume markers for the calling session, or a supplied session.","inputSchema":{"type":"object","properties":{"session_id":{"type":["string","null"]}}}},
        {"name":"request_compaction","description":"Queue an agent-requested compaction for the calling session, or a supplied session, when its harness has a live message transport.","inputSchema":{"type":"object","properties":{"session_id":{"type":["string","null"]},"prompt":{"type":"string"},"reason":{"type":"string"}}}}
    ]})
}

fn session_id_argument(
    args: &Value,
    caller_session_id: Option<&SessionId>,
) -> Result<SessionId, String> {
    match args.get("session_id") {
        Some(Value::String(id)) => Ok(SessionId(id.clone())),
        Some(Value::Null) | None => caller_session_id.cloned().ok_or(
            "session_id is unavailable; pass session_id explicitly or configure caller session context".into(),
        ),
        Some(_) => Err("session_id must be a string or null".into()),
    }
}

fn get_usage(args: &Value, deps: &Deps) -> Result<Value, String> {
    let provider = args
        .get("provider")
        .and_then(Value::as_str)
        .map(|p| p.parse::<Provider>())
        .transpose()
        .map_err(|e| e.to_string())?;
    let account = args
        .get("account")
        .and_then(Value::as_str)
        .map(|a| AccountId(a.into()));
    let store = deps
        .store
        .lock()
        .map_err(|_| "store lock poisoned".to_string())?;
    let mut latest: HashMap<String, UsageSample> = HashMap::new();
    for sample in store
        .latest_usage_samples_for(provider.as_ref(), account.as_ref())
        .map_err(|e| e.to_string())?
    {
        let key = format!("{:?}:{:?}", sample.provider, sample.account);
        if latest.get(&key).is_none_or(|old| old.at < sample.at) {
            latest.insert(key, sample);
        }
    }
    let usage = latest
        .into_values()
        .map(|sample| ProviderUsageSummary {
            provider: sample.provider,
            account: sample.account,
            windows: sample
                .windows
                .into_iter()
                .map(|(key, window)| UsageWindowSummary {
                    window: key.kind,
                    pct: window.pct,
                    resets_at: window.resets_at,
                    exceeded: window.exceeded,
                    // MCP surface stays lightweight: burn rate / exhaustion projection and
                    // per-window session counts are HTTP-API-only (see uw-web::status).
                    burn_rate_pct_per_hour: None,
                    active_sessions: 0,
                    depletes_at: None,
                })
                .collect(),
        })
        .collect();
    serde_json::to_value(StatusResponse {
        usage,
        last_updated: Utc::now(),
        provider_status: store.fetch_statuses().map_err(|e| e.to_string())?,
        keepalive_active_count: store.keepalive_active_count().map_err(|e| e.to_string())?,
    })
    .map_err(|e| e.to_string())
}

fn call_tool(
    name: &str,
    args: &Value,
    deps: &Deps,
    caller_session_id: Option<&SessionId>,
) -> Result<Value, String> {
    match name {
        "get_usage" => Ok(tool_result(get_usage(args, deps)?)),
        "get_resume_state" => {
            let session_id = session_id_argument(args, caller_session_id)?;
            let store = deps
                .store
                .lock()
                .map_err(|_| "store lock poisoned".to_string())?;
            let markers = store
                .resume_markers_for_session(&session_id)
                .map_err(|e| e.to_string())?;
            Ok(tool_result(
                json!({"session_id": session_id.0, "markers": markers}),
            ))
        }
        "request_compaction" => {
            let session_id = session_id_argument(args, caller_session_id)?;
            let prompt = args
                .get("prompt")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            let reason = args
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("requested through MCP")
                .to_string();
            let store = deps
                .store
                .lock()
                .map_err(|_| "store lock poisoned".to_string())?;
            let session = store.read_session(&session_id).map_err(|e| e.to_string())?;
            let adapter = deps
                .adapters
                .get(&session.harness)
                .ok_or_else(|| format!("no adapter registered for {:?}", session.harness))?;
            if !adapter.capabilities().can_trigger_compaction {
                return Ok(tool_result(
                    json!({"supported":false,"session_id":session_id.0,"message":"not supported for this harness: compaction cannot be triggered"}),
                ));
            }
            let request = CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id,
                kind: CompactionKind::AgentRequested,
                prompt,
                reason,
                status: CompactionStatus::Pending,
                created_at: Utc::now(),
            };
            store
                .insert_compaction_request(&request)
                .map_err(|e| e.to_string())?;
            Ok(tool_result(
                json!({"supported":true,"queued":true,"request":request}),
            ))
        }
        _ => Err(format!("unknown tool: {name}")),
    }
}

fn caller_session_id_from_env() -> Option<SessionId> {
    std::env::var("CLAUDE_CODE_SESSION_ID")
        .ok()
        .filter(|id| !id.is_empty())
        .map(SessionId)
}

fn caller_session_id_from_modern_request(params: &Value) -> Option<SessionId> {
    params
        .get("_meta")
        .and_then(|meta| meta.get(CALLER_SESSION_ID_KEY))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(|id| SessionId(id.into()))
        .or_else(caller_session_id_from_env)
}

/// Pure JSON-RPC request handling. The caller owns process I/O; this function does not
/// read stdin, write stdout, or panic on malformed/unknown requests.
pub fn handle_request(request: Value, deps: &Deps) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return error(id, -32600, "invalid request");
    };
    match method {
        "initialize" => result(
            id,
            json!({"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"uw-mcp","version":env!("CARGO_PKG_VERSION")}}),
        ),
        "initialized" | "notifications/initialized" => result(id, json!({})),
        "ping" => result(id, json!({})),
        "tools/list" => result(id, tool_definitions()),
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return error(id, -32602, "tools/call requires a tool name");
            };
            match call_tool(
                name,
                params.get("arguments").unwrap_or(&json!({})),
                deps,
                caller_session_id_from_env().as_ref(),
            ) {
                Ok(value) => result(id, value),
                Err(message) if message.starts_with("unknown tool:") => error(id, -32602, message),
                Err(message) => error(id, -32000, message),
            }
        }
        _ => error(id, -32601, format!("method not found: {method}")),
    }
}

fn server_meta() -> Value {
    json!({SERVER_INFO_KEY: {
        "name": "usagewindow",
        "version": env!("CARGO_PKG_VERSION")
    }})
}

fn modern_result(id: Value, mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object
            .entry("resultType")
            .or_insert_with(|| Value::String("complete".into()));
        object.insert("_meta".into(), server_meta());
    }
    result(id, value)
}

fn modern_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    error(id, code, message)
}

fn error_with_data(id: Value, code: i64, message: impl Into<String>, data: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code, "message":message.into(), "data":data}})
}

fn validate_request(request: &Value, headers: &HeaderMap) -> Result<(), (i64, String, Value)> {
    let invalid = |message: &str| (-32600, message.to_string(), Value::Null);
    let valid_id = request
        .get("id")
        .is_some_and(|id| id.is_string() || id.as_i64().is_some() || id.as_u64().is_some());
    if request.get("jsonrpc") != Some(&Value::String("2.0".into()))
        || !valid_id
        || !request.get("method").is_some_and(Value::is_string)
        || !request.get("params").is_some_and(Value::is_object)
    {
        return Err(invalid("invalid JSON-RPC request"));
    }

    let params = &request["params"];
    let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
        return Err((-32602, "params._meta is required".into(), Value::Null));
    };
    let Some(body_version) = meta.get(PROTOCOL_VERSION_KEY).and_then(Value::as_str) else {
        return Err((
            -32602,
            format!("params._meta.{PROTOCOL_VERSION_KEY} is required"),
            Value::Null,
        ));
    };
    if !meta
        .get(CLIENT_CAPABILITIES_KEY)
        .is_some_and(Value::is_object)
    {
        return Err((
            -32602,
            format!("params._meta.{CLIENT_CAPABILITIES_KEY} must be an object"),
            Value::Null,
        ));
    }
    if let Some(client_info) = meta.get(CLIENT_INFO_KEY)
        && (!client_info.get("name").is_some_and(Value::is_string)
            || !client_info.get("version").is_some_and(Value::is_string))
    {
        return Err((
            -32602,
            format!("params._meta.{CLIENT_INFO_KEY} must contain string name and version"),
            Value::Null,
        ));
    }
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    if header("mcp-protocol-version") != Some(body_version) {
        return Err((
            -32020,
            "MCP-Protocol-Version header mismatch".into(),
            Value::Null,
        ));
    }
    let method = request["method"].as_str().expect("validated method");
    if header("mcp-method") != Some(method) {
        return Err((-32020, "Mcp-Method header mismatch".into(), Value::Null));
    }
    if body_version != PROTOCOL_VERSION {
        return Err((
            -32022,
            format!("unsupported protocol version: {body_version}"),
            json!({"requested":body_version,"supported":[PROTOCOL_VERSION]}),
        ));
    }
    if method == "tools/call" {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return Err((
                -32602,
                "tools/call requires a tool name".into(),
                Value::Null,
            ));
        };
        if params
            .get("arguments")
            .is_some_and(|value| !value.is_object())
        {
            return Err((
                -32602,
                "tools/call arguments must be an object".into(),
                Value::Null,
            ));
        }
        if header("mcp-name") != Some(name) {
            return Err((-32020, "Mcp-Name header mismatch".into(), Value::Null));
        }
    } else if method == "tools/list" && params.get("cursor").is_some_and(|value| !value.is_string())
    {
        return Err((
            -32602,
            "tools/list cursor must be a string".into(),
            Value::Null,
        ));
    }
    Ok(())
}

fn handle_modern_request(request: Value, headers: &HeaderMap, deps: &Deps) -> (StatusCode, Value) {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    if let Err((code, message, data)) = validate_request(&request, headers) {
        let response = if data.is_null() {
            modern_error(id, code, message)
        } else {
            error_with_data(id, code, message, data)
        };
        return (StatusCode::BAD_REQUEST, response);
    }
    let method = request["method"].as_str().expect("validated method");
    let params = &request["params"];
    let value = match method {
        "server/discover" => json!({
            "resultType":"complete",
            "supportedVersions":[PROTOCOL_VERSION],
            "capabilities":{"tools":{}},
            "instructions":"Read usage and resume state. Compaction requests are capability-gated by the session harness.",
            "ttlMs":300_000,
            "cacheScope":"public"
        }),
        "tools/list" => {
            let mut definitions = tool_definitions();
            let object = definitions
                .as_object_mut()
                .expect("tool definitions object");
            object.insert("resultType".into(), json!("complete"));
            object.insert("ttlMs".into(), json!(300_000));
            object.insert("cacheScope".into(), json!("public"));
            definitions
        }
        "tools/call" => {
            let name = params["name"].as_str().expect("validated tool name");
            match call_tool(
                name,
                params.get("arguments").unwrap_or(&json!({})),
                deps,
                caller_session_id_from_modern_request(params).as_ref(),
            ) {
                Ok(value) => value,
                Err(message) if message.starts_with("unknown tool:") => {
                    return (StatusCode::BAD_REQUEST, modern_error(id, -32602, message));
                }
                Err(message) => {
                    return (
                        StatusCode::OK,
                        modern_result(
                            id,
                            json!({"resultType":"complete","content":[{"type":"text","text":message}],"isError":true}),
                        ),
                    );
                }
            }
        }
        _ => {
            return (
                StatusCode::NOT_FOUND,
                modern_error(id, -32601, format!("method not found: {method}")),
            );
        }
    };
    (StatusCode::OK, modern_result(id, value))
}

async fn mcp_http(State(deps): State<Arc<Deps>>, headers: HeaderMap, body: Bytes) -> Response {
    if headers
        .get("origin")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| !origin_is_allowed(origin))
    {
        return (
            StatusCode::FORBIDDEN,
            Json(modern_error(Value::Null, -32000, "Origin is not allowed")),
        )
            .into_response();
    }
    let request = match serde_json::from_slice::<Value>(&body) {
        Ok(request) => request,
        Err(parse_error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(modern_error(
                    Value::Null,
                    -32700,
                    format!("parse error: {parse_error}"),
                )),
            )
                .into_response();
        }
    };
    let (status, response) = handle_modern_request(request, &headers, &deps);
    (status, Json(response)).into_response()
}

fn origin_is_allowed(origin: &str) -> bool {
    let local = [
        "http://localhost",
        "https://localhost",
        "http://127.0.0.1",
        "https://127.0.0.1",
    ]
    .iter()
    .any(|prefix| {
        origin.strip_prefix(prefix).is_some_and(|suffix| {
            suffix.is_empty()
                || suffix.strip_prefix(':').is_some_and(|port| {
                    !port.is_empty() && port.chars().all(|c| c.is_ascii_digit())
                })
        })
    });
    local
        || std::env::var("UW_MCP_ALLOWED_ORIGINS")
            .ok()
            .is_some_and(|origins| origins.split(',').map(str::trim).any(|item| item == origin))
}

pub fn http_app(deps: Arc<Deps>) -> Router {
    Router::new().route("/mcp", post(mcp_http)).with_state(deps)
}

fn build_deps(path: &str) -> anyhow::Result<Deps> {
    let cache_path = std::env::var("UW_CLAUDE_CACHE_PATH")
        .unwrap_or_else(|_| format!("{path}.claude-usage-cache.json"));
    let adapters: HashMap<Provider, Arc<dyn HarnessAdapter>> = HashMap::from([
        (
            Provider::ClaudeCode,
            Arc::new(uw_adapters::claude_code::ClaudeCodeAdapter::real(
                cache_path,
                std::env::var("UW_CLAUDE_VERSION").unwrap_or_else(|_| "unknown".into()),
            )) as Arc<dyn HarnessAdapter>,
        ),
        (
            Provider::Codex,
            Arc::new(uw_adapters::codex::CodexAdapter::real()) as Arc<dyn HarnessAdapter>,
        ),
    ]);
    let mut adapters = adapters;
    if let Ok(token) = std::env::var("UW_CURSOR_ACCESS_TOKEN") {
        if !token.is_empty() && std::env::var("UW_CURSOR_SESSION_COOKIE").is_err() {
            adapters.insert(
                Provider::Cursor,
                Arc::new(uw_adapters::cursor::CursorAdapter::real(
                    uw_adapters::cursor::CursorAuth::Bearer(token),
                )) as Arc<dyn HarnessAdapter>,
            );
        }
    } else if let Ok(cookie) = std::env::var("UW_CURSOR_SESSION_COOKIE")
        && !cookie.is_empty()
    {
        adapters.insert(
            Provider::Cursor,
            Arc::new(uw_adapters::cursor::CursorAdapter::real(
                uw_adapters::cursor::CursorAuth::Cookie(cookie),
            )) as Arc<dyn HarnessAdapter>,
        );
    }
    Ok(Deps {
        store: Arc::new(Mutex::new(Store::open(path)?)),
        adapters,
    })
}

fn run_stdio(deps: &Deps) -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let line = line?;
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => handle_request(request, deps),
            Err(e) => error(Value::Null, -32700, format!("parse error: {e}")),
        };
        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var("UW_DB_PATH").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.usagewindow/usagewindow.db")
    });
    let deps = build_deps(&path)?;
    if std::env::args().any(|arg| arg == "--stdio")
        || std::env::var("UW_MCP_TRANSPORT").is_ok_and(|value| value == "stdio")
    {
        return run_stdio(&deps);
    }

    let address = std::env::var("UW_MCP_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:7880".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, http_app(Arc::new(deps))).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use uw_core::adapter::{
        AdapterResult, Capabilities, DeliveryOutcome, HarnessAdapter, SeedContext, SeedMode,
        StatusEvent,
    };
    use uw_store::Store;

    struct FakeAdapter {
        provider: Provider,
        capabilities: Capabilities,
    }
    #[async_trait]
    impl HarnessAdapter for FakeAdapter {
        fn provider(&self) -> Provider {
            self.provider.clone()
        }
        fn capabilities(&self) -> Capabilities {
            self.capabilities.clone()
        }
        async fn fetch_usage(&self, _: Option<&AccountId>) -> AdapterResult<UsageSample> {
            unimplemented!()
        }
        async fn detect_stop(&self, _: &SessionId) -> AdapterResult<Option<StopReason>> {
            unimplemented!()
        }
        async fn emit_status(
            &self,
            _: &SessionId,
            _: StatusEvent,
        ) -> AdapterResult<DeliveryOutcome> {
            unimplemented!()
        }
        async fn advise(&self, _: &SessionId, _: &str) -> AdapterResult<DeliveryOutcome> {
            unimplemented!()
        }
        async fn compact(
            &self,
            _: &SessionSummary,
            _: &CompactionRequest,
        ) -> AdapterResult<DeliveryOutcome> {
            unimplemented!()
        }
        async fn resume_session(&self, _: &SessionSummary, _: Option<&str>) -> AdapterResult<()> {
            unimplemented!()
        }
        async fn seed_new_session(&self, _: SeedMode, _: &SeedContext) -> AdapterResult<SessionId> {
            unimplemented!()
        }
    }
    fn deps() -> Deps {
        let store = Store::open_memory().unwrap();
        let mut adapters = HashMap::new();
        adapters.insert(
            Provider::Codex,
            Arc::new(FakeAdapter {
                provider: Provider::Codex,
                capabilities: Capabilities {
                    can_trigger_compaction: false,
                    can_advise_mid_turn: false,
                    can_inject_at_session_start: true,
                    can_observe_compaction: true,
                    reports_token_counts: false,
                    headless_resume: true,
                    seed_modes: vec![SeedMode::InitialPrompt],
                },
            }) as Arc<dyn HarnessAdapter>,
        );
        Deps {
            store: Arc::new(Mutex::new(store)),
            adapters,
        }
    }
    fn session(id: &str, harness: Provider) -> SessionSummary {
        SessionSummary {
            id: SessionId(id.into()),
            harness,
            model: None,
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
    #[test]
    fn initialize_returns_valid_response() {
        let response = handle_request(
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
            &deps(),
        );
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["protocolVersion"], "2024-11-05");
        assert!(response["result"]["capabilities"]["tools"].is_object());
    }
    #[test]
    fn tools_list_has_the_three_tools_and_schemas() {
        let response = handle_request(
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            &deps(),
        );
        let tools = response["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 3);
        for name in ["get_usage", "get_resume_state", "request_compaction"] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(tool["inputSchema"]["type"], "object");
        }
    }
    #[test]
    fn get_usage_returns_seeded_usage_data() {
        let deps = deps();
        let sample = UsageSample {
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
        };
        deps.store
            .lock()
            .unwrap()
            .insert_usage_sample(&sample)
            .unwrap();
        let response = handle_request(
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_usage","arguments":{}}}),
            &deps,
        );
        let usage: Value =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(usage["usage"][0]["windows"][0]["pct"], 42.0);
    }
    #[test]
    fn get_usage_decodes_only_the_latest_database_rows() {
        let deps = deps();
        let now = Utc::now();
        let window = WindowKey {
            provider: Provider::Codex,
            kind: WindowKind::Rolling { minutes: 300 },
        };
        let old = UsageSample {
            at: now - chrono::Duration::hours(1),
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::Codex,
            account: None,
            windows: HashMap::from([(
                window.clone(),
                UsageWindowState::new(10.0, false, true, None, None),
            )]),
            credits: None,
        };
        let latest = UsageSample {
            at: now,
            fetched_at: None,
            source: UsageSource::ProviderReported,
            provider: Provider::Codex,
            account: None,
            windows: HashMap::from([(
                window,
                UsageWindowState::new(84.0, false, true, None, None),
            )]),
            credits: None,
        };
        let store = deps.store.lock().unwrap();
        let old_id = store.insert_usage_sample(&old).unwrap();
        store
            .connection()
            .execute(
                "UPDATE usage_samples SET source='not-json' WHERE id=?1",
                [old_id],
            )
            .unwrap();
        store.insert_usage_sample(&latest).unwrap();
        drop(store);

        let response = handle_request(
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_usage","arguments":{}}}),
            &deps,
        );

        assert!(
            response.get("error").is_none(),
            "an unreadable historical row must not affect latest-only get_usage: {response}"
        );
        let usage: Value =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(usage["usage"][0]["windows"][0]["pct"], 84.0);
    }
    #[test]
    fn request_compaction_reports_codex_unsupported_without_inserting() {
        let deps = deps();
        let s = session("codex-session", Provider::Codex);
        deps.store.lock().unwrap().insert_session(&s).unwrap();
        let response = handle_request(
            json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"request_compaction","arguments":{"session_id":"codex-session"}}}),
            &deps,
        );
        assert_eq!(response["result"]["structuredContent"]["supported"], false);
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("not supported for this harness")
        );
        assert!(
            deps.store
                .lock()
                .unwrap()
                .pending_compaction_requests()
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn request_compaction_queues_an_agent_requested_claude_delivery() {
        let mut deps = deps();
        let s = session("claude-session", Provider::ClaudeCode);
        deps.store.lock().unwrap().insert_session(&s).unwrap();
        deps.adapters.insert(
            Provider::ClaudeCode,
            Arc::new(FakeAdapter {
                provider: Provider::ClaudeCode,
                capabilities: Capabilities {
                    can_trigger_compaction: true,
                    can_advise_mid_turn: false,
                    can_inject_at_session_start: true,
                    can_observe_compaction: true,
                    reports_token_counts: true,
                    headless_resume: true,
                    seed_modes: vec![SeedMode::InitialPrompt],
                },
            }),
        );
        let response = handle_request(
            json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"request_compaction","arguments":{"session_id":"claude-session","prompt":"/compact"}}}),
            &deps,
        );
        assert_eq!(response["result"]["structuredContent"]["supported"], true);
        let request = deps
            .store
            .lock()
            .unwrap()
            .pending_compaction_requests()
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(request.kind, CompactionKind::AgentRequested);
    }

    #[test]
    fn null_session_id_uses_caller_session_metadata() {
        let deps = deps();
        let s = session("caller-session", Provider::Codex);
        deps.store.lock().unwrap().insert_session(&s).unwrap();
        let response = handle_modern_request(
            json!({
                "jsonrpc":"2.0",
                "id":6,
                "method":"tools/call",
                "params":{
                    "name":"get_resume_state",
                    "arguments":{"session_id":null},
                    "_meta":{
                        "io.modelcontextprotocol/protocolVersion":"2026-07-28",
                        "io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},
                        "io.modelcontextprotocol/clientCapabilities":{},
                        "com.usagewindow/sessionId":"caller-session"
                    }
                }
            }),
            &{
                let mut headers = HeaderMap::new();
                headers.insert("mcp-protocol-version", "2026-07-28".parse().unwrap());
                headers.insert("mcp-method", "tools/call".parse().unwrap());
                headers.insert("mcp-name", "get_resume_state".parse().unwrap());
                headers
            },
            &deps,
        );
        assert_eq!(response.0, StatusCode::OK);
        assert_eq!(
            response.1["result"]["structuredContent"]["session_id"],
            "caller-session"
        );
    }

    #[test]
    fn session_id_is_optional_and_nullable_in_tool_schemas() {
        let definitions = tool_definitions();
        let tools = definitions["tools"].as_array().unwrap();
        for name in ["get_resume_state", "request_compaction"] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert!(tool["inputSchema"].get("required").is_none());
            assert_eq!(
                tool["inputSchema"]["properties"]["session_id"]["type"],
                json!(["string", "null"])
            );
        }
    }
    #[test]
    fn unknown_method_returns_json_rpc_error() {
        let response = handle_request(json!({"jsonrpc":"2.0","id":"x","method":"nope"}), &deps());
        assert_eq!(response["error"]["code"], -32601);
        assert_eq!(response["id"], "x");
    }

    mod modern_http {
        use super::*;
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        const META: &str = r#""_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"uw-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}"#;

        fn request(method: &str, name: Option<&str>, body: String) -> Request<Body> {
            let mut builder = Request::post("/mcp")
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", method);
            if let Some(name) = name {
                builder = builder.header("mcp-name", name);
            }
            builder.body(Body::from(body)).unwrap()
        }

        async fn json(response: axum::response::Response) -> Value {
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap()
        }

        #[tokio::test]
        async fn discover_uses_modern_envelope() {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":"d1","method":"server/discover","params":{{{META}}}}}"#
            );
            let response = http_app(Arc::new(deps()))
                .oneshot(request("server/discover", None, body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value = json(response).await;
            assert_eq!(value["result"]["resultType"], "complete");
            assert_eq!(value["result"]["supportedVersions"], json!(["2026-07-28"]));
            assert!(value["result"]["capabilities"]["tools"].is_object());
            assert_eq!(
                value["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
                "usagewindow"
            );
            assert_eq!(value["result"]["cacheScope"], "public");
        }

        #[tokio::test]
        async fn tools_list_and_call_work_without_shared_protocol_session() {
            let app = http_app(Arc::new(deps()));
            for id in [1, 2] {
                let body = format!(
                    r#"{{"jsonrpc":"2.0","id":{id},"method":"tools/list","params":{{{META}}}}}"#
                );
                let response = app
                    .clone()
                    .oneshot(request("tools/list", None, body))
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let value = json(response).await;
                assert_eq!(value["result"]["resultType"], "complete");
                assert_eq!(value["result"]["tools"].as_array().unwrap().len(), 3);
                assert_eq!(value["result"]["cacheScope"], "public");
            }

            let body = format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"get_usage","arguments":{{}},{META}}}}}"#
            );
            let response = app
                .oneshot(request("tools/call", Some("get_usage"), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value = json(response).await;
            assert_eq!(value["result"]["resultType"], "complete");
            assert!(value["result"]["structuredContent"]["usage"].is_array());
        }

        #[tokio::test]
        async fn rejects_missing_modern_metadata_and_header_mismatches() {
            let app = http_app(Arc::new(deps()));
            let missing_meta = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
            let response = app
                .clone()
                .oneshot(request("tools/list", None, missing_meta.into()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(json(response).await["error"]["code"], -32602);

            let body =
                format!(r#"{{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{{{META}}}}}"#);
            let response = app
                .oneshot(request("tools/call", None, body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(json(response).await["error"]["code"], -32020);

            let body = format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"get_usage","arguments":[],{META}}}}}"#
            );
            let response = http_app(Arc::new(deps()))
                .oneshot(request("tools/call", Some("get_usage"), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(json(response).await["error"]["code"], -32602);
        }

        #[tokio::test]
        async fn rejects_unsupported_versions_and_invalid_request_ids() {
            let unsupported_meta = META.replace("2026-07-28", "2099-01-01");
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{{{unsupported_meta}}}}}"#
            );
            let mut unsupported = request("tools/list", None, body);
            unsupported
                .headers_mut()
                .insert("mcp-protocol-version", "2099-01-01".parse().unwrap());
            let response = http_app(Arc::new(deps()))
                .oneshot(unsupported)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let value = json(response).await;
            assert_eq!(value["error"]["code"], -32022);
            assert_eq!(value["error"]["data"]["supported"], json!(["2026-07-28"]));

            let body = format!(
                r#"{{"jsonrpc":"2.0","id":{{}},"method":"tools/list","params":{{{META}}}}}"#
            );
            let response = http_app(Arc::new(deps()))
                .oneshot(request("tools/list", None, body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(json(response).await["error"]["code"], -32600);
        }

        #[tokio::test]
        async fn malformed_json_and_unknown_methods_have_spec_statuses() {
            let app = http_app(Arc::new(deps()));
            let response = app
                .clone()
                .oneshot(request("tools/list", None, "{".into()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(json(response).await["error"]["code"], -32700);

            let body = format!(
                r#"{{"jsonrpc":"2.0","id":9,"method":"unknown/method","params":{{{META}}}}}"#
            );
            let response = app
                .oneshot(request("unknown/method", None, body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(json(response).await["error"]["code"], -32601);
        }

        #[tokio::test]
        async fn get_and_delete_are_not_legacy_session_transports() {
            for method in ["GET", "DELETE"] {
                let request = Request::builder()
                    .method(method)
                    .uri("/mcp")
                    .body(Body::empty())
                    .unwrap();
                let response = http_app(Arc::new(deps())).oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
            }
        }

        #[tokio::test]
        async fn rejects_untrusted_browser_origins() {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":10,"method":"server/discover","params":{{{META}}}}}"#
            );
            let request = request("server/discover", None, body);
            let (mut parts, body) = request.into_parts();
            parts
                .headers
                .insert("origin", "https://attacker.example".parse().unwrap());
            let response = http_app(Arc::new(deps()))
                .oneshot(Request::from_parts(parts, body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        #[tokio::test]
        async fn modern_http_preserves_codex_compaction_gate() {
            let deps = deps();
            deps.store
                .lock()
                .unwrap()
                .insert_session(&session("codex-http", Provider::Codex))
                .unwrap();
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{{"name":"request_compaction","arguments":{{"session_id":"codex-http"}},{META}}}}}"#
            );
            let response = http_app(Arc::new(deps))
                .oneshot(request("tools/call", Some("request_compaction"), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let value = json(response).await;
            assert_eq!(value["result"]["structuredContent"]["supported"], false);
        }
    }
}
