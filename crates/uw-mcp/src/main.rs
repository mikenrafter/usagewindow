//! Minimal newline-delimited JSON-RPC MCP server for usagewindow.

use chrono::Utc;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::{Arc, Mutex};
use uw_core::adapter::HarnessAdapter;
use uw_core::api::{ProviderUsageSummary, StatusResponse, UsageWindowSummary};
use uw_core::model::*;
use uw_store::Store;

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
    json!({"content":[{"type":"text","text":text}],"structuredContent":value})
}

fn tool_definitions() -> Value {
    json!({"tools":[
        {"name":"get_usage","description":"Read the latest usage-window state.","inputSchema":{"type":"object","properties":{"provider":{"type":"string"},"account":{"type":"string"}}}},
        {"name":"get_resume_state","description":"Read resume markers for a session.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"}},"required":["session_id"]}},
        {"name":"request_compaction","description":"Queue a compaction request when this harness supports triggering compaction.","inputSchema":{"type":"object","properties":{"session_id":{"type":"string"},"prompt":{"type":"string"},"reason":{"type":"string"}},"required":["session_id"]}}
    ]})
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
    for sample in store.all_usage_samples().map_err(|e| e.to_string())? {
        if provider.as_ref().is_some_and(|p| p != &sample.provider)
            || account
                .as_ref()
                .is_some_and(|a| Some(a) != sample.account.as_ref())
        {
            continue;
        }
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
                })
                .collect(),
        })
        .collect();
    serde_json::to_value(StatusResponse {
        usage,
        last_updated: Utc::now(),
    })
    .map_err(|e| e.to_string())
}

fn call_tool(name: &str, args: &Value, deps: &Deps) -> Result<Value, String> {
    match name {
        "get_usage" => Ok(tool_result(get_usage(args, deps)?)),
        "get_resume_state" => {
            let id = args
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or("session_id is required")?;
            let session_id = SessionId(id.into());
            let store = deps
                .store
                .lock()
                .map_err(|_| "store lock poisoned".to_string())?;
            let markers = store
                .resume_markers_for_session(&session_id)
                .map_err(|e| e.to_string())?;
            Ok(tool_result(json!({"session_id": id, "markers": markers})))
        }
        "request_compaction" => {
            let id = args
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or("session_id is required")?;
            let session_id = SessionId(id.into());
            let prompt = args
                .get("prompt")
                .and_then(Value::as_str)
                .unwrap_or("/compact")
                .to_string();
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
                    json!({"supported":false,"session_id":id,"message":"not supported for this harness: compaction cannot be triggered"}),
                ));
            }
            let request = CompactionRequest {
                id: uuid::Uuid::new_v4(),
                session_id,
                kind: CompactionKind::AskNearLimit,
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
            match call_tool(name, params.get("arguments").unwrap_or(&json!({})), deps) {
                Ok(value) => result(id, value),
                Err(message) if message.starts_with("unknown tool:") => error(id, -32602, message),
                Err(message) => error(id, -32000, message),
            }
        }
        _ => error(id, -32601, format!("method not found: {method}")),
    }
}

fn main() -> anyhow::Result<()> {
    let path = std::env::var("UW_DB_PATH").unwrap_or_else(|_| "usagewindow.db".into());
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
    let deps = Deps {
        store: Arc::new(Mutex::new(Store::open(&path)?)),
        adapters,
    };
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let line = line?;
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => handle_request(request, &deps),
            Err(e) => error(Value::Null, -32700, format!("parse error: {e}")),
        };
        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }
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
    fn unknown_method_returns_json_rpc_error() {
        let response = handle_request(json!({"jsonrpc":"2.0","id":"x","method":"nope"}), &deps());
        assert_eq!(response["error"]["code"], -32601);
        assert_eq!(response["id"], "x");
    }
}
