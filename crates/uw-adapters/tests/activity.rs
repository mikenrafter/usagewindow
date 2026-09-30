use serde_json::{Value, json};
use uw_adapters::{claude_code::transcript_activity, codex::rollout_activity};
use uw_core::{activity::SessionActivity, model::SessionId};

fn lines(records: &[Value]) -> String {
    records
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}
fn claude_tool(id: &str, name: &str) -> Value {
    json!({"type":"assistant","sessionId":"session","message":{"content":[{"type":"tool_use","id":id,"name":name,"input":{"command":"sleep 270 && echo keepalive","run_in_background":true}}]}})
}
fn claude_result(id: &str, content: &str) -> Value {
    json!({"type":"user","sessionId":"session","message":{"content":[{"type":"tool_result","tool_use_id":id,"content":content}]}})
}
#[test]
fn claude_background_keepalive_remains_busy_after_stop_and_monitor_results() {
    let records = vec![
        claude_tool("launch", "Bash"),
        claude_result(
            "launch",
            "Command running in background with ID: bzarj83i7. Output is being written to: /tmp/tasks/bzarj83i7.output. You will be notified when it completes. To check interim output, use Read on that file path.",
        ),
        json!({"type":"system","subtype":"turn_duration","sessionId":"session"}),
        claude_tool("poll", "Read"),
        claude_result("poll", "No new output"),
    ];
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
}
#[test]
fn claude_matching_notifications_complete_multiple_background_tasks() {
    let mut records = vec![];
    for task in ["a", "b"] {
        records.push(claude_tool(task, "Bash"));
        records.push(claude_result(task,&format!("Command running in background with ID: {task}. Output is being written to: /tmp/tasks/{task}.output.")));
    }
    records.push(json!({"type":"user","sessionId":"session","message":{"content":"<task-notification>\n<task-id>a</task-id>\n<status>completed</status>\n</task-notification>"}}));
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
    records.push(json!({"type":"user","sessionId":"session","message":{"content":"<task-notification>\n<task-id>b</task-id>\n<task-id>__orphan_summary__:shell</task-id>\n<status>stopped</status>\n</task-notification>"}}));
    records.push(json!({"type":"system","subtype":"turn_duration","sessionId":"session"}));
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}
#[test]
fn claude_foreground_and_monitor_calls_require_matching_results() {
    for name in ["Bash", "Read", "Monitor", "ArbitraryTool"] {
        let mut records = vec![claude_tool("call", name)];
        assert_eq!(
            transcript_activity(&lines(&records), &SessionId("session".into())),
            SessionActivity::Busy
        );
        records.push(claude_result("call", "finished"));
        assert_eq!(
            transcript_activity(&lines(&records), &SessionId("session".into())),
            SessionActivity::Idle
        );
    }
}
#[test]
fn claude_does_not_infer_activity_from_command_keywords() {
    let records = [
        claude_tool("call", "Bash"),
        claude_result(
            "call",
            "keepalive monitor sleep are words in ordinary command output",
        ),
    ];
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}
#[test]
fn malformed_or_wrong_session_activity_fails_closed() {
    for content in [
        "",
        "{bad",
        "{\"sessionId\":\"other\",\"type\":\"assistant\",\"message\":{\"content\":[]}}",
    ] {
        assert_eq!(
            transcript_activity(content, &SessionId("session".into())),
            SessionActivity::Unknown
        );
    }
    let content = lines(&[json!({"type":"session_meta","payload":{"id":"other"}})]);
    assert_eq!(
        rollout_activity(&content, &SessionId("session".into())),
        SessionActivity::Unknown
    );
}
fn codex_start() -> Vec<Value> {
    vec![
        json!({"type":"session_meta","payload":{"id":"session"}}),
        json!({"type":"response_item","payload":{"type":"custom_tool_call","call_id":"call","name":"exec","input":"text(await tools.exec_command({cmd:\"sleep 270\"}));"}}),
    ]
}
#[test]
fn codex_pending_foreground_custom_call_is_busy() {
    assert_eq!(
        rollout_activity(&lines(&codex_start()), &SessionId("session".into())),
        SessionActivity::Busy
    );
}
#[test]
fn codex_completed_wrapper_does_not_hide_running_command() {
    let mut records = codex_start();
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call","output":[{"type":"input_text","text":"Script completed\nWall time 30.0 seconds\nOutput:\n"},{"type":"input_text","text":"{\"session_id\":88317,\"wall_time_seconds\":30.0,\"output\":\"\"}"}]}}));
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
}
#[test]
fn codex_completed_command_without_running_session_is_idle() {
    let mut records = codex_start();
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call","output":[{"type":"input_text","text":"Script completed\nWall time 1.0 seconds\nOutput:\n"},{"type":"input_text","text":"{\"exit_code\":0,\"wall_time_seconds\":1.0,\"output\":\"done\"}"}]}}));
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}

#[test]
fn claude_monitor_remains_busy_after_launch_returns_and_unrelated_work_finishes() {
    let records = [
        claude_tool("watch", "Monitor"),
        claude_result(
            "watch",
            "Monitor started (task b44939cpf, expires in 10m unless the source ends first; you get one notice at expiry). You will be notified on each event.",
        ),
        claude_tool("other", "Read"),
        claude_result("other", "finished"),
    ];
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
}

#[test]
fn claude_quoted_task_notification_cannot_clear_a_running_monitor() {
    let records = [
        claude_tool("watch", "Monitor"),
        claude_result(
            "watch",
            "Monitor started (task b44939cpf, expires in 10m unless the source ends first).",
        ),
        json!({"type":"user","sessionId":"session","message":{"content":"Please explain this example: <task-notification><task-id>b44939cpf</task-id><status>completed</status></task-notification>"}}),
    ];
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
}

#[test]
fn claude_sidechains_duplicates_errors_and_partial_records_are_safe() {
    let mut records = vec![
        claude_tool("call", "Bash"),
        claude_result("call", "done"),
        claude_tool("call", "Bash"),
    ];
    let mut sidechain = claude_tool("child", "Bash");
    sidechain["isSidechain"] = json!(true);
    records.push(sidechain);
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
    let mut error = claude_result(
        "failed",
        "Command running in background with ID: bad. Output is being written to: /tmp/bad",
    );
    error["message"]["content"][0]["is_error"] = json!(true);
    records.extend([claude_tool("failed", "Bash"), error]);
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
    assert_eq!(
        transcript_activity(
            &(lines(&records) + "\n{partial"),
            &SessionId("session".into())
        ),
        SessionActivity::Unknown
    );
}

#[tokio::test]
async fn claude_activity_reads_raw_file_and_checks_filename_identity() {
    use uw_core::adapter::HarnessAdapter;
    let id = "006d12d4-5826-495b-a799-c2998da1451c";
    let dir = std::env::temp_dir().join(format!("uw-activity-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{id}.jsonl"));
    let mut record = claude_tool("running", "Bash");
    record["sessionId"] = json!(id);
    std::fs::write(&path, lines(&[record])).unwrap();
    let mut session: uw_core::model::SessionSummary = serde_json::from_value(json!({
        "id": id, "harness":"ClaudeCode", "account":null, "cwd":"/tmp", "model":null,
        "first_seen":"2026-09-30T00:00:00Z", "last_seen":"2026-09-30T00:00:00Z", "launch_mode":"Interactive",
        "state_path":path.to_str().unwrap(), "last_active":null, "last_known_token_count":null,
        "context_window_size":null, "status":"Running", "compact_count":0
    })).unwrap_or_else(|_| panic!("use session fixture"));
    let adapter = uw_adapters::claude_code::ClaudeCodeAdapter::real(dir.join("cache"), "test");
    assert_eq!(
        adapter.session_activity(&session).await.unwrap(),
        SessionActivity::Busy
    );
    session.id = SessionId("other".into());
    assert_eq!(
        adapter.session_activity(&session).await.unwrap(),
        SessionActivity::Unknown
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn claude_terminal_notification_protects_its_new_turn() {
    let mut records = vec![
        claude_tool("launch", "Bash"),
        claude_result(
            "launch",
            "Command running in background with ID: task. Output is being written to: /tmp/task",
        ),
        json!({"type":"user","sessionId":"session","message":{"content":"<task-notification><task-id>task</task-id><status>completed</status></task-notification>"}}),
    ];
    assert_ne!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
    records.push(json!({"type":"system","subtype":"turn_duration","sessionId":"session"}));
    assert_eq!(
        transcript_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}

#[tokio::test]
async fn codex_activity_reads_raw_rollout_or_discovers_missing_path() {
    use uw_core::adapter::HarnessAdapter;
    let dir = std::env::temp_dir().join(format!("uw-rollout-activity-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rollout-2026-09-30T00-00-00-session.jsonl");
    std::fs::write(&path, lines(&codex_start())).unwrap();
    let mut session: uw_core::model::SessionSummary = serde_json::from_value(json!({
        "id":"session", "harness":"Codex", "model":null, "account":null, "cwd":"/tmp",
        "first_seen":"2026-09-30T00:00:00Z", "last_seen":"2026-09-30T00:00:00Z", "launch_mode":"Interactive",
        "state_path":path.to_str().unwrap(), "last_known_token_count":null, "context_window_size":null
    })).unwrap();
    let adapter = uw_adapters::codex::CodexAdapter::real().with_sessions_root(dir.clone());
    assert_eq!(
        adapter.session_activity(&session).await.unwrap(),
        SessionActivity::Busy
    );
    session.state_path = None;
    assert_eq!(
        adapter.session_activity(&session).await.unwrap(),
        SessionActivity::Busy
    );
    std::fs::write(&path, "{partial").unwrap();
    assert_eq!(
        adapter.session_activity(&session).await.unwrap(),
        SessionActivity::Unknown
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn codex_uncorrelated_exit_cannot_complete_yielded_command() {
    let mut records = codex_start();
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call","output":[{"type":"input_text","text":"{\"session_id\":88317,\"output\":\"\"}"}]}}));
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call","call_id":"poll","name":"exec","input":"text(await tools.write_stdin({session_id:88317})); text(await tools.exec_command({cmd:\"other\"}));"}}));
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"poll","output":[{"type":"input_text","text":"{\"exit_code\":0,\"output\":\"done\"}"}]}}));
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call","call_id":"terminal","name":"exec","input":"arbitrary"}}));
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"terminal","output":[{"type":"input_text","text":"{\"session_id\":88317,\"exit_code\":0,\"output\":\"done\"}"}]}}));
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}

#[test]
fn codex_session_meta_alternate_identity_and_disagreement() {
    let mut records = codex_start();
    records[0] = json!({"type":"session_meta","payload":{"session_id":"session"}});
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Busy
    );
    records[0]["payload"]["id"] = json!("other");
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Unknown
    );
}

#[test]
fn codex_exact_single_write_stdin_terminal_result_clears_matching_command() {
    let mut records = codex_start();
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call","output":[{"type":"input_text","text":"{\"session_id\":38134,\"output\":\"\"}"}]}}));
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call","call_id":"poll","name":"exec","input":"text(await tools.write_stdin({session_id:38134,chars:\"\",\"yield_time_ms\":1000,\"max_output_tokens\":1000}));\n"}}));
    records.push(json!({"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"poll","output":[{"type":"input_text","text":"{\"exit_code\":101,\"output\":\"done\"}"}]}}));
    assert_eq!(
        rollout_activity(&lines(&records), &SessionId("session".into())),
        SessionActivity::Idle
    );
}

#[test]
fn claude_partial_history_without_matching_tool_call_is_unknown() {
    assert_eq!(
        transcript_activity(
            &lines(&[claude_result("missing", "done")]),
            &SessionId("session".into())
        ),
        SessionActivity::Unknown
    );
    let record = json!({"type":"assistant","sessionId":"session","message":{"content":42}});
    assert_eq!(
        transcript_activity(&lines(&[record]), &SessionId("session".into())),
        SessionActivity::Unknown
    );
}
