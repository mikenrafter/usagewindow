use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use thiserror::Error;
use uw_core::model::*;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("not found")]
    NotFound,
}
pub type StoreResult<T> = Result<T, StoreError>;
pub struct Store {
    connection: Connection,
}
impl Store {
    pub fn open_memory() -> StoreResult<Self> {
        let c = Connection::open_in_memory()?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        let mut s = Self { connection: c };
        s.create_schema()?;
        Ok(s)
    }
    pub fn open(path: &str) -> StoreResult<Self> {
        let c = Connection::open(path)?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        let mut s = Self { connection: c };
        s.create_schema()?;
        Ok(s)
    }
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
    pub fn create_schema(&mut self) -> StoreResult<()> {
        self.connection.execute_batch(SCHEMA)?;
        Ok(())
    }
    pub fn insert_usage_sample(&self, s: &UsageSample) -> StoreResult<i64> {
        let tx = self.connection.unchecked_transaction()?;
        let mut id = 0;
        for (k, w) in &s.windows {
            let (kind, val) = encode_kind(&k.kind);
            tx.execute("INSERT INTO usage_samples(provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",params![json(&s.provider)?,opt_json(&s.account)?,kind,val,w.pct,w.resets_at,boolean(w.exceeded),boolean(w.active),json(&s.source)?,s.at,s.fetched_at,opt_json(&s.credits)?])?;
            id = tx.last_insert_rowid();
        }
        tx.commit()?;
        Ok(id)
    }
    pub fn read_usage_sample(&self, id: i64) -> StoreResult<UsageSample> {
        let r=self.connection.query_row("SELECT provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json FROM usage_samples WHERE id=?",[id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,f32>(4)?,r.get(5)?,r.get::<_,i64>(6)?,r.get::<_,i64>(7)?,r.get::<_,String>(8)?,r.get(9)?,r.get(10)?,r.get::<_,Option<String>>(11)?))).optional()?.ok_or(StoreError::NotFound)?;
        let provider: Provider = serde_json::from_str(&r.0)?;
        let account: Option<AccountId> = r.1.map(|x| serde_json::from_str(&x)).transpose()?;
        let at: DateTime<Utc> = r.9;
        let fetched_at: Option<DateTime<Utc>> = r.10;
        Ok(UsageSample {
            at,
            fetched_at,
            source: serde_json::from_str(&r.8)?,
            provider: provider.clone(),
            account,
            windows: std::collections::HashMap::from([(
                WindowKey {
                    provider,
                    kind: decode_kind(&r.2, &r.3),
                },
                UsageWindowState {
                    pct: r.4,
                    resets_at: r.5,
                    exceeded: r.6 != 0,
                    active: r.7 != 0,
                    scope: None,
                },
            )]),
            credits: r.11.map(|x| serde_json::from_str(&x)).transpose()?,
        })
    }
    pub fn all_usage_samples(&self) -> StoreResult<Vec<UsageSample>> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM usage_samples ORDER BY at,id")?;
        let ids: Vec<i64> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        ids.into_iter()
            .map(|id| self.read_usage_sample(id))
            .collect()
    }
    pub fn read_session(&self, id: &SessionId) -> StoreResult<SessionSummary> {
        let r = self.connection.query_row(
            "SELECT id,harness,model,account,cwd,state_path,context_window_size,last_known_token_count,launch_mode,pid,first_seen,last_seen,stopped_reason,superseded_by,reseeded_from FROM sessions WHERE id=?",
            [id.0.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, Option<String>>(3)?, row.get(4)?, row.get(5)?, row.get::<_, Option<i64>>(6)?, row.get::<_, Option<i64>>(7)?, row.get::<_, String>(8)?, row.get::<_, Option<i64>>(9)?, row.get(10)?, row.get(11)?, row.get::<_, Option<String>>(12)?, row.get::<_, Option<String>>(13)?, row.get::<_, Option<String>>(14)?)),
        ).optional()?.ok_or(StoreError::NotFound)?;
        Ok(SessionSummary {
            id: SessionId(r.0),
            harness: serde_json::from_str(&r.1)?,
            model: r.2.map(|x| serde_json::from_str(&x)).transpose()?,
            account: r.3.map(|x| serde_json::from_str(&x)).transpose()?,
            cwd: r.4,
            state_path: r.5,
            context_window_size: r.6.map(|x| x as u64),
            last_known_token_count: r.7.map(|x| x as u64),
            launch_mode: serde_json::from_str(&r.8)?,
            pid: r.9.map(|x| x as u32),
            first_seen: r.10,
            last_seen: r.11,
            stopped_reason: r.12.map(|x| serde_json::from_str(&x)).transpose()?,
            resume_marker: None,
            superseded_by: r.13.map(SessionId),
            reseeded_from: r.14.map(SessionId),
        })
    }
    pub fn list_sessions(&self) -> StoreResult<Vec<SessionSummary>> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM sessions ORDER BY last_seen DESC, id")?;
        let ids = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| self.read_session(&SessionId(id)))
            .collect()
    }
    pub fn resume_markers_for_session(&self, id: &SessionId) -> StoreResult<Vec<ResumeMarker>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,reason,resume_at,created_at,status,status_detail FROM resume_markers WHERE session_id=? ORDER BY created_at,id")?;
        let rows = stmt.query_map([id.0.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, reason, resume_at, created_at, status, detail) = row?;
            Ok(ResumeMarker {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                reason: serde_json::from_str(&reason)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                resume_at,
                created_at,
                status: decode_resume_status(&status, detail),
            })
        })
        .collect()
    }
    pub fn compaction_requests_for_session(
        &self,
        id: &SessionId,
    ) -> StoreResult<Vec<CompactionRequest>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,kind,prompt,reason,status,created_at FROM compaction_requests WHERE session_id=? ORDER BY created_at,id")?;
        let rows = stmt.query_map([id.0.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get(6)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, kind, prompt, reason, status, created_at) = row?;
            Ok(CompactionRequest {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                kind: serde_json::from_str(&kind)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                prompt,
                reason,
                status: decode_compaction_status(&status, None),
                created_at,
            })
        })
        .collect()
    }
    pub fn due_resume_markers(&self, now: DateTime<Utc>) -> StoreResult<Vec<ResumeMarker>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,reason,resume_at,created_at,status,status_detail FROM resume_markers WHERE status IN ('pending','scheduled') AND resume_at IS NOT NULL AND resume_at<=? ORDER BY resume_at,id")?;
        let rows = stmt.query_map([now], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, reason, resume_at, created_at, status, detail) = row?;
            Ok(ResumeMarker {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                reason: serde_json::from_str(&reason)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                resume_at,
                created_at,
                status: decode_resume_status(&status, detail),
            })
        })
        .collect()
    }
    pub fn insert_session(&self, s: &SessionSummary) -> StoreResult<()> {
        self.connection.execute("INSERT INTO sessions(id,harness,model,account,cwd,state_path,context_window_size,last_known_token_count,launch_mode,pid,first_seen,last_seen,stopped_reason,stopped_window_kind,superseded_by,reseeded_from) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",params![s.id.0,json(&s.harness)?,opt_json(&s.model)?,opt_json(&s.account)?,s.cwd,s.state_path,s.context_window_size.map(|x|x as i64),s.last_known_token_count.map(|x|x as i64),json(&s.launch_mode)?,s.pid.map(|x|x as i64),s.first_seen,s.last_seen,opt_json(&s.stopped_reason)?,Option::<String>::None,s.superseded_by.as_ref().map(|x|&x.0),s.reseeded_from.as_ref().map(|x|&x.0)])?;
        Ok(())
    }
    pub fn insert_reseed_summary(&self, summary: &ReseedSummary) -> StoreResult<()> {
        self.connection.execute("INSERT INTO idle_reseed_summaries(id,session_id,source_model,summary_text,token_count_before,token_count_after,created_at) VALUES(?,?,?,?,?,?,?)", params![summary.id.to_string(), summary.session_id.0, summary.source_model.0, summary.summary_text, summary.token_count_before as i64, summary.token_count_after as i64, summary.created_at])?;
        Ok(())
    }
    pub fn reseed_summaries_for_session(&self, id: &SessionId) -> StoreResult<Vec<ReseedSummary>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,source_model,summary_text,token_count_before,token_count_after,created_at FROM idle_reseed_summaries WHERE session_id=? ORDER BY created_at,id")?;
        let rows = stmt.query_map([id.0.as_str()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get(6)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, model, text, before, after, created_at) = row?;
            Ok(ReseedSummary {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                source_model: ModelId(model),
                summary_text: text,
                token_count_before: before as u64,
                token_count_after: after as u64,
                created_at,
            })
        })
        .collect()
    }
    pub fn link_reseeded_session(
        &self,
        new_id: &SessionId,
        source_id: &SessionId,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE sessions SET reseeded_from=? WHERE id=?",
            params![source_id.0, new_id.0],
        )?;
        Ok(())
    }
    pub fn keepalive_config(&self, id: &SessionId) -> StoreResult<KeepaliveState> {
        self.connection.query_row("SELECT session_id,enabled,last_ping_at,ping_day,ping_count FROM keepalive_config WHERE session_id=?", [id.0.as_str()], |r| Ok(KeepaliveState { session_id: SessionId(r.get(0)?), enabled: r.get::<_, i64>(1)? != 0, last_ping_at: r.get(2)?, ping_day: r.get(3)?, ping_count: r.get::<_, i64>(4)? as u32 })).optional()?.ok_or(StoreError::NotFound)
    }
    pub fn set_keepalive(&self, id: &SessionId, enabled: bool) -> StoreResult<()> {
        self.connection.execute("INSERT INTO keepalive_config(session_id,enabled,last_ping_at,ping_day,ping_count) VALUES(?,?,NULL,NULL,0) ON CONFLICT(session_id) DO UPDATE SET enabled=excluded.enabled", params![id.0, boolean(enabled)])?;
        Ok(())
    }
    pub fn record_keepalive_ping(&self, id: &SessionId, at: DateTime<Utc>) -> StoreResult<()> {
        let day = at.date_naive().to_string();
        self.connection.execute("UPDATE keepalive_config SET last_ping_at=?,ping_day=?,ping_count=CASE WHEN ping_day=? THEN ping_count+1 ELSE 1 END WHERE session_id=?", params![at, day, day, id.0])?;
        Ok(())
    }
    pub fn insert_resume_marker(&self, m: &ResumeMarker) -> StoreResult<()> {
        self.connection.execute("INSERT INTO resume_markers(id,session_id,reason,resume_at,created_at,status,status_detail) VALUES(?,?,?,?,?,?,?)",params![m.id.to_string(),m.session_id.0,json(&m.reason)?,m.resume_at,m.created_at,status_name(&m.status),status_detail(&m.status)])?;
        Ok(())
    }
    pub fn has_active_resume_marker(&self, session_id: &SessionId) -> StoreResult<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM resume_markers WHERE session_id=? AND status IN ('pending','scheduled'))",
            [session_id.0.as_str()],
            |row| row.get(0),
        )?)
    }
    pub fn update_resume_status(&self, id: uuid::Uuid, status: ResumeStatus) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE resume_markers SET status=?,status_detail=? WHERE id=?",
            params![status_name(&status), status_detail(&status), id.to_string()],
        )?;
        Ok(())
    }
    /// Claims a due resume marker before spawning the harness process.
    pub fn claim_resume_marker(&self, id: uuid::Uuid) -> StoreResult<bool> {
        Ok(self.connection.execute(
            "UPDATE resume_markers SET status='fired',status_detail=NULL WHERE id=? AND status IN ('pending','scheduled')",
            [id.to_string()],
        )? == 1)
    }
    pub fn insert_compaction_request(&self, request: &CompactionRequest) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO compaction_requests(id,session_id,kind,prompt,reason,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            params![request.id.to_string(), request.session_id.0, json(&request.kind)?, request.prompt, request.reason, compaction_status_name(&request.status), request.created_at, request.created_at],
        )?;
        Ok(())
    }
    pub fn pending_compaction_requests(&self) -> StoreResult<Vec<CompactionRequest>> {
        let mut statement = self.connection.prepare(
            "SELECT id,session_id,kind,prompt,reason,status,created_at FROM compaction_requests WHERE status='pending' ORDER BY created_at,id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get(6)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, kind, prompt, reason, status, created_at) = row?;
            Ok(CompactionRequest {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                kind: serde_json::from_str(&kind)?,
                prompt,
                reason,
                status: decode_compaction_status(&status, None),
                created_at,
            })
        })
        .collect()
    }
    /// Atomically claims a pending destructive delivery. False means another tick won.
    pub fn claim_compaction(&self, id: uuid::Uuid) -> StoreResult<bool> {
        Ok(self.connection.execute(
            "UPDATE compaction_requests SET status='sending',updated_at=? WHERE id=? AND status='pending'",
            params![Utc::now(), id.to_string()],
        )? == 1)
    }
    pub fn update_compaction_status(
        &self,
        id: uuid::Uuid,
        status: CompactionStatus,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE compaction_requests SET status=?,updated_at=? WHERE id=?",
            params![compaction_status_name(&status), Utc::now(), id.to_string()],
        )?;
        Ok(())
    }
    pub fn insert_threshold_override(&self, r: &ThresholdOverride) -> StoreResult<()> {
        self.connection.execute("INSERT INTO threshold_overrides(id,scope_kind,provider,model_value,session_value,field,value_json,updated_at) VALUES(?,?,?,?,?,?,?,?)",params![r.id.to_string(),r.scope_kind.as_str(),json(&r.provider)?,r.model_value.as_ref().map(|x|&x.0),r.session_value.as_ref().map(|x|&x.0),r.field,r.value_json,r.updated_at])?;
        Ok(())
    }
    pub fn set_threshold_override(&self, r: &ThresholdOverride) -> StoreResult<()> {
        self.connection.execute("INSERT INTO threshold_overrides(id,scope_kind,provider,model_value,session_value,field,value_json,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope_kind,provider,COALESCE(model_value,''),COALESCE(session_value,''),field) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at", params![r.id.to_string(),r.scope_kind.as_str(),json(&r.provider)?,r.model_value.as_ref().map(|x|&x.0),r.session_value.as_ref().map(|x|&x.0),r.field,r.value_json,r.updated_at])?;
        Ok(())
    }
    pub fn threshold_values(
        &self,
        scope: Option<&uw_core::model::ThresholdScope>,
    ) -> StoreResult<BTreeMap<String, String>> {
        let mut statement = self.connection.prepare("SELECT scope_kind,provider,model_value,session_value,field,value_json FROM threshold_overrides ORDER BY field")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let mut values = BTreeMap::new();
        for row in rows {
            let (kind, provider, model, session, field, value) = row?;
            let provider: Provider = serde_json::from_str(&provider)?;
            let matches = match scope {
                None => kind == "global",
                Some(s) => {
                    provider == s.provider
                        && model.as_deref() == s.model.as_ref().map(|x| x.0.as_str())
                        && session.as_deref() == s.session.as_ref().map(|x| x.0.as_str())
                }
            };
            if matches {
                values.insert(field, value);
            }
        }
        Ok(values)
    }
    pub fn cancel_resume_markers(&self, session_id: &SessionId) -> StoreResult<()> {
        self.connection.execute("UPDATE resume_markers SET status='cancelled',status_detail=NULL WHERE session_id=? AND status IN ('pending','scheduled')", [session_id.0.as_str()])?;
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct ThresholdOverride {
    pub id: uuid::Uuid,
    pub scope_kind: ThresholdScopeKind,
    pub provider: Provider,
    pub model_value: Option<ModelId>,
    pub session_value: Option<SessionId>,
    pub field: String,
    pub value_json: String,
    pub updated_at: DateTime<Utc>,
}
#[derive(Clone, Copy, Debug)]
pub enum ThresholdScopeKind {
    Global,
    Provider,
    Model,
    Session,
}
impl ThresholdScopeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Provider => "provider",
            Self::Model => "model",
            Self::Session => "session",
        }
    }
}
fn json<T: serde::Serialize>(x: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string(x)
}
fn opt_json<T: serde::Serialize>(x: &Option<T>) -> Result<Option<String>, serde_json::Error> {
    x.as_ref().map(json).transpose()
}
fn boolean(x: bool) -> i64 {
    if x { 1 } else { 0 }
}
fn encode_kind(k: &WindowKind) -> (&'static str, String) {
    match k {
        WindowKind::Rolling { minutes } => ("rolling", minutes.to_string()),
        WindowKind::WeeklyModel(m) => ("weekly_model", m.0.clone()),
        WindowKind::WeeklySurface(s) => ("weekly_surface", s.clone()),
        WindowKind::Custom(s) => ("custom", s.clone()),
    }
}
fn decode_kind(k: &str, v: &str) -> WindowKind {
    match k {
        "rolling" => WindowKind::Rolling {
            minutes: v.parse().unwrap_or_default(),
        },
        "weekly_model" => WindowKind::WeeklyModel(ModelId(v.into())),
        "weekly_surface" => WindowKind::WeeklySurface(v.into()),
        _ => WindowKind::Custom(v.into()),
    }
}
fn status_name(s: &ResumeStatus) -> &'static str {
    match s {
        ResumeStatus::Pending => "pending",
        ResumeStatus::Scheduled => "scheduled",
        ResumeStatus::Fired => "fired",
        ResumeStatus::Cancelled => "cancelled",
        ResumeStatus::Failed(_) => "failed",
    }
}
fn status_detail(s: &ResumeStatus) -> Option<&str> {
    if let ResumeStatus::Failed(x) = s {
        Some(x)
    } else {
        None
    }
}
fn decode_resume_status(status: &str, detail: Option<String>) -> ResumeStatus {
    match status {
        "pending" => ResumeStatus::Pending,
        "scheduled" => ResumeStatus::Scheduled,
        "fired" => ResumeStatus::Fired,
        "cancelled" => ResumeStatus::Cancelled,
        _ => ResumeStatus::Failed(detail.unwrap_or_else(|| "unknown failure".into())),
    }
}
fn compaction_status_name(s: &CompactionStatus) -> &'static str {
    match s {
        CompactionStatus::Pending => "pending",
        CompactionStatus::Sending => "sending",
        CompactionStatus::Sent => "sent",
        CompactionStatus::Failed(_) => "failed",
        CompactionStatus::Cancelled => "cancelled",
    }
}
fn decode_compaction_status(status: &str, detail: Option<String>) -> CompactionStatus {
    match status {
        "pending" => CompactionStatus::Pending,
        "sending" => CompactionStatus::Sending,
        "sent" => CompactionStatus::Sent,
        "cancelled" => CompactionStatus::Cancelled,
        _ => CompactionStatus::Failed(detail.unwrap_or_else(|| "unknown failure".into())),
    }
}
const SCHEMA: &str = r#"PRAGMA foreign_keys=ON;CREATE TABLE IF NOT EXISTS usage_samples(id INTEGER PRIMARY KEY,provider TEXT NOT NULL,account TEXT,window_kind TEXT NOT NULL,window_scope_value TEXT NOT NULL,pct REAL NOT NULL,resets_at TEXT,exceeded INTEGER NOT NULL,active INTEGER NOT NULL,source TEXT NOT NULL,at TEXT NOT NULL,fetched_at TEXT,credits_json TEXT);CREATE INDEX IF NOT EXISTS usage_samples_window_at ON usage_samples(provider,window_kind,window_scope_value,at);CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,harness TEXT NOT NULL,model TEXT,account TEXT,cwd TEXT NOT NULL,state_path TEXT,context_window_size INTEGER,last_known_token_count INTEGER,launch_mode TEXT NOT NULL,pid INTEGER,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,stopped_reason TEXT,stopped_window_kind TEXT,superseded_by TEXT,reseeded_from TEXT);CREATE TABLE IF NOT EXISTS resume_markers(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),reason TEXT NOT NULL,resume_at TEXT,created_at TEXT NOT NULL,status TEXT NOT NULL,status_detail TEXT);CREATE INDEX IF NOT EXISTS resume_markers_session_status ON resume_markers(session_id,status);CREATE UNIQUE INDEX IF NOT EXISTS resume_markers_active ON resume_markers(session_id) WHERE status IN ('pending','scheduled');CREATE TABLE IF NOT EXISTS compaction_requests(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),kind TEXT NOT NULL,prompt TEXT NOT NULL,reason TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE INDEX IF NOT EXISTS compaction_requests_session_status ON compaction_requests(session_id,status);CREATE TABLE IF NOT EXISTS threshold_overrides(id TEXT PRIMARY KEY,scope_kind TEXT NOT NULL,provider TEXT NOT NULL,model_value TEXT,session_value TEXT,field TEXT NOT NULL,value_json TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS threshold_overrides_key ON threshold_overrides(scope_kind,provider,COALESCE(model_value,''),COALESCE(session_value,''),field);CREATE TABLE IF NOT EXISTS idle_reseed_summaries(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,source_model TEXT NOT NULL,summary_text TEXT NOT NULL,token_count_before INTEGER NOT NULL,token_count_after INTEGER NOT NULL,created_at TEXT NOT NULL);CREATE TABLE IF NOT EXISTS keepalive_config(session_id TEXT PRIMARY KEY,enabled INTEGER NOT NULL,last_ping_at TEXT,ping_day TEXT,ping_count INTEGER NOT NULL DEFAULT 0);"#;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;
    fn session(id: &SessionId) -> SessionSummary {
        SessionSummary {
            id: id.clone(),
            harness: Provider::Codex,
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
    fn schema_and_sample_round_trip() {
        let mut s = Store::open_memory().unwrap();
        s.create_schema().unwrap();
        s.create_schema().unwrap();
        let sample = UsageSample {
            at: Utc::now(),
            fetched_at: None,
            source: UsageSource::LocalEstimate,
            provider: Provider::Codex,
            account: None,
            windows: HashMap::from([(
                WindowKey {
                    provider: Provider::Codex,
                    kind: WindowKind::Rolling { minutes: 300 },
                },
                UsageWindowState::new(12., false, true, None, None),
            )]),
            credits: None,
        };
        let id = s.insert_usage_sample(&sample).unwrap();
        assert_eq!(s.read_usage_sample(id).unwrap(), sample);
    }
    #[test]
    fn active_markers_are_unique() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        let m = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: sid,
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Pending,
        };
        s.insert_resume_marker(&m).unwrap();
        assert!(
            s.insert_resume_marker(&ResumeMarker {
                id: uuid::Uuid::new_v4(),
                ..m.clone()
            })
            .is_err()
        );
        s.update_resume_status(m.id, ResumeStatus::Cancelled)
            .unwrap();
        assert!(
            s.insert_resume_marker(&ResumeMarker {
                id: uuid::Uuid::new_v4(),
                ..m
            })
            .is_ok()
        );
    }

    #[test]
    fn claim_resume_marker_is_atomic() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        let marker = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: sid,
            reason: ResumeReason::ManuallyMarked,
            resume_at: Some(Utc::now()),
            created_at: Utc::now(),
            status: ResumeStatus::Scheduled,
        };
        s.insert_resume_marker(&marker).unwrap();
        assert!(s.claim_resume_marker(marker.id).unwrap());
        assert!(!s.claim_resume_marker(marker.id).unwrap());
        assert_eq!(
            s.resume_markers_for_session(&marker.session_id).unwrap()[0].status,
            ResumeStatus::Fired
        );
    }
    #[test]
    fn threshold_key_is_unique() {
        let s = Store::open_memory().unwrap();
        let r = ThresholdOverride {
            id: uuid::Uuid::new_v4(),
            scope_kind: ThresholdScopeKind::Provider,
            provider: Provider::Codex,
            model_value: None,
            session_value: None,
            field: "closing_pct".into(),
            value_json: "80".into(),
            updated_at: Utc::now(),
        };
        s.insert_threshold_override(&r).unwrap();
        assert!(
            s.insert_threshold_override(&ThresholdOverride {
                id: uuid::Uuid::new_v4(),
                ..r
            })
            .is_err()
        );
    }
    #[test]
    fn read_queries_list_sessions_and_related_records() {
        let s = Store::open_memory().unwrap();
        let id = SessionId("query-session".into());
        s.insert_session(&session(&id)).unwrap();
        assert_eq!(s.list_sessions().unwrap().len(), 1);
        assert!(s.resume_markers_for_session(&id).unwrap().is_empty());
        assert!(s.compaction_requests_for_session(&id).unwrap().is_empty());
    }
}
