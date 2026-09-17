use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
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
    pub fn insert_resume_marker(&self, m: &ResumeMarker) -> StoreResult<()> {
        self.connection.execute("INSERT INTO resume_markers(id,session_id,reason,resume_at,created_at,status,status_detail) VALUES(?,?,?,?,?,?,?)",params![m.id,m.session_id.0,json(&m.reason)?,m.resume_at,m.created_at,status_name(&m.status),status_detail(&m.status)])?;
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
            params![status_name(&status), status_detail(&status), id],
        )?;
        Ok(())
    }
    pub fn insert_compaction_request(&self, request: &CompactionRequest) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO compaction_requests(id,session_id,kind,prompt,reason,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            params![request.id, request.session_id.0, json(&request.kind)?, request.prompt, request.reason, compaction_status_name(&request.status), request.created_at, request.created_at],
        )?;
        Ok(())
    }
    pub fn pending_compaction_requests(&self) -> StoreResult<Vec<CompactionRequest>> {
        let mut statement = self.connection.prepare(
            "SELECT id,session_id,kind,prompt,reason,status,status_detail,created_at FROM compaction_requests WHERE status='pending' ORDER BY created_at,id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get(7)?,
            ))
        })?;
        rows.map(|row| {
            let (id, session_id, kind, prompt, reason, status, detail, created_at) = row?;
            Ok(CompactionRequest {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                session_id: SessionId(session_id),
                kind: serde_json::from_str(&kind)?,
                prompt,
                reason,
                status: decode_compaction_status(&status, detail),
                created_at,
            })
        })
        .collect()
    }
    /// Atomically claims a pending destructive delivery. False means another tick won.
    pub fn claim_compaction(&self, id: uuid::Uuid) -> StoreResult<bool> {
        Ok(self.connection.execute(
            "UPDATE compaction_requests SET status='sending',updated_at=? WHERE id=? AND status='pending'",
            params![Utc::now(), id],
        )? == 1)
    }
    pub fn update_compaction_status(
        &self,
        id: uuid::Uuid,
        status: CompactionStatus,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE compaction_requests SET status=?,status_detail=?,updated_at=? WHERE id=?",
            params![
                compaction_status_name(&status),
                compaction_status_detail(&status),
                Utc::now(),
                id
            ],
        )?;
        Ok(())
    }
    pub fn insert_threshold_override(&self, r: &ThresholdOverride) -> StoreResult<()> {
        self.connection.execute("INSERT INTO threshold_overrides(id,scope_kind,provider,model_value,session_value,field,value_json,updated_at) VALUES(?,?,?,?,?,?,?,?)",params![r.id,r.scope_kind.as_str(),json(&r.provider)?,r.model_value.as_ref().map(|x|&x.0),r.session_value.as_ref().map(|x|&x.0),r.field,r.value_json,r.updated_at])?;
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
fn compaction_status_detail(s: &CompactionStatus) -> Option<&str> {
    if let CompactionStatus::Failed(detail) = s {
        Some(detail)
    } else {
        None
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
const SCHEMA: &str = r#"PRAGMA foreign_keys=ON;CREATE TABLE IF NOT EXISTS usage_samples(id INTEGER PRIMARY KEY,provider TEXT NOT NULL,account TEXT,window_kind TEXT NOT NULL,window_scope_value TEXT NOT NULL,pct REAL NOT NULL,resets_at TEXT,exceeded INTEGER NOT NULL,active INTEGER NOT NULL,source TEXT NOT NULL,at TEXT NOT NULL,fetched_at TEXT,credits_json TEXT);CREATE INDEX IF NOT EXISTS usage_samples_window_at ON usage_samples(provider,window_kind,window_scope_value,at);CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,harness TEXT NOT NULL,model TEXT,account TEXT,cwd TEXT NOT NULL,state_path TEXT,context_window_size INTEGER,last_known_token_count INTEGER,launch_mode TEXT NOT NULL,pid INTEGER,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,stopped_reason TEXT,stopped_window_kind TEXT,superseded_by TEXT,reseeded_from TEXT);CREATE TABLE IF NOT EXISTS resume_markers(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),reason TEXT NOT NULL,resume_at TEXT,created_at TEXT NOT NULL,status TEXT NOT NULL,status_detail TEXT);CREATE INDEX IF NOT EXISTS resume_markers_session_status ON resume_markers(session_id,status);CREATE UNIQUE INDEX IF NOT EXISTS resume_markers_active ON resume_markers(session_id) WHERE status IN ('pending','scheduled');CREATE TABLE IF NOT EXISTS compaction_requests(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),kind TEXT NOT NULL,prompt TEXT NOT NULL,reason TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE INDEX IF NOT EXISTS compaction_requests_session_status ON compaction_requests(session_id,status);CREATE TABLE IF NOT EXISTS threshold_overrides(id TEXT PRIMARY KEY,scope_kind TEXT NOT NULL,provider TEXT NOT NULL,model_value TEXT,session_value TEXT,field TEXT NOT NULL,value_json TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS threshold_overrides_key ON threshold_overrides(scope_kind,provider,COALESCE(model_value,''),COALESCE(session_value,''),field);CREATE TABLE IF NOT EXISTS idle_reseed_summaries(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,source_model TEXT NOT NULL,summary_text TEXT NOT NULL,token_count_before INTEGER NOT NULL,token_count_after INTEGER NOT NULL,created_at TEXT NOT NULL);CREATE TABLE IF NOT EXISTS keepalive_config(session_id TEXT PRIMARY KEY,enabled INTEGER NOT NULL,last_ping_at TEXT);"#;

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
}
