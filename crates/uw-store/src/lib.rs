use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::BTreeMap;
use thiserror::Error;
use uw_core::adapter::TokenUsageRecord;
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
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        let mut s = Self { connection: c };
        s.create_schema()?;
        Ok(s)
    }
    pub fn open(path: &str) -> StoreResult<Self> {
        let c = Connection::open(path)?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        let mut s = Self { connection: c };
        s.create_schema()?;
        Ok(s)
    }
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
    pub fn create_schema(&mut self) -> StoreResult<()> {
        self.connection.execute_batch(SCHEMA)?;
        // `CREATE TABLE IF NOT EXISTS` above only shapes brand-new databases;
        // existing ones predating this column need it added by hand.
        if let Err(err) = self.connection.execute(
            "ALTER TABLE resume_markers ADD COLUMN requested_at TEXT",
            [],
        ) && !err.to_string().contains("duplicate column name")
        {
            return Err(err.into());
        }
        if let Err(err) = self.connection.execute(
            "ALTER TABLE compaction_requests ADD COLUMN status_detail TEXT",
            [],
        ) && !err.to_string().contains("duplicate column name")
        {
            return Err(err.into());
        }
        if let Err(err) = self
            .connection
            .execute("ALTER TABLE sessions ADD COLUMN title TEXT", [])
            && !err.to_string().contains("duplicate column name")
        {
            return Err(err.into());
        }
        if let Err(err) = self
            .connection
            .execute("ALTER TABLE usage_samples ADD COLUMN plan TEXT", [])
            && !err.to_string().contains("duplicate column name")
        {
            return Err(err.into());
        }
        Ok(())
    }
    pub fn insert_usage_sample(&self, s: &UsageSample) -> StoreResult<i64> {
        let tx = self.connection.unchecked_transaction()?;
        let mut id = 0;
        for (k, w) in &s.windows {
            let (kind, val) = encode_kind(&k.kind);
            tx.execute("INSERT INTO usage_samples(provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json,plan) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",params![json(&s.provider)?,opt_json(&s.account)?,kind,val,w.pct,w.resets_at,boolean(w.exceeded),boolean(w.active),json(&s.source)?,s.at,s.fetched_at,opt_json(&s.credits)?,s.plan.as_deref()])?;
            id = tx.last_insert_rowid();
        }
        tx.commit()?;
        Ok(id)
    }
    pub fn read_usage_sample(&self, id: i64) -> StoreResult<UsageSample> {
        self.connection
            .query_row(
                "SELECT provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json,plan FROM usage_samples WHERE id=?",
                [id],
                decode_usage_sample_row,
            )
            .optional()?
            .ok_or(StoreError::NotFound)
    }
    /// Single query, decoded row-by-row — replaces the old id-then-refetch (N+1) path.
    pub fn all_usage_samples(&self) -> StoreResult<Vec<UsageSample>> {
        let mut stmt = self.connection.prepare(
            "SELECT provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json,plan FROM usage_samples ORDER BY at,id",
        )?;
        let rows = stmt.query_map([], decode_usage_sample_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    /// Same rows as `all_usage_samples`, filtered at the SQL layer (hits the
    /// `(provider,window_kind,window_scope_value,at)` index) instead of loading every
    /// provider's/account's history just to discard most of it in Rust afterward.
    pub fn usage_samples_for(
        &self,
        provider: &Provider,
        account: Option<&AccountId>,
    ) -> StoreResult<Vec<UsageSample>> {
        let mut stmt = self.connection.prepare(
            "SELECT provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json,plan FROM usage_samples WHERE provider=?1 AND account IS ?2 ORDER BY at,id",
        )?;
        let rows = stmt.query_map(
            params![json(provider)?, account.map(json).transpose()?],
            decode_usage_sample_row,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    /// Return only the newest row for each usage window in a provider/account.
    /// The daemon records one row per window per poll, so callers displaying
    /// current status should never need to materialize the retention history.
    pub fn latest_usage_samples_for(
        &self,
        provider: Option<&Provider>,
        account: Option<&AccountId>,
    ) -> StoreResult<Vec<UsageSample>> {
        let sql = "SELECT provider,account,window_kind,window_scope_value,pct,resets_at,exceeded,active,source,at,fetched_at,credits_json,plan FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY provider,account,window_kind,window_scope_value ORDER BY at DESC,id DESC) AS rank FROM usage_samples WHERE (?1 IS NULL OR provider=?1) AND (?2 IS NULL OR account IS ?2)) WHERE rank=1 ORDER BY provider,account,window_kind,window_scope_value";
        let mut stmt = self.connection.prepare(sql)?;
        let rows = stmt.query_map(
            params![
                provider.map(json).transpose()?,
                account.map(json).transpose()?
            ],
            decode_usage_sample_row,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn prune_usage_before(&self, cutoff: DateTime<Utc>) -> StoreResult<usize> {
        Ok(self
            .connection
            .execute("DELETE FROM usage_samples WHERE at<?", [cutoff])?)
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
    /// Sessions whose id, cwd, or model contain `query` (case-insensitive substring),
    /// most recently seen first. `None`/empty behaves like `list_sessions`.
    pub fn search_sessions(&self, query: Option<&str>) -> StoreResult<Vec<SessionSummary>> {
        let Some(query) = query.filter(|q| !q.is_empty()) else {
            return self.list_sessions();
        };
        let pattern = format!("%{}%", query.replace('%', "\\%").replace('_', "\\_"));
        let mut stmt = self.connection.prepare(
            "SELECT id FROM sessions WHERE id LIKE ?1 ESCAPE '\\' OR cwd LIKE ?1 ESCAPE '\\' OR model LIKE ?1 ESCAPE '\\' ORDER BY last_seen DESC, id",
        )?;
        let ids = stmt
            .query_map([&pattern], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| self.read_session(&SessionId(id)))
            .collect()
    }
    /// Updates the user-editable fields on a tracked session (cwd/model/account).
    /// Everything else (stop state, resume lineage, token counts, ...) is
    /// system-managed and stays out of this path.
    pub fn update_session_editable(
        &self,
        id: &SessionId,
        cwd: &str,
        model: Option<&ModelId>,
        account: Option<&AccountId>,
    ) -> StoreResult<()> {
        let updated = self.connection.execute(
            "UPDATE sessions SET cwd=?,model=?,account=? WHERE id=?",
            params![cwd, opt_json(&model)?, opt_json(&account)?, id.0],
        )?;
        if updated == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }
    /// Removes a tracked session and everything keyed to it. `resume_markers` and
    /// `compaction_requests` are FK-enforced against `sessions`, so those rows must go
    /// first; the rest (`idle_reseed_summaries`, `keepalive_config`, `hook_messages`,
    /// `hook_events`) aren't FK-enforced but would otherwise be orphaned.
    pub fn delete_session(&self, id: &SessionId) -> StoreResult<()> {
        let tx = self.connection.unchecked_transaction()?;
        for table in [
            "resume_markers",
            "compaction_requests",
            "idle_reseed_summaries",
            "keepalive_config",
            "hook_messages",
            "hook_events",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE session_id=?"),
                [id.0.as_str()],
            )?;
        }
        let deleted = tx.execute("DELETE FROM sessions WHERE id=?", [id.0.as_str()])?;
        tx.commit()?;
        if deleted == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
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
    /// Returns the shared five-minute cache-warm approximation for a session.
    pub fn session_cache_warm(&self, id: &SessionId, now: DateTime<Utc>) -> StoreResult<bool> {
        let session = self.read_session(id)?;
        Ok(is_cache_warm(session.last_seen, now))
    }
    pub fn resume_markers_for_session(&self, id: &SessionId) -> StoreResult<Vec<ResumeMarker>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,reason,resume_at,requested_at,created_at,status,status_detail,message FROM resume_markers WHERE session_id=? ORDER BY created_at,id")?;
        let rows = stmt.query_map([id.0.as_str()], decode_resume_marker_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn active_resume_marker(&self, id: &SessionId) -> StoreResult<Option<ResumeMarker>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,reason,resume_at,requested_at,created_at,status,status_detail,message FROM resume_markers WHERE session_id=? AND status IN ('pending','scheduled')")?;
        Ok(stmt
            .query_row([id.0.as_str()], decode_resume_marker_row)
            .optional()?)
    }
    pub fn compaction_requests_for_session(
        &self,
        id: &SessionId,
    ) -> StoreResult<Vec<CompactionRequest>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,kind,prompt,reason,status,status_detail,created_at FROM compaction_requests WHERE session_id=? ORDER BY created_at,id")?;
        let rows = stmt.query_map([id.0.as_str()], |row| {
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
                kind: serde_json::from_str(&kind)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                prompt,
                reason,
                status: decode_compaction_status(&status, detail),
                created_at,
            })
        })
        .collect()
    }
    pub fn due_resume_markers(&self, now: DateTime<Utc>) -> StoreResult<Vec<ResumeMarker>> {
        let mut stmt = self.connection.prepare("SELECT id,session_id,reason,resume_at,requested_at,created_at,status,status_detail,message FROM resume_markers WHERE status IN ('pending','scheduled') AND resume_at IS NOT NULL AND resume_at<=? ORDER BY resume_at,id")?;
        let rows = stmt.query_map([now], decode_resume_marker_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn insert_session(&self, s: &SessionSummary) -> StoreResult<()> {
        self.connection.execute("INSERT INTO sessions(id,harness,model,account,cwd,state_path,context_window_size,last_known_token_count,launch_mode,pid,first_seen,last_seen,stopped_reason,stopped_window_kind,superseded_by,reseeded_from) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",params![s.id.0,json(&s.harness)?,opt_json(&s.model)?,opt_json(&s.account)?,s.cwd,s.state_path,s.context_window_size.map(|x|x as i64),s.last_known_token_count.map(|x|x as i64),json(&s.launch_mode)?,s.pid.map(|x|x as i64),s.first_seen,s.last_seen,opt_json(&s.stopped_reason)?,Option::<String>::None,s.superseded_by.as_ref().map(|x|&x.0),s.reseeded_from.as_ref().map(|x|&x.0)])?;
        Ok(())
    }
    pub fn upsert_session(&self, s: &SessionSummary) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO sessions(id,harness,model,account,cwd,state_path,context_window_size,last_known_token_count,launch_mode,pid,first_seen,last_seen,stopped_reason,stopped_window_kind,superseded_by,reseeded_from) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET harness=excluded.harness,model=COALESCE(excluded.model,sessions.model),account=COALESCE(excluded.account,sessions.account),cwd=excluded.cwd,state_path=COALESCE(excluded.state_path,sessions.state_path),context_window_size=COALESCE(excluded.context_window_size,sessions.context_window_size),last_known_token_count=COALESCE(excluded.last_known_token_count,sessions.last_known_token_count),pid=COALESCE(excluded.pid,sessions.pid),last_seen=MAX(sessions.last_seen,excluded.last_seen)",
            params![s.id.0,json(&s.harness)?,opt_json(&s.model)?,opt_json(&s.account)?,s.cwd,s.state_path,s.context_window_size.map(|x|x as i64),s.last_known_token_count.map(|x|x as i64),json(&s.launch_mode)?,s.pid.map(|x|x as i64),s.first_seen,s.last_seen,opt_json(&s.stopped_reason)?,Option::<String>::None,s.superseded_by.as_ref().map(|x|&x.0),s.reseeded_from.as_ref().map(|x|&x.0)],
        )?;
        Ok(())
    }
    /// Sets a session's title (e.g. from a discovered `ai-title` transcript
    /// record). A `None`/empty title from discovery never overwrites a title
    /// this session already has — discovery runs on every tick and not every
    /// pass has fresh title evidence.
    pub fn set_session_title(&self, id: &SessionId, title: &str) -> StoreResult<()> {
        if title.is_empty() {
            return Ok(());
        }
        self.connection.execute(
            "UPDATE sessions SET title=? WHERE id=?",
            params![title, id.0],
        )?;
        Ok(())
    }
    /// Resolves the title to show for a session: its own title if it has one,
    /// else the nearest ancestor's title by walking `reseeded_from`, so a
    /// freshly resumed session shows the conversation's real title instead of
    /// a raw id until it earns a title of its own. Bounded to guard against a
    /// cycle in `reseeded_from` (which should never happen, but a UI lookup
    /// must never hang on bad data).
    pub fn resolve_session_title(&self, id: &SessionId) -> StoreResult<Option<String>> {
        let mut current = id.clone();
        for _ in 0..32 {
            let row = self.connection.query_row(
                "SELECT title, reseeded_from FROM sessions WHERE id=?",
                [current.0.as_str()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            );
            let (title, reseeded_from) = match row.optional()? {
                Some(found) => found,
                None => return Ok(None),
            };
            if let Some(title) = title.filter(|t| !t.is_empty()) {
                return Ok(Some(title));
            }
            match reseeded_from {
                Some(next) if next != current.0 => current = SessionId(next),
                _ => return Ok(None),
            }
        }
        Ok(None)
    }
    pub fn update_session_observation(
        &self,
        id: &SessionId,
        last_seen: DateTime<Utc>,
        pid: Option<u32>,
        token_count: Option<u64>,
        context_window_size: Option<u64>,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE sessions SET last_seen=?,pid=COALESCE(?,pid),last_known_token_count=COALESCE(?,last_known_token_count),context_window_size=COALESCE(?,context_window_size) WHERE id=?",
            params![
                last_seen,
                pid.map(i64::from),
                token_count.map(|value| value as i64),
                context_window_size.map(|value| value as i64),
                id.0
            ],
        )?;
        Ok(())
    }
    pub fn insert_token_usage_records(
        &self,
        session_id: &SessionId,
        records: &[TokenUsageRecord],
    ) -> StoreResult<()> {
        let tx = self.connection.unchecked_transaction()?;
        for record in records {
            tx.execute(
                "INSERT OR IGNORE INTO session_token_usage(session_id,at,model,input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_output_tokens,total_tokens) VALUES(?,?,?,?,?,?,?,?,?)",
                params![
                    session_id.0,
                    record.at,
                    opt_json(&record.model)?,
                    record.input_tokens as i64,
                    record.cached_input_tokens as i64,
                    record.cache_write_input_tokens as i64,
                    record.output_tokens as i64,
                    record.reasoning_output_tokens as i64,
                    record.total_tokens as i64,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn token_usage_for_session(
        &self,
        session_id: &SessionId,
    ) -> StoreResult<Vec<TokenUsageRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT at,model,input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_output_tokens,total_tokens FROM session_token_usage WHERE session_id=? ORDER BY at",
        )?;
        let rows = statement.query_map([session_id.0.as_str()], |row| {
            let model: Option<String> = row.get(1)?;
            Ok(TokenUsageRecord {
                at: row.get(0)?,
                model: model
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                input_tokens: row.get::<_, i64>(2)? as u64,
                cached_input_tokens: row.get::<_, i64>(3)? as u64,
                cache_write_input_tokens: row.get::<_, i64>(4)? as u64,
                output_tokens: row.get::<_, i64>(5)? as u64,
                reasoning_output_tokens: row.get::<_, i64>(6)? as u64,
                total_tokens: row.get::<_, i64>(7)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn update_session_stop(
        &self,
        id: &SessionId,
        reason: Option<&StopReason>,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE sessions SET stopped_reason=? WHERE id=?",
            params![reason.map(json).transpose()?, id.0],
        )?;
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
    pub fn last_reseed_at(&self, id: &SessionId) -> StoreResult<Option<DateTime<Utc>>> {
        Ok(self.connection.query_row(
            "SELECT MAX(created_at) FROM idle_reseed_summaries WHERE session_id=?",
            [id.0.as_str()],
            |row| row.get(0),
        )?)
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
        self.connection.execute("INSERT INTO resume_markers(id,session_id,reason,resume_at,requested_at,created_at,status,status_detail,message) VALUES(?,?,?,?,?,?,?,?,?)",params![m.id.to_string(),m.session_id.0,json(&m.reason)?,m.resume_at,m.requested_at,m.created_at,status_name(&m.status),status_detail(&m.status),m.message])?;
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
    /// Resolves a manually queued marker's fire time against the policy engine's
    /// window/burn-rate floor. Only ever moves `pending` markers to `scheduled`;
    /// never touches a marker that's already scheduled, fired, or cancelled.
    pub fn set_resume_at(&self, id: uuid::Uuid, resume_at: DateTime<Utc>) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE resume_markers SET resume_at=?,status='scheduled',status_detail=NULL WHERE id=? AND status='pending'",
            params![resume_at, id.to_string()],
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
            "INSERT INTO compaction_requests(id,session_id,kind,prompt,reason,status,status_detail,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            params![request.id.to_string(), request.session_id.0, json(&request.kind)?, request.prompt, request.reason, compaction_status_name(&request.status), compaction_status_detail(&request.status), request.created_at, request.created_at],
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
            "UPDATE compaction_requests SET status='sending',status_detail=NULL,updated_at=? WHERE id=? AND status='pending'",
            params![Utc::now(), id.to_string()],
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
                id.to_string()
            ],
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
    /// Cancels pending compaction deliveries only. Rows already claimed
    /// (`sending`) are left alone — overwriting them races the daemon tick.
    pub fn cancel_compaction_requests(&self, session_id: &SessionId) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE compaction_requests SET status='cancelled',status_detail=NULL,updated_at=? WHERE session_id=? AND status='pending'",
            params![Utc::now(), session_id.0.as_str()],
        )?;
        Ok(())
    }
    pub fn enqueue_hook_message(
        &self,
        session_id: &SessionId,
        event: &str,
        text: &str,
    ) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO hook_messages(id,session_id,event,text,created_at,delivered_at) VALUES(?,?,?,?,?,NULL)",
            params![uuid::Uuid::new_v4().to_string(), session_id.0, event, text, Utc::now()],
        )?;
        Ok(())
    }
    pub fn record_hook_event(&self, session_id: &SessionId, event: &str) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO hook_events(id,session_id,event,created_at) VALUES(?,?,?,?)",
            params![
                uuid::Uuid::new_v4().to_string(),
                session_id.0,
                event,
                Utc::now()
            ],
        )?;
        Ok(())
    }
    pub fn latest_hook_event(&self, session_id: &SessionId) -> StoreResult<Option<String>> {
        Ok(self
            .connection
            .query_row(
                "SELECT event FROM hook_events WHERE session_id=? ORDER BY created_at DESC,id DESC LIMIT 1",
                [session_id.0.as_str()],
                |row| row.get(0),
            )
            .optional()?)
    }
    pub fn record_fetch_success(
        &self,
        provider: &Provider,
        account: Option<&AccountId>,
        at: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO provider_fetch_status(provider,account,last_attempt_at,last_success_at,last_error,consecutive_failures) VALUES(?,?,?,?,NULL,0) ON CONFLICT(provider,COALESCE(account,'')) DO UPDATE SET last_attempt_at=excluded.last_attempt_at,last_success_at=excluded.last_success_at,last_error=NULL,consecutive_failures=0",
            params![json(provider)?, opt_json(&account)?, at, at],
        )?;
        Ok(())
    }
    pub fn record_fetch_failure(
        &self,
        provider: &Provider,
        account: Option<&AccountId>,
        at: DateTime<Utc>,
        error: &str,
    ) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO provider_fetch_status(provider,account,last_attempt_at,last_success_at,last_error,consecutive_failures) VALUES(?,?,?,NULL,?,1) ON CONFLICT(provider,COALESCE(account,'')) DO UPDATE SET last_attempt_at=excluded.last_attempt_at,last_error=excluded.last_error,consecutive_failures=provider_fetch_status.consecutive_failures+1",
            params![json(provider)?, opt_json(&account)?, at, error],
        )?;
        Ok(())
    }
    pub fn fetch_statuses(&self) -> StoreResult<Vec<ProviderFetchStatus>> {
        let mut statement = self.connection.prepare(
            "SELECT provider,account,last_attempt_at,last_success_at,last_error,consecutive_failures FROM provider_fetch_status ORDER BY provider,account",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (provider, account, last_attempt_at, last_success_at, last_error, failures) = row?;
            Ok(ProviderFetchStatus {
                provider: serde_json::from_str(&provider)?,
                account: account.map(|x| serde_json::from_str(&x)).transpose()?,
                last_attempt_at,
                last_success_at,
                last_error,
                consecutive_failures: failures as u32,
            })
        })
        .collect()
    }
    pub fn keepalive_active_count(&self, now: DateTime<Utc>) -> StoreResult<u32> {
        let mut count = 0u32;
        let mut stmt = self
            .connection
            .prepare("SELECT session_id FROM keepalive_config WHERE enabled=1")?;
        let ids = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for id in ids {
            let session_id = SessionId(id);
            let session = self.read_session(&session_id)?;
            let records = self.token_usage_for_session(&session_id)?;
            if uw_core::model::is_keepalive_eligible(&session, &records, now) {
                count += 1;
            }
        }
        Ok(count)
    }
    pub fn insert_compaction_event(&self, event: &CompactionEvent) -> StoreResult<()> {
        self.connection.execute(
            "INSERT INTO compaction_events(id,session_id,source,trigger,context_pct_before,usage_window_pct_before,tokens_before,tokens_after,started_at,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            params![
                event.id.to_string(),
                event.session_id.0,
                compaction_source_name(&event.source),
                event.trigger,
                event.context_pct_before,
                event.usage_window_pct_before,
                event.tokens_before.map(|x| x as i64),
                event.tokens_after.map(|x| x as i64),
                event.started_at,
                event.completed_at,
            ],
        )?;
        Ok(())
    }
    /// Fills in the most recently opened (`completed_at IS NULL`) compaction event for
    /// this session. There should only ever be one open row per session, but we still
    /// pick the latest by `started_at` in case of overlap.
    pub fn complete_compaction_event(
        &self,
        session_id: &SessionId,
        tokens_after: Option<u64>,
        at: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.connection.execute(
            "UPDATE compaction_events SET completed_at=?,tokens_after=COALESCE(?,tokens_after) WHERE id=(SELECT id FROM compaction_events WHERE session_id=? AND completed_at IS NULL ORDER BY started_at DESC LIMIT 1)",
            params![at, tokens_after.map(|x| x as i64), session_id.0],
        )?;
        Ok(())
    }
    pub fn recent_compaction_events(&self, limit: u32) -> StoreResult<Vec<CompactionEvent>> {
        let mut statement = self.connection.prepare(
            "SELECT id,session_id,source,trigger,context_pct_before,usage_window_pct_before,tokens_before,tokens_after,started_at,completed_at FROM compaction_events ORDER BY started_at DESC,id DESC LIMIT ?",
        )?;
        let rows = statement.query_map([limit as i64], decode_compaction_event_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn compaction_events_for_session(
        &self,
        id: &SessionId,
    ) -> StoreResult<Vec<CompactionEvent>> {
        let mut statement = self.connection.prepare(
            "SELECT id,session_id,source,trigger,context_pct_before,usage_window_pct_before,tokens_before,tokens_after,started_at,completed_at FROM compaction_events WHERE session_id=? ORDER BY started_at,id",
        )?;
        let rows = statement.query_map([id.0.as_str()], decode_compaction_event_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
    pub fn take_hook_messages(
        &self,
        session_id: &SessionId,
        event: &str,
    ) -> StoreResult<Vec<String>> {
        let transaction = self.connection.unchecked_transaction()?;
        let messages = {
            let mut statement = transaction.prepare(
                "SELECT id,text FROM hook_messages WHERE session_id=? AND event=? AND delivered_at IS NULL ORDER BY created_at,id",
            )?;
            statement
                .query_map(params![session_id.0, event], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut claimed = Vec::new();
        for (id, text) in messages {
            if transaction.execute(
                "UPDATE hook_messages SET delivered_at=? WHERE id=? AND delivered_at IS NULL",
                params![Utc::now(), id],
            )? == 1
            {
                claimed.push(text);
            }
        }
        transaction.commit()?;
        Ok(claimed)
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
fn json_err(e: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}
fn decode_usage_sample_row(row: &rusqlite::Row) -> rusqlite::Result<UsageSample> {
    let provider: String = row.get(0)?;
    let account: Option<String> = row.get(1)?;
    let window_kind: String = row.get(2)?;
    let window_scope_value: String = row.get(3)?;
    let pct: f32 = row.get(4)?;
    let resets_at = row.get(5)?;
    let exceeded: i64 = row.get(6)?;
    let active: i64 = row.get(7)?;
    let source: String = row.get(8)?;
    let at: DateTime<Utc> = row.get(9)?;
    let fetched_at: Option<DateTime<Utc>> = row.get(10)?;
    let credits_json: Option<String> = row.get(11)?;
    let plan: Option<String> = row.get(12)?;
    let provider: Provider = serde_json::from_str(&provider).map_err(json_err)?;
    let account: Option<AccountId> = account
        .map(|x| serde_json::from_str(&x))
        .transpose()
        .map_err(json_err)?;
    Ok(UsageSample {
        at,
        fetched_at,
        source: serde_json::from_str(&source).map_err(json_err)?,
        provider: provider.clone(),
        account,
        plan,
        windows: std::collections::HashMap::from([(
            WindowKey {
                provider,
                kind: decode_kind(&window_kind, &window_scope_value),
            },
            UsageWindowState {
                pct,
                resets_at,
                exceeded: exceeded != 0,
                active: active != 0,
                scope: None,
            },
        )]),
        credits: credits_json
            .map(|x| serde_json::from_str(&x))
            .transpose()
            .map_err(json_err)?,
    })
}
fn decode_resume_marker_row(row: &rusqlite::Row) -> rusqlite::Result<ResumeMarker> {
    let id: String = row.get(0)?;
    let session_id: String = row.get(1)?;
    let reason: String = row.get(2)?;
    let resume_at = row.get(3)?;
    let requested_at = row.get(4)?;
    let created_at = row.get(5)?;
    let status: String = row.get(6)?;
    let detail: Option<String> = row.get(7)?;
    let message: Option<String> = row.get(8)?;
    Ok(ResumeMarker {
        id: uuid::Uuid::parse_str(&id)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        session_id: SessionId(session_id),
        reason: serde_json::from_str(&reason)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        resume_at,
        requested_at,
        created_at,
        status: decode_resume_status(&status, detail),
        message,
    })
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
fn compaction_source_name(s: &CompactionSource) -> &'static str {
    match s {
        CompactionSource::Inline => "inline",
        CompactionSource::External => "external",
    }
}
fn decode_compaction_source(s: &str) -> CompactionSource {
    match s {
        "inline" => CompactionSource::Inline,
        _ => CompactionSource::External,
    }
}
fn decode_compaction_event_row(row: &rusqlite::Row) -> rusqlite::Result<CompactionEvent> {
    let id: String = row.get(0)?;
    let session_id: String = row.get(1)?;
    let source: String = row.get(2)?;
    Ok(CompactionEvent {
        id: uuid::Uuid::parse_str(&id)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
        session_id: SessionId(session_id),
        source: decode_compaction_source(&source),
        trigger: row.get(3)?,
        context_pct_before: row.get(4)?,
        usage_window_pct_before: row.get(5)?,
        tokens_before: row.get::<_, Option<i64>>(6)?.map(|x| x as u64),
        tokens_after: row.get::<_, Option<i64>>(7)?.map(|x| x as u64),
        started_at: row.get(8)?,
        completed_at: row.get(9)?,
    })
}
const SCHEMA: &str = r#"PRAGMA foreign_keys=ON;CREATE TABLE IF NOT EXISTS usage_samples(id INTEGER PRIMARY KEY,provider TEXT NOT NULL,account TEXT,window_kind TEXT NOT NULL,window_scope_value TEXT NOT NULL,pct REAL NOT NULL,resets_at TEXT,exceeded INTEGER NOT NULL,active INTEGER NOT NULL,source TEXT NOT NULL,at TEXT NOT NULL,fetched_at TEXT,credits_json TEXT,plan TEXT);CREATE INDEX IF NOT EXISTS usage_samples_window_at ON usage_samples(provider,window_kind,window_scope_value,at);CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,harness TEXT NOT NULL,model TEXT,account TEXT,cwd TEXT NOT NULL,state_path TEXT,context_window_size INTEGER,last_known_token_count INTEGER,launch_mode TEXT NOT NULL,pid INTEGER,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,stopped_reason TEXT,stopped_window_kind TEXT,superseded_by TEXT,reseeded_from TEXT,title TEXT);CREATE TABLE IF NOT EXISTS session_token_usage(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),at TEXT NOT NULL,model TEXT,input_tokens INTEGER NOT NULL,cached_input_tokens INTEGER NOT NULL,cache_write_input_tokens INTEGER NOT NULL,output_tokens INTEGER NOT NULL,reasoning_output_tokens INTEGER NOT NULL,total_tokens INTEGER NOT NULL,UNIQUE(session_id,at,total_tokens,input_tokens,output_tokens));CREATE INDEX IF NOT EXISTS session_token_usage_session_at ON session_token_usage(session_id,at);CREATE TABLE IF NOT EXISTS resume_markers(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),reason TEXT NOT NULL,resume_at TEXT,requested_at TEXT,created_at TEXT NOT NULL,status TEXT NOT NULL,status_detail TEXT,message TEXT);CREATE INDEX IF NOT EXISTS resume_markers_session_status ON resume_markers(session_id,status);CREATE UNIQUE INDEX IF NOT EXISTS resume_markers_active ON resume_markers(session_id) WHERE status IN ('pending','scheduled');CREATE TABLE IF NOT EXISTS compaction_requests(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),kind TEXT NOT NULL,prompt TEXT NOT NULL,reason TEXT NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE INDEX IF NOT EXISTS compaction_requests_session_status ON compaction_requests(session_id,status);CREATE TABLE IF NOT EXISTS threshold_overrides(id TEXT PRIMARY KEY,scope_kind TEXT NOT NULL,provider TEXT NOT NULL,model_value TEXT,session_value TEXT,field TEXT NOT NULL,value_json TEXT NOT NULL,updated_at TEXT NOT NULL);CREATE UNIQUE INDEX IF NOT EXISTS threshold_overrides_key ON threshold_overrides(scope_kind,provider,COALESCE(model_value,''),COALESCE(session_value,''),field);CREATE TABLE IF NOT EXISTS idle_reseed_summaries(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,source_model TEXT NOT NULL,summary_text TEXT NOT NULL,token_count_before INTEGER NOT NULL,token_count_after INTEGER NOT NULL,created_at TEXT NOT NULL);CREATE TABLE IF NOT EXISTS keepalive_config(session_id TEXT PRIMARY KEY,enabled INTEGER NOT NULL,last_ping_at TEXT,ping_day TEXT,ping_count INTEGER NOT NULL DEFAULT 0);CREATE TABLE IF NOT EXISTS hook_messages(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,event TEXT NOT NULL,text TEXT NOT NULL,created_at TEXT NOT NULL,delivered_at TEXT);CREATE INDEX IF NOT EXISTS hook_messages_delivery ON hook_messages(session_id,event,delivered_at,created_at);CREATE TABLE IF NOT EXISTS hook_events(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,event TEXT NOT NULL,created_at TEXT NOT NULL);CREATE INDEX IF NOT EXISTS hook_events_session_created ON hook_events(session_id,created_at);CREATE TABLE IF NOT EXISTS provider_fetch_status(provider TEXT NOT NULL,account TEXT,last_attempt_at TEXT NOT NULL,last_success_at TEXT,last_error TEXT,consecutive_failures INTEGER NOT NULL DEFAULT 0);CREATE UNIQUE INDEX IF NOT EXISTS provider_fetch_status_key ON provider_fetch_status(provider,COALESCE(account,''));CREATE TABLE IF NOT EXISTS compaction_events(id TEXT PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),source TEXT NOT NULL,trigger TEXT,context_pct_before REAL,usage_window_pct_before REAL,tokens_before INTEGER,tokens_after INTEGER,started_at TEXT NOT NULL,completed_at TEXT);CREATE INDEX IF NOT EXISTS compaction_events_session_started ON compaction_events(session_id,started_at);CREATE INDEX IF NOT EXISTS compaction_events_started ON compaction_events(started_at);"#;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
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
    fn resolve_session_title_walks_the_reseeded_from_chain() {
        let store = Store::open_memory().unwrap();
        let original = SessionId("original".into());
        let resumed = SessionId("resumed".into());
        let untitled_grandchild = SessionId("untitled-grandchild".into());

        store.insert_session(&session(&original)).unwrap();
        store
            .set_session_title(&original, "Fix the retry loop")
            .unwrap();

        store
            .insert_session(&SessionSummary {
                reseeded_from: Some(original.clone()),
                ..session(&resumed)
            })
            .unwrap();

        store
            .insert_session(&SessionSummary {
                reseeded_from: Some(resumed.clone()),
                ..session(&untitled_grandchild)
            })
            .unwrap();

        assert_eq!(
            store.resolve_session_title(&original).unwrap(),
            Some("Fix the retry loop".into())
        );
        assert_eq!(
            store.resolve_session_title(&resumed).unwrap(),
            Some("Fix the retry loop".into()),
            "a session with no title of its own should inherit its ancestor's"
        );
        assert_eq!(
            store.resolve_session_title(&untitled_grandchild).unwrap(),
            Some("Fix the retry loop".into()),
            "inheritance should walk more than one hop"
        );

        store
            .set_session_title(&resumed, "Retry loop, take two")
            .unwrap();
        assert_eq!(
            store.resolve_session_title(&untitled_grandchild).unwrap(),
            Some("Retry loop, take two".into()),
            "should stop at the nearest titled ancestor, not always the root"
        );
    }

    #[test]
    fn resolve_session_title_is_none_when_nothing_in_the_chain_has_one() {
        let store = Store::open_memory().unwrap();
        let id = SessionId("no-title-anywhere".into());
        store.insert_session(&session(&id)).unwrap();
        assert_eq!(store.resolve_session_title(&id).unwrap(), None);
    }

    #[test]
    fn set_session_title_ignores_an_empty_title() {
        let store = Store::open_memory().unwrap();
        let id = SessionId("s".into());
        store.insert_session(&session(&id)).unwrap();
        store.set_session_title(&id, "Real title").unwrap();
        store.set_session_title(&id, "").unwrap();
        assert_eq!(
            store.resolve_session_title(&id).unwrap(),
            Some("Real title".into())
        );
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
            plan: Some("Plus".into()),
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
    fn session_cache_warm_uses_the_shared_approximation() {
        let store = Store::open_memory().unwrap();
        let now = Utc::now();
        let mut warm = session(&SessionId("warm".into()));
        warm.last_seen = now - Duration::minutes(CACHE_WARM_APPROXIMATION_MINUTES);
        store.insert_session(&warm).unwrap();
        let mut cold = session(&SessionId("cold".into()));
        cold.last_seen = now - Duration::minutes(CACHE_WARM_APPROXIMATION_MINUTES + 1);
        store.insert_session(&cold).unwrap();

        assert!(store.session_cache_warm(&warm.id, now).unwrap());
        assert!(!store.session_cache_warm(&cold.id, now).unwrap());
    }

    #[test]
    fn token_usage_history_is_idempotent_and_round_trips_all_accounting_fields() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("token-session".into());
        s.insert_session(&session(&sid)).unwrap();
        let records = vec![TokenUsageRecord {
            at: Utc::now(),
            model: Some(ModelId("gpt-5.6-luna".into())),
            input_tokens: 1000,
            cached_input_tokens: 800,
            cache_write_input_tokens: 50,
            output_tokens: 200,
            reasoning_output_tokens: 75,
            total_tokens: 1200,
        }];

        s.insert_token_usage_records(&sid, &records).unwrap();
        s.insert_token_usage_records(&sid, &records).unwrap();

        assert_eq!(s.token_usage_for_session(&sid).unwrap(), records);
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
            requested_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Pending,
            message: None,
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
            requested_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Scheduled,
            message: None,
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
    fn set_resume_at_resolves_pending_manual_marker_but_not_a_scheduled_one() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        let requested_at = Utc::now() + Duration::hours(1);
        let marker = ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: sid.clone(),
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            requested_at: Some(requested_at),
            created_at: Utc::now(),
            status: ResumeStatus::Pending,
            message: None,
        };
        s.insert_resume_marker(&marker).unwrap();
        assert_eq!(s.active_resume_marker(&sid).unwrap().unwrap().id, marker.id);

        let resolved_at = requested_at + Duration::minutes(30);
        s.set_resume_at(marker.id, resolved_at).unwrap();
        let resolved = s.active_resume_marker(&sid).unwrap().unwrap();
        assert_eq!(resolved.resume_at, Some(resolved_at));
        assert_eq!(resolved.status, ResumeStatus::Scheduled);

        // Already-scheduled markers are never re-resolved.
        let later = resolved_at + Duration::minutes(30);
        s.set_resume_at(marker.id, later).unwrap();
        assert_eq!(
            s.active_resume_marker(&sid).unwrap().unwrap().resume_at,
            Some(resolved_at)
        );
    }
    #[test]
    fn search_sessions_matches_id_cwd_or_model_case_insensitively() {
        let s = Store::open_memory().unwrap();
        let mut alpha = session(&SessionId("alpha-session".into()));
        alpha.cwd = "/home/v0id/repos/usagewindow".into();
        alpha.model = Some(ModelId("gpt-5.6-luna".into()));
        s.insert_session(&alpha).unwrap();
        let mut beta = session(&SessionId("beta-session".into()));
        beta.cwd = "/home/v0id/repos/other-project".into();
        beta.model = Some(ModelId("claude-opus".into()));
        s.insert_session(&beta).unwrap();

        assert_eq!(s.search_sessions(None).unwrap().len(), 2);
        assert_eq!(
            s.search_sessions(Some("USAGEWINDOW"))
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect::<Vec<_>>(),
            vec![SessionId("alpha-session".into())]
        );
        assert_eq!(
            s.search_sessions(Some("opus"))
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect::<Vec<_>>(),
            vec![SessionId("beta-session".into())]
        );
        assert!(s.search_sessions(Some("nonexistent")).unwrap().is_empty());
    }
    #[test]
    fn update_session_editable_changes_cwd_model_account_only() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        let mut original = session(&sid);
        original.stopped_reason = Some(StopReason::UsageLimit {
            window: WindowKey {
                provider: Provider::Codex,
                kind: WindowKind::Rolling { minutes: 300 },
            },
        });
        s.insert_session(&original).unwrap();

        s.update_session_editable(
            &sid,
            "/new/cwd",
            Some(&ModelId("gpt-5.6-luna".into())),
            Some(&AccountId("acct-1".into())),
        )
        .unwrap();

        let updated = s.read_session(&sid).unwrap();
        assert_eq!(updated.cwd, "/new/cwd");
        assert_eq!(updated.model, Some(ModelId("gpt-5.6-luna".into())));
        assert_eq!(updated.account, Some(AccountId("acct-1".into())));
        // Untouched by the edit.
        assert!(updated.stopped_reason.is_some());

        assert!(matches!(
            s.update_session_editable(&SessionId("missing".into()), "/x", None, None),
            Err(StoreError::NotFound)
        ));
    }
    #[test]
    fn delete_session_cascades_fk_enforced_children_and_reports_missing() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        s.insert_resume_marker(&ResumeMarker {
            id: uuid::Uuid::new_v4(),
            session_id: sid.clone(),
            reason: ResumeReason::ManuallyMarked,
            resume_at: None,
            requested_at: None,
            created_at: Utc::now(),
            status: ResumeStatus::Pending,
            message: None,
        })
        .unwrap();
        s.insert_compaction_request(&CompactionRequest {
            id: uuid::Uuid::new_v4(),
            session_id: sid.clone(),
            kind: CompactionKind::OpportunisticIdle,
            prompt: "p".into(),
            reason: "r".into(),
            status: CompactionStatus::Pending,
            created_at: Utc::now(),
        })
        .unwrap();

        s.delete_session(&sid).unwrap();

        assert!(matches!(s.read_session(&sid), Err(StoreError::NotFound)));
        assert!(s.resume_markers_for_session(&sid).unwrap().is_empty());
        assert!(s.compaction_requests_for_session(&sid).unwrap().is_empty());
        assert!(matches!(s.delete_session(&sid), Err(StoreError::NotFound)));
    }

    #[test]
    fn cancel_compaction_requests_only_cancels_pending() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        let pending = uuid::Uuid::new_v4();
        let sending = uuid::Uuid::new_v4();
        let sent = uuid::Uuid::new_v4();
        for (id, status) in [
            (pending, CompactionStatus::Pending),
            (sending, CompactionStatus::Sending),
            (sent, CompactionStatus::Sent),
        ] {
            s.insert_compaction_request(&CompactionRequest {
                id,
                session_id: sid.clone(),
                kind: CompactionKind::AgentRequested,
                prompt: "compact".into(),
                reason: "test".into(),
                status: CompactionStatus::Pending,
                created_at: Utc::now(),
            })
            .unwrap();
            if !matches!(status, CompactionStatus::Pending) {
                s.update_compaction_status(id, status).unwrap();
            }
        }

        s.cancel_compaction_requests(&sid).unwrap();

        let by_id: HashMap<_, _> = s
            .compaction_requests_for_session(&sid)
            .unwrap()
            .into_iter()
            .map(|r| (r.id, r.status))
            .collect();
        assert_eq!(by_id[&pending], CompactionStatus::Cancelled);
        assert_eq!(by_id[&sending], CompactionStatus::Sending);
        assert_eq!(by_id[&sent], CompactionStatus::Sent);
        assert!(!s.claim_compaction(pending).unwrap());
    }

    #[test]
    fn compaction_failure_detail_survives_round_trip() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("s".into());
        s.insert_session(&session(&sid)).unwrap();
        let id = uuid::Uuid::new_v4();
        s.insert_compaction_request(&CompactionRequest {
            id,
            session_id: sid.clone(),
            kind: CompactionKind::AgentRequested,
            prompt: "compact".into(),
            reason: "test".into(),
            status: CompactionStatus::Pending,
            created_at: Utc::now(),
        })
        .unwrap();

        s.update_compaction_status(
            id,
            CompactionStatus::Failed("app-server rejected thread/compact/start".into()),
        )
        .unwrap();

        let requests = s.compaction_requests_for_session(&sid).unwrap();
        assert!(matches!(
            &requests[0].status,
            CompactionStatus::Failed(detail)
                if detail == "app-server rejected thread/compact/start"
        ));
    }

    #[test]
    fn hard_boundary_failure_and_voided_resume_survive_reopen() {
        let path = std::env::temp_dir().join(format!(
            "usagewindow-hard-boundary-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let path_string = path.to_string_lossy().into_owned();
        let sid = SessionId("blocked-session".into());
        let compaction_id = uuid::Uuid::new_v4();
        let marker_id = uuid::Uuid::new_v4();
        {
            let store = Store::open(&path_string).unwrap();
            store.insert_session(&session(&sid)).unwrap();
            store
                .insert_compaction_request(&CompactionRequest {
                    id: compaction_id,
                    session_id: sid.clone(),
                    kind: CompactionKind::AgentRequested,
                    prompt: "compact".into(),
                    reason: "hard quota boundary".into(),
                    status: CompactionStatus::Failed("provider rejected compaction".into()),
                    created_at: Utc::now(),
                })
                .unwrap();
            store
                .insert_resume_marker(&ResumeMarker {
                    id: marker_id,
                    session_id: sid.clone(),
                    reason: ResumeReason::AutoDetectedLimit,
                    resume_at: Some(Utc::now()),
                    requested_at: None,
                    created_at: Utc::now(),
                    status: ResumeStatus::Scheduled,
                    message: None,
                })
                .unwrap();
            store.cancel_resume_markers(&sid).unwrap();
        }

        let reopened = Store::open(&path_string).unwrap();
        assert!(matches!(
            &reopened.compaction_requests_for_session(&sid).unwrap()[0].status,
            CompactionStatus::Failed(reason) if reason == "provider rejected compaction"
        ));
        let markers = reopened.resume_markers_for_session(&sid).unwrap();
        assert_eq!(markers[0].id, marker_id);
        assert_eq!(markers[0].status, ResumeStatus::Cancelled);
        assert!(reopened.due_resume_markers(Utc::now()).unwrap().is_empty());
        drop(reopened);
        let _ = std::fs::remove_file(path);
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

    #[test]
    fn session_observations_upsert_without_losing_lineage() {
        let s = Store::open_memory().unwrap();
        let id = SessionId("observed".into());
        let mut original = session(&id);
        original.reseeded_from = Some(SessionId("parent".into()));
        s.insert_session(&original).unwrap();

        let seen_at = original.last_seen + chrono::Duration::minutes(2);
        s.update_session_observation(&id, seen_at, Some(42), Some(123_456), Some(200_000))
            .unwrap();
        s.update_session_stop(&id, Some(&StopReason::UserQuit))
            .unwrap();

        let updated = s.read_session(&id).unwrap();
        assert_eq!(updated.last_seen, seen_at);
        assert_eq!(updated.pid, Some(42));
        assert_eq!(updated.last_known_token_count, Some(123_456));
        assert_eq!(updated.context_window_size, Some(200_000));
        assert_eq!(updated.stopped_reason, Some(StopReason::UserQuit));
        assert_eq!(updated.reseeded_from, original.reseeded_from);
    }

    #[test]
    fn store_sets_busy_timeout_and_prunes_old_usage() {
        let s = Store::open_memory().unwrap();
        let timeout: i64 = s
            .connection()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert!(timeout >= 5_000);

        let old = UsageSample {
            at: Utc::now() - chrono::Duration::days(31),
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
                UsageWindowState::new(1.0, false, true, None, None),
            )]),
            credits: None,
        };
        s.insert_usage_sample(&old).unwrap();
        assert_eq!(
            s.prune_usage_before(Utc::now() - chrono::Duration::days(30))
                .unwrap(),
            1
        );
        assert!(s.all_usage_samples().unwrap().is_empty());
    }

    #[test]
    fn hook_messages_are_claimed_once_by_session_and_event() {
        let s = Store::open_memory().unwrap();
        let id = SessionId("hooked".into());
        s.enqueue_hook_message(&id, "Stop", "one").unwrap();
        s.enqueue_hook_message(&id, "SessionStart", "two").unwrap();
        assert_eq!(s.take_hook_messages(&id, "Stop").unwrap(), vec!["one"]);
        assert!(s.take_hook_messages(&id, "Stop").unwrap().is_empty());
        assert_eq!(
            s.take_hook_messages(&id, "SessionStart").unwrap(),
            vec!["two"]
        );
        s.record_hook_event(&id, "UserPromptSubmit").unwrap();
        s.record_hook_event(&id, "Stop").unwrap();
        assert_eq!(s.latest_hook_event(&id).unwrap().as_deref(), Some("Stop"));
    }

    #[test]
    fn fetch_status_tracks_success_and_failure_without_clobbering_history() {
        let s = Store::open_memory().unwrap();
        let t0 = Utc::now();
        s.record_fetch_failure(&Provider::Codex, None, t0, "boom")
            .unwrap();
        s.record_fetch_failure(
            &Provider::Codex,
            None,
            t0 + Duration::minutes(1),
            "boom again",
        )
        .unwrap();
        let statuses = s.fetch_statuses().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].consecutive_failures, 2);
        assert_eq!(statuses[0].last_error.as_deref(), Some("boom again"));
        assert!(statuses[0].last_success_at.is_none());

        let t1 = t0 + Duration::minutes(2);
        s.record_fetch_success(&Provider::Codex, None, t1).unwrap();
        let statuses = s.fetch_statuses().unwrap();
        assert_eq!(statuses[0].consecutive_failures, 0);
        assert_eq!(statuses[0].last_error, None);
        assert_eq!(statuses[0].last_success_at, Some(t1));
    }

    #[test]
    fn compaction_events_open_close_and_list_newest_first() {
        let s = Store::open_memory().unwrap();
        let sid = SessionId("compact-session".into());
        s.insert_session(&session(&sid)).unwrap();
        let started = Utc::now();
        s.insert_compaction_event(&CompactionEvent {
            id: uuid::Uuid::new_v4(),
            session_id: sid.clone(),
            source: CompactionSource::External,
            trigger: Some("auto".into()),
            context_pct_before: Some(90.0),
            usage_window_pct_before: Some(40.0),
            tokens_before: Some(150_000),
            tokens_after: None,
            started_at: started,
            completed_at: None,
        })
        .unwrap();
        let completed_at = started + Duration::minutes(1);
        s.complete_compaction_event(&sid, Some(5_000), completed_at)
            .unwrap();

        let events = s.compaction_events_for_session(&sid).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens_after, Some(5_000));
        assert_eq!(events[0].completed_at, Some(completed_at));

        let recent = s.recent_compaction_events(10).unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].source, CompactionSource::External);
    }

    #[test]
    fn keepalive_active_count_counts_only_eligible_enabled_rows() {
        use uw_core::adapter::TokenUsageRecord;
        let s = Store::open_memory().unwrap();
        let now = Utc::now();
        let a = SessionId("a".into());
        let b = SessionId("b".into());
        let mut warm = session(&a);
        warm.last_seen = now - chrono::Duration::minutes(1);
        s.insert_session(&warm).unwrap();
        s.insert_session(&session(&b)).unwrap();
        s.set_keepalive(&a, true).unwrap();
        s.set_keepalive(&b, false).unwrap();
        s.insert_token_usage_records(
            &a,
            &[TokenUsageRecord {
                at: now - chrono::Duration::minutes(1),
                model: None,
                input_tokens: 1,
                cached_input_tokens: 0,
                cache_write_input_tokens: 0,
                output_tokens: 1,
                reasoning_output_tokens: 0,
                total_tokens: 2,
            }],
        )
        .unwrap();
        assert_eq!(s.keepalive_active_count(now).unwrap(), 1);
        let cold_id = SessionId("cold".into());
        let mut cold = session(&cold_id);
        cold.last_seen = now - chrono::Duration::minutes(10);
        s.insert_session(&cold).unwrap();
        s.set_keepalive(&cold_id, true).unwrap();
        assert_eq!(s.keepalive_active_count(now).unwrap(), 1);
    }
}
