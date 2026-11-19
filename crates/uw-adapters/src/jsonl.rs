use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Default)]
pub(crate) struct JsonlCache<T> {
    files: HashMap<PathBuf, Cursor<T>>,
}

struct Cursor<T> {
    identity: (u64, u64),
    fingerprint: (u64, Option<SystemTime>),
    offset: u64,
    head: Vec<u8>,
    tail: Vec<u8>,
    partial: bool,
    state: T,
}

impl<T: Default> JsonlCache<T> {
    pub fn is_complete(&self, path: &Path) -> bool {
        self.files.get(path).is_some_and(|cursor| !cursor.partial)
    }

    pub fn get_mut(&mut self, path: &Path) -> Option<&mut T> {
        self.files.get_mut(path).map(|cursor| &mut cursor.state)
    }

    #[cfg(test)]
    pub fn offset(&self, path: &Path) -> u64 {
        self.files.get(path).map_or(0, |cursor| cursor.offset)
    }

    pub fn retain(&mut self, mut present: impl FnMut(&Path) -> bool) {
        self.files.retain(|path, _| present(path));
    }

    /// Folds appended JSONL records into per-file state. Incomplete final JSON is
    /// retried on append; replacement, shrinking, and changed prefix boundaries
    /// reset the fold. Callers drain usage only after a successful scan.
    pub fn scan(
        &mut self,
        path: &Path,
        mut observe: impl FnMut(&mut T, &str),
    ) -> std::io::Result<&mut T> {
        let mut file = File::open(path)?;
        let metadata = file.metadata()?;
        let identity = (metadata.dev(), metadata.ino());
        let fingerprint = (metadata.len(), metadata.modified().ok());
        let cursor = self.files.entry(path.to_owned()).or_insert_with(|| Cursor {
            identity,
            fingerprint,
            offset: 0,
            head: Vec::new(),
            tail: Vec::new(),
            partial: false,
            state: T::default(),
        });
        let mut head = vec![0; cursor.head.len()];
        let head_matches = file.read_exact(&mut head).is_ok() && head == cursor.head;
        let mut boundary = vec![0; cursor.tail.len()];
        let boundary_matches = metadata.len() >= cursor.offset
            && file
                .seek(SeekFrom::Start(
                    cursor.offset.saturating_sub(boundary.len() as u64),
                ))
                .is_ok()
            && file.read_exact(&mut boundary).is_ok()
            && boundary == cursor.tail;
        if identity != cursor.identity
            || !head_matches
            || !boundary_matches
            || (fingerprint.0 == cursor.fingerprint.0 && fingerprint.1 != cursor.fingerprint.1)
        {
            *cursor = Cursor {
                identity,
                fingerprint,
                offset: 0,
                head: Vec::new(),
                tail: Vec::new(),
                partial: false,
                state: T::default(),
            };
        }
        file.seek(SeekFrom::Start(cursor.offset))?;
        let mut reader = BufReader::new(file.take(metadata.len().saturating_sub(cursor.offset)));
        let mut line = String::new();
        cursor.partial = false;
        loop {
            line.clear();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                break;
            }
            if !line.ends_with('\n')
                && !line.trim().is_empty()
                && serde_json::from_str::<serde::de::IgnoredAny>(&line).is_err()
            {
                cursor.partial = true;
                break;
            }
            observe(&mut cursor.state, line.trim_end_matches(['\n', '\r']));
            cursor.offset += bytes as u64;
        }
        let mut file = reader.into_inner().into_inner();
        let tail_len = cursor.offset.min(256) as usize;
        file.seek(SeekFrom::Start(cursor.offset - tail_len as u64))?;
        cursor.tail.resize(tail_len, 0);
        file.read_exact(&mut cursor.tail)?;
        file.seek(SeekFrom::Start(0))?;
        cursor.head.resize(cursor.offset.min(256) as usize, 0);
        file.read_exact(&mut cursor.head)?;
        cursor.fingerprint = fingerprint;
        Ok(&mut cursor.state)
    }
}

/// A stale acknowledgement must not consume usage from a rewritten file
/// [discovery_keeps_new_usage_when_an_old_write_is_acknowledged].
pub(crate) fn acknowledge_usage(
    pending: &mut Vec<uw_core::adapter::TokenUsageRecord>,
    committed: &[uw_core::adapter::TokenUsageRecord],
) {
    if pending.starts_with(committed) {
        pending.drain(..committed.len());
        if pending.is_empty() {
            pending.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn scan(cache: &mut JsonlCache<Vec<serde_json::Value>>, path: &Path) -> Vec<serde_json::Value> {
        std::mem::take(
            cache
                .scan(path, |values, line| {
                    if let Ok(value) = serde_json::from_str(line) {
                        values.push(value);
                    }
                })
                .unwrap(),
        )
    }

    // why: a partial record must be counted exactly once when its final bytes arrive.
    #[test]
    fn append_retries_partial_records_and_does_not_replay_old_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, "1\n{\"partial\":").unwrap();
        let mut cache = JsonlCache::default();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(1)]);
        assert!(scan(&mut cache, &path).is_empty());
        assert!(!cache.is_complete(&path));
        write!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap(),
            "true}}\n2\n"
        )
        .unwrap();
        assert_eq!(
            scan(&mut cache, &path),
            [serde_json::json!({"partial": true}), serde_json::json!(2)]
        );
        assert!(cache.is_complete(&path));
        assert!(scan(&mut cache, &path).is_empty());
    }

    // why: truncation invalidates offsets even when the rewritten file grows past its old length.
    #[test]
    fn truncation_and_regrowth_reset_the_fold() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, "1\n2\n").unwrap();
        let mut cache = JsonlCache::default();
        assert_eq!(
            scan(&mut cache, &path),
            [serde_json::json!(1), serde_json::json!(2)]
        );
        std::fs::write(&path, "3\n").unwrap();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(3)]);
        std::fs::write(&path, "4\n5\n6\n").unwrap();
        assert_eq!(
            scan(&mut cache, &path),
            [
                serde_json::json!(4),
                serde_json::json!(5),
                serde_json::json!(6)
            ]
        );
    }

    // why: rotation may preserve length and mtime, so inode identity must invalidate the old cursor.
    #[test]
    fn replacement_with_same_length_replays_the_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, "1\n").unwrap();
        let mut cache = JsonlCache::default();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(1)]);
        std::fs::rename(&path, dir.path().join("old")).unwrap();
        std::fs::write(&path, "2\n").unwrap();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(2)]);
    }
    // why: size, mtime, and boundary bytes can all survive rotation; the new inode must still restart discovery.
    #[test]
    fn replacement_with_identical_bytes_and_mtime_resets_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        std::fs::write(&path, "1\n").unwrap();
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut cache = JsonlCache::default();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(1)]);
        std::fs::rename(&path, dir.path().join("old")).unwrap();
        std::fs::write(&path, "1\n").unwrap();
        File::options().write(true).open(&path).unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified)).unwrap();
        assert_eq!(scan(&mut cache, &path), [serde_json::json!(1)]);
    }

}
