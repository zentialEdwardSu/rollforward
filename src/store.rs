//! Local persistence: the [`LocalStore`] trait plus a redb implementation.
//!
//! The engine depends only on the [`LocalStore`] trait; the concrete backend is
//! injected by the caller, mirroring how [`crate::remote::RemoteStorage`] hides
//! the remote. [`RedbStore`] is the backend used for testing and the default
//! `new_local` engine — a production host may supply its own.
//!
//! Each write method is a *self-contained atomic step*: the trait never exposes
//! a transaction handle, so no backend type (e.g. redb's `WriteTransaction`)
//! leaks across the boundary. The redb implementation batches each step into a
//! single write transaction internally — committing makes the step durable,
//! aborting rolls it back. That is the engine's transaction-rollback guarantee.

use std::path::Path;
use std::sync::RwLock;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::types::{OplogCacheEntry, SyncError};

/// Local durable state for the engine. Reads return owned values; each write is
/// an atomic domain operation with no exposed transaction handle.
///
/// Exported over FFI (`with_foreign`) so a host can inject its own store.
/// uniFFI requires owned args and forbids tuples, so signatures use `String`/
/// `Vec<u8>` and the [`OplogCacheEntry`] record instead of `(u64, Vec<u8>)`.
#[uniffi::export(with_foreign)]
pub trait LocalStore: Send + Sync {
    /// Read a file's serialized state, if tracked.
    fn get_file_state(&self, file_id: String) -> Result<Option<Vec<u8>>, SyncError>;
    /// List all tracked file ids.
    fn list_files(&self) -> Result<Vec<String>, SyncError>;
    /// Read all cached oplog entries for a file, ascending by sequence.
    fn list_oplogs(&self, file_id: String) -> Result<Vec<OplogCacheEntry>, SyncError>;
    /// Read the latest baseline sequence recorded for a file.
    fn get_baseline_meta(&self, file_id: String) -> Result<Option<u64>, SyncError>;
    /// Read the last-synced sequence for a file.
    fn get_sync_cursor(&self, file_id: String) -> Result<Option<u64>, SyncError>;

    /// Atomically persist a file's serialized state, its sync cursor, and
    /// (optionally) one cached oplog entry. All-or-nothing.
    fn persist_file(
        &self,
        file_id: String,
        state: Vec<u8>,
        head: u64,
        cache_entry: Option<OplogCacheEntry>,
    ) -> Result<(), SyncError>;

    /// Batch-cache oplog entries fetched from the remote, keyed by
    /// `(file_id, sequence)`. Idempotent (overwrites the same key). This is a
    /// performance hint for incremental reads, not authoritative state: the
    /// remote listing is the source of truth, and cached rows are only ever
    /// consumed as a byte-source for sequences that still appear in that listing
    /// (so stale rows below a remote truncation are inert). An empty batch is a
    /// no-op.
    fn cache_oplogs(&self, file_id: String, entries: Vec<OplogCacheEntry>)
    -> Result<(), SyncError>;

    /// Atomically commit a truncation: drop cached oplogs `<= up_to` and record
    /// the new baseline sequence.
    fn commit_truncation(&self, file_id: String, up_to: u64) -> Result<(), SyncError>;

    /// Release the backend's durable resources (for a file-locked backend such
    /// as redb, this frees the on-disk lock so the same path can be reopened).
    /// Idempotent: closing an already-closed store is a no-op. Calls after close
    /// return an error rather than operating on a released backend.
    fn close(&self) -> Result<(), SyncError>;
}

/// Per-file serialized state (see `engine::TrackedFile`).
const FILES: TableDefinition<&str, &[u8]> = TableDefinition::new("files");
/// Cached oplog entries keyed by `(file_id, sequence)`.
const OPLOG: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("oplog");
/// Latest baseline sequence per file.
const BASELINE_META: TableDefinition<&str, u64> = TableDefinition::new("baseline_meta");
/// Last successfully-synced sequence per file (drives status reporting).
const SYNC_CURSOR: TableDefinition<&str, u64> = TableDefinition::new("sync_cursor");

/// Map a redb error into a [`SyncError::IoError`].
fn db_err<E: std::fmt::Display>(e: E) -> SyncError {
    SyncError::IoError { msg: e.to_string() }
}

/// A [`LocalStore`] backed by an embedded redb database — the backend used in
/// tests and by the default `new_local` engine.
#[derive(uniffi::Object)]
pub struct RedbStore {
    /// The underlying redb database handle. Held in an `Option` so [`close`]
    /// can drop it — redb releases its on-disk file lock only when the
    /// `Database` is dropped (it exposes no explicit close) — while the
    /// `RedbStore` (and any `Arc<SyncEngine>` owning it) stays alive. `None`
    /// after close; every access goes through [`with_db`], which errors then.
    ///
    /// [`close`]: RedbStore::close
    /// [`with_db`]: RedbStore::with_db
    db: RwLock<Option<Database>>,
}

impl RedbStore {
    /// Open (creating if needed) a store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SyncError> {
        let db = Database::create(path).map_err(db_err)?;
        // Materialize all tables up front so read transactions never fail on a
        // missing table before the first write.
        let txn = db.begin_write().map_err(db_err)?;
        {
            txn.open_table(FILES).map_err(db_err)?;
            txn.open_table(OPLOG).map_err(db_err)?;
            txn.open_table(BASELINE_META).map_err(db_err)?;
            txn.open_table(SYNC_CURSOR).map_err(db_err)?;
        }
        txn.commit().map_err(db_err)?;
        Ok(Self {
            db: RwLock::new(Some(db)),
        })
    }

    /// Borrow the open `Database` for the duration of `f`. Errors if the store
    /// has been closed, so no method operates on a released backend.
    fn with_db<F, T>(&self, f: F) -> Result<T, SyncError>
    where
        F: FnOnce(&Database) -> Result<T, SyncError>,
    {
        let guard = self.db.read().expect("redb store lock poisoned");
        let db = guard.as_ref().ok_or_else(|| SyncError::IoError {
            msg: "redb store is closed".into(),
        })?;
        f(db)
    }

    /// Run `f` inside a single write transaction, committing on success.
    fn write<F>(&self, f: F) -> Result<(), SyncError>
    where
        F: FnOnce(&WriteTransaction) -> Result<(), SyncError>,
    {
        self.with_db(|db| {
            let txn = db.begin_write().map_err(db_err)?;
            f(&txn)?;
            txn.commit().map_err(db_err)
        })
    }
}

impl LocalStore for RedbStore {
    fn get_file_state(&self, file_id: String) -> Result<Option<Vec<u8>>, SyncError> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(db_err)?;
            let t = txn.open_table(FILES).map_err(db_err)?;
            Ok(t.get(file_id.as_str())
                .map_err(db_err)?
                .map(|v| v.value().to_vec()))
        })
    }

    fn list_files(&self) -> Result<Vec<String>, SyncError> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(db_err)?;
            let t = txn.open_table(FILES).map_err(db_err)?;
            let mut out = Vec::new();
            for row in t.iter().map_err(db_err)? {
                let (k, _) = row.map_err(db_err)?;
                out.push(k.value().to_string());
            }
            Ok(out)
        })
    }

    fn list_oplogs(&self, file_id: String) -> Result<Vec<OplogCacheEntry>, SyncError> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(db_err)?;
            let t = txn.open_table(OPLOG).map_err(db_err)?;
            let mut out = Vec::new();
            for row in t
                .range((file_id.as_str(), 0)..=(file_id.as_str(), u64::MAX))
                .map_err(db_err)?
            {
                let (k, v) = row.map_err(db_err)?;
                out.push(OplogCacheEntry {
                    sequence: k.value().1,
                    data: v.value().to_vec(),
                });
            }
            Ok(out)
        })
    }

    fn get_baseline_meta(&self, file_id: String) -> Result<Option<u64>, SyncError> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(db_err)?;
            let t = txn.open_table(BASELINE_META).map_err(db_err)?;
            Ok(t.get(file_id.as_str()).map_err(db_err)?.map(|v| v.value()))
        })
    }

    fn get_sync_cursor(&self, file_id: String) -> Result<Option<u64>, SyncError> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(db_err)?;
            let t = txn.open_table(SYNC_CURSOR).map_err(db_err)?;
            Ok(t.get(file_id.as_str()).map_err(db_err)?.map(|v| v.value()))
        })
    }

    fn persist_file(
        &self,
        file_id: String,
        state: Vec<u8>,
        head: u64,
        cache_entry: Option<OplogCacheEntry>,
    ) -> Result<(), SyncError> {
        self.write(|txn| {
            {
                let mut t = txn.open_table(FILES).map_err(db_err)?;
                t.insert(file_id.as_str(), state.as_slice())
                    .map_err(db_err)?;
            }
            {
                let mut t = txn.open_table(SYNC_CURSOR).map_err(db_err)?;
                t.insert(file_id.as_str(), head).map_err(db_err)?;
            }
            if let Some(entry) = &cache_entry {
                let mut t = txn.open_table(OPLOG).map_err(db_err)?;
                t.insert((file_id.as_str(), entry.sequence), entry.data.as_slice())
                    .map_err(db_err)?;
            }
            Ok(())
        })
    }

    fn cache_oplogs(
        &self,
        file_id: String,
        entries: Vec<OplogCacheEntry>,
    ) -> Result<(), SyncError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.write(|txn| {
            let mut t = txn.open_table(OPLOG).map_err(db_err)?;
            for entry in &entries {
                t.insert((file_id.as_str(), entry.sequence), entry.data.as_slice())
                    .map_err(db_err)?;
            }
            Ok(())
        })
    }

    fn commit_truncation(&self, file_id: String, up_to: u64) -> Result<(), SyncError> {
        self.write(|txn| {
            {
                let mut t = txn.open_table(OPLOG).map_err(db_err)?;
                // Collect keys first to avoid mutating while iterating.
                let keys: Vec<u64> = t
                    .range((file_id.as_str(), 0)..=(file_id.as_str(), up_to))
                    .map_err(db_err)?
                    .map(|row| row.map(|(k, _)| k.value().1))
                    .collect::<Result<_, _>>()
                    .map_err(db_err)?;
                for seq in keys {
                    t.remove((file_id.as_str(), seq)).map_err(db_err)?;
                }
            }
            {
                let mut t = txn.open_table(BASELINE_META).map_err(db_err)?;
                t.insert(file_id.as_str(), up_to).map_err(db_err)?;
            }
            Ok(())
        })
    }

    fn close(&self) -> Result<(), SyncError> {
        // Take the Database out and drop it: redb releases the on-disk file
        // lock on Database drop (there is no explicit close in redb). Idempotent
        // — a second close finds None and does nothing.
        let db = self.db.write().expect("redb store lock poisoned").take();
        drop(db);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store_at(dir: &TempDir) -> RedbStore {
        RedbStore::open(dir.path().join("state.redb")).unwrap()
    }

    /// Build a cache entry record for tests.
    fn cache(seq: u64, data: &[u8]) -> OplogCacheEntry {
        OplogCacheEntry {
            sequence: seq,
            data: data.to_vec(),
        }
    }

    #[test]
    fn file_state_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = store_at(&dir);
            store
                .persist_file("f1".into(), b"hello".to_vec(), 0, None)
                .unwrap();
        }
        // Reopen: state must survive.
        let store = store_at(&dir);
        assert_eq!(
            store.get_file_state("f1".into()).unwrap().unwrap(),
            b"hello"
        );
    }

    #[test]
    fn persist_file_writes_cursor_and_oplog_atomically() {
        let dir = TempDir::new().unwrap();
        let store = store_at(&dir);
        store
            .persist_file("f1".into(), b"state".to_vec(), 7, Some(cache(7, b"entry")))
            .unwrap();
        assert_eq!(store.get_sync_cursor("f1".into()).unwrap(), Some(7));
        assert_eq!(
            store.list_oplogs("f1".into()).unwrap(),
            vec![cache(7, b"entry")]
        );
    }

    #[test]
    fn truncation_drops_old_oplogs_and_sets_baseline() {
        let dir = TempDir::new().unwrap();
        let store = store_at(&dir);
        for seq in 1..=5u64 {
            store
                .persist_file(
                    "f1".into(),
                    b"s".to_vec(),
                    seq,
                    Some(cache(seq, &[u8::try_from(seq).unwrap()])),
                )
                .unwrap();
        }
        assert_eq!(store.list_oplogs("f1".into()).unwrap().len(), 5);

        store.commit_truncation("f1".into(), 3).unwrap();

        let remaining: Vec<u64> = store
            .list_oplogs("f1".into())
            .unwrap()
            .into_iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(remaining, vec![4, 5]);
        assert_eq!(store.get_baseline_meta("f1".into()).unwrap(), Some(3));
    }

    #[test]
    fn close_releases_lock_so_path_can_be_reopened() {
        // The point of close(): redb holds an exclusive file lock for the
        // Database's lifetime, so a still-alive RedbStore would make reopening
        // the same path fail. close() must free the lock while the RedbStore
        // value itself lives on — letting a host reconstruct after an error.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = RedbStore::open(&path).unwrap();
        store
            .persist_file("f1".into(), b"v".to_vec(), 1, None)
            .unwrap();

        store.close().unwrap();

        // Store outlives close, but the lock is gone: reopen must succeed and
        // see the committed state.
        let reopened = RedbStore::open(&path).expect("reopen after close");
        assert_eq!(reopened.get_file_state("f1".into()).unwrap().unwrap(), b"v");

        // Operations after close fail loudly rather than touching a released db.
        assert!(store.get_file_state("f1".into()).is_err());
        // close() is idempotent.
        store.close().unwrap();
    }

    #[test]
    fn cache_oplogs_batch_inserts_is_idempotent_and_empty_is_noop() {
        let dir = TempDir::new().unwrap();
        let store = store_at(&dir);
        // Empty batch is a no-op.
        store.cache_oplogs("f1".into(), Vec::new()).unwrap();
        assert!(store.list_oplogs("f1".into()).unwrap().is_empty());

        // Batch insert lands all entries, ascending.
        store
            .cache_oplogs("f1".into(), vec![cache(2, b"two"), cache(1, b"one")])
            .unwrap();
        assert_eq!(
            store.list_oplogs("f1".into()).unwrap(),
            vec![cache(1, b"one"), cache(2, b"two")]
        );
        // Re-caching the same key overwrites, not duplicates (idempotent).
        store
            .cache_oplogs("f1".into(), vec![cache(1, b"one")])
            .unwrap();
        assert_eq!(store.list_oplogs("f1".into()).unwrap().len(), 2);
    }

    #[test]
    fn commit_truncation_drops_cached_rows_from_cache_oplogs() {
        // A cached prefix (as load_entries would populate) is dropped by a
        // later truncation exactly like locally-persisted oplogs.
        let dir = TempDir::new().unwrap();
        let store = store_at(&dir);
        store
            .cache_oplogs(
                "f1".into(),
                (1..=5u64).map(|s| cache(s, &[u8::try_from(s).unwrap()])).collect(),
            )
            .unwrap();
        store.commit_truncation("f1".into(), 3).unwrap();
        let remaining: Vec<u64> = store
            .list_oplogs("f1".into())
            .unwrap()
            .into_iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(remaining, vec![4, 5]);
    }
}
