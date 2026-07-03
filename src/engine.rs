//! The core sync engine: a state machine coordinating local edits, the remote
//! append-only oplog, CRDT/binary merge, rollback, and truncation.
//!
//! ## Model
//! The remote oplog is the source of truth. `sync` rebuilds a file's state
//! purely from the remote (latest baseline + surviving oplog deltas), so a
//! fresh engine on a new device reconstructs identical content — the same path
//! that makes post-truncation rebuild correct. Local redb state is a
//! durability cache.
//!
//! ## Fault tolerance
//! - **CAS retry**: local writes publish with [`RemoteStorage::put_oplog_cas`];
//!   on a concurrent-sequence collision the engine integrates the winner,
//!   rebases onto the new head, and retries (bounded).
//! - **Transaction rollback**: each local persist runs in one redb write
//!   transaction; sync performs all remote reads *before* persisting, so a
//!   mid-sync remote failure leaves the store untouched.
//! - **Chunk-level resume**: binary chunk uploads consult and update the
//!   `CHUNK_XFER` table, skipping already-transferred chunks after an interrupt.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::binary::{self, BinaryMerge};
use crate::oplog;
use crate::remote::RemoteStorage;
use crate::store::LocalStore;
use crate::text::TextDoc;
use crate::types::{
    BinaryConflictPolicy, ChangeType, EngineNotificationListener, OpLogEntry, OplogCacheEntry,
    SyncError,
};

/// Maximum CAS-retry attempts before giving up on a concurrent write.
const MAX_CAS_ATTEMPTS: u32 = 8;

/// In-memory content state for a tracked file.
enum FileState {
    /// A CRDT text document.
    Text(TextDoc),
    /// A binary file's current chunk manifest (ordered hashes).
    Binary {
        /// Ordered chunk hashes making up the file.
        manifest: Vec<String>,
    },
}

/// Engine-side per-file tracking.
struct TrackedFile {
    /// Highest sequence integrated into `state`.
    head: u64,
    /// Current content state.
    state: FileState,
}

/// Durable projection of [`TrackedFile`] stored in redb.
#[derive(Serialize, Deserialize)]
enum PersistedState {
    /// Text: a v1 full-update snapshot plus head sequence.
    Text {
        /// yrs full-state update bytes.
        full_update: Vec<u8>,
        /// Highest integrated sequence.
        head: u64,
    },
    /// Binary: the current manifest plus head sequence.
    Binary {
        /// Ordered chunk hashes.
        manifest: Vec<String>,
        /// Highest integrated sequence.
        head: u64,
    },
}

/// The sync engine handle exported over uniFFI.
#[derive(uniffi::Object)]
pub struct SyncEngine {
    /// This device's stable client identifier.
    client_id: String,
    /// In-memory tracked-file table under a single lock.
    files: RwLock<HashMap<String, TrackedFile>>,
    /// Local durable state (backend injected by the caller).
    store: Arc<dyn LocalStore>,
    /// Remote dumb storage.
    remote: Arc<dyn RemoteStorage>,
    /// UI notification sink.
    listener: Arc<dyn EngineNotificationListener>,
    /// Conflict policy for genuinely divergent binary edits.
    binary_policy: BinaryConflictPolicy,
    /// File ids with a `sync` currently in flight. A second concurrent `sync`
    /// of the same file is rejected rather than re-entering the state machine.
    in_flight: Mutex<HashSet<String>>,
}

/// RAII guard that marks a file as syncing for its lifetime and clears the mark
/// on drop — so every `sync` exit path (success, early return, error) releases
/// the phase lock, returning the file to the idle state.
struct SyncGuard<'a> {
    /// Set of in-flight file ids to remove from on drop.
    in_flight: &'a Mutex<HashSet<String>>,
    /// The file id this guard holds.
    file_id: String,
}

impl Drop for SyncGuard<'_> {
    fn drop(&mut self) {
        self.in_flight
            .lock()
            .expect("in_flight lock poisoned")
            .remove(&self.file_id);
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// zstd compression level for baseline snapshots.
const ZSTD_LEVEL: i32 = 3;

/// True if the highest sequence in `entries` is claimed by more than one entry
/// (a forked tip). `entries` is assumed sorted ascending by sequence.
// Called once (by `sync`) but kept separate as a named, self-describing
// predicate; inlining would obscure the convergence-trigger condition.
#[allow(clippy::single_call_fn)]
fn tip_is_forked(entries: &[OpLogEntry]) -> bool {
    let Some(max_seq) = entries.last().map(|e| e.sequence) else {
        return false;
    };
    entries.iter().filter(|e| e.sequence == max_seq).count() > 1
}

impl SyncEngine {
    /// Construct an engine over injected [`RemoteStorage`] and [`LocalStore`]
    /// backends (Rust/tests). The caller chooses both implementations.
    pub fn with_backends(
        client_id: impl Into<String>,
        store: Arc<dyn LocalStore>,
        remote: Arc<dyn RemoteStorage>,
        listener: Arc<dyn EngineNotificationListener>,
        binary_policy: BinaryConflictPolicy,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            files: RwLock::new(HashMap::new()),
            store,
            remote,
            listener,
            binary_policy,
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    /// Decode every remote oplog entry for a file, sorted by (sequence, client).
    fn load_entries(&self, file_id: &str) -> Result<Vec<OpLogEntry>, SyncError> {
        let mut items = self.remote.list_oplogs(file_id.to_owned())?;
        oplog::sort_items(&mut items);
        let mut entries = Vec::with_capacity(items.len());
        for item in &items {
            let bytes = self
                .remote
                .get_oplog(file_id.to_owned(), item.remote_path.clone())?;
            let entry: OpLogEntry = serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Latest baseline (decompressed full-update, sequence) for a text file.
    /// Used by truncation to chain onto the previous baseline.
    fn load_baseline(&self, file_id: &str) -> Result<Option<(Vec<u8>, u64)>, SyncError> {
        let seqs = self.remote.list_baselines(file_id.to_owned())?;
        let Some(&seq) = seqs.last() else {
            return Ok(None);
        };
        let Some(compressed) = self.remote.get_baseline(file_id.to_owned(), seq)? else {
            return Ok(None);
        };
        let raw = zstd::decode_all(compressed.as_slice())
            .map_err(|e| SyncError::IoError { msg: e.to_string() })?;
        Ok(Some((raw, seq)))
    }

    /// Every baseline (decompressed full-update, sequence), ascending. Used for
    /// rebuild: when orphan branches were each truncated independently there can
    /// be more than one baseline; applying them all merges the branches (each is
    /// a full yrs state, and yrs `apply_update` is order-independent).
    fn load_all_baselines(&self, file_id: &str) -> Result<Vec<(Vec<u8>, u64)>, SyncError> {
        let seqs = self.remote.list_baselines(file_id.to_owned())?;
        let mut out = Vec::with_capacity(seqs.len());
        for seq in seqs {
            if let Some(compressed) = self.remote.get_baseline(file_id.to_owned(), seq)? {
                let raw = zstd::decode_all(compressed.as_slice())
                    .map_err(|e| SyncError::IoError { msg: e.to_string() })?;
                out.push((raw, seq));
            }
        }
        Ok(out)
    }

    /// Build text state from all baselines plus deltas above the highest one.
    fn build_text(&self, file_id: &str, entries: &[OpLogEntry]) -> Result<TrackedFile, SyncError> {
        let baselines = self.load_all_baselines(file_id)?;
        let baseline_seq = baselines.iter().map(|(_, s)| *s).max().unwrap_or(0);

        // Apply every baseline first (merges orphan branches), then the deltas
        // above the highest baseline sequence.
        let mut doc = TextDoc::new();
        for (bytes, _) in &baselines {
            doc.apply_delta(bytes)?;
        }
        let mut head = baseline_seq;
        for e in entries {
            head = head.max(e.sequence);
            if e.sequence <= baseline_seq {
                continue;
            }
            if let ChangeType::TextDelta { delta } = &e.change_type {
                doc.apply_delta(delta)?;
            }
        }
        Ok(TrackedFile {
            head,
            state: FileState::Text(doc),
        })
    }

    /// Resolve binary state by replaying manifests in sequence order, running a
    /// three-way merge at any forked sequence.
    fn build_binary(&self, entries: &[OpLogEntry]) -> (TrackedFile, bool) {
        let mut current: Vec<String> = Vec::new();
        let mut head = 0u64;
        let mut needs_copy = false;

        // Group entries by sequence to detect forks (same seq, >1 client).
        let mut i = 0;
        while i < entries.len() {
            let seq = entries[i].sequence;
            let mut group = vec![&entries[i]];
            let mut j = i + 1;
            while j < entries.len() && entries[j].sequence == seq {
                group.push(&entries[j]);
                j += 1;
            }
            head = head.max(seq);

            let manifests: Vec<Vec<String>> = group
                .iter()
                .filter_map(|e| match &e.change_type {
                    ChangeType::BinarySnapshot { chunk_hashes } => Some(chunk_hashes.clone()),
                    _ => None,
                })
                .collect();

            match manifests.len() {
                0 => {} // Delete or non-binary at this seq; ignore.
                1 => current.clone_from(&manifests[0]),
                _ => {
                    // Fork: three-way merge the first two forked manifests
                    // against the pre-fork state.
                    let merged = binary::merge_manifests(
                        &current,
                        &manifests[0],
                        &manifests[1],
                        self.binary_policy,
                    );
                    current = match merged {
                        BinaryMerge::Unchanged(m) | BinaryMerge::FastForward(m) => m,
                        BinaryMerge::Conflict {
                            resolved,
                            needs_copy: nc,
                        } => {
                            needs_copy |= nc;
                            resolved
                        }
                    };
                }
            }
            i = j;
        }
        (
            TrackedFile {
                head,
                state: FileState::Binary { manifest: current },
            },
            needs_copy,
        )
    }

    /// Persist a file's state, oplog cache row, and sync cursor atomically.
    fn persist_file(
        &self,
        file_id: &str,
        file: &TrackedFile,
        cache_entry: Option<&OpLogEntry>,
    ) -> Result<(), SyncError> {
        let persisted = match &file.state {
            FileState::Text(doc) => PersistedState::Text {
                full_update: doc.full_update(),
                head: file.head,
            },
            FileState::Binary { manifest } => PersistedState::Binary {
                manifest: manifest.clone(),
                head: file.head,
            },
        };
        let bytes = serde_json::to_vec(&persisted)
            .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;

        // Serialize the optional cache entry before the atomic persist.
        let cache = cache_entry
            .map(|entry| {
                serde_json::to_vec(entry)
                    .map(|data| OplogCacheEntry {
                        sequence: entry.sequence,
                        data,
                    })
                    .map_err(|e| SyncError::SerdeError { msg: e.to_string() })
            })
            .transpose()?;
        self.store
            .persist_file(file_id.to_owned(), bytes, file.head, cache)
    }

    /// Publish an entry with CAS retry, rebasing text deltas onto the winner on
    /// each collision. Returns the sequence actually published at.
    fn publish_with_retry(
        &self,
        file_id: &str,
        file: &mut TrackedFile,
        make_entry: impl Fn(u64) -> OpLogEntry,
    ) -> Result<OpLogEntry, SyncError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > MAX_CAS_ATTEMPTS {
                return Err(SyncError::RetryExhausted { attempts });
            }
            let seq = file.head + 1;
            let entry = make_entry(seq);
            match self.remote.put_oplog_cas(file_id.to_owned(), entry.clone()) {
                Ok(()) => {
                    file.head = seq;
                    return Ok(entry);
                }
                Err(SyncError::Conflict { .. }) => {
                    // A peer took this sequence. Integrate everything from the
                    // remote (idempotent for CRDT text) and retry one higher.
                    self.integrate_remote(file_id, file)?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fold all remote entries into `file`, advancing `head`. Idempotent for
    /// text (CRDT); for binary it re-resolves the manifest.
    fn integrate_remote(&self, file_id: &str, file: &mut TrackedFile) -> Result<(), SyncError> {
        let entries = self.load_entries(file_id)?;
        match &mut file.state {
            FileState::Text(_) => {
                let rebuilt = self.build_text(file_id, &entries)?;
                *file = rebuilt;
            }
            FileState::Binary { .. } => {
                let (rebuilt, _copy) = self.build_binary(&entries);
                *file = rebuilt;
            }
        }
        Ok(())
    }

    /// Upload chunks with per-chunk resume: skip chunks already marked done (or
    /// already present remotely), marking each done after a successful upload.
    fn upload_chunks(
        &self,
        chunks: &[(crate::types::ChunkInfo, Vec<u8>)],
    ) -> Result<(), SyncError> {
        for (info, bytes) in chunks {
            if self.store.is_chunk_done(info.hash.clone())? {
                continue;
            }
            self.remote.put_chunk(info.hash.clone(), bytes.clone())?;
            self.store.mark_chunk_done(info.hash.clone())?;
        }
        Ok(())
    }
}

#[uniffi::export]
impl SyncEngine {
    /// Apply a local text edit, publishing the incremental CRDT delta. Returns
    /// the published sequence.
    pub fn modify_text(&self, file_id: String, new_content: String) -> Result<u64, SyncError> {
        let mut files = self.files.write().expect("files lock poisoned");
        let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
            head: 0,
            state: FileState::Text(TextDoc::new()),
        });
        // Ensure the state is text (a freshly-tracked binary file can't take text edits).
        if !matches!(file.state, FileState::Text(_)) {
            return Err(SyncError::IoError {
                msg: format!("{file_id} is not a text file"),
            });
        }
        let delta = match &mut file.state {
            FileState::Text(doc) => doc.set_text(&new_content),
            FileState::Binary { .. } => unreachable!(),
        };

        let client_id = self.client_id.clone();
        let entry = self.publish_with_retry(&file_id, file, |seq| OpLogEntry {
            sequence: seq,
            client_id: client_id.clone(),
            timestamp: now_millis(),
            change_type: ChangeType::TextDelta {
                delta: delta.clone(),
            },
        })?;
        let seq = entry.sequence;
        self.persist_file(&file_id, file, Some(&entry))?;
        self.remote.put_status(self.client_id.clone(), file.head)?;
        drop(files);
        self.listener.on_file_content_updated(file_id);
        Ok(seq)
    }

    /// Apply a local binary edit: chunk, upload new chunks (resumable), and
    /// publish the manifest snapshot. Returns the published sequence.
    pub fn modify_binary(&self, file_id: String, data: Vec<u8>) -> Result<u64, SyncError> {
        // Chunk + upload outside the files lock (I/O heavy, lock-free append).
        let chunks = binary::chunk_data(&data);
        self.upload_chunks(&chunks)?;
        let man: Vec<String> = chunks.iter().map(|(c, _)| c.hash.clone()).collect();

        let mut files = self.files.write().expect("files lock poisoned");
        let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
            head: 0,
            state: FileState::Binary {
                manifest: Vec::new(),
            },
        });
        if !matches!(file.state, FileState::Binary { .. }) {
            return Err(SyncError::IoError {
                msg: format!("{file_id} is not a binary file"),
            });
        }

        let client_id = self.client_id.clone();
        let entry = self.publish_with_retry(&file_id, file, |seq| OpLogEntry {
            sequence: seq,
            client_id: client_id.clone(),
            timestamp: now_millis(),
            change_type: ChangeType::BinarySnapshot {
                chunk_hashes: man.clone(),
            },
        })?;
        let seq = entry.sequence;
        file.state = FileState::Binary { manifest: man };
        self.persist_file(&file_id, file, Some(&entry))?;
        self.remote.put_status(self.client_id.clone(), file.head)?;
        drop(files);
        self.listener.on_file_content_updated(file_id);
        Ok(seq)
    }

    /// Pull the remote oplog and rebuild the file's converged state. All remote
    /// reads happen before the single local persist, so a mid-sync remote
    /// failure leaves local state unchanged.
    pub fn sync(&self, file_id: String) -> Result<(), SyncError> {
        // Re-entrancy guard: if a sync of this file is already in flight, that
        // one covers the request — do not re-enter the state machine.
        {
            let mut in_flight = self.in_flight.lock().expect("in_flight lock poisoned");
            if !in_flight.insert(file_id.clone()) {
                return Ok(());
            }
        }
        let _guard = SyncGuard {
            in_flight: &self.in_flight,
            file_id: file_id.clone(),
        };

        let entries = self.load_entries(&file_id)?;
        if entries.is_empty() {
            return Ok(());
        }
        // Infer kind from the first content-bearing entry.
        let is_text = entries
            .iter()
            .any(|e| matches!(e.change_type, ChangeType::TextDelta { .. }));

        let (file, needs_copy) = if is_text {
            (self.build_text(&file_id, &entries)?, false)
        } else {
            self.build_binary(&entries)
        };

        {
            let mut files = self.files.write().expect("files lock poisoned");
            self.persist_file(&file_id, &file, None)?;
            self.remote.put_status(self.client_id.clone(), file.head)?;
            files.insert(file_id.clone(), file);
        }

        if needs_copy {
            self.listener
                .on_conflict_copy_requested(file_id.clone(), format!("{file_id} (conflict)"));
        }
        self.listener.on_file_content_updated(file_id.clone());

        // If the log tip is forked (two clients published the same max sequence),
        // publish a unifying entry so the branches reconverge on the remote. Once
        // a single entry sits at the tip, later syncs see no tip-fork and stop.
        if tip_is_forked(&entries) {
            self.publish_convergence(&file_id, is_text)?;
        }
        Ok(())
    }

    /// Publish a unifying entry at `head+1` capturing the already-merged state,
    /// collapsing a forked log tip back to a single head.
    fn publish_convergence(&self, file_id: &str, is_text: bool) -> Result<(), SyncError> {
        let mut files = self.files.write().expect("files lock poisoned");
        let Some(file) = files.get_mut(file_id) else {
            return Ok(());
        };
        let client_id = self.client_id.clone();
        let entry = if is_text {
            let delta = match &file.state {
                FileState::Text(doc) => doc.full_update(),
                FileState::Binary { .. } => return Ok(()),
            };
            self.publish_with_retry(file_id, file, |seq| OpLogEntry {
                sequence: seq,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: ChangeType::TextDelta {
                    delta: delta.clone(),
                },
            })?
        } else {
            let manifest = match &file.state {
                FileState::Binary { manifest } => manifest.clone(),
                FileState::Text(_) => return Ok(()),
            };
            self.publish_with_retry(file_id, file, |seq| OpLogEntry {
                sequence: seq,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: ChangeType::BinarySnapshot {
                    chunk_hashes: manifest.clone(),
                },
            })?
        };
        self.persist_file(file_id, file, Some(&entry))?;
        self.remote.put_status(self.client_id.clone(), file.head)?;
        Ok(())
    }

    /// Roll a file back to the state at sequence `k` by appending a *new*
    /// compensating entry (history is never mutated). Returns the new sequence.
    pub fn rollback(&self, file_id: String, k: u64) -> Result<u64, SyncError> {
        let entries = self.load_entries(&file_id)?;
        if entries.is_empty() {
            return Err(SyncError::IoError {
                msg: format!("nothing to roll back for {file_id}"),
            });
        }
        let is_text = entries
            .iter()
            .any(|e| matches!(e.change_type, ChangeType::TextDelta { .. }));

        let mut files = self.files.write().expect("files lock poisoned");
        let seq = if is_text {
            // Rebuild the content as of k, then drive the live doc to it.
            let at_k = {
                let baseline = self.load_baseline(&file_id)?;
                let baseline_seq = baseline.as_ref().map_or(0, |(_, s)| *s);
                let baseline_bytes = baseline.as_ref().map(|(b, _)| b.as_slice());
                let deltas: Vec<Vec<u8>> = entries
                    .iter()
                    .filter(|e| e.sequence > baseline_seq && e.sequence <= k)
                    .filter_map(|e| match &e.change_type {
                        ChangeType::TextDelta { delta } => Some(delta.clone()),
                        _ => None,
                    })
                    .collect();
                TextDoc::from_baseline_and_deltas(baseline_bytes, &deltas)?.content()
            };
            // Make sure the live file reflects current head before editing.
            let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
                head: 0,
                state: FileState::Text(TextDoc::new()),
            });
            self.integrate_remote(&file_id, file)?;
            let delta = match &mut file.state {
                FileState::Text(doc) => doc.set_text(&at_k),
                FileState::Binary { .. } => {
                    return Err(SyncError::IoError {
                        msg: "kind mismatch during rollback".into(),
                    });
                }
            };
            let client_id = self.client_id.clone();
            let entry = self.publish_with_retry(&file_id, file, |s| OpLogEntry {
                sequence: s,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: ChangeType::TextDelta {
                    delta: delta.clone(),
                },
            })?;
            self.persist_file(&file_id, file, Some(&entry))?;
            entry.sequence
        } else {
            // Binary: republish the manifest recorded at (or just before) k.
            let manifest = entries
                .iter()
                .filter(|e| e.sequence <= k)
                .rev()
                .find_map(|e| match &e.change_type {
                    ChangeType::BinarySnapshot { chunk_hashes } => Some(chunk_hashes.clone()),
                    _ => None,
                })
                .ok_or_else(|| SyncError::IoError {
                    msg: format!("no binary snapshot at or before sequence {k}"),
                })?;
            let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
                head: 0,
                state: FileState::Binary {
                    manifest: Vec::new(),
                },
            });
            self.integrate_remote(&file_id, file)?;
            let client_id = self.client_id.clone();
            let entry = self.publish_with_retry(&file_id, file, |s| OpLogEntry {
                sequence: s,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: ChangeType::BinarySnapshot {
                    chunk_hashes: manifest.clone(),
                },
            })?;
            file.state = FileState::Binary {
                manifest: manifest.clone(),
            };
            self.persist_file(&file_id, file, Some(&entry))?;
            entry.sequence
        };
        self.remote.put_status(self.client_id.clone(), seq)?;
        drop(files);
        self.listener.on_file_content_updated(file_id);
        Ok(seq)
    }

    /// Truncate history to keep at most `n` recent versions, using the safe low
    /// watermark across all clients. Uploads a compressed baseline, deletes
    /// superseded oplogs, and garbage-collects orphaned binary chunks.
    pub fn truncate(&self, file_id: String, n: u64) -> Result<(), SyncError> {
        let entries = self.load_entries(&file_id)?;
        let Some(latest) = entries.iter().map(|e| e.sequence).max() else {
            return Ok(());
        };
        // Safe low watermark = min synced sequence across clients (default to
        // latest if no status has been published).
        let statuses = self.remote.list_statuses()?;
        let watermark = statuses
            .iter()
            .map(|s| s.last_synced_sequence)
            .min()
            .unwrap_or(latest);
        // Cut line: keep the newest `n`, but never past what all clients have seen.
        let cut = latest.saturating_sub(n).min(watermark);
        if cut == 0 {
            return Ok(()); // Nothing safely truncatable yet.
        }

        let is_text = entries
            .iter()
            .any(|e| matches!(e.change_type, ChangeType::TextDelta { .. }));
        if is_text {
            // Merge 0..=cut into one full-update baseline and compress it.
            let existing = self.load_baseline(&file_id)?;
            let existing_seq = existing.as_ref().map_or(0, |(_, s)| *s);
            let existing_bytes = existing.as_ref().map(|(b, _)| b.as_slice());
            let deltas: Vec<Vec<u8>> = entries
                .iter()
                .filter(|e| e.sequence > existing_seq && e.sequence <= cut)
                .filter_map(|e| match &e.change_type {
                    ChangeType::TextDelta { delta } => Some(delta.clone()),
                    _ => None,
                })
                .collect();
            let doc = TextDoc::from_baseline_and_deltas(existing_bytes, &deltas)?;
            let full = doc.full_update();
            let compressed = zstd::encode_all(full.as_slice(), ZSTD_LEVEL)
                .map_err(|e| SyncError::IoError { msg: e.to_string() })?;
            self.remote.put_baseline(file_id.clone(), cut, compressed)?;
        }

        // Delete superseded oplogs (<= cut) from the remote and local cache.
        let mut items = self.remote.list_oplogs(file_id.clone())?;
        oplog::sort_items(&mut items);
        for item in items.iter().filter(|i| i.sequence <= cut) {
            self.remote
                .delete_oplog(file_id.clone(), item.remote_path.clone())?;
        }
        self.store.commit_truncation(file_id.clone(), cut)?;

        // Binary GC: delete chunks referenced by no surviving manifest.
        if !is_text {
            let survivors = self.load_entries(&file_id)?;
            let live: Vec<Vec<String>> = survivors
                .iter()
                .filter_map(|e| match &e.change_type {
                    ChangeType::BinarySnapshot { chunk_hashes } => Some(chunk_hashes.clone()),
                    _ => None,
                })
                .collect();
            let stored = self.remote.list_chunks()?;
            for orphan in binary::orphaned_chunks(&live, &stored) {
                self.remote.delete_chunk(orphan)?;
            }
        }
        Ok(())
    }

    /// Current text content of a tracked file, or an error if it is not a
    /// tracked text file.
    pub fn get_text(&self, file_id: String) -> Result<String, SyncError> {
        let files = self.files.read().expect("files lock poisoned");
        match files.get(&file_id).map(|f| &f.state) {
            Some(FileState::Text(doc)) => Ok(doc.content()),
            _ => Err(SyncError::IoError {
                msg: format!("{file_id} is not a tracked text file"),
            }),
        }
    }

    /// Current chunk manifest of a tracked binary file.
    pub fn get_manifest(&self, file_id: String) -> Result<Vec<String>, SyncError> {
        let files = self.files.read().expect("files lock poisoned");
        match files.get(&file_id).map(|f| &f.state) {
            Some(FileState::Binary { manifest }) => Ok(manifest.clone()),
            _ => Err(SyncError::IoError {
                msg: format!("{file_id} is not a tracked binary file"),
            }),
        }
    }

    /// Current head sequence of a tracked file (0 if untracked).
    pub fn head(&self, file_id: String) -> u64 {
        self.files
            .read()
            .expect("files lock poisoned")
            .get(&file_id)
            .map_or(0, |f| f.head)
    }

    /// True while a [`SyncEngine::sync`] of `file_id` is in flight. Lets a host
    /// observe the `Idle`/`Syncing` phase and avoid redundant triggers.
    pub fn is_syncing(&self, file_id: String) -> bool {
        self.in_flight
            .lock()
            .expect("in_flight lock poisoned")
            .contains(&file_id)
    }
}
