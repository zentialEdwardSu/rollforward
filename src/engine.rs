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
    BinaryConflictPolicy, BinaryFileState, BinaryModification, BinaryModificationResult,
    ChangeType, EngineNotificationListener, MaintenanceReport, OpLogEntry, OplogCacheEntry,
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
        /// Whether the current head is a deletion tombstone.
        deleted: bool,
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
        /// Added after the initial format; absent values mean present.
        #[serde(default)]
        deleted: bool,
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

    /// Decode every remote oplog entry for a file, sorted by (sequence, client),
    /// discarding the list of entries newly fetched from the remote. Used by
    /// callers (rollback/truncate) that do not update the local oplog cache.
    fn load_entries(&self, file_id: &str) -> Result<Vec<OpLogEntry>, SyncError> {
        Ok(self.load_entries_caching(file_id)?.0)
    }

    /// Decode every remote oplog entry for a file, sorted by (sequence, client),
    /// serving each entry's bytes from the local redb oplog cache when possible
    /// and issuing a remote `get_oplog` only for entries not already cached.
    ///
    /// Returns `(all_entries, newly_fetched)` where `newly_fetched` is the subset
    /// pulled from the remote this call — the caller passes it to
    /// [`LocalStore::cache_oplogs`] *after* the authoritative persist so the next
    /// sync avoids re-fetching them.
    ///
    /// Correctness:
    /// - Every remote read happens here, before any caller-side persist, so a
    ///   mid-read failure leaves local state untouched (rollback guarantee).
    /// - A **forked** sequence (claimed by more than one client) is always
    ///   re-fetched, never served from the single-slot cache — so fork detection
    ///   and three-way merge see every side.
    /// - The cache is consulted only for sequences present in the remote listing,
    ///   so rows left stale by a remote truncation on another client are inert.
    fn load_entries_caching(
        &self,
        file_id: &str,
    ) -> Result<(Vec<OpLogEntry>, Vec<OplogCacheEntry>), SyncError> {
        let mut items = self.remote.list_oplogs(file_id.to_owned())?;
        oplog::sort_items(&mut items);
        let forked: HashSet<u64> = oplog::forked_sequences(&items).into_iter().collect();

        let mut cached: HashMap<u64, Vec<u8>> = HashMap::new();
        for e in self.store.list_oplogs(file_id.to_owned())? {
            cached.insert(e.sequence, e.data);
        }

        let mut entries = Vec::with_capacity(items.len());
        let mut fetched = Vec::new();
        for item in &items {
            let bytes = if forked.contains(&item.sequence) {
                // A forked sequence is always re-fetched — never served from the
                // single-slot cache — so every side reaches the merge.
                self.remote
                    .get_oplog(file_id.to_owned(), item.remote_path.clone())?
            } else if let Some(b) = cached.get(&item.sequence) {
                b.clone()
            } else {
                let b = self
                    .remote
                    .get_oplog(file_id.to_owned(), item.remote_path.clone())?;
                fetched.push(OplogCacheEntry {
                    sequence: item.sequence,
                    data: b.clone(),
                });
                b
            };
            let entry: OpLogEntry = serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            entries.push(entry);
        }
        Ok((entries, fetched))
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
    fn build_binary(&self, entries: &[OpLogEntry]) -> Result<(TrackedFile, bool), SyncError> {
        let mut current: Option<Vec<String>> = None;
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

            let states: Vec<Option<Vec<String>>> = group
                .iter()
                .filter_map(|e| match &e.change_type {
                    ChangeType::BinarySnapshot { chunk_hashes } => Some(Some(chunk_hashes.clone())),
                    ChangeType::Delete => Some(None),
                    _ => None,
                })
                .collect();

            match states.len() {
                0 => {}
                1 => current.clone_from(&states[0]),
                _ => {
                    let left = &states[0];
                    let right = &states[1];
                    if left == right {
                        current.clone_from(left);
                    } else if left.is_none() || right.is_none() {
                        match self.binary_policy {
                            BinaryConflictPolicy::Manual => {
                                return Err(SyncError::ConflictNeedResolution);
                            }
                            BinaryConflictPolicy::KeepLocal | BinaryConflictPolicy::KeepBoth => {
                                needs_copy |= self.binary_policy == BinaryConflictPolicy::KeepBoth;
                                current.clone_from(left);
                            }
                            BinaryConflictPolicy::KeepRemote => current.clone_from(right),
                        }
                    } else if left == &current {
                        current.clone_from(right);
                    } else if right == &current {
                        current.clone_from(left);
                    } else {
                        match (left, right) {
                            (Some(left), Some(right)) => {
                                let base = current.as_deref().unwrap_or(&[]);
                                current = Some(
                                    match binary::merge_manifests(
                                        base,
                                        left,
                                        right,
                                        self.binary_policy,
                                    ) {
                                        BinaryMerge::Unchanged(m) | BinaryMerge::FastForward(m) => {
                                            m
                                        }
                                        BinaryMerge::NeedsResolution => {
                                            return Err(SyncError::ConflictNeedResolution);
                                        }
                                        BinaryMerge::Conflict {
                                            resolved,
                                            needs_copy: nc,
                                        } => {
                                            needs_copy |= nc;
                                            resolved
                                        }
                                    },
                                );
                            }
                            _ => unreachable!("mixed present/deleted fork handled above"),
                        }
                    }
                }
            }
            i = j;
        }
        Ok((
            TrackedFile {
                head,
                state: FileState::Binary {
                    manifest: current.clone().unwrap_or_default(),
                    deleted: current.is_none(),
                },
            },
            needs_copy,
        ))
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
            FileState::Binary { manifest, deleted } => PersistedState::Binary {
                manifest: manifest.clone(),
                deleted: *deleted,
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
                let (rebuilt, _copy) = self.build_binary(&entries)?;
                *file = rebuilt;
            }
        }
        Ok(())
    }

    /// Build the union chunk-location index across every pack index on the
    /// remote: `hash -> (pack_id, offset, length)`. Pack indexes are immutable
    /// and content-addressed, so unioning them needs no coordination; a hash in
    /// more than one pack resolves to whichever wins the fold (bytes identical).
    fn load_pack_index(&self) -> Result<HashMap<String, (String, u64, u32)>, SyncError> {
        let mut map = HashMap::new();
        for index_id in self.remote.list_pack_indexes()? {
            let bytes = self.remote.get_pack_index(index_id)?;
            let index: binary::PackIndex = serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            for c in index.chunks {
                map.entry(c.hash)
                    .or_insert((index.pack_id.clone(), c.offset, c.length));
            }
        }
        Ok(map)
    }

    /// Pack and upload the chunks not already stored remotely, with pack-level
    /// resume. Dedup is decided against the union pack index (content-addressed),
    /// so re-uploading identical content produces no new packs. `put_pack` /
    /// `put_pack_index` are idempotent by id, so an upload interrupted after some
    /// packs are written completes safely on retry (already-written packs re-PUT
    /// as no-ops).
    fn upload_packs(&self, chunks: &[(crate::types::ChunkInfo, Vec<u8>)]) -> Result<(), SyncError> {
        let existing = self.load_pack_index()?;
        let new: Vec<(crate::types::ChunkInfo, Vec<u8>)> = chunks
            .iter()
            .filter(|(c, _)| !existing.contains_key(&c.hash))
            .cloned()
            .collect();
        for (pack_id, bytes, index) in binary::build_packs(&new) {
            let index_bytes = serde_json::to_vec(&index)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            self.remote.put_pack(pack_id.clone(), bytes)?;
            self.remote.put_pack_index(pack_id, index_bytes)?;
        }
        Ok(())
    }

    /// Garbage-collect every pack against the `live` chunk set: delete dead
    /// packs, repack mostly-dead ones (see [`binary::classify_pack`]). Each
    /// repack writes the new pack *before* deleting the old, so an interruption
    /// never strands a live chunk — at worst a superseded pack lingers until the
    /// next pass.
    /// Union of every chunk hash still referenced by any surviving binary
    /// manifest across *all* files on the remote. Read authoritatively from the
    /// remote oplog (not the local `files` map) because another client may track
    /// files this client has never opened. A chunk absent from this set is dead
    /// for the whole store and its pack may be reclaimed.
    fn global_live_chunks(&self) -> Result<HashSet<String>, SyncError> {
        let mut live = HashSet::new();
        for fid in self.remote.list_files()? {
            for entry in self.load_entries(&fid)? {
                if let ChangeType::BinarySnapshot { chunk_hashes } = entry.change_type {
                    live.extend(chunk_hashes);
                }
            }
        }
        Ok(live)
    }

    /// Truncate one file without running pack GC. Batch maintenance invokes
    /// this for every file first, then computes liveness and collects once.
    fn truncate_file_history(&self, file_id: &str, n: u64) -> Result<(bool, u64, bool), SyncError> {
        let entries = self.load_entries(file_id)?;
        let Some(latest) = entries.iter().map(|e| e.sequence).max() else {
            return Ok((false, 0, false));
        };
        let watermark = self
            .remote
            .list_statuses()?
            .iter()
            .map(|status| status.last_synced_sequence)
            .min()
            .unwrap_or(latest);
        let cut = latest.saturating_sub(n).min(watermark);
        if cut == 0 {
            return Ok((false, 0, true));
        }

        let is_text = entries
            .iter()
            .any(|entry| matches!(entry.change_type, ChangeType::TextDelta { .. }));
        if is_text {
            let existing = self.load_baseline(file_id)?;
            let existing_seq = existing.as_ref().map_or(0, |(_, sequence)| *sequence);
            let existing_bytes = existing.as_ref().map(|(bytes, _)| bytes.as_slice());
            let deltas: Vec<Vec<u8>> = entries
                .iter()
                .filter(|entry| entry.sequence > existing_seq && entry.sequence <= cut)
                .filter_map(|entry| match &entry.change_type {
                    ChangeType::TextDelta { delta } => Some(delta.clone()),
                    _ => None,
                })
                .collect();
            let doc = TextDoc::from_baseline_and_deltas(existing_bytes, &deltas)?;
            let compressed =
                zstd::encode_all(doc.full_update().as_slice(), ZSTD_LEVEL).map_err(|error| {
                    SyncError::IoError {
                        msg: error.to_string(),
                    }
                })?;
            self.remote
                .put_baseline(file_id.to_owned(), cut, compressed)?;
        }

        let mut deleted = 0;
        let mut items = self.remote.list_oplogs(file_id.to_owned())?;
        oplog::sort_items(&mut items);
        for item in items.iter().filter(|item| item.sequence <= cut) {
            self.remote
                .delete_oplog(file_id.to_owned(), item.remote_path.clone())?;
            deleted += 1;
        }
        self.store.commit_truncation(file_id.to_owned(), cut)?;
        Ok((true, deleted, false))
    }

    fn gc_packs(&self, live: &HashSet<String>) -> Result<(u64, u64, u64), SyncError> {
        let mut deleted = 0;
        let mut repacked = 0;
        let mut reclaimed = 0;
        for index_id in self.remote.list_pack_indexes()? {
            let bytes = self.remote.get_pack_index(index_id.clone())?;
            let index: binary::PackIndex = serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            match binary::classify_pack(&index.chunks, live) {
                binary::PackGc::Keep | binary::PackGc::KeepMixed => {}
                binary::PackGc::Delete => {
                    reclaimed += index
                        .chunks
                        .iter()
                        .map(|chunk| u64::from(chunk.length))
                        .sum::<u64>();
                    self.remote.delete_pack(index.pack_id)?;
                    self.remote.delete_pack_index(index_id)?;
                    deleted += 1;
                }
                binary::PackGc::Repack { live: live_hashes } => {
                    let total = index
                        .chunks
                        .iter()
                        .map(|chunk| u64::from(chunk.length))
                        .sum::<u64>();
                    let live_bytes = index
                        .chunks
                        .iter()
                        .filter(|chunk| live_hashes.contains(&chunk.hash))
                        .map(|chunk| u64::from(chunk.length))
                        .sum::<u64>();
                    reclaimed += total.saturating_sub(live_bytes);
                    self.repack_live(&index, &live_hashes)?;
                    repacked += 1;
                }
            }
        }
        Ok((deleted, repacked, reclaimed))
    }

    /// Rewrite the surviving chunks of `old` into a fresh pack, then delete the
    /// old pack and its index. Reads each live chunk's bytes by range from the
    /// old pack and re-packs them (a repacked pack is fully live, so it survives
    /// subsequent passes). No-op-safe: if the repacked chunks all already live
    /// elsewhere, `build_packs` still emits a self-contained pack for them.
    fn repack_live(
        &self,
        old: &binary::PackIndex,
        live_hashes: &[String],
    ) -> Result<(), SyncError> {
        let mut chunks: Vec<(crate::types::ChunkInfo, Vec<u8>)> = Vec::new();
        for c in &old.chunks {
            if live_hashes.contains(&c.hash) {
                let data = self
                    .remote
                    .get_pack_range(old.pack_id.clone(), c.offset, c.length)?;
                // Verify before rewriting: repacking a corrupt/truncated read
                // would content-address the bad bytes into a fresh pack under a
                // new id, silently laundering corruption. Fail loud instead.
                if !binary::verify_chunk(&c.hash, &data) {
                    return Err(SyncError::Corrupt {
                        hash: c.hash.clone(),
                    });
                }
                chunks.push((
                    crate::types::ChunkInfo {
                        hash: c.hash.clone(),
                        offset: c.offset,
                        length: c.length,
                    },
                    data,
                ));
            }
        }
        for (pack_id, bytes, index) in binary::build_packs(&chunks) {
            let index_bytes = serde_json::to_vec(&index)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            self.remote.put_pack(pack_id.clone(), bytes)?;
            self.remote.put_pack_index(pack_id, index_bytes)?;
        }
        // Old pack superseded: its live chunks now live in the fresh pack(s).
        self.remote.delete_pack(old.pack_id.clone())?;
        self.remote.delete_pack_index(old.pack_id.clone())?;
        Ok(())
    }
}

#[uniffi::export]
impl SyncEngine {
    /// Reassemble and return the current bytes of a tracked binary file,
    /// resolving each manifest chunk to a byte range within its pack. Errors if
    /// the file is not a tracked binary file or a chunk cannot be located.
    pub fn read_binary(&self, file_id: String) -> Result<Vec<u8>, SyncError> {
        let manifest = {
            let files = self.files.read().expect("files lock poisoned");
            match files.get(&file_id).map(|f| &f.state) {
                Some(FileState::Binary {
                    manifest,
                    deleted: false,
                }) => manifest.clone(),
                Some(FileState::Binary { deleted: true, .. }) => {
                    return Err(SyncError::IoError {
                        msg: format!("{file_id} is deleted"),
                    });
                }
                _ => {
                    return Err(SyncError::IoError {
                        msg: format!("{file_id} is not a tracked binary file"),
                    });
                }
            }
        };
        let index = self.load_pack_index()?;
        binary::reassemble(&manifest, |hash| {
            let (pack_id, offset, length) = index.get(hash).ok_or_else(|| SyncError::IoError {
                msg: format!("chunk {hash} not found in any pack"),
            })?;
            let bytes = self
                .remote
                .get_pack_range(pack_id.clone(), *offset, *length)?;
            // Content is hash-addressed: verify the bytes actually hash to the
            // manifest hash. Catches truncated Range responses and in-transit
            // corruption that a network backend can deliver but a length-only
            // read would accept as valid.
            if !binary::verify_chunk(hash, &bytes) {
                return Err(SyncError::Corrupt { hash: hash.into() });
            }
            Ok(bytes)
        })
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
        self.upload_packs(&chunks)?;
        let man: Vec<String> = chunks.iter().map(|(c, _)| c.hash.clone()).collect();

        let mut files = self.files.write().expect("files lock poisoned");
        let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
            head: 0,
            state: FileState::Binary {
                manifest: Vec::new(),
                deleted: false,
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
        file.state = FileState::Binary {
            manifest: man,
            deleted: false,
        };
        self.persist_file(&file_id, file, Some(&entry))?;
        self.remote.put_status(self.client_id.clone(), file.head)?;
        drop(files);
        self.listener.on_file_content_updated(file_id);
        Ok(seq)
    }

    /// Apply several binary edits with one shared chunk-dedup/index pass. New
    /// chunks from different files are packed together up to the normal pack
    /// target, which avoids one tiny pack per tiny file. Pack upload completes
    /// before any manifest oplog is published.
    pub fn modify_binaries(
        &self,
        modifications: Vec<BinaryModification>,
    ) -> Result<Vec<BinaryModificationResult>, SyncError> {
        if modifications.is_empty() {
            return Ok(Vec::new());
        }
        let prepared: Vec<(String, Vec<String>, Vec<(crate::types::ChunkInfo, Vec<u8>)>)> =
            modifications
                .into_iter()
                .map(|modification| {
                    let chunks = binary::chunk_data(&modification.data);
                    let manifest = chunks.iter().map(|(chunk, _)| chunk.hash.clone()).collect();
                    (modification.file_id, manifest, chunks)
                })
                .collect();
        let combined: Vec<(crate::types::ChunkInfo, Vec<u8>)> = prepared
            .iter()
            .flat_map(|(_, _, chunks)| chunks.iter().cloned())
            .collect();
        self.upload_packs(&combined)?;

        let mut files = self.files.write().expect("files lock poisoned");
        let mut results = Vec::with_capacity(prepared.len());
        let mut updated = Vec::with_capacity(prepared.len());
        for (file_id, manifest, _) in prepared {
            let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
                head: 0,
                state: FileState::Binary {
                    manifest: Vec::new(),
                    deleted: false,
                },
            });
            if !matches!(file.state, FileState::Binary { .. }) {
                return Err(SyncError::IoError {
                    msg: format!("{file_id} is not a binary file"),
                });
            }
            let client_id = self.client_id.clone();
            let entry = self.publish_with_retry(&file_id, file, |sequence| OpLogEntry {
                sequence,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: ChangeType::BinarySnapshot {
                    chunk_hashes: manifest.clone(),
                },
            })?;
            file.state = FileState::Binary {
                manifest: manifest.clone(),
                deleted: false,
            };
            self.persist_file(&file_id, file, Some(&entry))?;
            self.remote.put_status(self.client_id.clone(), file.head)?;
            results.push(BinaryModificationResult {
                file_id: file_id.clone(),
                sequence: entry.sequence,
                manifest,
            });
            updated.push(file_id);
        }
        drop(files);
        for file_id in updated {
            self.listener.on_file_content_updated(file_id);
        }
        Ok(results)
    }

    /// Publish a deletion tombstone for a binary file. Repeated deletion of an
    /// already-deleted tracked file is idempotent and returns its current head.
    pub fn delete_binary(&self, file_id: String) -> Result<u64, SyncError> {
        let mut files = self.files.write().expect("files lock poisoned");
        let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
            head: 0,
            state: FileState::Binary {
                manifest: Vec::new(),
                deleted: false,
            },
        });
        let already_deleted = match &file.state {
            FileState::Binary { deleted, .. } => *deleted,
            FileState::Text(_) => {
                return Err(SyncError::IoError {
                    msg: format!("{file_id} is not a binary file"),
                });
            }
        };
        if already_deleted {
            return Ok(file.head);
        }

        let client_id = self.client_id.clone();
        let entry = self.publish_with_retry(&file_id, file, |seq| OpLogEntry {
            sequence: seq,
            client_id: client_id.clone(),
            timestamp: now_millis(),
            change_type: ChangeType::Delete,
        })?;
        file.state = FileState::Binary {
            manifest: Vec::new(),
            deleted: true,
        };
        self.persist_file(&file_id, file, Some(&entry))?;
        self.remote.put_status(self.client_id.clone(), file.head)?;
        let seq = entry.sequence;
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

        let (entries, fetched) = self.load_entries_caching(&file_id)?;
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
            self.build_binary(&entries)?
        };

        {
            let mut files = self.files.write().expect("files lock poisoned");
            self.persist_file(&file_id, &file, None)?;
            self.remote.put_status(self.client_id.clone(), file.head)?;
            files.insert(file_id.clone(), file);
        }
        // Warm the local oplog cache with entries fetched this sync, so the next
        // sync (and CAS-retry rebase) serves them locally instead of re-GETting.
        // Runs after the authoritative persist above; a failure here is a
        // cache-only miss the next sync self-heals, but we surface it (Rule 12).
        self.store.cache_oplogs(file_id.clone(), fetched)?;

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
            let binary_state = match &file.state {
                FileState::Binary { manifest, deleted } => (manifest.clone(), *deleted),
                FileState::Text(_) => return Ok(()),
            };
            self.publish_with_retry(file_id, file, |seq| OpLogEntry {
                sequence: seq,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: if binary_state.1 {
                    ChangeType::Delete
                } else {
                    ChangeType::BinarySnapshot {
                        chunk_hashes: binary_state.0.clone(),
                    }
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
            // Binary: republish the snapshot or deletion recorded at/before k.
            let state_at_k = entries
                .iter()
                .filter(|e| e.sequence <= k)
                .rev()
                .find_map(|e| match &e.change_type {
                    ChangeType::BinarySnapshot { chunk_hashes } => Some(Some(chunk_hashes.clone())),
                    ChangeType::Delete => Some(None),
                    _ => None,
                })
                .ok_or_else(|| SyncError::IoError {
                    msg: format!("no binary state at or before sequence {k}"),
                })?;
            let file = files.entry(file_id.clone()).or_insert_with(|| TrackedFile {
                head: 0,
                state: FileState::Binary {
                    manifest: Vec::new(),
                    deleted: false,
                },
            });
            self.integrate_remote(&file_id, file)?;
            let client_id = self.client_id.clone();
            let entry = self.publish_with_retry(&file_id, file, |s| OpLogEntry {
                sequence: s,
                client_id: client_id.clone(),
                timestamp: now_millis(),
                change_type: match &state_at_k {
                    Some(manifest) => ChangeType::BinarySnapshot {
                        chunk_hashes: manifest.clone(),
                    },
                    None => ChangeType::Delete,
                },
            })?;
            file.state = FileState::Binary {
                manifest: state_at_k.clone().unwrap_or_default(),
                deleted: state_at_k.is_none(),
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
        self.truncate_file_history(&file_id, n)?;
        let live = self.global_live_chunks()?;
        self.gc_packs(&live)?;
        Ok(())
    }

    /// Truncate a set of file histories, then run exactly one global pack GC.
    pub fn truncate_many(
        &self,
        file_ids: Vec<String>,
        n: u64,
    ) -> Result<MaintenanceReport, SyncError> {
        let unique: HashSet<String> = file_ids.into_iter().collect();
        let mut report = MaintenanceReport {
            files_considered: unique.len() as u64,
            ..MaintenanceReport::default()
        };
        for file_id in unique {
            let (truncated, deleted, deferred) = self.truncate_file_history(&file_id, n)?;
            report.files_truncated += u64::from(truncated);
            report.oplogs_deleted += deleted;
            report.deferred_files += u64::from(deferred);
        }
        let live = self.global_live_chunks()?;
        let (deleted, repacked, reclaimed) = self.gc_packs(&live)?;
        report.packs_deleted = deleted;
        report.packs_repacked = repacked;
        report.bytes_reclaimed = reclaimed;
        Ok(report)
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
            Some(FileState::Binary {
                manifest,
                deleted: false,
            }) => Ok(manifest.clone()),
            Some(FileState::Binary { deleted: true, .. }) => Err(SyncError::IoError {
                msg: format!("{file_id} is deleted"),
            }),
            _ => Err(SyncError::IoError {
                msg: format!("{file_id} is not a tracked binary file"),
            }),
        }
    }

    /// Current logical binary state, including deletion tombstones.
    pub fn get_binary_state(&self, file_id: String) -> Result<BinaryFileState, SyncError> {
        let files = self.files.read().expect("files lock poisoned");
        match files.get(&file_id) {
            Some(TrackedFile {
                head,
                state: FileState::Binary { manifest, deleted },
            }) if *deleted => Ok(BinaryFileState::Deleted { head: *head }),
            Some(TrackedFile {
                head,
                state: FileState::Binary { manifest, .. },
            }) => Ok(BinaryFileState::Present {
                manifest: manifest.clone(),
                head: *head,
            }),
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

    /// Release the local store's durable resources. For the redb-backed store
    /// this frees the on-disk file lock so the same db path can be reopened —
    /// useful after an error when a host wants to reconstruct the engine
    /// against the same path without dropping every reference first. The engine
    /// must not be used afterward; subsequent store operations return an error.
    pub fn close(&self) -> Result<(), SyncError> {
        self.store.close()
    }
}
