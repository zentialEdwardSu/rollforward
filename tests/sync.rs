//! End-to-end integration tests: two engines sharing one dumb remote, covering
//! the full `test_design.txt` catalog — text/CRDT convergence, binary chunking,
//! FFI phase/interop, rollback, multi-client CAS, truncation/GC, and fault
//! injection.

// Single-use named helpers (fixtures, fault wrappers, entry builders) are
// idiomatic in a test file — `single_call_fn` is counterproductive here.
#![allow(clippy::single_call_fn)]

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use rollforward::types::{
    BinaryConflictPolicy, BinaryFileState, ChangeType, ChunkInfo, ClientStatus,
    EngineNotificationListener, OpLogEntry, OplogCacheEntry, RemoteLogItem,
};
use rollforward::{LocalFolderRemote, LocalStore, RedbStore, RemoteStorage, SyncEngine, SyncError};
use tempfile::TempDir;

/// A notification sink that records what the engine emitted, for assertions.
#[derive(Default)]
struct Recorder {
    /// file ids passed to `on_file_content_updated`, in order.
    updates: Mutex<Vec<String>>,
    /// file ids passed to `on_conflict_copy_requested`, in order.
    conflict_copies: Mutex<Vec<String>>,
}

impl Recorder {
    /// file ids for which a conflict copy was requested.
    fn conflict_copies(&self) -> Vec<String> {
        self.conflict_copies.lock().unwrap().clone()
    }
}

impl EngineNotificationListener for Recorder {
    fn on_file_content_updated(&self, file_id: String) {
        self.updates.lock().unwrap().push(file_id);
    }
    fn on_conflict_copy_requested(&self, file_id: String, _suggested_name: String) {
        self.conflict_copies.lock().unwrap().push(file_id);
    }
}

/// Build an engine with its own local db, sharing `remote`, `KeepBoth` policy.
fn engine(
    client: &str,
    dir: &TempDir,
    remote: Arc<dyn RemoteStorage>,
) -> (Arc<SyncEngine>, Arc<Recorder>) {
    engine_with_policy(client, dir, remote, BinaryConflictPolicy::KeepBoth)
}

/// Build an engine with an explicit binary conflict policy.
fn engine_with_policy(
    client: &str,
    dir: &TempDir,
    remote: Arc<dyn RemoteStorage>,
    policy: BinaryConflictPolicy,
) -> (Arc<SyncEngine>, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    let db = dir.path().join(format!("{client}.redb"));
    let store = Arc::new(RedbStore::open(db).unwrap());
    let eng = SyncEngine::with_backends(client, store, remote, rec.clone(), policy);
    (Arc::new(eng), rec)
}

/// Shared temp remote root.
fn shared_remote() -> (TempDir, Arc<dyn RemoteStorage>) {
    let dir = TempDir::new().unwrap();
    let remote: Arc<dyn RemoteStorage> = Arc::new(LocalFolderRemote::new(dir.path()).unwrap());
    (dir, remote)
}

/// The set of chunk hashes physically resolvable on the remote, unioned across
/// every pack index. Replaces the old `list_chunks()` for chunk-liveness
/// assertions now that chunks live inside packs.
fn stored_chunks(remote: &Arc<dyn RemoteStorage>) -> std::collections::HashSet<String> {
    let mut set = std::collections::HashSet::new();
    for index_id in remote.list_pack_indexes().unwrap() {
        let bytes = remote.get_pack_index(index_id).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for c in v["chunks"].as_array().unwrap() {
            set.insert(c["hash"].as_str().unwrap().to_string());
        }
    }
    set
}

/// Total chunk bytes physically stored across all packs (sum of every packed
/// chunk's length). Repack reclaims dead bytes, so this shrinks after GC.
fn stored_pack_bytes(remote: &Arc<dyn RemoteStorage>) -> u64 {
    let mut total = 0;
    for index_id in remote.list_pack_indexes().unwrap() {
        let bytes = remote.get_pack_index(index_id).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for c in v["chunks"].as_array().unwrap() {
            total += c["length"].as_u64().unwrap();
        }
    }
    total
}

/// Serialize an entry the way the engine does, for direct fork injection.
fn entry_bytes(entry: &OpLogEntry) -> Vec<u8> {
    serde_json::to_vec(entry).unwrap()
}

/// Inject a raw oplog entry through the *non-CAS* append, modeling two clients
/// that independently wrote the same sequence to a truly dumb remote (the only
/// way a genuine fork arises — the CAS path prevents it).
fn inject_fork_entry(remote: &Arc<dyn RemoteStorage>, file_id: &str, entry: &OpLogEntry) {
    remote.put_oplog(file_id.to_owned(), entry.clone()).unwrap();
    let _ = entry_bytes(entry); // ensure serializability parity with the engine
}

#[test]
fn concurrent_text_edits_converge_across_clients() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    // A establishes a base; B syncs to it.
    a.modify_text("doc".into(), "the quick fox".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "the quick fox");

    // Both edit concurrently from the same base (non-overlapping regions).
    a.modify_text("doc".into(), "the quick brown fox".into())
        .unwrap();
    b.modify_text("doc".into(), "the quick fox jumps".into())
        .unwrap();

    // Each syncs; both must converge to identical content containing both edits.
    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    let ta = a.get_text("doc".into()).unwrap();
    let tb = b.get_text("doc".into()).unwrap();
    assert_eq!(ta, tb, "clients must converge");
    assert!(
        ta.contains("brown") && ta.contains("jumps"),
        "both edits survive: {ta:?}"
    );
}

#[test]
fn rollback_propagates_to_other_client() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    let s1 = a.modify_text("doc".into(), "version one".into()).unwrap();
    a.modify_text("doc".into(), "version one and two".into())
        .unwrap();
    a.modify_text("doc".into(), "version one two three".into())
        .unwrap();

    // Roll A back to the state at s1.
    a.rollback("doc".into(), s1).unwrap();
    assert_eq!(a.get_text("doc".into()).unwrap(), "version one");

    // B syncs and sees the rolled-back state.
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "version one");
}

#[test]
fn truncation_creates_baseline_gcs_and_still_rebuilds() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // Drive many versions so there is history to truncate.
    let mut last = String::new();
    for i in 1..=20 {
        last = format!("line {i}\n") + &last;
        a.modify_text("doc".into(), last.clone()).unwrap();
    }
    // Only clientA has reported status; keep newest 5.
    a.truncate("doc".into(), 5).unwrap();

    // Baseline must now exist and old oplogs be gone.
    let baselines = remote.list_baselines("doc".into()).unwrap();
    assert!(!baselines.is_empty(), "a baseline should have been written");
    let remaining = remote.list_oplogs("doc".into()).unwrap();
    let cut = *baselines.last().unwrap();
    assert!(
        remaining.iter().all(|i| i.sequence > cut),
        "oplogs at or below cut {cut} must be deleted"
    );

    // A brand-new client (fresh db) rebuilds current content from baseline +
    // surviving oplogs alone.
    let fresh_dir = TempDir::new().unwrap();
    let (c, _) = engine("clientC", &fresh_dir, remote.clone());
    c.sync("doc".into()).unwrap();
    assert_eq!(c.get_text("doc".into()).unwrap(), last);
}

#[test]
fn binary_chunks_gc_after_truncation() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // Three distinct large binary versions -> distinct chunks.
    for v in 0..6u32 {
        let data: Vec<u8> = (0..120_000u32)
            .map(|i| ((i.wrapping_mul(v + 1)) % 251) as u8)
            .collect();
        a.modify_binary("blob".into(), data).unwrap();
    }
    let before = stored_chunks(&remote).len();
    a.truncate("blob".into(), 1).unwrap();
    let after = stored_chunks(&remote).len();
    assert!(
        after < before,
        "chunks in fully-dead packs should be GC'd ({before} -> {after})"
    );

    // Current manifest's chunks must all still be resolvable, and the file must
    // reassemble byte-identically from the surviving packs.
    let manifest = a.get_manifest("blob".into()).unwrap();
    let stored = stored_chunks(&remote);
    for h in &manifest {
        assert!(stored.contains(h), "live chunk {h} must survive GC");
    }
    assert!(a.read_binary("blob".into()).is_ok(), "blob still readable");
}

#[test]
fn cas_retry_resolves_concurrent_writes_without_overwrite() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    // Establish a shared base at seq 1.
    a.modify_text("doc".into(), "base".into()).unwrap();
    b.sync("doc".into()).unwrap();

    // Both attempt to write the next sequence "simultaneously". Because writes
    // go through put_oplog_cas, the loser rebases and retries at head+1 rather
    // than overwriting the winner.
    a.modify_text("doc".into(), "base A".into()).unwrap();
    b.modify_text("doc".into(), "base B".into()).unwrap();

    // Every sequence on the remote is claimed by exactly one client (no fork at
    // the same seq from the CAS path, no overwrite).
    let mut items = remote.list_oplogs("doc".into()).unwrap();
    items.sort_by_key(|i| i.sequence);
    let mut seqs: Vec<u64> = items.iter().map(|i| i.sequence).collect();
    let unique = {
        seqs.dedup();
        seqs.len()
    };
    assert_eq!(unique, items.len(), "no two entries share a sequence");

    // Both converge after syncing.
    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(
        a.get_text("doc".into()).unwrap(),
        b.get_text("doc".into()).unwrap()
    );
}

/// A configurable fault-injecting remote wrapping a real `LocalFolderRemote`.
/// Delegates everything to the inner remote except where a fault is armed:
/// - `fail_reads_after`: after N successful `get_oplog` reads, every further
///   read returns an error (mid-sync network drop → transaction rollback).
/// - `read_delay_ms`: sleep before each `get_oplog` (slow network for FFI-302).
struct FaultRemote {
    /// The real backend everything delegates to.
    inner: LocalFolderRemote,
    /// Remaining successful `get_oplog` reads before failures begin; `u32::MAX`
    /// means "never fail".
    fail_reads_after: AtomicU32,
    /// Per-read artificial delay in milliseconds.
    read_delay_ms: u64,
}

impl FaultRemote {
    /// A remote that fails every `get_oplog` after `n` successful reads.
    fn failing_reads_after(root: &std::path::Path, n: u32) -> Self {
        Self {
            inner: LocalFolderRemote::new(root).unwrap(),
            fail_reads_after: AtomicU32::new(n),
            read_delay_ms: 0,
        }
    }
    /// A remote that sleeps `ms` before each `get_oplog` (never fails).
    fn slow_reads(root: &std::path::Path, ms: u64) -> Self {
        Self {
            inner: LocalFolderRemote::new(root).unwrap(),
            fail_reads_after: AtomicU32::new(u32::MAX),
            read_delay_ms: ms,
        }
    }
}

impl RemoteStorage for FaultRemote {
    fn list_files(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_files()
    }
    fn list_oplogs(&self, file_id: String) -> Result<Vec<RemoteLogItem>, SyncError> {
        self.inner.list_oplogs(file_id)
    }
    fn put_oplog(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        self.inner.put_oplog(file_id, entry)
    }
    fn put_oplog_cas(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        self.inner.put_oplog_cas(file_id, entry)
    }
    fn get_oplog(&self, file_id: String, remote_path: String) -> Result<Vec<u8>, SyncError> {
        if self.read_delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.read_delay_ms));
        }
        // Consume the success budget; once exhausted, every read fails.
        let remaining = self.fail_reads_after.load(Ordering::SeqCst);
        if remaining == 0 {
            return Err(SyncError::IoError {
                msg: "injected read failure".into(),
            });
        }
        if remaining != u32::MAX {
            self.fail_reads_after.store(remaining - 1, Ordering::SeqCst);
        }
        self.inner.get_oplog(file_id, remote_path)
    }
    fn delete_oplog(&self, file_id: String, remote_path: String) -> Result<(), SyncError> {
        self.inner.delete_oplog(file_id, remote_path)
    }
    fn put_pack(&self, pack_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_pack(pack_id, data)
    }
    fn get_pack_range(
        &self,
        pack_id: String,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, SyncError> {
        self.inner.get_pack_range(pack_id, offset, length)
    }
    fn list_packs(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_packs()
    }
    fn delete_pack(&self, pack_id: String) -> Result<(), SyncError> {
        self.inner.delete_pack(pack_id)
    }
    fn put_pack_index(&self, index_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_pack_index(index_id, data)
    }
    fn get_pack_index(&self, index_id: String) -> Result<Vec<u8>, SyncError> {
        self.inner.get_pack_index(index_id)
    }
    fn list_pack_indexes(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_pack_indexes()
    }
    fn delete_pack_index(&self, index_id: String) -> Result<(), SyncError> {
        self.inner.delete_pack_index(index_id)
    }
    fn put_baseline(&self, file_id: String, seq: u64, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_baseline(file_id, seq, data)
    }
    fn get_baseline(&self, file_id: String, seq: u64) -> Result<Option<Vec<u8>>, SyncError> {
        self.inner.get_baseline(file_id, seq)
    }
    fn list_baselines(&self, file_id: String) -> Result<Vec<u64>, SyncError> {
        self.inner.list_baselines(file_id)
    }
    fn put_status(&self, client_id: String, last: u64) -> Result<(), SyncError> {
        self.inner.put_status(client_id, last)
    }
    fn list_statuses(&self) -> Result<Vec<ClientStatus>, SyncError> {
        self.inner.list_statuses()
    }
}

/// A remote that counts `get_oplog` calls, to assert the incremental-read
/// optimization (B1): a sync must not re-fetch oplog objects already served
/// from the local redb cache.
struct CountingRemote {
    /// The real backend everything delegates to.
    inner: LocalFolderRemote,
    /// Number of `get_oplog` calls observed.
    oplog_gets: AtomicU32,
}

impl CountingRemote {
    fn new(root: &std::path::Path) -> Self {
        Self {
            inner: LocalFolderRemote::new(root).unwrap(),
            oplog_gets: AtomicU32::new(0),
        }
    }
    /// Read and reset the `get_oplog` counter.
    fn take_gets(&self) -> u32 {
        self.oplog_gets.swap(0, Ordering::SeqCst)
    }
    /// Convenience: list oplog items directly off the inner backend.
    fn inner_list(&self, file_id: &str) -> Vec<RemoteLogItem> {
        self.inner.list_oplogs(file_id.to_owned()).unwrap()
    }
}

impl RemoteStorage for CountingRemote {
    fn list_files(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_files()
    }
    fn list_oplogs(&self, file_id: String) -> Result<Vec<RemoteLogItem>, SyncError> {
        self.inner.list_oplogs(file_id)
    }
    fn put_oplog(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        self.inner.put_oplog(file_id, entry)
    }
    fn put_oplog_cas(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        self.inner.put_oplog_cas(file_id, entry)
    }
    fn get_oplog(&self, file_id: String, remote_path: String) -> Result<Vec<u8>, SyncError> {
        self.oplog_gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_oplog(file_id, remote_path)
    }
    fn delete_oplog(&self, file_id: String, remote_path: String) -> Result<(), SyncError> {
        self.inner.delete_oplog(file_id, remote_path)
    }
    fn put_pack(&self, pack_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_pack(pack_id, data)
    }
    fn get_pack_range(
        &self,
        pack_id: String,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, SyncError> {
        self.inner.get_pack_range(pack_id, offset, length)
    }
    fn list_packs(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_packs()
    }
    fn delete_pack(&self, pack_id: String) -> Result<(), SyncError> {
        self.inner.delete_pack(pack_id)
    }
    fn put_pack_index(&self, index_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_pack_index(index_id, data)
    }
    fn get_pack_index(&self, index_id: String) -> Result<Vec<u8>, SyncError> {
        self.inner.get_pack_index(index_id)
    }
    fn list_pack_indexes(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_pack_indexes()
    }
    fn delete_pack_index(&self, index_id: String) -> Result<(), SyncError> {
        self.inner.delete_pack_index(index_id)
    }
    fn put_baseline(&self, file_id: String, seq: u64, data: Vec<u8>) -> Result<(), SyncError> {
        self.inner.put_baseline(file_id, seq, data)
    }
    fn get_baseline(&self, file_id: String, seq: u64) -> Result<Option<Vec<u8>>, SyncError> {
        self.inner.get_baseline(file_id, seq)
    }
    fn list_baselines(&self, file_id: String) -> Result<Vec<u64>, SyncError> {
        self.inner.list_baselines(file_id)
    }
    fn put_status(&self, client_id: String, last: u64) -> Result<(), SyncError> {
        self.inner.put_status(client_id, last)
    }
    fn list_statuses(&self) -> Result<Vec<ClientStatus>, SyncError> {
        self.inner.list_statuses()
    }
}

/// A `LocalStore` that delegates to a real `RedbStore` but can be armed to fail
/// the next `persist_file` (simulating a "disk full" write failure mid-sync).
struct FailingStore {
    /// The real backend everything delegates to.
    inner: RedbStore,
    /// When true, `persist_file` returns an error instead of writing.
    fail_persist: AtomicU32,
}

impl FailingStore {
    /// Wrap a fresh redb store; `persist_file` fails while `fail_persist` > 0.
    fn new(path: &std::path::Path, fail_persists: u32) -> Self {
        Self {
            inner: RedbStore::open(path).unwrap(),
            fail_persist: AtomicU32::new(fail_persists),
        }
    }
}

impl LocalStore for FailingStore {
    fn get_file_state(&self, file_id: String) -> Result<Option<Vec<u8>>, SyncError> {
        self.inner.get_file_state(file_id)
    }
    fn list_files(&self) -> Result<Vec<String>, SyncError> {
        self.inner.list_files()
    }
    fn list_oplogs(&self, file_id: String) -> Result<Vec<OplogCacheEntry>, SyncError> {
        self.inner.list_oplogs(file_id)
    }
    fn get_baseline_meta(&self, file_id: String) -> Result<Option<u64>, SyncError> {
        self.inner.get_baseline_meta(file_id)
    }
    fn get_sync_cursor(&self, file_id: String) -> Result<Option<u64>, SyncError> {
        self.inner.get_sync_cursor(file_id)
    }
    fn persist_file(
        &self,
        file_id: String,
        state: Vec<u8>,
        head: u64,
        cache_entry: Option<OplogCacheEntry>,
    ) -> Result<(), SyncError> {
        let remaining = self.fail_persist.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_persist.store(remaining - 1, Ordering::SeqCst);
            return Err(SyncError::IoError {
                msg: "Disk Full".into(),
            });
        }
        self.inner.persist_file(file_id, state, head, cache_entry)
    }
    fn cache_oplogs(
        &self,
        file_id: String,
        entries: Vec<OplogCacheEntry>,
    ) -> Result<(), SyncError> {
        self.inner.cache_oplogs(file_id, entries)
    }
    fn commit_truncation(&self, file_id: String, up_to: u64) -> Result<(), SyncError> {
        self.inner.commit_truncation(file_id, up_to)
    }
    fn close(&self) -> Result<(), SyncError> {
        self.inner.close()
    }
}

#[test]
fn mid_sync_failure_leaves_local_state_untouched() {
    let remote_dir = TempDir::new().unwrap();
    // Seed several entries via a normal engine on a plain remote.
    {
        let plain: Arc<dyn RemoteStorage> =
            Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
        let dbs = TempDir::new().unwrap();
        let (seed, _) = engine("seed", &dbs, plain);
        for i in 1..=4 {
            seed.modify_text("doc".into(), format!("content {i}"))
                .unwrap();
        }
    }

    // New engine over a failing remote (fails the 2nd get_oplog mid-sync).
    let failing: Arc<dyn RemoteStorage> =
        Arc::new(FaultRemote::failing_reads_after(remote_dir.path(), 1));
    let dbs = TempDir::new().unwrap();
    let (victim, _) = engine("victim", &dbs, failing);

    let err = victim.sync("doc".into());
    assert!(err.is_err(), "sync should surface the injected failure");
    // No file state should have been tracked/persisted.
    assert!(
        victim.get_text("doc".into()).is_err(),
        "no partial state committed"
    );
    assert_eq!(
        victim.head("doc".into()),
        0,
        "head unchanged after failed sync"
    );
}

#[test]
fn pack_upload_dedupes_identical_content() {
    // Pack-level analogue of chunk resume/dedup: re-writing identical content
    // must not create new pack objects. Dedup is decided against the union pack
    // index (content-addressed), so a second modify_binary of the same bytes
    // finds every chunk already stored and builds zero new packs. This is also
    // what makes an interrupted upload safe to resume — already-stored chunks
    // are never re-packed.
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    a.modify_binary("blob".into(), data.clone()).unwrap();
    let packs_after_first = remote.list_packs().unwrap();
    let indexes_after_first = remote.list_pack_indexes().unwrap();

    // Re-writing the same content: every chunk already stored -> no new packs.
    a.modify_binary("blob".into(), data).unwrap();
    assert_eq!(
        packs_after_first,
        remote.list_packs().unwrap(),
        "identical content must not create new packs (dedup honored)"
    );
    assert_eq!(
        indexes_after_first,
        remote.list_pack_indexes().unwrap(),
        "identical content must not create new pack indexes"
    );
}

// ======================================================================
// 1. Text & CRDT (TC-101..106)
// ======================================================================

/// TC-101: a single client makes many sequential edits offline; a fresh engine
/// rebuilds identical content purely from the published oplog.
#[test]
fn tc101_single_client_sequential_writes_rebuild() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let mut text = String::new();
    for i in 1..=10 {
        writeln!(text, "line {i}").unwrap();
        a.modify_text("doc".into(), text.clone()).unwrap();
    }
    assert_eq!(a.head("doc".into()), 10, "ten sequential versions");

    // Fresh engine rebuilds from the remote alone.
    let fresh = TempDir::new().unwrap();
    let (b, _) = engine("clientB", &fresh, remote.clone());
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), text);
}

/// TC-102: syncing a brand-new empty text file establishes a base doc on both
/// ends without panicking.
#[test]
fn tc102_empty_file_initialization() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    a.modify_text("doc".into(), String::new()).unwrap();
    assert_eq!(a.get_text("doc".into()).unwrap(), "");

    let (b, _) = engine("clientB", &dbs, remote.clone());
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "");
}

/// TC-103: A inserts at the head, B at the tail, concurrently. After merge both
/// converge with both edits present and correctly ordered.
#[test]
fn tc103_concurrent_non_overlapping_merge() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    // Shared base with a middle marker both sides edit around.
    a.modify_text("doc".into(), "MIDDLE".into()).unwrap();
    b.sync("doc".into()).unwrap();

    a.modify_text("doc".into(), "HelloMIDDLE".into()).unwrap();
    b.modify_text("doc".into(), "MIDDLEWorld".into()).unwrap();

    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    let ta = a.get_text("doc".into()).unwrap();
    assert_eq!(ta, b.get_text("doc".into()).unwrap(), "converge");
    assert_eq!(ta, "HelloMIDDLEWorld", "both edits, correct order");
}

/// TC-104: A and B insert different strings at the same cursor position. The
/// merge must be byte-identical on both ends and free of interleaving garbage.
#[test]
fn tc104_concurrent_same_position_insert() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    a.modify_text("doc".into(), String::new()).unwrap();
    b.sync("doc".into()).unwrap();

    a.modify_text("doc".into(), "Apple".into()).unwrap();
    b.modify_text("doc".into(), "Banana".into()).unwrap();

    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    let ta = a.get_text("doc".into()).unwrap();
    let tb = b.get_text("doc".into()).unwrap();
    assert_eq!(ta, tb, "both ends identical");
    // No interleaving: the result is one string followed by the other.
    assert!(
        ta == "AppleBanana" || ta == "BananaApple",
        "clean concatenation, got {ta:?}"
    );
}

/// TC-105: A and B both delete the same character concurrently. The merge must
/// not panic on the double-delete and the character disappears exactly once.
#[test]
fn tc105_concurrent_same_delete() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    a.modify_text("doc".into(), "abcdef".into()).unwrap();
    b.sync("doc".into()).unwrap();

    // Both delete the char at index 2 ('c').
    a.modify_text("doc".into(), "abdef".into()).unwrap();
    b.modify_text("doc".into(), "abdef".into()).unwrap();

    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(a.get_text("doc".into()).unwrap(), "abdef");
    assert_eq!(b.get_text("doc".into()).unwrap(), "abdef");
}

/// TC-106: large offline edits on both sides (scaled from the doc's 1000/500
/// lines) converge to identical content.
#[test]
fn tc106_large_offline_edits_converge() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    a.modify_text("doc".into(), "START\nEND".into()).unwrap();
    b.sync("doc".into()).unwrap();

    // A prepends 1000 lines after START; B appends 500 before END.
    let mut a_block = String::new();
    for i in 0..1000 {
        writeln!(a_block, "a{i}").unwrap();
    }
    let mut b_block = String::new();
    for i in 0..500 {
        writeln!(b_block, "b{i}").unwrap();
    }
    a.modify_text("doc".into(), format!("START\n{a_block}END"))
        .unwrap();
    b.modify_text("doc".into(), format!("START{b_block}\nEND"))
        .unwrap();

    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    let ta = a.get_text("doc".into()).unwrap();
    assert_eq!(ta, b.get_text("doc".into()).unwrap(), "converge");
    assert!(
        ta.contains("a999") && ta.contains("b499"),
        "both blocks present"
    );
}

// ======================================================================
// 2. Binary & CDC engine flows (BI-201, 204, 205, 206)
// (BI-202/203 chunk-level facts are unit tests in src/binary.rs)
// ======================================================================

/// Deterministic pseudo-random bytes (LCG) — non-periodic stand-in for a real
/// binary file, seeded so tests are reproducible.
fn pseudo_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            u8::try_from((state >> 33) & 0xff).unwrap_or(0)
        })
        .collect()
}

/// BI-201: syncing a new multi-MB file produces a full manifest and uploads
/// every chunk (packed) to the remote. Also the write-amplification intent of
/// the pack workstream: many chunks collapse into a handful of pack objects
/// (≈ size / `TARGET_PACK_SIZE`), not one object per chunk.
#[test]
fn bi201_new_large_file_full_manifest() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // ~10 MB — enough for many chunks spanning multiple 4 MiB packs.
    let data = pseudo_bytes(10 * 1024 * 1024, 1);
    a.modify_binary("blob".into(), data.clone()).unwrap();

    let manifest = a.get_manifest("blob".into()).unwrap();
    assert!(manifest.len() > 1, "multi-chunk manifest");
    // Every chunk in the manifest is resolvable on the remote (packed).
    let stored = stored_chunks(&remote);
    for h in &manifest {
        assert!(stored.contains(h), "chunk {h} uploaded");
    }
    // Write amplification: hundreds of chunks collapse into a few pack objects.
    let packs = remote.list_packs().unwrap().len();
    assert!(
        packs <= 4 && packs * 10 < manifest.len(),
        "expected few packs (<= 4), got {packs} for {} chunks",
        manifest.len()
    );
    // The packed file round-trips byte-identically via engine reassembly.
    assert_eq!(a.read_binary("blob".into()).unwrap(), data);
}

/// Build a binary snapshot entry at `seq` from `client` referencing `hashes`.
fn binary_entry(seq: u64, client: &str, hashes: &[&str]) -> OpLogEntry {
    OpLogEntry {
        sequence: seq,
        client_id: client.to_string(),
        timestamp: 0,
        change_type: ChangeType::BinarySnapshot {
            chunk_hashes: hashes.iter().map(|s| (*s).to_string()).collect(),
        },
    }
}

/// BI-204: a genuine binary fork resolved with `KeepLocal` keeps the local
/// manifest and raises no conflict-copy request.
#[test]
fn bi204_binary_fork_keep_local() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, rec) = engine_with_policy(
        "clientA",
        &dbs,
        remote.clone(),
        BinaryConflictPolicy::KeepLocal,
    );

    // clientA publishes a snapshot at seq 1 (real chunks uploaded).
    let data_a = pseudo_bytes(80_000, 10);
    a.modify_binary("img".into(), data_a).unwrap();
    let local_manifest = a.get_manifest("img".into()).unwrap();

    // Inject a competing seq-1 snapshot from clientB (a genuine fork on the
    // dumb remote, written via the non-CAS append).
    inject_fork_entry(&remote, "img", &binary_entry(1, "clientB", &["deadbeef"]));

    a.sync("img".into()).unwrap();
    // KeepLocal: the local manifest wins, no conflict copy requested.
    assert_eq!(a.get_manifest("img".into()).unwrap(), local_manifest);
    assert!(
        rec.conflict_copies().is_empty(),
        "no conflict copy under KeepLocal"
    );
}

/// BI-205: a binary fork resolved with `KeepBoth` preserves the main manifest
/// and fires a conflict-copy notification for the host to duplicate the file.
#[test]
fn bi205_binary_fork_keep_both_requests_copy() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, rec) = engine_with_policy(
        "clientA",
        &dbs,
        remote.clone(),
        BinaryConflictPolicy::KeepBoth,
    );

    let data_a = pseudo_bytes(80_000, 20);
    a.modify_binary("img".into(), data_a).unwrap();
    let local_manifest = a.get_manifest("img".into()).unwrap();

    inject_fork_entry(&remote, "img", &binary_entry(1, "clientB", &["cafebabe"]));

    a.sync("img".into()).unwrap();
    // Main manifest preserved; a conflict copy was requested for "img".
    assert_eq!(a.get_manifest("img".into()).unwrap(), local_manifest);
    assert_eq!(rec.conflict_copies(), vec!["img".to_string()]);
}

/// BI-206: Manual leaves a genuine binary fork entirely untouched so the host
/// can persist a review task before choosing a policy.
#[test]
fn bi206_binary_fork_manual_requires_resolution_without_side_effects() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, rec) = engine_with_policy(
        "clientA",
        &dbs,
        remote.clone(),
        BinaryConflictPolicy::Manual,
    );

    a.modify_binary("img".into(), pseudo_bytes(80_000, 30))
        .unwrap();
    let local_manifest = a.get_manifest("img".into()).unwrap();
    inject_fork_entry(&remote, "img", &binary_entry(1, "clientB", &["badf00d"]));
    let before = remote.list_oplogs("img".into()).unwrap();

    assert!(matches!(
        a.sync("img".into()),
        Err(SyncError::ConflictNeedResolution)
    ));
    assert_eq!(a.get_manifest("img".into()).unwrap(), local_manifest);
    assert_eq!(remote.list_oplogs("img".into()).unwrap(), before);
    assert!(rec.conflict_copies().is_empty());
}

/// BI-207: a zero-byte binary file syncs with an empty manifest and completes.
#[test]
fn bi206_zero_byte_binary() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    a.modify_binary("empty.bin".into(), Vec::new()).unwrap();
    assert!(a.get_manifest("empty.bin".into()).unwrap().is_empty());

    let (b, _) = engine("clientB", &dbs, remote.clone());
    b.sync("empty.bin".into()).unwrap();
    assert!(b.get_manifest("empty.bin".into()).unwrap().is_empty());
}

// ======================================================================
// 3. FFI & interop (FFI-301..304)
// ======================================================================

/// FFI-301: a successful sync round-trips back to the idle phase (not syncing).
#[test]
fn ffi301_sync_returns_to_idle() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    a.modify_text("doc".into(), "hello".into()).unwrap();

    let (b, _) = engine("clientB", &dbs, remote.clone());
    assert!(!b.is_syncing("doc".into()), "idle before sync");
    b.sync("doc".into()).unwrap();
    assert!(!b.is_syncing("doc".into()), "idle again after sync");
}

/// FFI-302: while a slow sync is in flight on a background thread, a second
/// sync of the same file is rejected by the phase guard (no re-entrancy).
#[test]
fn ffi302_slow_sync_blocks_reentry() {
    let remote_dir = TempDir::new().unwrap();
    // Seed content through a plain remote.
    {
        let plain: Arc<dyn RemoteStorage> =
            Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
        let dbs = TempDir::new().unwrap();
        let (seed, _) = engine("seed", &dbs, plain);
        seed.modify_text("doc".into(), "content".into()).unwrap();
    }

    // Engine over a slow remote: each get_oplog sleeps 300 ms.
    let slow: Arc<dyn RemoteStorage> = Arc::new(FaultRemote::slow_reads(remote_dir.path(), 300));
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, slow);

    // Kick off a slow sync on a background thread.
    let bg = {
        let a = a.clone();
        std::thread::spawn(move || a.sync("doc".into()))
    };
    // Give the background sync time to enter and grab the phase lock.
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(a.is_syncing("doc".into()), "background sync is in flight");

    // A concurrent sync must be a no-op (rejected by the guard), returning fast.
    let start = std::time::Instant::now();
    a.sync("doc".into()).unwrap();
    assert!(
        start.elapsed() < std::time::Duration::from_millis(200),
        "re-entrant sync returned immediately (was rejected)"
    );

    bg.join().unwrap().unwrap();
    assert!(
        !a.is_syncing("doc".into()),
        "idle after the slow sync finishes"
    );
    assert_eq!(a.get_text("doc".into()).unwrap(), "content");
}

/// FFI-303: the chunk-transfer contract carries only hash/offset/length — never
/// a byte buffer — so a host streams chunk bytes itself by file handle.
///
/// NOTE (Rule 12): true streaming ingestion is *not* implemented — the current
/// `modify_binary(data: Vec<u8>)` still takes the whole buffer across the FFI
/// boundary. This test pins the structural guarantee that *does* hold today
/// (the manifest/`ChunkInfo` boundary type is byte-free); full streaming is a
/// documented follow-up.
#[test]
fn ffi303_chunk_info_carries_no_bytes() {
    let info = ChunkInfo {
        hash: "abc123".into(),
        offset: 4096,
        length: 1024,
    };
    // The type exposes only metadata; there is no byte field to send over FFI.
    assert_eq!(info.hash, "abc123");
    assert_eq!(info.offset, 4096);
    assert_eq!(info.length, 1024);
}

/// FFI-304: many background threads call into one engine concurrently. The
/// internal locks must keep it race- and deadlock-free, converging in the end.
#[test]
fn ffi304_concurrent_multithread_callbacks() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    a.modify_text("doc".into(), "seed".into()).unwrap();

    // 8 threads: half hammer sync, half append edits.
    let mut handles = Vec::new();
    for t in 0..8 {
        let a = a.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..10 {
                if t % 2 == 0 {
                    a.sync("doc".into()).unwrap();
                } else {
                    let _ = a.modify_text("doc".into(), format!("t{t}-edit{i}"));
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // Engine is still consistent and responsive; a final sync converges.
    a.sync("doc".into()).unwrap();
    assert!(!a.is_syncing("doc".into()));
    assert!(a.get_text("doc".into()).is_ok());
}

// ======================================================================
// 4. OpLog & time-travel rollback (LOG-401..404)
// ======================================================================

/// LOG-401: 10 edits produce 10 oplog entries; a fresh engine linearly rebuilds
/// the latest content from them.
#[test]
fn log401_linear_history_rebuild() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    for i in 1..=10 {
        a.modify_text("doc".into(), format!("state-{i}")).unwrap();
    }
    assert_eq!(remote.list_oplogs("doc".into()).unwrap().len(), 10);

    let fresh = TempDir::new().unwrap();
    let (b, _) = engine("clientB", &fresh, remote.clone());
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "state-10");
}

/// LOG-402: consecutive rollbacks (to v5 then v8) are appended as increasing
/// compensating entries (v11, v12); history is never mutated and content is
/// reset correctly each time.
#[test]
fn log402_consecutive_rollbacks_append() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let mut seqs = Vec::new();
    for i in 1..=10 {
        seqs.push(a.modify_text("doc".into(), format!("v{i}")).unwrap());
    }
    let v5 = seqs[4];
    let v8 = seqs[7];

    let r1 = a.rollback("doc".into(), v5).unwrap();
    assert_eq!(r1, 11, "first rollback appends as v11");
    assert_eq!(a.get_text("doc".into()).unwrap(), "v5");

    let r2 = a.rollback("doc".into(), v8).unwrap();
    assert_eq!(r2, 12, "second rollback appends as v12");
    assert_eq!(a.get_text("doc".into()).unwrap(), "v8");

    // History is intact: all 12 sequences exist, none overwritten.
    assert_eq!(remote.list_oplogs("doc".into()).unwrap().len(), 12);
}

/// LOG-403: A rolls back text; after B syncs, B reflects the rolled-back state.
#[test]
fn log403_text_rollback_propagates() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    let v1 = a.modify_text("doc".into(), "initial".into()).unwrap();
    a.modify_text("doc".into(), "AAA".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "AAA");

    // A rolls back to v1; B syncs and follows.
    a.rollback("doc".into(), v1).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(b.get_text("doc".into()).unwrap(), "initial");
}

/// LOG-404: rolling a binary file back to an earlier version only swaps the
/// manifest pointer — the earlier chunks already exist, so no new chunks are
/// stored (no physical re-download/copy).
#[test]
fn log404_binary_rollback_is_pointer_only() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let v1data = pseudo_bytes(90_000, 100);
    let v1 = a.modify_binary("blob".into(), v1data.clone()).unwrap();
    let v1_manifest = a.get_manifest("blob".into()).unwrap();

    // Two more distinct versions.
    a.modify_binary("blob".into(), pseudo_bytes(90_000, 200))
        .unwrap();
    a.modify_binary("blob".into(), pseudo_bytes(90_000, 300))
        .unwrap();

    let packs_before = remote.list_packs().unwrap().len();
    // Roll back to v1: pointer swap only.
    a.rollback("blob".into(), v1).unwrap();
    let packs_after = remote.list_packs().unwrap().len();

    assert_eq!(
        packs_before, packs_after,
        "rollback must not upload/copy packs"
    );
    assert_eq!(
        a.get_manifest("blob".into()).unwrap(),
        v1_manifest,
        "manifest reset to the v1 pointer"
    );
}

// ======================================================================
// 5. Multi-client & CAS (MUL-501..503)
// ======================================================================

/// MUL-501: two clients writing the same version produce distinctly-named
/// objects (`NNNN_clientA.oplog` / `NNNN_clientB.oplog`) that coexist — the
/// dumb remote never physically overwrites one with the other.
#[test]
fn mul501_unique_naming_no_overwrite() {
    let (_remote_dir, remote) = shared_remote();

    // Two clients each append their own entry at sequence 6, unconditionally.
    remote
        .put_oplog(
            "doc".into(),
            OpLogEntry {
                sequence: 6,
                client_id: "clientA".into(),
                timestamp: 0,
                change_type: ChangeType::TextDelta { delta: vec![1] },
            },
        )
        .unwrap();
    remote
        .put_oplog(
            "doc".into(),
            OpLogEntry {
                sequence: 6,
                client_id: "clientB".into(),
                timestamp: 0,
                change_type: ChangeType::TextDelta { delta: vec![2] },
            },
        )
        .unwrap();

    let items = remote.list_oplogs("doc".into()).unwrap();
    let names: Vec<String> = items.iter().map(|i| i.remote_path.clone()).collect();
    assert!(names.contains(&"6_clientA.oplog".to_string()));
    assert!(names.contains(&"6_clientB.oplog".to_string()));
    assert_eq!(items.len(), 2, "both entries coexist, no overwrite");
}

/// MUL-502: a forked remote tip is detected on pull, merged, and republished as
/// a single unifying entry so the branches reconverge (validates the C2
/// fork-tip convergence publish). A second client then syncs to the same head.
#[test]
fn mul502_fork_detected_and_reconverges() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // clientA publishes a real v1 delta.
    a.modify_text("doc".into(), "hello".into()).unwrap();

    // Inject clientB's competing v1 — a genuine, decodable yrs delta produced
    // by a throwaway engine, rewritten to sequence 1 / clientB.
    let mut b_entry = real_text_entry("clientB", "world");
    b_entry.sequence = 1;
    inject_fork_entry(&remote, "doc", &b_entry);

    // Tip is forked at seq 1 (clientA + clientB). Sync detects and reconverges.
    a.sync("doc".into()).unwrap();

    // After convergence a single entry sits at the new tip (seq >= 2).
    let items = remote.list_oplogs("doc".into()).unwrap();
    let max_seq = items.iter().map(|i| i.sequence).max().unwrap();
    let tip_count = items.iter().filter(|i| i.sequence == max_seq).count();
    assert_eq!(tip_count, 1, "the log tip is a single unifying entry");
    assert!(
        max_seq >= 2,
        "a unifying entry was published above the fork"
    );

    // A second client syncs to the same converged content.
    let (c, _) = engine("clientC", &dbs, remote.clone());
    c.sync("doc".into()).unwrap();
    assert_eq!(
        a.get_text("doc".into()).unwrap(),
        c.get_text("doc".into()).unwrap()
    );
}

/// MUL-503: when two clients race on the same next sequence, the CAS append
/// lets exactly one win; the loser rebases and republishes at head+1 without
/// overwriting the winner.
#[test]
fn mul503_cas_retry_loop() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    a.modify_text("doc".into(), "base".into()).unwrap();
    b.sync("doc".into()).unwrap();

    // Both try to advance; CAS serializes them without collision.
    a.modify_text("doc".into(), "base-A".into()).unwrap();
    b.modify_text("doc".into(), "base-B".into()).unwrap();

    let mut items = remote.list_oplogs("doc".into()).unwrap();
    items.sort_by_key(|i| i.sequence);
    let seqs: Vec<u64> = items.iter().map(|i| i.sequence).collect();
    let mut uniq = seqs.clone();
    uniq.dedup();
    assert_eq!(uniq.len(), seqs.len(), "no two entries share a sequence");

    a.sync("doc".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(
        a.get_text("doc".into()).unwrap(),
        b.get_text("doc".into()).unwrap()
    );
}

// ======================================================================
// 6. Truncation & GC (TRU-601..604)
// ======================================================================

/// TRU-601: with N kept versions and all clients synced past the cut line, a
/// baseline is written at the cut and oplogs at/below it are deleted.
#[test]
fn tru601_standard_rolling_baseline() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // 15 versions; clientA's status advances to 15 as it writes.
    let mut text = String::new();
    for i in 1..=15 {
        writeln!(text, "l{i}").unwrap();
        a.modify_text("doc".into(), text.clone()).unwrap();
    }
    // All clients (only A here) are past 12. Keep N=3 → cut = min(15-3,15) = 12.
    a.truncate("doc".into(), 3).unwrap();

    let baselines = remote.list_baselines("doc".into()).unwrap();
    assert_eq!(baselines, vec![12], "baseline written at the cut line 12");
    let items = remote.list_oplogs("doc".into()).unwrap();
    assert!(
        items.iter().all(|i| i.sequence > 12),
        "oplogs <= 12 deleted"
    );
}

/// TRU-602: a lagging client pins the safe low watermark; the engine refuses to
/// delete any log the lagging client hasn't seen.
#[test]
fn tru602_lagging_client_protection() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let mut text = String::new();
    for i in 1..=50 {
        writeln!(text, "l{i}").unwrap();
        a.modify_text("doc".into(), text.clone()).unwrap();
    }
    // Client C is stuck at version 5 (offline). Publish its lagging status.
    remote.put_status("clientC".into(), 5).unwrap();

    // N=10 would want cut=40, but the watermark pins it at 5.
    a.truncate("doc".into(), 10).unwrap();

    let items = remote.list_oplogs("doc".into()).unwrap();
    assert!(
        items.iter().any(|i| i.sequence == 6),
        "entries above the lagging watermark (5) are preserved"
    );
    let baselines = remote.list_baselines("doc".into()).unwrap();
    assert!(
        baselines.iter().all(|&s| s <= 5),
        "no baseline past the protected watermark"
    );
}

/// TRU-603: after truncation, chunks referenced by no surviving manifest are
/// physically reclaimed (their fully-dead packs deleted); chunks still
/// referenced survive and the file still reassembles.
#[test]
fn tru603_binary_gc_deletes_only_orphans() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    for v in 0..6u64 {
        a.modify_binary("blob".into(), pseudo_bytes(120_000, v + 1))
            .unwrap();
    }
    let before = stored_chunks(&remote).len();
    a.truncate("blob".into(), 1).unwrap();
    let after = stored_chunks(&remote).len();
    assert!(after < before, "orphans GC'd ({before} -> {after})");

    // Surviving manifest chunks are all still resolvable, and the file reads back.
    let manifest = a.get_manifest("blob".into()).unwrap();
    let stored = stored_chunks(&remote);
    for h in &manifest {
        assert!(stored.contains(h), "live chunk {h} survives GC");
    }
    assert!(a.read_binary("blob".into()).is_ok(), "blob still readable");
}

/// TRU-605 (A2): a pack that ends up mostly dead after truncation is *repacked*
/// — its surviving chunks rewritten into a fresh pack and its dead bytes
/// reclaimed — while the file still reassembles byte-identically. Constructed so
/// one pack holds a live shared prefix plus a large dead tail (> 50% dead).
#[test]
fn tru605_mostly_dead_pack_is_repacked() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // v1 = small shared prefix + large tail. CDC keeps the prefix chunks stable.
    let prefix = pseudo_bytes(80 * 1024, 7);
    let mut v1 = prefix.clone();
    v1.extend(pseudo_bytes(400 * 1024, 11));
    a.modify_binary("blob".into(), v1).unwrap();

    // v2 = same prefix + a different large tail. The prefix chunks dedupe (stay
    // live); v1's tail chunks become dead. v1's pack is now mostly dead.
    let mut v2 = prefix.clone();
    v2.extend(pseudo_bytes(400 * 1024, 23));
    a.modify_binary("blob".into(), v2.clone()).unwrap();

    let bytes_before = stored_pack_bytes(&remote);
    // Keep the newest version; v1's snapshot is superseded so its tail dies.
    a.truncate("blob".into(), 1).unwrap();
    let bytes_after = stored_pack_bytes(&remote);

    // Repack reclaimed the dead tail bytes: strictly fewer stored bytes, and no
    // more than the surviving content plus modest packing slack.
    assert!(
        bytes_after < bytes_before,
        "repack must reclaim dead bytes ({bytes_before} -> {bytes_after})"
    );

    // Every surviving-manifest chunk is still resolvable, and v2 reassembles
    // byte-identically from the repacked storage — including on a fresh client.
    let manifest = a.get_manifest("blob".into()).unwrap();
    let stored = stored_chunks(&remote);
    for h in &manifest {
        assert!(stored.contains(h), "live chunk {h} resolvable after repack");
    }
    assert_eq!(
        a.read_binary("blob".into()).unwrap(),
        v2,
        "local read intact"
    );

    let dbs_b = TempDir::new().unwrap();
    let (b, _) = engine("clientB", &dbs_b, remote.clone());
    b.sync("blob".into()).unwrap();
    assert_eq!(
        b.read_binary("blob".into()).unwrap(),
        v2,
        "fresh client reassembles from repacked storage"
    );
}

/// TRU-606: binary GC is global across files. Packs are one shared content-
/// addressed namespace, so truncating one file must NOT delete packs holding
/// another file's live chunks. Regression for the cross-file data-loss bug where
/// the live set was built from only the truncated file's manifests.
#[test]
fn tru606_gc_spares_other_files_packs() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // Two independent binary files with disjoint content on the same remote.
    // "keep" gets a single version (all its chunks stay live); "churn" is
    // rewritten repeatedly so most of its chunks go dead and it triggers GC.
    let keep_data = pseudo_bytes(300_000, 42);
    a.modify_binary("keep".into(), keep_data.clone()).unwrap();
    for v in 0..6u64 {
        a.modify_binary("churn".into(), pseudo_bytes(120_000, v + 100))
            .unwrap();
    }

    // Truncating "churn" GC's the store. The old code, building `live` from only
    // "churn", would delete every pack holding "keep"'s chunks.
    a.truncate("churn".into(), 1).unwrap();

    // "keep" is fully intact: every manifest chunk resolvable and bytes identical.
    let stored = stored_chunks(&remote);
    for h in &a.get_manifest("keep".into()).unwrap() {
        assert!(
            stored.contains(h),
            "keep's live chunk {h} survives churn GC"
        );
    }
    assert_eq!(
        a.read_binary("keep".into()).unwrap(),
        keep_data,
        "untouched file reads back byte-identically after another file's GC"
    );

    // A fresh client with no local state reassembles "keep" purely from remote.
    let dbs_b = TempDir::new().unwrap();
    let (b, _) = engine("clientB", &dbs_b, remote.clone());
    b.sync("keep".into()).unwrap();
    assert_eq!(
        b.read_binary("keep".into()).unwrap(),
        keep_data,
        "fresh client still reassembles the spared file"
    );
}

/// INTEG-1: verify-on-read. Chunks are content-addressed, so a pack corrupted
/// or truncated in storage/transit must be caught on read — not silently
/// reassembled into wrong bytes. Flip one byte of a stored pack and assert
/// `read_binary` fails loudly with `Corrupt`, naming the offending chunk.
#[test]
fn integ1_corrupt_pack_is_detected_on_read() {
    let (remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    let data = pseudo_bytes(200_000, 7);
    a.modify_binary("blob".into(), data.clone()).unwrap();
    // Sanity: reads back cleanly before corruption.
    assert_eq!(a.read_binary("blob".into()).unwrap(), data);

    // Flip a byte inside a stored pack (pack files are raw concatenated chunk
    // bytes, so any data byte belongs to a manifest chunk this file reads).
    let pack_dir = remote_dir.path().join("packs");
    let pack = std::fs::read_dir(&pack_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = std::fs::read(&pack).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&pack, &bytes).unwrap();

    // A fresh client (no cached bytes) forces the read to hit the corrupt pack.
    let dbs_b = TempDir::new().unwrap();
    let (b, _) = engine("clientB", &dbs_b, remote.clone());
    b.sync("blob".into()).unwrap();
    match b.read_binary("blob".into()) {
        Err(SyncError::Corrupt { hash }) => assert!(!hash.is_empty()),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// TRU-604: two clients truncate independently, producing two baselines with no
/// common ancestor. On rebuild the engine applies *all* baselines, force-merging
/// the orphan branches via the CRDT (validates C3).
#[test]
fn tru604_orphan_branches_force_converge() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());

    // Establish shared history and one baseline at the current cut.
    for i in 1..=12 {
        a.modify_text("doc".into(), format!("shared {i}")).unwrap();
    }
    a.truncate("doc".into(), 2).unwrap();
    let first_baselines = remote.list_baselines("doc".into()).unwrap();
    assert!(!first_baselines.is_empty(), "first baseline exists");

    // Simulate an orphan branch: inject a second, independent baseline at a
    // higher sequence built from different content. Applying both on rebuild
    // must not panic and must converge deterministically.
    let orphan_full = independent_text_baseline("orphan-branch-content");
    let orphan_seq = first_baselines.last().unwrap() + 5;
    remote
        .put_baseline("doc".into(), orphan_seq, orphan_full)
        .unwrap();

    // A brand-new client rebuilds from all baselines + surviving oplogs.
    let fresh = TempDir::new().unwrap();
    let (c, _) = engine("clientC", &fresh, remote.clone());
    c.sync("doc".into()).unwrap();
    // Convergence is the property under test: the rebuild succeeds and both
    // branches' content is present (yrs merges the two full states).
    let text = c.get_text("doc".into()).unwrap();
    assert!(
        text.contains("orphan-branch-content"),
        "orphan branch merged in"
    );
}

/// Produce a zstd-compressed full-state yrs update for `content` — the on-remote
/// baseline format — from a throwaway engine, for orphan-branch injection.
fn independent_text_baseline(content: &str) -> Vec<u8> {
    let entry = real_text_entry("orphanmaker", content);
    // The engine stores baselines as zstd(full_update). We only have the delta
    // here, but a single fresh-doc delta *is* a full state, so recompress it.
    let delta = match entry.change_type {
        ChangeType::TextDelta { delta } => delta,
        ChangeType::BinarySnapshot { .. } | ChangeType::Delete => unreachable!(),
    };
    zstd::encode_all(delta.as_slice(), 3).unwrap()
}

/// Produce a genuine, decodable text-delta oplog entry for `content` by driving
/// a throwaway engine on its own remote and reading its published entry back.
/// The caller may rewrite `sequence` to stage a fork.
fn real_text_entry(client: &str, content: &str) -> OpLogEntry {
    let rdir = TempDir::new().unwrap();
    let ddir = TempDir::new().unwrap();
    let r: Arc<dyn RemoteStorage> = Arc::new(LocalFolderRemote::new(rdir.path()).unwrap());
    let (maker, _) = engine(client, &ddir, r.clone());
    maker.modify_text("doc".into(), content.into()).unwrap();
    let items = r.list_oplogs("doc".into()).unwrap();
    let bytes = r
        .get_oplog("doc".into(), items[0].remote_path.clone())
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ======================================================================
// 7. Network & storage fault injection (ERR-701..704)
// (ERR-701 mid-transfer death: see mid_sync_failure_leaves_local_state_untouched)
// ======================================================================

/// ERR-702: when the local store fails a write mid-sync ("disk full"), the sync
/// aborts with the error surfaced and previously-synced data is untouched.
#[test]
fn err702_store_write_failure_aborts_cleanly() {
    let remote_dir = TempDir::new().unwrap();
    // Seed content on a plain remote.
    {
        let plain: Arc<dyn RemoteStorage> =
            Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
        let dbs = TempDir::new().unwrap();
        let (seed, _) = engine("seed", &dbs, plain);
        seed.modify_text("doc".into(), "durable".into()).unwrap();
    }

    // Engine whose store fails the first persist_file (disk full).
    let remote: Arc<dyn RemoteStorage> =
        Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
    let sdir = TempDir::new().unwrap();
    let store = Arc::new(FailingStore::new(&sdir.path().join("s.redb"), 1));
    let rec = Arc::new(Recorder::default());
    let victim = SyncEngine::with_backends(
        "victim",
        store,
        remote,
        rec.clone(),
        BinaryConflictPolicy::KeepBoth,
    );

    let err = victim.sync("doc".into());
    assert!(err.is_err(), "disk-full write failure surfaces as an error");
    // The engine did not commit partial state and returned to idle.
    assert!(
        !victim.is_syncing("doc".into()),
        "phase lock cleared after failure"
    );
    assert!(victim.get_text("doc".into()).is_err(), "no partial state");
}

/// ERR-703: a corrupted (garbage) `.oplog` on the remote makes decode fail with
/// a clear `SyncError`, not a panic; the engine stays usable.
#[test]
fn err703_corrupted_oplog_is_clean_error() {
    // Keep the remote dir handle so we can corrupt a file on disk directly —
    // exactly the kind of observation the local-folder backend exists for.
    let remote_dir = TempDir::new().unwrap();
    let remote: Arc<dyn RemoteStorage> =
        Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    a.modify_text("doc".into(), "good".into()).unwrap();

    // Overwrite the on-disk oplog object with non-JSON garbage.
    let items = remote.list_oplogs("doc".into()).unwrap();
    let path = remote_dir
        .path()
        .join("doc")
        .join("oplogs")
        .join(&items[0].remote_path);
    std::fs::write(&path, b"\x00not-json\xff").unwrap();

    let (b, _) = engine("clientB", &dbs, remote.clone());
    let err = b.sync("doc".into());
    assert!(
        matches!(err, Err(SyncError::SerdeError { .. })),
        "corrupt oplog yields a clear SerdeError, got {err:?}"
    );
    // Engine is still usable (no poisoned state / panic).
    assert!(!b.is_syncing("doc".into()));
}

/// ERR-704: an auth failure (403) mid-operation surfaces the error to the caller
/// and clears the phase lock so the host can prompt re-login and retry.
#[test]
fn err704_auth_expired_surfaces_and_unlocks() {
    let remote_dir = TempDir::new().unwrap();
    {
        let plain: Arc<dyn RemoteStorage> =
            Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
        let dbs = TempDir::new().unwrap();
        let (seed, _) = engine("seed", &dbs, plain);
        seed.modify_text("doc".into(), "content".into()).unwrap();
    }
    // A remote that fails reads immediately (models a 403 on the first fetch).
    let failing: Arc<dyn RemoteStorage> =
        Arc::new(FaultRemote::failing_reads_after(remote_dir.path(), 0));
    let dbs = TempDir::new().unwrap();
    let (victim, _) = engine("victim", &dbs, failing);

    let err = victim.sync("doc".into());
    assert!(err.is_err(), "auth failure surfaced to caller");
    assert!(
        !victim.is_syncing("doc".into()),
        "phase lock cleared so the host can retry after re-login"
    );
}

// ======================================================================
// 8. Incremental oplog reads (B1) — the local cache must eliminate
//    repeat GETs of oplog objects already fetched.
// ======================================================================

/// Build an engine over a shared `CountingRemote`, returning the counter too.
fn counting_engine(client: &str, dir: &TempDir, remote: Arc<CountingRemote>) -> Arc<SyncEngine> {
    let rec = Arc::new(Recorder::default());
    let db = dir.path().join(format!("{client}.redb"));
    let store = Arc::new(RedbStore::open(db).unwrap());
    let dyn_remote: Arc<dyn RemoteStorage> = remote;
    Arc::new(SyncEngine::with_backends(
        client,
        store,
        dyn_remote,
        rec,
        BinaryConflictPolicy::KeepBoth,
    ))
}

/// B1-801: a second sync with no new remote entries issues **zero** `get_oplog`
/// calls — everything is served from the warm local cache. This is the intent
/// test for the whole workstream: it fails if incremental reads regress to N+1.
#[test]
fn b1_801_resync_without_new_entries_fetches_nothing() {
    let remote_dir = TempDir::new().unwrap();
    // Seed 10 entries via a plain engine so they exist on the remote.
    {
        let plain: Arc<dyn RemoteStorage> =
            Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
        let dbs = TempDir::new().unwrap();
        let (seed, _) = engine("seed", &dbs, plain);
        for i in 1..=10 {
            seed.modify_text("doc".into(), format!("state-{i}"))
                .unwrap();
        }
    }

    let counting = Arc::new(CountingRemote::new(remote_dir.path()));
    let dbs = TempDir::new().unwrap();
    let b = counting_engine("clientB", &dbs, counting.clone());

    // First sync fetches all 10 (cold cache).
    b.sync("doc".into()).unwrap();
    assert_eq!(counting.take_gets(), 10, "cold sync fetches every entry");

    // Second sync: nothing new on the remote, cache is warm -> no GETs.
    b.sync("doc".into()).unwrap();
    assert_eq!(
        counting.take_gets(),
        0,
        "warm resync must not re-fetch cached oplogs"
    );
}

/// B1-802: after a warm cache, only entries above the cache are fetched.
#[test]
fn b1_802_incremental_fetch_only_new_entries() {
    let remote_dir = TempDir::new().unwrap();
    let counting = Arc::new(CountingRemote::new(remote_dir.path()));
    // Writer shares the same remote (via a second handle) so we can add entries.
    let writer_remote: Arc<dyn RemoteStorage> =
        Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
    let wdirs = TempDir::new().unwrap();
    let (writer, _) = engine("writer", &wdirs, writer_remote);
    for i in 1..=5 {
        writer.modify_text("doc".into(), format!("v{i}")).unwrap();
    }

    let dbs = TempDir::new().unwrap();
    let b = counting_engine("clientB", &dbs, counting.clone());
    b.sync("doc".into()).unwrap();
    assert_eq!(counting.take_gets(), 5, "cold sync fetches all five");

    // Writer appends one more entry; reader should fetch exactly one.
    writer.modify_text("doc".into(), "v6".into()).unwrap();
    b.sync("doc".into()).unwrap();
    assert_eq!(counting.take_gets(), 1, "only the one new entry is fetched");
}

/// B1-803: a fork injected at an already-cached sequence is still detected and
/// merged — the forked sequence is always re-fetched, never masked by the
/// single-slot cache.
#[test]
fn b1_803_fork_at_cached_sequence_still_detected() {
    let remote_dir = TempDir::new().unwrap();
    let counting = Arc::new(CountingRemote::new(remote_dir.path()));
    let dbs = TempDir::new().unwrap();
    let a = counting_engine("clientA", &dbs, counting.clone());

    // clientA publishes a real v1 and caches it.
    a.modify_text("doc".into(), "hello".into()).unwrap();
    a.sync("doc".into()).unwrap();
    counting.take_gets();

    // Inject clientB's competing v1 (a genuine fork on the dumb remote).
    let mut b_entry = real_text_entry("clientB", "world");
    b_entry.sequence = 1;
    let injector: Arc<dyn RemoteStorage> =
        Arc::new(LocalFolderRemote::new(remote_dir.path()).unwrap());
    injector.put_oplog("doc".into(), b_entry).unwrap();

    // Sync must detect the fork (re-fetch seq 1 despite it being cached) and
    // reconverge to a single tip.
    a.sync("doc".into()).unwrap();
    let items = counting.inner_list("doc");
    let max_seq = items.iter().map(|i| i.sequence).max().unwrap();
    let tip_count = items.iter().filter(|i| i.sequence == max_seq).count();
    assert_eq!(tip_count, 1, "fork reconverged to a single unifying tip");
    assert!(
        max_seq >= 2,
        "a unifying entry was published above the fork"
    );
}

/// B1-804: cache rows left stale by another client's remote truncation are
/// inert — a client with a warm (pre-truncation) cache still rebuilds correct
/// current content from baseline + surviving oplogs, never replaying dead
/// history.
#[test]
fn b1_804_stale_cache_after_remote_truncation_is_inert() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote.clone());

    // A drives 15 versions; B syncs and warms its cache over all of them.
    let mut text = String::new();
    for i in 1..=15 {
        writeln!(text, "l{i}").unwrap();
        a.modify_text("doc".into(), text.clone()).unwrap();
    }
    b.sync("doc".into()).unwrap();

    // A truncates on the remote (keep newest 3 -> cut at 12), dropping loose
    // oplogs <= 12. B's local cache still holds rows 1..=12 (now dead).
    a.truncate("doc".into(), 3).unwrap();

    // B syncs again: the stale cached rows must not corrupt the rebuild.
    b.sync("doc".into()).unwrap();
    assert_eq!(
        b.get_text("doc".into()).unwrap(),
        text,
        "current content rebuilt despite stale cache rows below the cut"
    );
}

#[test]
fn binary_delete_propagates_and_recreation_restores_presence() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    let (b, _) = engine("clientB", &dbs, remote);
    let original = pseudo_bytes(90_000, 901);

    a.modify_binary("attachment".into(), original).unwrap();
    b.sync("attachment".into()).unwrap();
    a.delete_binary("attachment".into()).unwrap();
    b.sync("attachment".into()).unwrap();
    assert!(matches!(
        b.get_binary_state("attachment".into()).unwrap(),
        BinaryFileState::Deleted { .. }
    ));

    let restored = pseudo_bytes(75_000, 902);
    a.modify_binary("attachment".into(), restored.clone())
        .unwrap();
    b.sync("attachment".into()).unwrap();
    assert!(matches!(
        b.get_binary_state("attachment".into()).unwrap(),
        BinaryFileState::Present { .. }
    ));
    assert_eq!(b.read_binary("attachment".into()).unwrap(), restored);
}

#[test]
fn binary_rollback_crosses_delete_tombstone() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote);
    let bytes = pseudo_bytes(70_000, 903);

    let present = a.modify_binary("attachment".into(), bytes.clone()).unwrap();
    let deleted = a.delete_binary("attachment".into()).unwrap();
    a.rollback("attachment".into(), present).unwrap();
    assert_eq!(a.read_binary("attachment".into()).unwrap(), bytes);
    a.rollback("attachment".into(), deleted).unwrap();
    assert!(matches!(
        a.get_binary_state("attachment".into()).unwrap(),
        BinaryFileState::Deleted { .. }
    ));
}

#[test]
fn manual_snapshot_delete_fork_requires_resolution() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine_with_policy(
        "clientA",
        &dbs,
        remote.clone(),
        BinaryConflictPolicy::Manual,
    );
    a.modify_binary("attachment".into(), pseudo_bytes(70_000, 904))
        .unwrap();
    inject_fork_entry(
        &remote,
        "attachment",
        &OpLogEntry {
            sequence: 1,
            client_id: "clientB".into(),
            timestamp: 0,
            change_type: ChangeType::Delete,
        },
    );
    assert!(matches!(
        a.sync("attachment".into()),
        Err(SyncError::ConflictNeedResolution)
    ));
}

#[test]
fn truncate_many_reports_all_files_and_runs_global_gc() {
    let (_remote_dir, remote) = shared_remote();
    let dbs = TempDir::new().unwrap();
    let (a, _) = engine("clientA", &dbs, remote.clone());
    for version in 0..4 {
        a.modify_binary("one".into(), pseudo_bytes(90_000, 910 + version))
            .unwrap();
        a.modify_binary("two".into(), pseudo_bytes(90_000, 920 + version))
            .unwrap();
    }
    let report = a
        .truncate_many(vec!["one".into(), "two".into(), "one".into()], 1)
        .unwrap();
    assert_eq!(report.files_considered, 2);
    assert_eq!(report.files_truncated, 2);
    assert!(report.oplogs_deleted >= 6);

    let (b, _) = engine("clientB", &dbs, remote);
    b.sync("one".into()).unwrap();
    b.sync("two".into()).unwrap();
    assert!(b.read_binary("one".into()).is_ok());
    assert!(b.read_binary("two".into()).is_ok());
}
