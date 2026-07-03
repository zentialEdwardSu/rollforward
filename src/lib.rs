//! `rollforward` — a decentralized, dumb-remote-storage sync engine.
//!
//! See `design.md` for the architecture. The engine core ([`engine::SyncEngine`])
//! depends only on two traits — [`remote::RemoteStorage`] (the dumb remote) and
//! [`store::LocalStore`] (local durable state) — and the caller injects the
//! implementations via [`SyncEngine::with_backends`].
//!
//! [`remote::LocalFolderRemote`] and [`store::RedbStore`] are **test/local
//! implementations**, not built-in defaults: they let the engine run and be
//! *observed* locally — you can inspect the remote as a plain directory tree
//! and the store as a redb file on disk. A production host supplies its own
//! backends (network drive / S3 / WebDAV, and whatever durable store it wants).
//! [`new_local`] is a convenience that wires both local impls together for
//! tests and local runs.

// These pedantic lints are intentionally relaxed crate-wide:
// - `missing_errors_doc` / `missing_panics_doc`: every fallible fn returns the
//   single crate error `SyncError`; per-fn `# Errors` prose would be noise.
// - `must_use_candidate`: getters here are used for their side-effect-free
//   values at call sites; `#[must_use]` on each adds churn without safety gain.
// - `needless_pass_by_value`: uniFFI-exported methods must take owned `String` /
//   `Vec<u8>` by value — that is the FFI ABI contract, not a Rust choice.
// - `doc_markdown`: doc text references names like blake3/fastcdc/redb/zstd
//   without backticks by design.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::doc_markdown
)]

pub mod binary;
pub mod engine;
pub mod oplog;
pub mod remote;
pub mod store;
pub mod text;
pub mod types;

use std::sync::Arc;

pub use engine::SyncEngine;
pub use remote::{LocalFolderRemote, RemoteStorage};
pub use store::{LocalStore, RedbStore};
pub use types::{
    BinaryConflictPolicy, ChangeType, ChunkInfo, EngineNotificationListener, OpLogEntry,
    RemoteLogItem, SyncError,
};

/// Assemble a [`SyncEngine`] over the local/test backends: a [`RedbStore`] at
/// `db_path` and a [`LocalFolderRemote`] rooted at `remote_root`, with
/// `KeepBoth` binary conflict handling.
///
/// This is a convenience for tests and local runs where both the store (redb
/// file) and the remote (a directory) can be inspected directly. Production
/// callers construct the engine with their own backends via
/// [`SyncEngine::with_backends`] instead.
#[uniffi::export]
pub fn new_local(
    client_id: String,
    db_path: String,
    remote_root: String,
    listener: Arc<dyn EngineNotificationListener>,
) -> Result<Arc<SyncEngine>, SyncError> {
    let store = Arc::new(RedbStore::open(db_path)?);
    let remote = Arc::new(LocalFolderRemote::new(remote_root)?);
    Ok(Arc::new(SyncEngine::with_backends(
        client_id,
        store,
        remote,
        listener,
        BinaryConflictPolicy::KeepBoth,
    )))
}

/// Assemble a [`SyncEngine`] over caller-injected backends and an explicit
/// binary conflict policy. The `remote` and `store` may be host-implemented
/// (foreign trait objects) or the built-in [`LocalFolderRemote`]/[`RedbStore`].
///
/// This is the FFI mirror of [`SyncEngine::with_backends`], exposed so hosts can
/// exercise conflict scenarios (custom remote, chosen policy) from their own
/// language.
#[uniffi::export]
pub fn new_engine(
    client_id: String,
    store: Arc<dyn LocalStore>,
    remote: Arc<dyn RemoteStorage>,
    listener: Arc<dyn EngineNotificationListener>,
    binary_policy: BinaryConflictPolicy,
) -> Arc<SyncEngine> {
    Arc::new(SyncEngine::with_backends(
        client_id,
        store,
        remote,
        listener,
        binary_policy,
    ))
}

/// Open a [`RedbStore`] at `db_path` as an FFI-usable handle (for hosts wiring
/// their own engine via [`new_engine`]).
#[uniffi::export]
pub fn open_redb_store(db_path: String) -> Result<Arc<RedbStore>, SyncError> {
    Ok(Arc::new(RedbStore::open(db_path)?))
}

/// Open a [`LocalFolderRemote`] rooted at `root` as an FFI-usable handle.
#[uniffi::export]
pub fn open_local_folder_remote(root: String) -> Result<Arc<LocalFolderRemote>, SyncError> {
    Ok(Arc::new(LocalFolderRemote::new(root)?))
}

uniffi::setup_scaffolding!();
