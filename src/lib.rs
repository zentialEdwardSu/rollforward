//! `rollforward` — a decentralized, dumb-remote-storage sync engine.
//!
//! See `design.md` for the architecture. The engine core ([`engine::SyncEngine`])
//! depends only on two traits — [`remote::RemoteStorage`] (the dumb remote) and
//! [`store::LocalStore`] (local durable state) — and the caller injects the
//! implementations via [`SyncEngine::with_backends`].
//!
//! [`store::RedbStore`] is the **default local-store backend**: the convenience
//! constructors [`new_local`] and [`new_with_remote`] wire it up so a host gets
//! durable local state without implementing [`store::LocalStore`] itself. A host
//! that wants a different durable store still injects one via [`new_engine`] /
//! [`SyncEngine::with_backends`]. Because redb holds an exclusive file lock for
//! the life of its handle, hosts should call [`SyncEngine::close`] to release it
//! (e.g. before reopening the same path after an error).
//!
//! [`remote::LocalFolderRemote`] remains a **test/local remote** — no
//! constructor defaults the remote, since production hosts point at their own
//! (network drive / S3 / WebDAV). Both built-ins are directly inspectable (a
//! redb file, a directory tree), which is how the tests observe behavior.

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

pub mod adapter;
pub mod binary;
pub mod core;
// v1's linear-sequence engine/storage modules are intentionally not linked in
// v2. Their source remains in-tree temporarily for archaeology only.
pub mod runtime;
pub mod text;
mod types;
pub mod v2_remote;
pub mod v2_store;
pub mod v2_types;

use std::sync::Arc;

pub use adapter::{
    EngineEventListenerV2, LocalReplica, NoopEventListenerV2, RemoteStorageV2, TextMergePolicy,
    TextMergeResult,
};
pub use runtime::SyncRuntime;
pub use types::SyncError;
pub use v2_remote::LocalFolderRemoteV2;
pub use v2_store::{RedbRuntimeStore, RuntimeStore};
pub use v2_types::*;

/// Assemble a [`SyncEngine`] over the local/test backends: a [`RedbStore`] at
/// `db_path` and a [`LocalFolderRemote`] rooted at `remote_root`, with
/// `KeepBoth` binary conflict handling.
///
/// This is a convenience for tests and local runs where both the store (redb
/// file) and the remote (a directory) can be inspected directly. Production
/// callers construct the engine with their own backends via
/// [`SyncEngine::with_backends`] instead.
#[uniffi::export]
#[cfg(any())]
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

/// Assemble a [`SyncEngine`] with the built-in [`RedbStore`] at `db_path` as the
/// default local store, an injected `remote`, and an explicit binary conflict
/// policy.
///
/// This is the middle ground between [`new_local`] (both backends built-in) and
/// [`new_engine`] (both injected): a host that is happy with redb for local
/// durable state but supplies its own remote (S3/WebDAV/…) and wants to choose
/// the conflict policy. Call [`SyncEngine::close`] to release the redb file lock
/// when done.
#[uniffi::export]
#[cfg(any())]
pub fn new_with_remote(
    client_id: String,
    db_path: String,
    remote: Arc<dyn RemoteStorage>,
    listener: Arc<dyn EngineNotificationListener>,
    binary_policy: BinaryConflictPolicy,
) -> Result<Arc<SyncEngine>, SyncError> {
    let store = Arc::new(RedbStore::open(db_path)?);
    Ok(Arc::new(SyncEngine::with_backends(
        client_id,
        store,
        remote,
        listener,
        binary_policy,
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
#[cfg(any())]
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
#[cfg(any())]
pub fn open_redb_store(db_path: String) -> Result<Arc<RedbStore>, SyncError> {
    Ok(Arc::new(RedbStore::open(db_path)?))
}

/// Open a [`LocalFolderRemote`] rooted at `root` as an FFI-usable handle.
#[uniffi::export]
#[cfg(any())]
pub fn open_local_folder_remote(root: String) -> Result<Arc<LocalFolderRemote>, SyncError> {
    Ok(Arc::new(LocalFolderRemote::new(root)?))
}

/// Construct the complete v2 synchronization runtime over injected adapters.
#[uniffi::export]
pub fn new_runtime(
    client_id: String,
    store: Arc<dyn RuntimeStore>,
    remote: Arc<dyn RemoteStorageV2>,
    local: Arc<dyn LocalReplica>,
    listener: Arc<dyn EngineEventListenerV2>,
) -> Result<Arc<SyncRuntime>, SyncError> {
    Ok(Arc::new(SyncRuntime::with_backends(
        client_id, store, remote, local, listener,
    )?))
}

/// Open the isolated v2 redb state database used by the runtime.
#[uniffi::export]
pub fn open_redb_runtime_store(path: String) -> Result<Arc<RedbRuntimeStore>, SyncError> {
    Ok(Arc::new(RedbRuntimeStore::open(path)?))
}

uniffi::setup_scaffolding!();
