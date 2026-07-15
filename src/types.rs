//! FFI boundary contract: flat records and enums shared with native hosts,
//! plus the UI notification callback trait.

use serde::{Deserialize, Serialize};

/// A content-defined chunk of a binary file: its hash and position in the file.
#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// blake3 hash of the chunk bytes, hex-encoded.
    pub hash: String,
    /// Byte offset of the chunk within the file.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: u32,
}

/// The payload of a single change recorded in an [`OpLogEntry`].
#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    /// A yrs (y-crdt) update, v1-encoded.
    TextDelta {
        /// Opaque yrs update bytes produced by `encode_update_v1`.
        delta: Vec<u8>,
    },
    /// The full chunk manifest of a binary file at this version.
    BinarySnapshot {
        /// Hex blake3 hashes of the file's chunks, in file order.
        chunk_hashes: Vec<String>,
    },
    /// The file was deleted at this version.
    Delete,
}

/// One entry in the append-only operation log for a file.
#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpLogEntry {
    /// Monotonic version number within the file's log.
    pub sequence: u64,
    /// Identifier of the client that authored this entry.
    pub client_id: String,
    /// Author-side wall-clock timestamp (millis since epoch).
    pub timestamp: i64,
    /// The change carried by this entry.
    pub change_type: ChangeType,
}

/// A remote log listing item, discovered by scanning the dumb remote.
#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq)]
pub struct RemoteLogItem {
    /// Sequence parsed from the remote filename.
    pub sequence: u64,
    /// Client id parsed from the remote filename.
    pub client_id: String,
    /// Remote-relative path of the `.oplog` object.
    pub remote_path: String,
}

/// A client's synced-progress marker: `(client_id, last_synced_sequence)` as an
/// FFI-friendly record (uniFFI cannot export bare tuples).
#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq)]
pub struct ClientStatus {
    /// The reporting client's id.
    pub client_id: String,
    /// The highest sequence that client has synced.
    pub last_synced_sequence: u64,
}

/// A cached oplog entry: its sequence plus serialized bytes. FFI-friendly
/// replacement for the `(u64, Vec<u8>)` tuple the store used internally.
#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq)]
pub struct OplogCacheEntry {
    /// Sequence number of the cached entry.
    pub sequence: u64,
    /// Serialized [`OpLogEntry`] bytes.
    pub data: Vec<u8>,
}

/// How to resolve a genuine content conflict between two binary versions.
#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryConflictPolicy {
    /// Stop before applying a genuine divergent binary fork so the host can
    /// collect the user's decision. No local state or remote convergence entry
    /// is written for that sync attempt.
    Manual,
    /// Discard the remote version, keep the local one.
    KeepLocal,
    /// Discard the local version, keep the remote one.
    KeepRemote,
    /// Keep both; the host is asked to duplicate one side under a new name.
    KeepBoth,
}

/// Current logical state of a tracked binary file.
#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryFileState {
    /// The file exists and is described by this ordered chunk manifest.
    Present { manifest: Vec<String>, head: u64 },
    /// The latest binary operation is a deletion tombstone.
    Deleted { head: u64 },
}

/// Aggregate result of a batch history-truncation and pack-GC pass.
#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub files_considered: u64,
    pub files_truncated: u64,
    pub oplogs_deleted: u64,
    pub deferred_files: u64,
    pub packs_deleted: u64,
    pub packs_repacked: u64,
    pub bytes_reclaimed: u64,
}

/// Errors crossing the FFI boundary.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SyncError {
    /// An I/O or storage-backend failure.
    #[error("I/O error: {msg}")]
    IoError {
        /// Human-readable detail.
        msg: String,
    },
    /// A binary conflict requires a host-supplied policy decision.
    #[error("sync conflict, user policy required")]
    ConflictNeedResolution,
    /// A CAS append lost a race with a concurrent writer at the same sequence.
    #[error("compare-and-swap conflict at sequence {sequence}")]
    Conflict {
        /// The contended sequence number.
        sequence: u64,
    },
    /// The bounded CAS-retry loop gave up.
    #[error("retry exhausted after {attempts} attempts")]
    RetryExhausted {
        /// Number of attempts made.
        attempts: u32,
    },
    /// A stored blob failed to (de)serialize.
    #[error("serialization error: {msg}")]
    SerdeError {
        /// Human-readable detail.
        msg: String,
    },
    /// A fetched chunk's bytes did not hash to their content address — the
    /// object was corrupted or truncated in storage or in transit. Content is
    /// addressed by hash, so this is always detectable on read; the caller
    /// should re-fetch (a transient transfer fault) or treat the pack as bad.
    #[error("chunk {hash} failed integrity check (corrupt or truncated)")]
    Corrupt {
        /// The expected content-address (blake3 hex) of the chunk.
        hash: String,
    },
}

/// UI-facing notifications emitted by the engine. Remote I/O is *not* routed
/// here — that lives behind [`crate::remote::RemoteStorage`]. This trait only
/// tells the host when to refresh or to duplicate a conflicting file.
#[uniffi::export(with_foreign)]
pub trait EngineNotificationListener: Send + Sync {
    /// The file's merged content changed; the host should refresh its view.
    fn on_file_content_updated(&self, file_id: String);
    /// A `KeepBoth` binary conflict occurred; the host should duplicate the
    /// file under a conflict-marked name (e.g. `A (conflict).png`).
    fn on_conflict_copy_requested(&self, file_id: String, suggested_name: String);
}
