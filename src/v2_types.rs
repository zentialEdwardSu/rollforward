//! Flat v2 records and enums shared by the planner, runtime, and UniFFI hosts.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Stable identity of one synchronized resource inside a host-defined scope.
#[derive(
    uniffi::Record, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ResourceKey {
    pub scope_id: String,
    pub resource_id: String,
}

#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceKind {
    Binary,
    Text,
}

impl ResourceKey {
    pub fn new(scope_id: impl Into<String>, resource_id: impl Into<String>) -> Self {
        Self {
            scope_id: scope_id.into(),
            resource_id: resource_id.into(),
        }
    }

    pub fn encoded(&self) -> String {
        format!("{}\0{}", self.scope_id, self.resource_id)
    }
}

/// Logical state reported by a local replica.
#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    Missing,
    Present {
        content_id: String,
        size: u64,
        version_token: String,
    },
    Deleted {
        version_token: String,
    },
}

impl ReplicaState {
    pub fn present(
        content_id: impl Into<String>,
        size: u64,
        version_token: impl Into<String>,
    ) -> Self {
        Self::Present {
            content_id: content_id.into(),
            size,
            version_token: version_token.into(),
        }
    }

    pub fn content_id(&self) -> Option<&str> {
        match self {
            Self::Present { content_id, .. } => Some(content_id),
            _ => None,
        }
    }

    pub fn version_token(&self) -> Option<&str> {
        match self {
            Self::Present { version_token, .. } | Self::Deleted { version_token } => {
                Some(version_token)
            }
            Self::Missing => None,
        }
    }
}

/// Last local/remote state acknowledged by this client.
#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BaselineState {
    Present {
        content_id: String,
        local_version: String,
        remote_heads: Vec<String>,
    },
    Deleted {
        remote_heads: Vec<String>,
    },
}

impl BaselineState {
    pub fn present(
        content_id: impl Into<String>,
        local_version: impl Into<String>,
        remote_heads: Vec<String>,
    ) -> Self {
        Self::Present {
            content_id: content_id.into(),
            local_version: local_version.into(),
            remote_heads,
        }
    }
}

/// Materialized state of the current remote DAG heads.
#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteResourceState {
    Missing,
    Present {
        content_id: String,
        size: u64,
        heads: Vec<String>,
    },
    Deleted {
        heads: Vec<String>,
    },
    Forked {
        heads: Vec<String>,
    },
}

impl RemoteResourceState {
    pub fn present(content_id: impl Into<String>, size: u64, mut heads: Vec<String>) -> Self {
        heads.sort();
        heads.dedup();
        Self::Present {
            content_id: content_id.into(),
            size,
            heads,
        }
    }

    pub fn heads(&self) -> Vec<String> {
        match self {
            Self::Present { heads, .. } | Self::Deleted { heads } | Self::Forked { heads } => {
                heads.clone()
            }
            Self::Missing => Vec::new(),
        }
    }

    pub fn content_id(&self) -> Option<&str> {
        match self {
            Self::Present { content_id, .. } => Some(content_id),
            _ => None,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::Present { size, .. } => *size,
            _ => 0,
        }
    }
}

#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictKind {
    InitialDivergence,
    ContentVsContent,
    DeleteVsModify,
    ModifyVsDelete,
    RemoteFork,
    PreconditionFailed,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub id: String,
    pub resource: ResourceKey,
    pub kind: ConflictKind,
    pub local: ReplicaState,
    pub remote_heads: Vec<String>,
    pub created_at: i64,
}

impl ConflictRecord {
    pub fn new(
        resource: ResourceKey,
        kind: ConflictKind,
        local: &ReplicaState,
        remote: &RemoteResourceState,
    ) -> Self {
        let mut heads = remote.heads();
        heads.sort();
        let seed = serde_json::to_vec(&(resource.clone(), kind, local.clone(), heads.clone()))
            .unwrap_or_default();
        Self {
            id: blake3::hash(&seed).to_hex().to_string(),
            resource,
            kind,
            local: local.clone(),
            remote_heads: heads,
            created_at: now_millis(),
        }
    }
}

#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanOperation {
    Noop {
        key: ResourceKey,
    },
    EstablishBaseline {
        key: ResourceKey,
        local: ReplicaState,
        remote_heads: Vec<String>,
    },
    EstablishDeletedBaseline {
        key: ResourceKey,
        remote_heads: Vec<String>,
    },
    Upload {
        key: ResourceKey,
        local: ReplicaState,
    },
    Download {
        key: ResourceKey,
        remote: RemoteResourceState,
    },
    PublishDelete {
        key: ResourceKey,
    },
    ApplyDelete {
        key: ResourceKey,
        local: ReplicaState,
        remote_heads: Vec<String>,
    },
    Conflict {
        record: ConflictRecord,
    },
}

impl PlanOperation {
    pub fn key(&self) -> &ResourceKey {
        match self {
            Self::Noop { key }
            | Self::EstablishBaseline { key, .. }
            | Self::EstablishDeletedBaseline { key, .. }
            | Self::Upload { key, .. }
            | Self::Download { key, .. }
            | Self::PublishDelete { key }
            | Self::ApplyDelete { key, .. } => key,
            Self::Conflict { record } => &record.resource,
        }
    }
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPlan {
    pub operations: Vec<PlanOperation>,
}

#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceChange {
    BinarySnapshot {
        content_id: String,
        size: u64,
        chunk_hashes: Vec<String>,
    },
    TextDelta {
        delta: Vec<u8>,
    },
    Delete,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub id: String,
    pub resource: ResourceKey,
    pub parents: Vec<String>,
    pub author: String,
    pub client_counter: u64,
    pub timestamp: i64,
    pub change: ResourceChange,
}

impl Commit {
    pub fn create(
        resource: ResourceKey,
        mut parents: Vec<String>,
        author: String,
        client_counter: u64,
        change: ResourceChange,
    ) -> Self {
        parents.sort();
        parents.dedup();
        let timestamp = now_millis();
        let bytes = serde_json::to_vec(&(
            resource.clone(),
            &parents,
            &author,
            client_counter,
            timestamp,
            &change,
        ))
        .unwrap_or_default();
        let digest = blake3::hash(&bytes).to_hex();
        let id = format!("{author}:{client_counter}:{digest}");
        Self {
            id,
            resource,
            parents,
            author,
            client_counter,
            timestamp,
            change,
        }
    }
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCursor {
    pub client_id: String,
    pub scope_id: String,
    pub counter: u64,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogScanRequest {
    pub scopes: Vec<String>,
    pub cursors: Vec<CatalogCursor>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEvent {
    pub client_id: String,
    pub counter: u64,
    pub commit_id: String,
    pub resource: ResourceKey,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDelta {
    pub events: Vec<CatalogEvent>,
    pub cursors: Vec<CatalogCursor>,
    pub snapshot_cursors: Vec<CatalogCursor>,
}

/// A self-contained catalog generation used as the visibility boundary for
/// history truncation. Events contain every commit that remains discoverable;
/// cursors describe the immutable segments covered by the snapshot.
#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub generation: u64,
    pub scope_id: String,
    pub events: Vec<CatalogEvent>,
    pub cursors: Vec<CatalogCursor>,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCompaction {
    pub snapshot: CatalogSnapshot,
    pub obsolete_commit_ids: Vec<String>,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBatch {
    pub commits: Vec<Commit>,
    pub packs: Vec<PackObject>,
    pub indexes: Vec<PackIndexObject>,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBatchResult {
    pub visible_commits: Vec<String>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackObject {
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackIndexObject {
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub hash: String,
    pub pack_id: String,
    pub offset: u64,
    pub length: u32,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackRange {
    pub hash: String,
    pub pack_id: String,
    pub offset: u64,
    pub length: u32,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeData {
    pub hash: String,
    pub data: Vec<u8>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAck {
    pub client_id: String,
    pub resource: ResourceKey,
    pub observed_heads: Vec<String>,
    pub pinned_commits: Vec<String>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalResource {
    pub key: ResourceKey,
    pub kind: ResourceKind,
    pub state: ReplicaState,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceContent {
    pub key: ResourceKey,
    pub kind: ResourceKind,
    pub version_token: String,
    pub data: Vec<u8>,
}

#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationPrecondition {
    Missing,
    Version { version_token: String },
}

#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalMutation {
    WritePresent {
        key: ResourceKey,
        data: Vec<u8>,
        precondition: MutationPrecondition,
    },
    ApplyDelete {
        key: ResourceKey,
        precondition: MutationPrecondition,
    },
    CreateCopy {
        key: ResourceKey,
        data: Vec<u8>,
    },
}

#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyStatus {
    Applied,
    PreconditionFailed,
    Failed,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalApplyResult {
    pub key: ResourceKey,
    pub status: ApplyStatus,
    pub version_token: Option<String>,
    pub error: Option<String>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRequest {
    pub scopes: Vec<String>,
}

#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeStatus {
    Complete,
    Partial,
    Blocked,
    Failed,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncReport {
    pub run_id: String,
    pub uploaded: u64,
    pub downloaded: u64,
    pub deleted_remote: u64,
    pub deleted_local: u64,
    pub unchanged: u64,
    pub conflicts: u64,
    pub failures: u64,
    pub scopes: Vec<ScopeReport>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeReport {
    pub scope_id: String,
    pub status: ScopeStatus,
    pub conflicts: u64,
    pub failures: u64,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictQuery {
    pub scope_id: Option<String>,
}

#[derive(uniffi::Enum, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionChoice {
    Local,
    Delete,
    Remote { commit_id: String },
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreservedVersion {
    pub source: VersionChoice,
    pub target: ResourceKey,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveConflictRequest {
    pub conflict_id: String,
    pub primary: VersionChoice,
    pub preserved: Vec<PreservedVersion>,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    pub commit_id: String,
    pub parents: Vec<String>,
    pub timestamp: i64,
    pub author: String,
    pub deleted: bool,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackRequest {
    pub resource: ResourceKey,
    pub commit_id: String,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceRequest {
    pub scopes: Vec<String>,
    pub history_retention_versions: u64,
}

#[derive(uniffi::Record, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct V2MaintenanceReport {
    pub resources: u64,
    pub commits_deleted: u64,
    pub deferred: u64,
    pub packs_deleted: u64,
    pub packs_repacked: u64,
    pub bytes_reclaimed: u64,
}

#[derive(uniffi::Record, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineEvent {
    pub run_id: String,
    pub operation_id: String,
    pub stage: String,
    pub resource: Option<ResourceKey>,
    pub detail_json: String,
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}
