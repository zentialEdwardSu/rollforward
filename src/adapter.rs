//! Batch, FFI-friendly ports implemented by local replicas and remote stores.

use std::sync::Arc;

use crate::types::SyncError;
use crate::v2_types::{
    CatalogCompaction, CatalogDelta, CatalogScanRequest, ChunkLocation, Commit, CommitBatch,
    CommitBatchResult, EngineEvent, LocalApplyResult, LocalMutation, LocalResource, PackRange,
    RangeData, ResourceAck, ResourceContent, ResourceKey,
};

#[uniffi::export(with_foreign)]
pub trait LocalReplica: Send + Sync {
    fn list_resources(&self, scopes: Vec<String>) -> Result<Vec<LocalResource>, SyncError>;
    fn read_resources(&self, keys: Vec<ResourceKey>) -> Result<Vec<ResourceContent>, SyncError>;
    fn apply_mutations(
        &self,
        mutations: Vec<LocalMutation>,
    ) -> Result<Vec<LocalApplyResult>, SyncError>;
}

#[uniffi::export(with_foreign)]
pub trait RemoteStorageV2: Send + Sync {
    fn scan_catalog(&self, request: CatalogScanRequest) -> Result<CatalogDelta, SyncError>;
    fn load_commits(&self, ids: Vec<String>) -> Result<Vec<Commit>, SyncError>;
    fn commit_batch(&self, batch: CommitBatch) -> Result<CommitBatchResult, SyncError>;
    fn lookup_chunks(&self, hashes: Vec<String>) -> Result<Vec<ChunkLocation>, SyncError>;
    fn read_ranges(&self, ranges: Vec<PackRange>) -> Result<Vec<RangeData>, SyncError>;
    fn write_acknowledgements(&self, acknowledgements: Vec<ResourceAck>) -> Result<(), SyncError>;
    fn list_acknowledgements(
        &self,
        resources: Vec<ResourceKey>,
    ) -> Result<Vec<ResourceAck>, SyncError>;
    fn list_pack_ids(&self) -> Result<Vec<String>, SyncError>;
    fn delete_pack_objects(&self, pack_ids: Vec<String>) -> Result<(), SyncError>;
    fn compact_catalog(&self, compaction: CatalogCompaction) -> Result<(), SyncError>;
}

#[uniffi::export(with_foreign)]
pub trait EngineEventListenerV2: Send + Sync {
    fn on_events(&self, events: Vec<EngineEvent>);
}

#[derive(uniffi::Object)]
pub struct NoopEventListenerV2;

#[uniffi::export]
impl NoopEventListenerV2 {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl EngineEventListenerV2 for NoopEventListenerV2 {
    fn on_events(&self, _events: Vec<EngineEvent>) {}
}
