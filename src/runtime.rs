//! V2 synchronization runtime: inventory, planning, execution, and recovery.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::adapter::{
    EngineEventListenerV2, LocalReplica, RemoteStorageV2, TextMergePolicy, TextMergeResult,
};
use crate::binary;
use crate::core::plan_inventory;
use crate::text::TextDoc;
use crate::types::SyncError;
use crate::v2_store::RuntimeStore;
use crate::v2_types::*;

fn serde_error(error: impl std::fmt::Display) -> SyncError {
    SyncError::SerdeError {
        msg: error.to_string(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredBaseline {
    key: ResourceKey,
    state: BaselineState,
    last_operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OperationJournal {
    id: String,
    key: ResourceKey,
    stage: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RunJournal {
    id: String,
    operations: Vec<OperationJournal>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredCheckpoint {
    key: ResourceKey,
    commit_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct RuntimeState {
    counter: u64,
    baselines: BTreeMap<String, StoredBaseline>,
    commits: BTreeMap<String, Commit>,
    cursors: Vec<CatalogCursor>,
    conflicts: BTreeMap<String, ConflictRecord>,
    kinds: BTreeMap<String, ResourceKind>,
    text_states: BTreeMap<String, Vec<u8>>,
    active_run: Option<RunJournal>,
    retired_clients: BTreeSet<String>,
    checkpoints: BTreeMap<String, StoredCheckpoint>,
    catalog_events_since_snapshot: BTreeMap<String, u64>,
}

impl RuntimeState {
    fn load(store: &dyn RuntimeStore) -> Result<Self, SyncError> {
        store.load_runtime_state()?.map_or_else(
            || Ok(Self::default()),
            |bytes| serde_json::from_slice(&bytes).map_err(serde_error),
        )
    }

    fn save(&self, store: &dyn RuntimeStore) -> Result<(), SyncError> {
        store.save_runtime_state(serde_json::to_vec(self).map_err(serde_error)?)
    }

    fn baseline_pairs(&self, scopes: &HashSet<String>) -> Vec<(ResourceKey, BaselineState)> {
        self.baselines
            .values()
            .filter(|entry| scopes.is_empty() || scopes.contains(&entry.key.scope_id))
            .map(|entry| (entry.key.clone(), entry.state.clone()))
            .collect()
    }

    fn resource_commits(&self, key: &ResourceKey) -> Vec<&Commit> {
        self.commits
            .values()
            .filter(|commit| &commit.resource == key)
            .collect()
    }

    fn heads(&self, key: &ResourceKey) -> Vec<String> {
        let commits = self.resource_commits(key);
        let ids: BTreeSet<_> = commits.iter().map(|commit| commit.id.clone()).collect();
        let parents: BTreeSet<_> = commits
            .iter()
            .flat_map(|commit| commit.parents.iter().cloned())
            .collect();
        ids.difference(&parents).cloned().collect()
    }

    fn is_descendant(&self, candidate: &str, ancestor: &str) -> bool {
        let mut pending = vec![candidate.to_owned()];
        let mut visited = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if id == ancestor {
                return true;
            }
            if visited.insert(id.clone())
                && let Some(commit) = self.commits.get(&id)
            {
                pending.extend(commit.parents.iter().cloned());
            }
        }
        false
    }

    fn remote_keys(&self, scopes: &HashSet<String>) -> BTreeSet<ResourceKey> {
        self.commits
            .values()
            .filter(|commit| scopes.is_empty() || scopes.contains(&commit.resource.scope_id))
            .map(|commit| commit.resource.clone())
            .collect()
    }

    fn text_doc(&self, key: &ResourceKey) -> Result<TextDoc, SyncError> {
        let mut deltas: Vec<(String, Vec<u8>)> = self
            .resource_commits(key)
            .into_iter()
            .filter_map(|commit| match &commit.change {
                ResourceChange::TextDelta { delta } => Some((commit.id.clone(), delta.clone())),
                _ => None,
            })
            .collect();
        deltas.sort_by(|left, right| left.0.cmp(&right.0));
        TextDoc::from_baseline_and_deltas(
            None,
            &deltas
                .into_iter()
                .map(|(_, delta)| delta)
                .collect::<Vec<_>>(),
        )
    }

    fn text_doc_at(&self, key: &ResourceKey, head: &str) -> Result<TextDoc, SyncError> {
        let mut reachable = BTreeSet::new();
        let mut pending = vec![head.to_owned()];
        while let Some(id) = pending.pop() {
            if !reachable.insert(id.clone()) {
                continue;
            }
            if let Some(commit) = self
                .commits
                .get(&id)
                .filter(|commit| &commit.resource == key)
            {
                pending.extend(commit.parents.iter().cloned());
            }
        }
        let mut deltas: Vec<_> = reachable
            .into_iter()
            .filter_map(|id| {
                self.commits
                    .get(&id)
                    .and_then(|commit| match &commit.change {
                        ResourceChange::TextDelta { delta } => Some((id, delta.clone())),
                        _ => None,
                    })
            })
            .collect();
        deltas.sort_by(|left, right| left.0.cmp(&right.0));
        TextDoc::from_baseline_and_deltas(
            None,
            &deltas
                .into_iter()
                .map(|(_, delta)| delta)
                .collect::<Vec<_>>(),
        )
    }

    fn remote_state(&self, key: &ResourceKey) -> Result<RemoteResourceState, SyncError> {
        let heads = self.heads(key);
        if heads.is_empty() {
            return Ok(RemoteResourceState::Missing);
        }
        let changes: Vec<_> = heads
            .iter()
            .filter_map(|id| self.commits.get(id).map(|commit| &commit.change))
            .collect();
        if changes
            .iter()
            .all(|change| matches!(change, ResourceChange::Delete))
        {
            return Ok(RemoteResourceState::Deleted { heads });
        }
        if changes
            .iter()
            .all(|change| matches!(change, ResourceChange::TextDelta { .. }))
        {
            let text = self.text_doc(key)?.content();
            return Ok(RemoteResourceState::present(
                blake3::hash(text.as_bytes()).to_hex().to_string(),
                text.len() as u64,
                heads,
            ));
        }
        let snapshots: Vec<_> = changes
            .iter()
            .filter_map(|change| match change {
                ResourceChange::BinarySnapshot {
                    content_id, size, ..
                } => Some((content_id.as_str(), *size)),
                _ => None,
            })
            .collect();
        if snapshots.len() == changes.len() && snapshots.iter().all(|value| value == &snapshots[0])
        {
            return Ok(RemoteResourceState::present(
                snapshots[0].0,
                snapshots[0].1,
                heads,
            ));
        }
        Ok(RemoteResourceState::Forked { heads })
    }

    fn remote_pairs(
        &self,
        scopes: &HashSet<String>,
    ) -> Result<Vec<(ResourceKey, RemoteResourceState)>, SyncError> {
        self.remote_keys(scopes)
            .into_iter()
            .map(|key| Ok((key.clone(), self.remote_state(&key)?)))
            .collect()
    }
}

/// Complete v2 synchronization engine.
#[derive(uniffi::Object)]
pub struct SyncRuntime {
    client_id: String,
    local: Arc<dyn LocalReplica>,
    remote: Arc<dyn RemoteStorageV2>,
    store: Arc<dyn RuntimeStore>,
    listener: Arc<dyn EngineEventListenerV2>,
    text_merge_policy: Option<Arc<dyn TextMergePolicy>>,
    state: Mutex<RuntimeState>,
}

impl SyncRuntime {
    pub fn with_backends(
        client_id: String,
        store: Arc<dyn RuntimeStore>,
        remote: Arc<dyn RemoteStorageV2>,
        local: Arc<dyn LocalReplica>,
        listener: Arc<dyn EngineEventListenerV2>,
    ) -> Result<Self, SyncError> {
        let state = RuntimeState::load(store.as_ref())?;
        Ok(Self {
            client_id,
            local,
            remote,
            store,
            listener,
            text_merge_policy: None,
            state: Mutex::new(state),
        })
    }

    /// Construct a runtime with a host-provided syntax-aware text merger.
    pub fn with_text_merge_policy(
        client_id: String,
        store: Arc<dyn RuntimeStore>,
        remote: Arc<dyn RemoteStorageV2>,
        local: Arc<dyn LocalReplica>,
        listener: Arc<dyn EngineEventListenerV2>,
        text_merge_policy: Arc<dyn TextMergePolicy>,
    ) -> Result<Self, SyncError> {
        let mut runtime = Self::with_backends(client_id, store, remote, local, listener)?;
        runtime.text_merge_policy = Some(text_merge_policy);
        Ok(runtime)
    }

    fn emit(&self, events: Vec<EngineEvent>) {
        if !events.is_empty() {
            self.listener.on_events(events);
        }
    }

    /// Publish one event immediately while retaining it in the run journal
    /// returned by `execute`. Long-running adapters can therefore surface
    /// progress before the whole reconciliation finishes.
    fn emit_progress(&self, events: &mut Vec<EngineEvent>, event: EngineEvent) {
        self.emit(vec![event.clone()]);
        events.push(event);
    }

    fn refresh_remote(&self, state: &mut RuntimeState, scopes: &[String]) -> Result<(), SyncError> {
        let delta = self.remote.scan_catalog(CatalogScanRequest {
            scopes: scopes.to_vec(),
            cursors: state.cursors.clone(),
        })?;
        let snapshot_coverage: BTreeMap<_, _> = delta
            .snapshot_cursors
            .iter()
            .map(|cursor| {
                (
                    (cursor.client_id.clone(), cursor.scope_id.clone()),
                    cursor.counter,
                )
            })
            .collect();
        for event in &delta.events {
            if event.counter
                <= snapshot_coverage
                    .get(&(event.client_id.clone(), event.resource.scope_id.clone()))
                    .copied()
                    .unwrap_or(0)
            {
                continue;
            }
            *state
                .catalog_events_since_snapshot
                .entry(event.resource.scope_id.clone())
                .or_default() += 1;
        }
        let missing: Vec<_> = delta
            .events
            .iter()
            .filter(|event| !state.commits.contains_key(&event.commit_id))
            .map(|event| event.commit_id.clone())
            .collect();
        for commit in self.remote.load_commits(missing)? {
            state.commits.insert(commit.id.clone(), commit);
        }
        let mut cursors: BTreeMap<(String, String), u64> = state
            .cursors
            .drain(..)
            .map(|cursor| ((cursor.client_id, cursor.scope_id), cursor.counter))
            .collect();
        for cursor in delta.cursors {
            cursors
                .entry((cursor.client_id, cursor.scope_id))
                .and_modify(|counter| *counter = (*counter).max(cursor.counter))
                .or_insert(cursor.counter);
        }
        state.cursors = cursors
            .into_iter()
            .map(|((client_id, scope_id), counter)| CatalogCursor {
                client_id,
                scope_id,
                counter,
            })
            .collect();
        Ok(())
    }

    fn snapshot_generation(&self) -> u64 {
        let millis = now_millis().max(0) as u64;
        let digest = blake3::hash(self.client_id.as_bytes());
        let suffix = u16::from_be_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]);
        (millis << 16) | u64::from(suffix)
    }

    fn snapshot_large_catalogs(&self, state: &mut RuntimeState) -> Result<(), SyncError> {
        let due: Vec<_> = state
            .catalog_events_since_snapshot
            .iter()
            .filter(|(_, events)| **events >= 10_000)
            .map(|(scope, _)| scope.clone())
            .collect();
        for scope in due {
            let events = state
                .commits
                .values()
                .filter(|commit| commit.resource.scope_id == scope)
                .map(|commit| CatalogEvent {
                    client_id: commit.author.clone(),
                    counter: commit.client_counter,
                    commit_id: commit.id.clone(),
                    resource: commit.resource.clone(),
                })
                .collect();
            let cursors = state
                .cursors
                .iter()
                .filter(|cursor| cursor.scope_id == scope)
                .cloned()
                .collect();
            self.remote.compact_catalog(CatalogCompaction {
                snapshot: CatalogSnapshot {
                    generation: self.snapshot_generation(),
                    scope_id: scope.clone(),
                    events,
                    cursors,
                },
                obsolete_commit_ids: Vec::new(),
            })?;
            state.catalog_events_since_snapshot.insert(scope, 0);
        }
        Ok(())
    }

    fn local_inventory(
        &self,
        state: &RuntimeState,
        scopes: &[String],
    ) -> Result<Vec<LocalResource>, SyncError> {
        let mut resources = self.local.list_resources(scopes.to_vec())?;
        let mut reads = Vec::new();
        for resource in &mut resources {
            if let ReplicaState::Present {
                content_id,
                version_token,
                ..
            } = &mut resource.state
                && content_id.is_empty()
            {
                let cached = state
                    .baselines
                    .get(&resource.key.encoded())
                    .and_then(|entry| match &entry.state {
                        BaselineState::Present {
                            content_id,
                            local_version,
                            ..
                        } if local_version == version_token => Some(content_id.clone()),
                        _ => None,
                    });
                if let Some(cached) = cached {
                    *content_id = cached;
                } else {
                    reads.push(resource.key.clone());
                }
            }
        }
        if !reads.is_empty() {
            let contents: HashMap<_, _> = self
                .local
                .read_resources(reads)?
                .into_iter()
                .map(|content| (content.key.clone(), content))
                .collect();
            for resource in &mut resources {
                if let ReplicaState::Present {
                    content_id, size, ..
                } = &mut resource.state
                    && content_id.is_empty()
                {
                    let content =
                        contents
                            .get(&resource.key)
                            .ok_or_else(|| SyncError::IoError {
                                msg: format!(
                                    "local replica did not return {}",
                                    resource.key.encoded()
                                ),
                            })?;
                    *content_id = blake3::hash(&content.data).to_hex().to_string();
                    *size = content.data.len() as u64;
                }
            }
        }
        Ok(resources)
    }

    fn build_plan(
        &self,
        state: &RuntimeState,
        scopes: &[String],
        local: &[LocalResource],
    ) -> Result<SyncPlan, SyncError> {
        let scope_set: HashSet<_> = scopes.iter().cloned().collect();
        let mut plan = plan_inventory(
            local
                .iter()
                .map(|resource| (resource.key.clone(), resource.state.clone()))
                .collect(),
            state.baseline_pairs(&scope_set),
            state.remote_pairs(&scope_set)?,
        );
        let kinds: HashMap<_, _> = local
            .iter()
            .map(|resource| (resource.key.clone(), resource.kind))
            .collect();
        for operation in &mut plan.operations {
            if let PlanOperation::Conflict { record } = operation
                && record.kind == ConflictKind::ContentVsContent
                && kinds.get(&record.resource) == Some(&ResourceKind::Text)
            {
                if let Some(resource) = local
                    .iter()
                    .find(|resource| resource.key == record.resource)
                {
                    *operation = PlanOperation::Upload {
                        key: resource.key.clone(),
                        local: resource.state.clone(),
                    };
                }
            }
        }
        if let Some(policy) = self.text_merge_policy.as_ref() {
            plan.operations.sort_by(|left, right| {
                left.key()
                    .scope_id
                    .cmp(&right.key().scope_id)
                    .then_with(|| {
                        policy
                            .resource_priority(left.key())
                            .cmp(&policy.resource_priority(right.key()))
                    })
                    .then_with(|| left.key().resource_id.cmp(&right.key().resource_id))
            });
        }
        Ok(plan)
    }

    fn content_for_remote(
        &self,
        state: &RuntimeState,
        key: &ResourceKey,
        selected_head: Option<&str>,
    ) -> Result<Vec<u8>, SyncError> {
        let heads = state.heads(key);
        let head = selected_head
            .map(str::to_owned)
            .or_else(|| heads.first().cloned())
            .ok_or_else(|| SyncError::IoError {
                msg: format!("remote resource {} has no head", key.encoded()),
            })?;
        let commit = state.commits.get(&head).ok_or_else(|| SyncError::IoError {
            msg: format!("missing commit {head}"),
        })?;
        match &commit.change {
            ResourceChange::Delete => Err(SyncError::IoError {
                msg: format!("{} is deleted", key.encoded()),
            }),
            ResourceChange::TextDelta { .. } => {
                Ok(state.text_doc_at(key, &head)?.content().into_bytes())
            }
            ResourceChange::BinarySnapshot { chunk_hashes, .. } => {
                let locations: HashMap<_, _> = self
                    .remote
                    .lookup_chunks(chunk_hashes.clone())?
                    .into_iter()
                    .map(|location| (location.hash.clone(), location))
                    .collect();
                let ranges: Vec<_> = chunk_hashes
                    .iter()
                    .map(|hash| {
                        let location = locations.get(hash).ok_or_else(|| SyncError::IoError {
                            msg: format!("chunk {hash} is missing"),
                        })?;
                        Ok(PackRange {
                            hash: hash.clone(),
                            pack_id: location.pack_id.clone(),
                            offset: location.offset,
                            length: location.length,
                        })
                    })
                    .collect::<Result<_, SyncError>>()?;
                let chunks: HashMap<_, _> = self
                    .remote
                    .read_ranges(ranges)?
                    .into_iter()
                    .map(|range| (range.hash, range.data))
                    .collect();
                let mut output = Vec::new();
                for hash in chunk_hashes {
                    output.extend_from_slice(chunks.get(hash).ok_or_else(|| {
                        SyncError::IoError {
                            msg: format!("range {hash} was not returned"),
                        }
                    })?);
                }
                Ok(output)
            }
        }
    }

    fn next_counter(state: &mut RuntimeState) -> u64 {
        state.counter += 1;
        state.counter
    }

    fn publish_bytes(
        &self,
        state: &mut RuntimeState,
        key: ResourceKey,
        kind: ResourceKind,
        data: Vec<u8>,
        parents: Vec<String>,
    ) -> Result<Commit, SyncError> {
        let counter = Self::next_counter(state);
        let mut batch = CommitBatch::default();
        let change = match kind {
            ResourceKind::Binary => {
                let chunks = binary::chunk_data(&data);
                let existing: HashSet<_> = self
                    .remote
                    .lookup_chunks(chunks.iter().map(|(chunk, _)| chunk.hash.clone()).collect())?
                    .into_iter()
                    .map(|location| location.hash)
                    .collect();
                let missing: Vec<_> = chunks
                    .iter()
                    .filter(|(chunk, _)| !existing.contains(&chunk.hash))
                    .cloned()
                    .collect();
                for (id, bytes, index) in binary::build_packs(&missing) {
                    batch.packs.push(PackObject {
                        id: id.clone(),
                        data: bytes,
                    });
                    batch.indexes.push(PackIndexObject {
                        id,
                        data: serde_json::to_vec(&index).map_err(serde_error)?,
                    });
                }
                ResourceChange::BinarySnapshot {
                    content_id: blake3::hash(&data).to_hex().to_string(),
                    size: data.len() as u64,
                    chunk_hashes: chunks.into_iter().map(|(chunk, _)| chunk.hash).collect(),
                }
            }
            ResourceKind::Text => {
                let mut document = state.text_doc(&key)?;
                let text = std::str::from_utf8(&data).map_err(serde_error)?;
                ResourceChange::TextDelta {
                    delta: document.set_text(text),
                }
            }
        };
        let commit = Commit::create(key, parents, self.client_id.clone(), counter, change);
        batch.commits.push(commit.clone());
        self.remote.commit_batch(batch)?;
        state.commits.insert(commit.id.clone(), commit.clone());
        Ok(commit)
    }

    fn publish_delete(
        &self,
        state: &mut RuntimeState,
        key: ResourceKey,
        parents: Vec<String>,
    ) -> Result<Commit, SyncError> {
        let commit = Commit::create(
            key,
            parents,
            self.client_id.clone(),
            Self::next_counter(state),
            ResourceChange::Delete,
        );
        self.remote.commit_batch(CommitBatch {
            commits: vec![commit.clone()],
            ..CommitBatch::default()
        })?;
        state.commits.insert(commit.id.clone(), commit.clone());
        Ok(commit)
    }

    fn precondition(local: &ReplicaState) -> MutationPrecondition {
        local
            .version_token()
            .map_or(MutationPrecondition::Missing, |token| {
                MutationPrecondition::Version {
                    version_token: token.to_owned(),
                }
            })
    }

    fn set_present_baseline(
        state: &mut RuntimeState,
        key: ResourceKey,
        content_id: String,
        local_version: String,
        remote_heads: Vec<String>,
        operation_id: String,
    ) {
        state.baselines.insert(
            key.encoded(),
            StoredBaseline {
                key,
                state: BaselineState::Present {
                    content_id,
                    local_version,
                    remote_heads,
                },
                last_operation_id: operation_id,
            },
        );
    }

    fn set_deleted_baseline(
        state: &mut RuntimeState,
        key: ResourceKey,
        remote_heads: Vec<String>,
        operation_id: String,
    ) {
        state.baselines.insert(
            key.encoded(),
            StoredBaseline {
                key,
                state: BaselineState::Deleted { remote_heads },
                last_operation_id: operation_id,
            },
        );
    }

    fn report_scopes(
        request: &SyncRequest,
        plan: &SyncPlan,
        failures: &HashMap<String, u64>,
    ) -> Vec<ScopeReport> {
        request
            .scopes
            .iter()
            .map(|scope| {
                let conflicts = plan
                    .operations
                    .iter()
                    .filter(|operation| {
                        operation.key().scope_id == *scope
                            && matches!(operation, PlanOperation::Conflict { .. })
                    })
                    .count() as u64;
                let failed = failures.get(scope).copied().unwrap_or(0);
                let status = if failed > 0 {
                    ScopeStatus::Failed
                } else if conflicts > 0 {
                    ScopeStatus::Partial
                } else {
                    ScopeStatus::Complete
                };
                ScopeReport {
                    scope_id: scope.clone(),
                    status,
                    conflicts,
                    failures: failed,
                }
            })
            .collect()
    }

    fn execute(
        &self,
        request: &SyncRequest,
        state: &mut RuntimeState,
        local: &[LocalResource],
        plan: &SyncPlan,
        run_id: &str,
    ) -> Result<(SyncReport, Vec<EngineEvent>), SyncError> {
        let local_by_key: HashMap<_, _> = local
            .iter()
            .map(|resource| (resource.key.clone(), resource))
            .collect();
        let mut report = SyncReport {
            run_id: run_id.to_owned(),
            ..SyncReport::default()
        };
        let mut failures = HashMap::<String, u64>::new();
        let mut events = Vec::new();
        self.emit_progress(
            &mut events,
            EngineEvent {
                run_id: run_id.into(),
                operation_id: run_id.into(),
                stage: "run.start".into(),
                resource: None,
                detail_json: serde_json::json!({
                    "scopes": request.scopes.len(),
                    "local_resources": local.len(),
                    "operations": plan.operations.len(),
                })
                .to_string(),
            },
        );

        state
            .conflicts
            .retain(|_, conflict| !request.scopes.contains(&conflict.resource.scope_id));
        for (index, operation) in plan.operations.iter().enumerate() {
            let operation_id = format!("{run_id}:{index}");
            let decision = match operation {
                PlanOperation::Noop { .. } => "noop",
                PlanOperation::EstablishBaseline { .. } => "establish_baseline",
                PlanOperation::EstablishDeletedBaseline { .. } => "establish_deleted_baseline",
                PlanOperation::Upload { .. } => "upload",
                PlanOperation::Download { .. } => "download",
                PlanOperation::PublishDelete { .. } => "publish_delete",
                PlanOperation::ApplyDelete { .. } => "apply_delete",
                PlanOperation::Conflict { .. } => "conflict",
            };
            self.emit_progress(
                &mut events,
                EngineEvent {
                    run_id: run_id.into(),
                    operation_id: operation_id.clone(),
                    stage: "resource.scan".into(),
                    resource: Some(operation.key().clone()),
                    detail_json: "{}".into(),
                },
            );
            self.emit_progress(
                &mut events,
                EngineEvent {
                    run_id: run_id.into(),
                    operation_id: operation_id.clone(),
                    stage: "resource.compare".into(),
                    resource: Some(operation.key().clone()),
                    detail_json: serde_json::json!({"decision": decision}).to_string(),
                },
            );
            self.emit_progress(
                &mut events,
                EngineEvent {
                    run_id: run_id.into(),
                    operation_id,
                    stage: "resource.decision".into(),
                    resource: Some(operation.key().clone()),
                    detail_json: serde_json::json!({"decision": decision}).to_string(),
                },
            );
            if let PlanOperation::Conflict { record } = operation {
                state.conflicts.insert(record.id.clone(), record.clone());
                report.conflicts += 1;
                self.emit_progress(
                    &mut events,
                    EngineEvent {
                        run_id: run_id.into(),
                        operation_id: record.id.clone(),
                        stage: "conflict".into(),
                        resource: Some(record.resource.clone()),
                        detail_json: serde_json::json!({
                            "kind": format!("{:?}", record.kind),
                            "remote_heads": record.remote_heads.len(),
                        })
                        .to_string(),
                    },
                );
            }
        }

        let journal = RunJournal {
            id: run_id.to_owned(),
            operations: plan
                .operations
                .iter()
                .enumerate()
                .map(|(index, operation)| OperationJournal {
                    id: format!("{run_id}:{index}"),
                    key: operation.key().clone(),
                    stage: "planned".into(),
                })
                .collect(),
        };
        state.active_run = Some(journal);
        state.save(self.store.as_ref())?;

        let uploads: Vec<_> = plan
            .operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    PlanOperation::Upload { .. } | PlanOperation::PublishDelete { .. }
                )
            })
            .collect();
        let upload_keys: Vec<_> = uploads
            .iter()
            .filter_map(|operation| match operation {
                PlanOperation::Upload { key, .. } => Some(key.clone()),
                _ => None,
            })
            .collect();
        if !uploads.is_empty() {
            self.emit_progress(
                &mut events,
                EngineEvent {
                    run_id: run_id.into(),
                    operation_id: format!("{run_id}:upload"),
                    stage: "upload.prepare".into(),
                    resource: None,
                    detail_json: serde_json::json!({"resources": uploads.len()}).to_string(),
                },
            );
        }
        let contents: HashMap<_, _> = self
            .local
            .read_resources(upload_keys)?
            .into_iter()
            .map(|content| (content.key.clone(), content))
            .collect();
        let mut all_chunks = Vec::new();
        let mut manifests = HashMap::new();
        let mut failed_upload_keys = HashSet::new();
        let mut merged_texts = HashMap::<ResourceKey, Vec<u8>>::new();
        for operation in &uploads {
            if let PlanOperation::Upload { key, local } = operation {
                let Some(content) = contents.get(key) else {
                    failed_upload_keys.insert(key.clone());
                    *failures.entry(key.scope_id.clone()).or_default() += 1;
                    report.failures += 1;
                    self.emit_progress(
                        &mut events,
                        EngineEvent {
                            run_id: run_id.into(),
                            operation_id: format!("{run_id}:read"),
                            stage: "error".into(),
                            resource: Some(key.clone()),
                            detail_json: serde_json::json!({
                                "error": "local replica did not return requested content"
                            })
                            .to_string(),
                        },
                    );
                    continue;
                };
                if local.version_token() != Some(content.version_token.as_str()) {
                    failed_upload_keys.insert(key.clone());
                    *failures.entry(key.scope_id.clone()).or_default() += 1;
                    report.failures += 1;
                    continue;
                }
                if content.kind == ResourceKind::Text
                    && let Some(policy) = self.text_merge_policy.as_ref()
                    && let Some(StoredBaseline {
                        state: BaselineState::Present { remote_heads, .. },
                        ..
                    }) = state.baselines.get(&key.encoded())
                    && *remote_heads != state.heads(key)
                    && let Some(full) = state.text_states.get(&key.encoded())
                {
                    let base = TextDoc::from_baseline_and_deltas(Some(full), &[])?
                        .content()
                        .into_bytes();
                    let remote = state.text_doc(key)?.content().into_bytes();
                    match policy.merge_text(key, &base, &content.data, &remote)? {
                        TextMergeResult::NotHandled => {}
                        TextMergeResult::Merged(data) => {
                            std::str::from_utf8(&data).map_err(serde_error)?;
                            merged_texts.insert(key.clone(), data);
                        }
                        TextMergeResult::Conflict(reason) => {
                            failed_upload_keys.insert(key.clone());
                            let remote_state = state.remote_state(key)?;
                            let record = ConflictRecord::new(
                                key.clone(),
                                ConflictKind::ContentVsContent,
                                local,
                                &remote_state,
                            );
                            state.conflicts.insert(record.id.clone(), record.clone());
                            report.conflicts += 1;
                            self.emit_progress(
                                &mut events,
                                EngineEvent {
                                    run_id: run_id.into(),
                                    operation_id: record.id,
                                    stage: "conflict".into(),
                                    resource: Some(key.clone()),
                                    detail_json: serde_json::json!({
                                        "kind": "ContentVsContent",
                                        "reason": reason,
                                        "structured_text": true,
                                    })
                                    .to_string(),
                                },
                            );
                            continue;
                        }
                    }
                }
                if content.kind == ResourceKind::Binary {
                    let chunks = binary::chunk_data(&content.data);
                    manifests.insert(
                        key.clone(),
                        chunks
                            .iter()
                            .map(|(chunk, _)| chunk.hash.clone())
                            .collect::<Vec<_>>(),
                    );
                    all_chunks.extend(chunks);
                }
            }
        }
        let existing: HashSet<_> = self
            .remote
            .lookup_chunks(
                all_chunks
                    .iter()
                    .map(|(chunk, _)| chunk.hash.clone())
                    .collect(),
            )?
            .into_iter()
            .map(|location| location.hash)
            .collect();
        let new_chunks: Vec<_> = all_chunks
            .into_iter()
            .filter(|(chunk, _)| !existing.contains(&chunk.hash))
            .collect();
        let built_packs = binary::build_packs(&new_chunks);
        if !uploads.is_empty() {
            self.emit_progress(&mut events, EngineEvent {
                run_id: run_id.into(),
                operation_id: format!("{run_id}:upload"),
                stage: "upload.plan".into(),
                resource: None,
                detail_json: serde_json::json!({
                    "resources": uploads.len(),
                    "packs": built_packs.len(),
                    "bytes": built_packs.iter().map(|(_, data, _)| data.len() as u64).sum::<u64>(),
                    "chunks": new_chunks.len(),
                })
                .to_string(),
            });
        }
        let mut batch = CommitBatch {
            packs: built_packs
                .iter()
                .map(|(id, data, _)| PackObject {
                    id: id.clone(),
                    data: data.clone(),
                })
                .collect(),
            indexes: built_packs
                .iter()
                .map(|(id, _, index)| PackIndexObject {
                    id: id.clone(),
                    data: serde_json::to_vec(index).unwrap_or_default(),
                })
                .collect(),
            ..CommitBatch::default()
        };
        let mut committed_local = Vec::new();
        for operation in uploads {
            let key = operation.key().clone();
            if failed_upload_keys.contains(&key) {
                continue;
            }
            let parents = state.heads(&key);
            let counter = Self::next_counter(state);
            let change = match operation {
                PlanOperation::PublishDelete { .. } => ResourceChange::Delete,
                PlanOperation::Upload { .. } => {
                    let content = &contents[&key];
                    match content.kind {
                        ResourceKind::Binary => ResourceChange::BinarySnapshot {
                            content_id: blake3::hash(&content.data).to_hex().to_string(),
                            size: content.data.len() as u64,
                            chunk_hashes: manifests.remove(&key).unwrap_or_default(),
                        },
                        ResourceKind::Text => {
                            let mut doc = if merged_texts.contains_key(&key) {
                                state.text_doc(&key)?
                            } else {
                                state
                                    .baselines
                                    .get(&key.encoded())
                                    .and_then(|_| state.text_states.get(&key.encoded()))
                                    .map_or_else(
                                        || Ok(TextDoc::new()),
                                        |full| TextDoc::from_baseline_and_deltas(Some(full), &[]),
                                    )?
                            };
                            let data = merged_texts.get(&key).unwrap_or(&content.data);
                            let text = std::str::from_utf8(data).map_err(serde_error)?;
                            ResourceChange::TextDelta {
                                delta: doc.set_text(text),
                            }
                        }
                    }
                }
                _ => continue,
            };
            let commit = Commit::create(
                key.clone(),
                parents,
                self.client_id.clone(),
                counter,
                change,
            );
            committed_local.push((key, commit.clone()));
            batch.commits.push(commit);
        }
        if !batch.commits.is_empty() || !batch.packs.is_empty() {
            self.emit_progress(
                &mut events,
                EngineEvent {
                    run_id: run_id.into(),
                    operation_id: format!("{run_id}:upload"),
                    stage: "upload.transfer".into(),
                    resource: None,
                    detail_json: serde_json::json!({
                        "commits": batch.commits.len(),
                        "packs": batch.packs.len(),
                        "bytes": batch.packs.iter().map(|pack| pack.data.len() as u64).sum::<u64>(),
                    })
                    .to_string(),
                },
            );
            self.remote.commit_batch(batch)?;
            for (key, commit) in committed_local {
                state.commits.insert(commit.id.clone(), commit.clone());
                self.emit_progress(
                    &mut events,
                    EngineEvent {
                        run_id: run_id.into(),
                        operation_id: commit.id.clone(),
                        stage: "commit.catalog".into(),
                        resource: Some(key.clone()),
                        detail_json: serde_json::json!({
                            "commit": &commit.id,
                            "parents": commit.parents.len(),
                        })
                        .to_string(),
                    },
                );
                let heads = state.heads(&key);
                match &commit.change {
                    ResourceChange::Delete => {
                        Self::set_deleted_baseline(state, key.clone(), heads, run_id.into());
                        report.deleted_remote += 1;
                    }
                    ResourceChange::BinarySnapshot { content_id, .. } => {
                        let local_resource = local_by_key[&key];
                        Self::set_present_baseline(
                            state,
                            key.clone(),
                            content_id.clone(),
                            local_resource
                                .state
                                .version_token()
                                .unwrap_or_default()
                                .to_owned(),
                            heads,
                            run_id.into(),
                        );
                        report.uploaded += 1;
                    }
                    ResourceChange::TextDelta { .. } => {
                        let merged = state.text_doc(&key)?;
                        let bytes = merged.content().into_bytes();
                        let current = local_by_key[&key];
                        let results =
                            self.local
                                .apply_mutations(vec![LocalMutation::WritePresent {
                                    key: key.clone(),
                                    data: bytes.clone(),
                                    precondition: Self::precondition(&current.state),
                                }])?;
                        if let Some(result) = results
                            .first()
                            .filter(|result| result.status == ApplyStatus::Applied)
                        {
                            state
                                .text_states
                                .insert(key.encoded(), merged.full_update());
                            Self::set_present_baseline(
                                state,
                                key.clone(),
                                blake3::hash(&bytes).to_hex().to_string(),
                                result.version_token.clone().unwrap_or_default(),
                                heads,
                                run_id.into(),
                            );
                            report.uploaded += 1;
                        }
                    }
                }
            }
            state.save(self.store.as_ref())?;
        }

        for operation in &plan.operations {
            let key = operation.key().clone();
            let outcome: Result<(), SyncError> = (|| match operation {
                PlanOperation::Noop { .. } => {
                    report.unchanged += 1;
                    Ok(())
                }
                PlanOperation::EstablishBaseline {
                    local,
                    remote_heads,
                    ..
                } => {
                    if state.kinds.get(&key.encoded()) == Some(&ResourceKind::Text) {
                        let document = state.text_doc(&key)?;
                        state
                            .text_states
                            .insert(key.encoded(), document.full_update());
                    }
                    Self::set_present_baseline(
                        state,
                        key,
                        local.content_id().unwrap_or_default().to_owned(),
                        local.version_token().unwrap_or_default().to_owned(),
                        remote_heads.clone(),
                        run_id.into(),
                    );
                    report.unchanged += 1;
                    Ok(())
                }
                PlanOperation::EstablishDeletedBaseline { remote_heads, .. } => {
                    Self::set_deleted_baseline(state, key, remote_heads.clone(), run_id.into());
                    report.unchanged += 1;
                    Ok(())
                }
                PlanOperation::Download { remote, .. } => {
                    let selected = remote.heads().into_iter().next();
                    if let Some(commit) = selected.as_ref().and_then(|head| state.commits.get(head))
                        && let ResourceChange::BinarySnapshot {
                            size, chunk_hashes, ..
                        } = &commit.change
                    {
                        let locations = self.remote.lookup_chunks(chunk_hashes.clone())?;
                        let mut packs = BTreeMap::<String, (u64, u64)>::new();
                        for location in locations {
                            let entry = packs.entry(location.pack_id).or_default();
                            entry.0 += 1;
                            entry.1 += u64::from(location.length);
                        }
                        self.emit_progress(&mut events, EngineEvent {
                            run_id: run_id.into(),
                            operation_id: format!("{run_id}:download"),
                            stage: "download.plan".into(),
                            resource: Some(key.clone()),
                            detail_json: serde_json::json!({
                                "bytes": size,
                                "chunks": chunk_hashes.len(),
                                "packs": packs.iter().map(|(id, (ranges, bytes))| serde_json::json!({
                                    "pack": &id[..id.len().min(12)],
                                    "ranges": ranges,
                                    "bytes": bytes,
                                })).collect::<Vec<_>>(),
                            })
                            .to_string(),
                        });
                    }
                    let bytes = self.content_for_remote(state, &key, None)?;
                    let local_state = local_by_key
                        .get(&key)
                        .map_or(ReplicaState::Missing, |resource| resource.state.clone());
                    let result = self
                        .local
                        .apply_mutations(vec![LocalMutation::WritePresent {
                            key: key.clone(),
                            data: bytes.clone(),
                            precondition: Self::precondition(&local_state),
                        }])?
                        .into_iter()
                        .next()
                        .ok_or_else(|| SyncError::IoError {
                            msg: "local replica returned no apply result".into(),
                        })?;
                    if result.status != ApplyStatus::Applied {
                        Err(SyncError::IoError {
                            msg: result
                                .error
                                .unwrap_or_else(|| "local precondition failed".into()),
                        })
                    } else {
                        if selected
                            .as_ref()
                            .and_then(|head| state.commits.get(head))
                            .is_some_and(|commit| {
                                matches!(commit.change, ResourceChange::TextDelta { .. })
                            })
                        {
                            let document = state.text_doc(&key)?;
                            state
                                .text_states
                                .insert(key.encoded(), document.full_update());
                            state.kinds.insert(key.encoded(), ResourceKind::Text);
                        }
                        Self::set_present_baseline(
                            state,
                            key,
                            remote.content_id().unwrap_or_default().to_owned(),
                            result.version_token.unwrap_or_default(),
                            remote.heads(),
                            run_id.into(),
                        );
                        report.downloaded += 1;
                        self.emit_progress(
                            &mut events,
                            EngineEvent {
                                run_id: run_id.into(),
                                operation_id: format!("{run_id}:download"),
                                stage: "download.write".into(),
                                resource: Some(operation.key().clone()),
                                detail_json: serde_json::json!({"bytes": bytes.len()}).to_string(),
                            },
                        );
                        Ok(())
                    }
                }
                PlanOperation::ApplyDelete {
                    local,
                    remote_heads,
                    ..
                } => {
                    let result = self
                        .local
                        .apply_mutations(vec![LocalMutation::ApplyDelete {
                            key: key.clone(),
                            precondition: Self::precondition(local),
                        }])?
                        .into_iter()
                        .next()
                        .ok_or_else(|| SyncError::IoError {
                            msg: "local replica returned no delete result".into(),
                        })?;
                    if result.status != ApplyStatus::Applied {
                        Err(SyncError::IoError {
                            msg: result
                                .error
                                .unwrap_or_else(|| "local delete precondition failed".into()),
                        })
                    } else {
                        Self::set_deleted_baseline(state, key, remote_heads.clone(), run_id.into());
                        report.deleted_local += 1;
                        self.emit_progress(
                            &mut events,
                            EngineEvent {
                                run_id: run_id.into(),
                                operation_id: format!("{run_id}:delete"),
                                stage: "delete.apply".into(),
                                resource: Some(operation.key().clone()),
                                detail_json: "{}".into(),
                            },
                        );
                        Ok(())
                    }
                }
                PlanOperation::Upload { .. }
                | PlanOperation::PublishDelete { .. }
                | PlanOperation::Conflict { .. } => Ok(()),
            })();
            if let Err(error) = outcome {
                *failures
                    .entry(operation.key().scope_id.clone())
                    .or_default() += 1;
                report.failures += 1;
                self.emit_progress(
                    &mut events,
                    EngineEvent {
                        run_id: run_id.into(),
                        operation_id: run_id.into(),
                        stage: "error".into(),
                        resource: Some(operation.key().clone()),
                        detail_json: serde_json::json!({"error": error.to_string()}).to_string(),
                    },
                );
            }
            state.save(self.store.as_ref())?;
        }

        let acknowledgements: Vec<_> = plan
            .operations
            .iter()
            .filter(|operation| !matches!(operation, PlanOperation::Conflict { .. }))
            .map(|operation| ResourceAck {
                client_id: self.client_id.clone(),
                resource: operation.key().clone(),
                observed_heads: state.heads(operation.key()),
                pinned_commits: Vec::new(),
            })
            .collect();
        self.remote.write_acknowledgements(acknowledgements)?;
        state.active_run = None;
        state.save(self.store.as_ref())?;
        report.scopes = Self::report_scopes(request, plan, &failures);
        self.emit_progress(
            &mut events,
            EngineEvent {
                run_id: run_id.into(),
                operation_id: run_id.into(),
                stage: "run.complete".into(),
                resource: None,
                detail_json: serde_json::json!({
                    "uploaded": report.uploaded,
                    "downloaded": report.downloaded,
                    "deleted_remote": report.deleted_remote,
                    "deleted_local": report.deleted_local,
                    "unchanged": report.unchanged,
                    "conflicts": report.conflicts,
                    "failures": report.failures,
                })
                .to_string(),
            },
        );
        Ok((report, events))
    }
}

#[uniffi::export]
impl SyncRuntime {
    pub fn preview(&self, request: SyncRequest) -> Result<SyncPlan, SyncError> {
        let mut state = self
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .clone();
        self.refresh_remote(&mut state, &request.scopes)?;
        let local = self.local_inventory(&state, &request.scopes)?;
        self.build_plan(&state, &request.scopes, &local)
    }

    pub fn reconcile(&self, request: SyncRequest) -> Result<SyncReport, SyncError> {
        let run_id = format!("{}-{}", self.client_id, now_millis());
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        let recovered = state.active_run.take();
        self.emit(vec![EngineEvent {
            run_id: run_id.clone(),
            operation_id: run_id.clone(),
            stage: "remote.scan.start".into(),
            resource: None,
            detail_json: serde_json::json!({"scopes": request.scopes.len()}).to_string(),
        }]);
        self.refresh_remote(&mut state, &request.scopes)?;
        self.emit(vec![EngineEvent {
            run_id: run_id.clone(),
            operation_id: run_id.clone(),
            stage: "remote.scan.complete".into(),
            resource: None,
            detail_json: serde_json::json!({"commits": state.commits.len()}).to_string(),
        }]);
        self.snapshot_large_catalogs(&mut state)?;
        self.emit(vec![EngineEvent {
            run_id: run_id.clone(),
            operation_id: run_id.clone(),
            stage: "local.scan.start".into(),
            resource: None,
            detail_json: "{}".into(),
        }]);
        let local = self.local_inventory(&state, &request.scopes)?;
        self.emit(vec![EngineEvent {
            run_id: run_id.clone(),
            operation_id: run_id.clone(),
            stage: "local.scan.complete".into(),
            resource: None,
            detail_json: serde_json::json!({"resources": local.len()}).to_string(),
        }]);
        for resource in &local {
            state.kinds.insert(resource.key.encoded(), resource.kind);
        }
        let plan = self.build_plan(&state, &request.scopes, &local)?;
        self.emit(vec![EngineEvent {
            run_id: run_id.clone(),
            operation_id: run_id.clone(),
            stage: "plan.complete".into(),
            resource: None,
            detail_json: serde_json::json!({"operations": plan.operations.len()}).to_string(),
        }]);
        if let Some(previous) = recovered {
            self.emit(vec![EngineEvent {
                run_id: run_id.clone(),
                operation_id: previous.id,
                stage: "recovery".into(),
                resource: None,
                detail_json: "{}".into(),
            }]);
        }
        let (report, _events) = self.execute(&request, &mut state, &local, &plan, &run_id)?;
        drop(state);
        Ok(report)
    }

    pub fn list_conflicts(&self, query: ConflictQuery) -> Vec<ConflictRecord> {
        self.state
            .lock()
            .expect("runtime state lock poisoned")
            .conflicts
            .values()
            .filter(|conflict| {
                query
                    .scope_id
                    .as_ref()
                    .is_none_or(|scope| scope == &conflict.resource.scope_id)
            })
            .cloned()
            .collect()
    }

    pub fn history(&self, resource: ResourceKey) -> Vec<VersionRecord> {
        let state = self.state.lock().expect("runtime state lock poisoned");
        let mut history: Vec<_> = state
            .resource_commits(&resource)
            .into_iter()
            .map(|commit| VersionRecord {
                commit_id: commit.id.clone(),
                parents: commit.parents.clone(),
                timestamp: commit.timestamp,
                author: commit.author.clone(),
                deleted: matches!(commit.change, ResourceChange::Delete),
            })
            .collect();
        history.sort_by_key(|record| record.timestamp);
        history
    }

    pub fn resolve(&self, request: ResolveConflictRequest) -> Result<SyncReport, SyncError> {
        let scope = {
            let mut state = self.state.lock().expect("runtime state lock poisoned");
            let record = state
                .conflicts
                .get(&request.conflict_id)
                .cloned()
                .ok_or_else(|| SyncError::IoError {
                    msg: "conflict no longer exists".into(),
                })?;
            let current_heads = state.heads(&record.resource);
            if current_heads != record.remote_heads {
                state.conflicts.remove(&request.conflict_id);
                state.save(self.store.as_ref())?;
                return Err(SyncError::ConflictNeedResolution);
            }
            let local =
                self.local_inventory(&state, std::slice::from_ref(&record.resource.scope_id))?;
            let current = local
                .iter()
                .find(|resource| resource.key == record.resource);
            let current_state =
                current.map_or(ReplicaState::Missing, |resource| resource.state.clone());
            if current_state.version_token() != record.local.version_token()
                || current_state.content_id() != record.local.content_id()
            {
                state.conflicts.remove(&request.conflict_id);
                state.save(self.store.as_ref())?;
                return Err(SyncError::ConflictNeedResolution);
            }

            for preserved in &request.preserved {
                let data = match &preserved.source {
                    VersionChoice::Local => {
                        self.local
                            .read_resources(vec![record.resource.clone()])?
                            .into_iter()
                            .next()
                            .ok_or_else(|| SyncError::IoError {
                                msg: "local conflict side disappeared".into(),
                            })?
                            .data
                    }
                    VersionChoice::Remote { commit_id } => {
                        self.content_for_remote(&state, &record.resource, Some(commit_id))?
                    }
                    VersionChoice::Delete => {
                        return Err(SyncError::IoError {
                            msg: "a deletion cannot be preserved as a copy".into(),
                        });
                    }
                };
                let result = self
                    .local
                    .apply_mutations(vec![LocalMutation::CreateCopy {
                        key: preserved.target.clone(),
                        data,
                    }])?
                    .into_iter()
                    .next()
                    .ok_or_else(|| SyncError::IoError {
                        msg: "local replica returned no copy result".into(),
                    })?;
                if result.status != ApplyStatus::Applied {
                    return Err(SyncError::IoError {
                        msg: result
                            .error
                            .unwrap_or_else(|| "copy target already exists".into()),
                    });
                }
            }

            let key = record.resource.clone();
            match &request.primary {
                VersionChoice::Local => {
                    let content = self
                        .local
                        .read_resources(vec![key.clone()])?
                        .into_iter()
                        .next()
                        .ok_or_else(|| SyncError::IoError {
                            msg: "local conflict side disappeared".into(),
                        })?;
                    let commit = self.publish_bytes(
                        &mut state,
                        key.clone(),
                        content.kind,
                        content.data.clone(),
                        current_heads,
                    )?;
                    let content_id = blake3::hash(&content.data).to_hex().to_string();
                    Self::set_present_baseline(
                        &mut state,
                        key.clone(),
                        content_id,
                        content.version_token,
                        vec![commit.id],
                        request.conflict_id.clone(),
                    );
                }
                VersionChoice::Delete => {
                    if !matches!(
                        current_state,
                        ReplicaState::Missing | ReplicaState::Deleted { .. }
                    ) {
                        let result = self
                            .local
                            .apply_mutations(vec![LocalMutation::ApplyDelete {
                                key: key.clone(),
                                precondition: Self::precondition(&current_state),
                            }])?
                            .into_iter()
                            .next()
                            .ok_or_else(|| SyncError::IoError {
                                msg: "local replica returned no delete result".into(),
                            })?;
                        if result.status != ApplyStatus::Applied {
                            return Err(SyncError::IoError {
                                msg: result
                                    .error
                                    .unwrap_or_else(|| "delete precondition failed".into()),
                            });
                        }
                    }
                    let commit = self.publish_delete(&mut state, key.clone(), current_heads)?;
                    Self::set_deleted_baseline(
                        &mut state,
                        key.clone(),
                        vec![commit.id],
                        request.conflict_id.clone(),
                    );
                }
                VersionChoice::Remote { commit_id } => {
                    if !record.remote_heads.contains(commit_id) {
                        return Err(SyncError::IoError {
                            msg: "selected remote version is no longer a head".into(),
                        });
                    }
                    let selected = state.commits.get(commit_id).cloned().ok_or_else(|| {
                        SyncError::IoError {
                            msg: "selected remote commit is unavailable".into(),
                        }
                    })?;
                    match selected.change {
                        ResourceChange::Delete => {
                            if !matches!(
                                current_state,
                                ReplicaState::Missing | ReplicaState::Deleted { .. }
                            ) {
                                let result = self
                                    .local
                                    .apply_mutations(vec![LocalMutation::ApplyDelete {
                                        key: key.clone(),
                                        precondition: Self::precondition(&current_state),
                                    }])?
                                    .into_iter()
                                    .next()
                                    .ok_or_else(|| SyncError::IoError {
                                        msg: "local replica returned no delete result".into(),
                                    })?;
                                if result.status != ApplyStatus::Applied {
                                    return Err(SyncError::IoError {
                                        msg: result
                                            .error
                                            .unwrap_or_else(|| "delete precondition failed".into()),
                                    });
                                }
                            }
                            let commit =
                                self.publish_delete(&mut state, key.clone(), current_heads)?;
                            Self::set_deleted_baseline(
                                &mut state,
                                key.clone(),
                                vec![commit.id],
                                request.conflict_id.clone(),
                            );
                        }
                        change => {
                            let data = self.content_for_remote(&state, &key, Some(commit_id))?;
                            let kind = if matches!(change, ResourceChange::TextDelta { .. }) {
                                ResourceKind::Text
                            } else {
                                ResourceKind::Binary
                            };
                            let result = self
                                .local
                                .apply_mutations(vec![LocalMutation::WritePresent {
                                    key: key.clone(),
                                    data: data.clone(),
                                    precondition: Self::precondition(&current_state),
                                }])?
                                .into_iter()
                                .next()
                                .ok_or_else(|| SyncError::IoError {
                                    msg: "local replica returned no write result".into(),
                                })?;
                            if result.status != ApplyStatus::Applied {
                                return Err(SyncError::IoError {
                                    msg: result
                                        .error
                                        .unwrap_or_else(|| "write precondition failed".into()),
                                });
                            }
                            let commit = self.publish_bytes(
                                &mut state,
                                key.clone(),
                                kind,
                                data.clone(),
                                current_heads,
                            )?;
                            Self::set_present_baseline(
                                &mut state,
                                key.clone(),
                                blake3::hash(&data).to_hex().to_string(),
                                result.version_token.unwrap_or_default(),
                                vec![commit.id],
                                request.conflict_id.clone(),
                            );
                        }
                    }
                }
            }
            state.conflicts.remove(&request.conflict_id);
            state.save(self.store.as_ref())?;
            record.resource.scope_id
        };
        self.reconcile(SyncRequest {
            scopes: vec![scope],
        })
    }

    pub fn rollback(&self, request: RollbackRequest) -> Result<SyncReport, SyncError> {
        let scope = request.resource.scope_id.clone();
        {
            let mut state = self.state.lock().expect("runtime state lock poisoned");
            let selected = state
                .commits
                .get(&request.commit_id)
                .cloned()
                .filter(|commit| commit.resource == request.resource)
                .ok_or_else(|| SyncError::IoError {
                    msg: "rollback version is unavailable".into(),
                })?;
            let local = self.local_inventory(&state, std::slice::from_ref(&scope))?;
            let current = local
                .iter()
                .find(|resource| resource.key == request.resource)
                .map_or(ReplicaState::Missing, |resource| resource.state.clone());
            let parents = state.heads(&request.resource);
            match selected.change {
                ResourceChange::Delete => {
                    if !matches!(
                        current,
                        ReplicaState::Missing | ReplicaState::Deleted { .. }
                    ) {
                        let result = self
                            .local
                            .apply_mutations(vec![LocalMutation::ApplyDelete {
                                key: request.resource.clone(),
                                precondition: Self::precondition(&current),
                            }])?
                            .into_iter()
                            .next()
                            .ok_or_else(|| SyncError::IoError {
                                msg: "local replica returned no rollback result".into(),
                            })?;
                        if result.status != ApplyStatus::Applied {
                            return Err(SyncError::IoError {
                                msg: "rollback delete precondition failed".into(),
                            });
                        }
                    }
                    let commit =
                        self.publish_delete(&mut state, request.resource.clone(), parents)?;
                    Self::set_deleted_baseline(
                        &mut state,
                        request.resource.clone(),
                        vec![commit.id],
                        "rollback".into(),
                    );
                }
                change => {
                    let data = self.content_for_remote(
                        &state,
                        &request.resource,
                        Some(&request.commit_id),
                    )?;
                    let kind = if matches!(change, ResourceChange::TextDelta { .. }) {
                        ResourceKind::Text
                    } else {
                        ResourceKind::Binary
                    };
                    let result = self
                        .local
                        .apply_mutations(vec![LocalMutation::WritePresent {
                            key: request.resource.clone(),
                            data: data.clone(),
                            precondition: Self::precondition(&current),
                        }])?
                        .into_iter()
                        .next()
                        .ok_or_else(|| SyncError::IoError {
                            msg: "local replica returned no rollback result".into(),
                        })?;
                    if result.status != ApplyStatus::Applied {
                        return Err(SyncError::IoError {
                            msg: "rollback write precondition failed".into(),
                        });
                    }
                    let commit = self.publish_bytes(
                        &mut state,
                        request.resource.clone(),
                        kind,
                        data.clone(),
                        parents,
                    )?;
                    Self::set_present_baseline(
                        &mut state,
                        request.resource.clone(),
                        blake3::hash(&data).to_hex().to_string(),
                        result.version_token.unwrap_or_default(),
                        vec![commit.id],
                        "rollback".into(),
                    );
                }
            }
            state.save(self.store.as_ref())?;
        }
        self.reconcile(SyncRequest {
            scopes: vec![scope],
        })
    }

    pub fn maintain(&self, request: MaintenanceRequest) -> Result<V2MaintenanceReport, SyncError> {
        if request.history_retention_versions == 0 {
            return Ok(V2MaintenanceReport::default());
        }
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        // A global incremental refresh is required before computing the live
        // pack set. Cursors keep this proportional to new catalog segments.
        self.refresh_remote(&mut state, &[])?;
        let scopes: HashSet<_> = request.scopes.into_iter().collect();
        let keys = state.remote_keys(&scopes);
        let mut report = V2MaintenanceReport {
            resources: keys.len() as u64,
            ..V2MaintenanceReport::default()
        };
        let acknowledgements = self
            .remote
            .list_acknowledgements(keys.iter().cloned().collect())?;
        let mut obsolete_by_scope = BTreeMap::<String, BTreeSet<String>>::new();

        for key in &keys {
            let commits = state.resource_commits(key);
            if commits.len() as u64 <= request.history_retention_versions {
                state.checkpoints.remove(&key.encoded());
                continue;
            }
            let heads = state.heads(key);
            let conflicted = heads.len() != 1
                || state
                    .conflicts
                    .values()
                    .any(|conflict| conflict.resource == *key)
                || state.active_run.as_ref().is_some_and(|run| {
                    run.operations.iter().any(|operation| operation.key == *key)
                });
            let kind = state.kinds.get(&key.encoded()).copied().unwrap_or(
                match commits.last().map(|commit| &commit.change) {
                    Some(ResourceChange::TextDelta { .. }) => ResourceKind::Text,
                    _ => ResourceKind::Binary,
                },
            );
            // Text rollback currently depends on its causal updates. Do not
            // reclaim it until retained-version checkpoint materialization is
            // available; binary snapshots and tombstones are self-contained.
            if conflicted || kind == ResourceKind::Text {
                report.deferred += 1;
                continue;
            }

            let checkpoint = state.checkpoints.get(&key.encoded()).cloned();
            let Some(checkpoint) = checkpoint else {
                let head = heads[0].clone();
                let selected =
                    state
                        .commits
                        .get(&head)
                        .cloned()
                        .ok_or_else(|| SyncError::IoError {
                            msg: format!("missing head commit {head}"),
                        })?;
                let checkpoint_commit = match selected.change {
                    ResourceChange::Delete => {
                        self.publish_delete(&mut state, key.clone(), vec![head])?
                    }
                    _ => {
                        let content = self.content_for_remote(&state, key, Some(&head))?;
                        self.publish_bytes(
                            &mut state,
                            key.clone(),
                            ResourceKind::Binary,
                            content,
                            vec![head],
                        )?
                    }
                };
                state.checkpoints.insert(
                    key.encoded(),
                    StoredCheckpoint {
                        key: key.clone(),
                        commit_id: checkpoint_commit.id.clone(),
                    },
                );
                self.remote.write_acknowledgements(vec![ResourceAck {
                    client_id: self.client_id.clone(),
                    resource: key.clone(),
                    observed_heads: vec![checkpoint_commit.id],
                    pinned_commits: Vec::new(),
                }])?;
                report.deferred += 1;
                continue;
            };

            let active_clients: BTreeSet<_> = state
                .resource_commits(key)
                .into_iter()
                .map(|commit| commit.author.clone())
                .filter(|client| !state.retired_clients.contains(client))
                .chain(std::iter::once(self.client_id.clone()))
                .collect();
            let all_acknowledged = active_clients.iter().all(|client| {
                acknowledgements.iter().any(|ack| {
                    ack.client_id == *client
                        && ack.resource == *key
                        && ack
                            .observed_heads
                            .iter()
                            .any(|head| state.is_descendant(head, &checkpoint.commit_id))
                })
            });
            if !all_acknowledged {
                report.deferred += 1;
                continue;
            }

            let mut ordered: Vec<_> = state
                .resource_commits(key)
                .into_iter()
                .map(|commit| (commit.timestamp, commit.id.clone()))
                .collect();
            ordered.sort_by(|left, right| right.cmp(left));
            let mut retained: BTreeSet<_> = ordered
                .iter()
                .take(request.history_retention_versions as usize)
                .map(|(_, id)| id.clone())
                .collect();
            retained.insert(checkpoint.commit_id.clone());
            for ack in acknowledgements.iter().filter(|ack| ack.resource == *key) {
                retained.extend(ack.pinned_commits.iter().cloned());
            }
            let obsolete = state
                .resource_commits(key)
                .into_iter()
                .filter(|commit| !retained.contains(&commit.id))
                .map(|commit| commit.id.clone())
                .collect::<Vec<_>>();
            if obsolete.is_empty() {
                state.checkpoints.remove(&key.encoded());
                continue;
            }
            obsolete_by_scope
                .entry(key.scope_id.clone())
                .or_default()
                .extend(obsolete);
        }

        for (scope, obsolete) in &obsolete_by_scope {
            let retained: Vec<_> = state
                .commits
                .values()
                .filter(|commit| {
                    commit.resource.scope_id == *scope && !obsolete.contains(&commit.id)
                })
                .collect();
            let events = retained
                .iter()
                .map(|commit| CatalogEvent {
                    client_id: commit.author.clone(),
                    counter: commit.client_counter,
                    commit_id: commit.id.clone(),
                    resource: commit.resource.clone(),
                })
                .collect();
            let cursors = state
                .cursors
                .iter()
                .filter(|cursor| cursor.scope_id == *scope)
                .cloned()
                .collect();
            self.remote.compact_catalog(CatalogCompaction {
                snapshot: CatalogSnapshot {
                    generation: self.snapshot_generation(),
                    scope_id: scope.clone(),
                    events,
                    cursors,
                },
                obsolete_commit_ids: obsolete.iter().cloned().collect(),
            })?;
            for id in obsolete {
                state.commits.remove(id);
                report.commits_deleted += 1;
            }
            state
                .checkpoints
                .retain(|_, checkpoint| checkpoint.key.scope_id != *scope);
        }

        let live_hashes: BTreeSet<_> = state
            .commits
            .values()
            .flat_map(|commit| match &commit.change {
                ResourceChange::BinarySnapshot { chunk_hashes, .. } => chunk_hashes.clone(),
                _ => Vec::new(),
            })
            .collect();
        let live_packs: BTreeSet<_> = self
            .remote
            .lookup_chunks(live_hashes.into_iter().collect())?
            .into_iter()
            .map(|location| location.pack_id)
            .collect();
        let dead_packs: Vec<_> = self
            .remote
            .list_pack_ids()?
            .into_iter()
            .filter(|pack| !live_packs.contains(pack))
            .collect();
        if !dead_packs.is_empty() {
            report.packs_deleted = dead_packs.len() as u64;
            self.remote.delete_pack_objects(dead_packs)?;
        }
        state.save(self.store.as_ref())?;
        Ok(report)
    }

    pub fn retire_client(&self, client_id: String) -> Result<(), SyncError> {
        let mut state = self.state.lock().expect("runtime state lock poisoned");
        state.retired_clients.insert(client_id);
        state.save(self.store.as_ref())
    }

    pub fn close(&self) -> Result<(), SyncError> {
        self.store.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{EngineEventListenerV2, LocalReplica};
    use crate::{LocalFolderRemoteV2, RedbRuntimeStore};

    #[derive(Default)]
    struct MemoryReplica {
        files: Mutex<BTreeMap<ResourceKey, (Vec<u8>, u64)>>,
    }
    impl LocalReplica for MemoryReplica {
        fn list_resources(&self, scopes: Vec<String>) -> Result<Vec<LocalResource>, SyncError> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| scopes.is_empty() || scopes.contains(&key.scope_id))
                .map(|(key, (data, version))| LocalResource {
                    key: key.clone(),
                    kind: ResourceKind::Binary,
                    state: ReplicaState::present(
                        blake3::hash(data).to_hex().to_string(),
                        data.len() as u64,
                        version.to_string(),
                    ),
                })
                .collect())
        }
        fn read_resources(
            &self,
            keys: Vec<ResourceKey>,
        ) -> Result<Vec<ResourceContent>, SyncError> {
            let files = self.files.lock().unwrap();
            Ok(keys
                .into_iter()
                .filter_map(|key| {
                    files.get(&key).map(|(data, version)| ResourceContent {
                        key,
                        kind: ResourceKind::Binary,
                        version_token: version.to_string(),
                        data: data.clone(),
                    })
                })
                .collect())
        }
        fn apply_mutations(
            &self,
            mutations: Vec<LocalMutation>,
        ) -> Result<Vec<LocalApplyResult>, SyncError> {
            let mut files = self.files.lock().unwrap();
            Ok(mutations
                .into_iter()
                .map(|mutation| {
                    let key = match &mutation {
                        LocalMutation::WritePresent { key, .. }
                        | LocalMutation::ApplyDelete { key, .. }
                        | LocalMutation::CreateCopy { key, .. } => key.clone(),
                    };
                    let matches = match &mutation {
                        LocalMutation::WritePresent { precondition, .. }
                        | LocalMutation::ApplyDelete { precondition, .. } => match precondition {
                            MutationPrecondition::Missing => !files.contains_key(&key),
                            MutationPrecondition::Version { version_token } => files
                                .get(&key)
                                .is_some_and(|(_, version)| version.to_string() == *version_token),
                        },
                        LocalMutation::CreateCopy { .. } => !files.contains_key(&key),
                    };
                    if !matches {
                        return LocalApplyResult {
                            key,
                            status: ApplyStatus::PreconditionFailed,
                            version_token: None,
                            error: None,
                        };
                    }
                    let version = files.get(&key).map_or(1, |(_, version)| version + 1);
                    match mutation {
                        LocalMutation::WritePresent { data, .. }
                        | LocalMutation::CreateCopy { data, .. } => {
                            files.insert(key.clone(), (data, version));
                        }
                        LocalMutation::ApplyDelete { .. } => {
                            files.remove(&key);
                        }
                    }
                    LocalApplyResult {
                        key,
                        status: ApplyStatus::Applied,
                        version_token: Some(version.to_string()),
                        error: None,
                    }
                })
                .collect())
        }
    }
    struct Events;
    impl EngineEventListenerV2 for Events {
        fn on_events(&self, _: Vec<EngineEvent>) {}
    }

    fn runtime(root: &std::path::Path, client: &str, local: Arc<MemoryReplica>) -> SyncRuntime {
        SyncRuntime::with_backends(
            client.into(),
            Arc::new(RedbRuntimeStore::open(root.join(format!("{client}.redb"))).unwrap()),
            Arc::new(LocalFolderRemoteV2::open(root.join("remote")).unwrap()),
            local,
            Arc::new(Events),
        )
        .unwrap()
    }

    #[test]
    fn two_replicas_upload_then_download() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = ResourceKey::new("scope", "a.bin");
        let first = Arc::new(MemoryReplica::default());
        first
            .files
            .lock()
            .unwrap()
            .insert(key.clone(), (b"hello".to_vec(), 1));
        runtime(dir.path(), "a", first)
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();
        let second = Arc::new(MemoryReplica::default());
        let report = runtime(dir.path(), "b", second.clone())
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();
        assert_eq!(report.downloaded, 1);
        assert_eq!(second.files.lock().unwrap()[&key].0, b"hello");
    }

    #[test]
    fn three_binary_heads_are_preserved_as_a_remote_fork() {
        let key = ResourceKey::new("scope", "fork.bin");
        let mut state = RuntimeState::default();
        for (counter, author, byte) in [(1, "a", b'a'), (1, "b", b'b'), (1, "c", b'c')] {
            let data = vec![byte];
            let commit = Commit::create(
                key.clone(),
                Vec::new(),
                author.into(),
                counter,
                ResourceChange::BinarySnapshot {
                    content_id: blake3::hash(&data).to_hex().to_string(),
                    size: 1,
                    chunk_hashes: vec![blake3::hash(&data).to_hex().to_string()],
                },
            );
            state.commits.insert(commit.id.clone(), commit);
        }
        assert!(
            matches!(state.remote_state(&key).unwrap(), RemoteResourceState::Forked { heads } if heads.len() == 3)
        );
    }

    #[test]
    fn conflict_does_not_block_unrelated_resource_in_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let conflict_key = ResourceKey::new("scope", "conflict.bin");
        let remote_local = Arc::new(MemoryReplica::default());
        remote_local
            .files
            .lock()
            .unwrap()
            .insert(conflict_key.clone(), (b"remote".to_vec(), 1));
        runtime(dir.path(), "a", remote_local)
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();

        let second = Arc::new(MemoryReplica::default());
        second
            .files
            .lock()
            .unwrap()
            .insert(conflict_key, (b"local".to_vec(), 1));
        second
            .files
            .lock()
            .unwrap()
            .insert(ResourceKey::new("scope", "new.bin"), (b"new".to_vec(), 1));
        let report = runtime(dir.path(), "b", second)
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();
        assert_eq!(report.conflicts, 1);
        assert_eq!(report.uploaded, 1);
        assert_eq!(report.scopes[0].status, ScopeStatus::Partial);
    }

    #[test]
    fn maintenance_waits_for_checkpoint_ack_then_compacts_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = ResourceKey::new("scope", "history.bin");
        let local = Arc::new(MemoryReplica::default());
        local
            .files
            .lock()
            .unwrap()
            .insert(key.clone(), (b"v0".to_vec(), 1));
        let engine = runtime(dir.path(), "a", local.clone());
        for version in 0..6u64 {
            local.files.lock().unwrap().insert(
                key.clone(),
                (format!("v{version}").into_bytes(), version + 1),
            );
            engine
                .reconcile(SyncRequest {
                    scopes: vec!["scope".into()],
                })
                .unwrap();
        }

        let first = engine
            .maintain(MaintenanceRequest {
                scopes: vec!["scope".into()],
                history_retention_versions: 2,
            })
            .unwrap();
        assert_eq!(first.commits_deleted, 0);
        assert_eq!(first.deferred, 1);

        engine
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();
        let second = engine
            .maintain(MaintenanceRequest {
                scopes: vec!["scope".into()],
                history_retention_versions: 2,
            })
            .unwrap();
        assert!(second.commits_deleted >= 4);

        let fresh = Arc::new(MemoryReplica::default());
        runtime(dir.path(), "fresh", fresh.clone())
            .reconcile(SyncRequest {
                scopes: vec!["scope".into()],
            })
            .unwrap();
        assert_eq!(fresh.files.lock().unwrap()[&key].0, b"v5");
    }
}
