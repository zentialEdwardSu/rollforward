//! Local-folder reference implementation of the v2 immutable catalog remote.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::adapter::RemoteStorageV2;
use crate::binary::PackIndex;
use crate::types::SyncError;
use crate::v2_types::{
    CatalogCompaction, CatalogCursor, CatalogDelta, CatalogEvent, CatalogScanRequest,
    CatalogSnapshot, ChunkLocation, Commit, CommitBatch, CommitBatchResult, PackRange, RangeData,
    ResourceAck, ResourceKey,
};

fn io(error: impl std::fmt::Display) -> SyncError {
    SyncError::IoError {
        msg: error.to_string(),
    }
}

fn object_name(id: &str) -> String {
    blake3::hash(id.as_bytes()).to_hex().to_string()
}

fn write_immutable(path: &Path, data: &[u8]) -> Result<(), SyncError> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io)?;
    }
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&temp, data).map_err(io)?;
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(_error) if path.exists() => {
            let _ = fs::remove_file(temp);
            Ok(())
        }
        Err(error) => Err(io(error)),
    }
}

fn files_recursive(root: &Path) -> Result<Vec<PathBuf>, SyncError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut pending = vec![root.to_owned()];
    let mut output = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            if entry.file_type().map_err(io)?.is_dir() {
                pending.push(entry.path());
            } else {
                output.push(entry.path());
            }
        }
    }
    Ok(output)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogSegment {
    client_id: String,
    scope_id: String,
    first_counter: u64,
    last_counter: u64,
    events: Vec<CatalogEvent>,
}

#[derive(uniffi::Object)]
pub struct LocalFolderRemoteV2 {
    root: PathBuf,
}

impl LocalFolderRemoteV2 {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, SyncError> {
        let root = root.into().join("_rollforward").join("v2");
        fs::create_dir_all(&root).map_err(io)?;
        Ok(Self { root })
    }

    fn commits(&self) -> PathBuf {
        self.root.join("commits")
    }
    fn packs(&self) -> PathBuf {
        self.root.join("packs")
    }
    fn indexes(&self) -> PathBuf {
        self.root.join("pack-indexes")
    }
    fn segments(&self) -> PathBuf {
        self.root.join("catalog").join("clients")
    }
    fn acknowledgements(&self) -> PathBuf {
        self.root.join("acknowledgements")
    }
    fn snapshots(&self) -> PathBuf {
        self.root.join("catalog").join("snapshots")
    }

    fn load_snapshots(&self, scopes: &HashSet<String>) -> Result<Vec<CatalogSnapshot>, SyncError> {
        let latest_files: Vec<_> = files_recursive(&self.snapshots())?
            .into_iter()
            .filter(|path| path.file_name().is_some_and(|name| name == "latest.json"))
            .collect();
        let mut output = Vec::new();
        for latest in latest_files {
            let generation: u64 =
                serde_json::from_slice(&fs::read(&latest).map_err(io)?).map_err(|error| {
                    SyncError::SerdeError {
                        msg: error.to_string(),
                    }
                })?;
            let bytes = fs::read(
                latest
                    .parent()
                    .expect("snapshot latest has a parent")
                    .join(format!("{generation:020}.json")),
            )
            .map_err(io)?;
            let snapshot: CatalogSnapshot =
                serde_json::from_slice(&bytes).map_err(|error| SyncError::SerdeError {
                    msg: error.to_string(),
                })?;
            if scopes.is_empty() || scopes.contains(&snapshot.scope_id) {
                output.push(snapshot);
            }
        }
        Ok(output)
    }
}

#[uniffi::export]
impl LocalFolderRemoteV2 {
    #[uniffi::constructor]
    pub fn new(root: String) -> Result<std::sync::Arc<Self>, SyncError> {
        Ok(std::sync::Arc::new(Self::open(root)?))
    }
}

impl RemoteStorageV2 for LocalFolderRemoteV2 {
    fn scan_catalog(&self, request: CatalogScanRequest) -> Result<CatalogDelta, SyncError> {
        let cursors: BTreeMap<_, _> = request
            .cursors
            .into_iter()
            .map(|cursor| ((cursor.client_id, cursor.scope_id), cursor.counter))
            .collect();
        let scopes: HashSet<_> = request.scopes.into_iter().collect();
        let mut events = Vec::new();
        let mut next = cursors.clone();
        let mut covered = BTreeMap::new();
        let mut snapshot_cursors = Vec::new();
        for snapshot in self.load_snapshots(&scopes)? {
            for cursor in snapshot.cursors {
                snapshot_cursors.push(cursor.clone());
                let key = (cursor.client_id, cursor.scope_id);
                covered.insert(key.clone(), cursor.counter);
                next.entry(key)
                    .and_modify(|value| *value = (*value).max(cursor.counter))
                    .or_insert(cursor.counter);
            }
            for event in snapshot.events {
                let key = (event.client_id.clone(), event.resource.scope_id.clone());
                if event.counter > cursors.get(&key).copied().unwrap_or(0)
                    && (scopes.is_empty() || scopes.contains(&event.resource.scope_id))
                {
                    events.push(event);
                }
            }
        }
        for path in files_recursive(&self.segments())? {
            let segment: CatalogSegment = serde_json::from_slice(&fs::read(path).map_err(io)?)
                .map_err(|error| SyncError::SerdeError {
                    msg: error.to_string(),
                })?;
            if !scopes.is_empty() && !scopes.contains(&segment.scope_id) {
                continue;
            }
            let cursor_key = (segment.client_id.clone(), segment.scope_id.clone());
            let cursor = cursors
                .get(&cursor_key)
                .copied()
                .unwrap_or(0)
                .max(covered.get(&cursor_key).copied().unwrap_or(0));
            if segment.last_counter <= cursor {
                continue;
            }
            for event in segment.events {
                if event.counter > cursor
                    && (scopes.is_empty() || scopes.contains(&event.resource.scope_id))
                {
                    events.push(event);
                }
            }
            next.entry(cursor_key)
                .and_modify(|value| *value = (*value).max(segment.last_counter))
                .or_insert(segment.last_counter);
        }
        events.sort_by(|left, right| {
            (left.client_id.as_str(), left.counter).cmp(&(right.client_id.as_str(), right.counter))
        });
        Ok(CatalogDelta {
            events,
            cursors: next
                .into_iter()
                .map(|((client_id, scope_id), counter)| CatalogCursor {
                    client_id,
                    scope_id,
                    counter,
                })
                .collect(),
            snapshot_cursors,
        })
    }

    fn load_commits(&self, ids: Vec<String>) -> Result<Vec<Commit>, SyncError> {
        ids.into_iter()
            .map(|id| {
                let bytes = fs::read(self.commits().join(object_name(&id))).map_err(io)?;
                serde_json::from_slice(&bytes).map_err(|error| SyncError::SerdeError {
                    msg: error.to_string(),
                })
            })
            .collect()
    }

    fn commit_batch(&self, batch: CommitBatch) -> Result<CommitBatchResult, SyncError> {
        for pack in batch.packs {
            write_immutable(&self.packs().join(&pack.id), &pack.data)?;
        }
        for index in batch.indexes {
            write_immutable(&self.indexes().join(&index.id), &index.data)?;
        }
        for commit in &batch.commits {
            let bytes = serde_json::to_vec(commit).map_err(|error| SyncError::SerdeError {
                msg: error.to_string(),
            })?;
            write_immutable(&self.commits().join(object_name(&commit.id)), &bytes)?;
        }

        let mut by_client_scope: BTreeMap<(String, String), Vec<&Commit>> = BTreeMap::new();
        for commit in &batch.commits {
            by_client_scope
                .entry((commit.author.clone(), commit.resource.scope_id.clone()))
                .or_default()
                .push(commit);
        }
        for ((client_id, scope_id), mut commits) in by_client_scope {
            commits.sort_by_key(|commit| commit.client_counter);
            let events: Vec<_> = commits
                .iter()
                .map(|commit| CatalogEvent {
                    client_id: client_id.clone(),
                    counter: commit.client_counter,
                    commit_id: commit.id.clone(),
                    resource: commit.resource.clone(),
                })
                .collect();
            let first_counter = events.first().map_or(0, |event| event.counter);
            let last_counter = events.last().map_or(0, |event| event.counter);
            let segment = CatalogSegment {
                client_id: client_id.clone(),
                scope_id: scope_id.clone(),
                first_counter,
                last_counter,
                events,
            };
            let bytes = serde_json::to_vec(&segment).map_err(|error| SyncError::SerdeError {
                msg: error.to_string(),
            })?;
            let digest = blake3::hash(&bytes).to_hex();
            let path = self
                .segments()
                .join(object_name(&client_id))
                .join(object_name(&scope_id))
                .join(format!(
                    "{first_counter:020}-{last_counter:020}-{digest}.json"
                ));
            write_immutable(&path, &bytes)?;
        }
        Ok(CommitBatchResult {
            visible_commits: batch.commits.into_iter().map(|commit| commit.id).collect(),
        })
    }

    fn lookup_chunks(&self, hashes: Vec<String>) -> Result<Vec<ChunkLocation>, SyncError> {
        let wanted: HashSet<_> = hashes.into_iter().collect();
        let mut found = BTreeMap::new();
        for path in files_recursive(&self.indexes())? {
            let index: PackIndex =
                serde_json::from_slice(&fs::read(path).map_err(io)?).map_err(|error| {
                    SyncError::SerdeError {
                        msg: error.to_string(),
                    }
                })?;
            for chunk in index.chunks {
                if wanted.contains(&chunk.hash) {
                    found.entry(chunk.hash.clone()).or_insert(ChunkLocation {
                        hash: chunk.hash,
                        pack_id: index.pack_id.clone(),
                        offset: chunk.offset,
                        length: chunk.length,
                    });
                }
            }
        }
        Ok(found.into_values().collect())
    }

    fn read_ranges(&self, ranges: Vec<PackRange>) -> Result<Vec<RangeData>, SyncError> {
        ranges
            .into_iter()
            .map(|range| {
                let mut file = fs::File::open(self.packs().join(&range.pack_id)).map_err(io)?;
                file.seek(SeekFrom::Start(range.offset)).map_err(io)?;
                let mut data = vec![0; range.length as usize];
                file.read_exact(&mut data).map_err(io)?;
                if blake3::hash(&data).to_hex().as_str() != range.hash {
                    return Err(SyncError::Corrupt { hash: range.hash });
                }
                Ok(RangeData {
                    hash: range.hash,
                    data,
                })
            })
            .collect()
    }

    fn write_acknowledgements(&self, acknowledgements: Vec<ResourceAck>) -> Result<(), SyncError> {
        for ack in acknowledgements {
            let id = format!("{}\0{}", ack.client_id, ack.resource.encoded());
            let path = self.acknowledgements().join(object_name(&id));
            fs::create_dir_all(self.acknowledgements()).map_err(io)?;
            fs::write(
                path,
                serde_json::to_vec(&ack).map_err(|error| SyncError::SerdeError {
                    msg: error.to_string(),
                })?,
            )
            .map_err(io)?;
        }
        Ok(())
    }

    fn list_acknowledgements(
        &self,
        resources: Vec<ResourceKey>,
    ) -> Result<Vec<ResourceAck>, SyncError> {
        let wanted: HashSet<_> = resources.into_iter().collect();
        let mut output = Vec::new();
        for path in files_recursive(&self.acknowledgements())? {
            let ack: ResourceAck =
                serde_json::from_slice(&fs::read(path).map_err(io)?).map_err(|error| {
                    SyncError::SerdeError {
                        msg: error.to_string(),
                    }
                })?;
            if wanted.is_empty() || wanted.contains(&ack.resource) {
                output.push(ack);
            }
        }
        Ok(output)
    }

    fn list_pack_ids(&self) -> Result<Vec<String>, SyncError> {
        Ok(files_recursive(&self.packs())?
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .collect())
    }

    fn delete_pack_objects(&self, pack_ids: Vec<String>) -> Result<(), SyncError> {
        for id in pack_ids {
            let _ = fs::remove_file(self.packs().join(&id));
            let _ = fs::remove_file(self.indexes().join(&id));
        }
        Ok(())
    }

    fn compact_catalog(&self, compaction: CatalogCompaction) -> Result<(), SyncError> {
        let snapshot_dir = self
            .snapshots()
            .join(object_name(&compaction.snapshot.scope_id));
        fs::create_dir_all(&snapshot_dir).map_err(io)?;
        let generation = compaction.snapshot.generation;
        let snapshot_path = snapshot_dir.join(format!("{generation:020}.json"));
        write_immutable(
            &snapshot_path,
            &serde_json::to_vec(&compaction.snapshot).map_err(|error| SyncError::SerdeError {
                msg: error.to_string(),
            })?,
        )?;
        let latest = snapshot_dir.join("latest.json");
        let temporary = snapshot_dir.join(format!("latest-{}.tmp", std::process::id()));
        fs::write(
            &temporary,
            serde_json::to_vec(&generation).map_err(|error| SyncError::SerdeError {
                msg: error.to_string(),
            })?,
        )
        .map_err(io)?;
        if latest.exists() {
            fs::remove_file(&latest).map_err(io)?;
        }
        fs::rename(temporary, latest).map_err(io)?;

        let covered: BTreeMap<_, _> = compaction
            .snapshot
            .cursors
            .iter()
            .map(|cursor| {
                (
                    (cursor.client_id.clone(), cursor.scope_id.clone()),
                    cursor.counter,
                )
            })
            .collect();
        for path in files_recursive(&self.segments())? {
            let segment: CatalogSegment = serde_json::from_slice(&fs::read(&path).map_err(io)?)
                .map_err(|error| SyncError::SerdeError {
                    msg: error.to_string(),
                })?;
            if segment.last_counter
                <= covered
                    .get(&(segment.client_id, segment.scope_id))
                    .copied()
                    .unwrap_or(0)
            {
                fs::remove_file(path).map_err(io)?;
            }
        }
        for id in compaction.obsolete_commit_ids {
            let path = self.commits().join(object_name(&id));
            if path.exists() {
                fs::remove_file(path).map_err(io)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2_types::{Commit, ResourceChange};

    #[test]
    fn segment_is_visibility_barrier_and_cursor_is_incremental() {
        let dir = tempfile::TempDir::new().unwrap();
        let remote = LocalFolderRemoteV2::open(dir.path()).unwrap();
        let commit = Commit::create(
            ResourceKey::new("scope", "a"),
            Vec::new(),
            "client".into(),
            1,
            ResourceChange::Delete,
        );
        remote
            .commit_batch(CommitBatch {
                commits: vec![commit.clone()],
                ..CommitBatch::default()
            })
            .unwrap();
        let first = remote
            .scan_catalog(CatalogScanRequest {
                scopes: vec!["scope".into()],
                cursors: Vec::new(),
            })
            .unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(remote.load_commits(vec![commit.id]).unwrap().len(), 1);
        let second = remote
            .scan_catalog(CatalogScanRequest {
                scopes: vec!["scope".into()],
                cursors: first.cursors,
            })
            .unwrap();
        assert!(second.events.is_empty());
    }
}
