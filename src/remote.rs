//! The dumb-remote abstraction and a local-folder implementation for testing.
//!
//! The remote is "dumb": it stores and lists opaque objects and supports one
//! atomic primitive — a compare-and-swap append ([`RemoteStorage::put_oplog_cas`])
//! used to detect concurrent writers at the same sequence. All higher-level
//! merge/convergence logic lives in the engine.

use std::fs;
use std::path::PathBuf;

use crate::oplog::{OPLOG_EXT, format_oplog_name, parse_oplog_name};
use crate::types::{ClientStatus, OpLogEntry, RemoteLogItem, SyncError};

/// Storage backend for the sync engine. Implementations may be a local folder
/// (testing), or a network drive / S3 / WebDAV in production.
///
/// Exported over FFI (`with_foreign`) so a host language can inject its own
/// backend. uniFFI requires owned argument types (`String`/`Vec<u8>`, not
/// slices) and forbids tuple returns, so signatures use owned values and the
/// [`ClientStatus`] record instead of `(String, u64)`.
#[uniffi::export(with_foreign)]
pub trait RemoteStorage: Send + Sync {
    /// List the ids of every file that has oplog history on the remote. Used by
    /// binary GC to compute a *global* live-chunk set: packs are a shared,
    /// content-addressed namespace across all files, so reclaiming a pack is
    /// only safe once no surviving manifest of *any* file references its chunks.
    fn list_files(&self) -> Result<Vec<String>, SyncError>;
    /// List all oplog objects for a file.
    fn list_oplogs(&self, file_id: String) -> Result<Vec<RemoteLogItem>, SyncError>;
    /// Append an oplog entry unconditionally, overwriting any object with the
    /// same `{sequence}_{client_id}` name.
    fn put_oplog(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError>;
    /// Compare-and-swap append: fail with [`SyncError::Conflict`] if *any*
    /// client already published this sequence. Underpins CAS-retry.
    fn put_oplog_cas(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError>;
    /// Fetch the raw bytes of an oplog object by its remote-relative path.
    fn get_oplog(&self, file_id: String, remote_path: String) -> Result<Vec<u8>, SyncError>;
    /// Delete an oplog object.
    fn delete_oplog(&self, file_id: String, remote_path: String) -> Result<(), SyncError>;

    /// Store a pack object (many concatenated chunks) under its content address.
    /// Idempotent: writing an existing pack id is a no-op success.
    fn put_pack(&self, pack_id: String, data: Vec<u8>) -> Result<(), SyncError>;
    /// Fetch a byte range `[offset, offset+length)` from a pack — the chunk's
    /// bytes, located via a pack index.
    fn get_pack_range(
        &self,
        pack_id: String,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, SyncError>;
    /// List all stored pack ids (for garbage collection / repack).
    fn list_packs(&self) -> Result<Vec<String>, SyncError>;
    /// Delete a pack object by id.
    fn delete_pack(&self, pack_id: String) -> Result<(), SyncError>;

    /// Store a pack index object (the `hash -> offset/length` map for one pack).
    /// Idempotent by id (`index_id == pack_id`).
    fn put_pack_index(&self, index_id: String, data: Vec<u8>) -> Result<(), SyncError>;
    /// Fetch a pack index's bytes by id.
    fn get_pack_index(&self, index_id: String) -> Result<Vec<u8>, SyncError>;
    /// List all stored pack index ids.
    fn list_pack_indexes(&self) -> Result<Vec<String>, SyncError>;
    /// Delete a pack index object by id.
    fn delete_pack_index(&self, index_id: String) -> Result<(), SyncError>;

    /// Store a compressed baseline snapshot for a file at a given sequence.
    fn put_baseline(&self, file_id: String, seq: u64, data: Vec<u8>) -> Result<(), SyncError>;
    /// Fetch a baseline snapshot, if present.
    fn get_baseline(&self, file_id: String, seq: u64) -> Result<Option<Vec<u8>>, SyncError>;
    /// List available baseline sequences for a file, ascending.
    fn list_baselines(&self, file_id: String) -> Result<Vec<u64>, SyncError>;

    /// Publish this client's synced-progress marker.
    fn put_status(&self, client_id: String, last_synced_sequence: u64) -> Result<(), SyncError>;
    /// Read every client's progress marker.
    fn list_statuses(&self) -> Result<Vec<ClientStatus>, SyncError>;
}

/// A [`RemoteStorage`] backed by a local directory tree — the dumb remote used
/// in tests. Layout:
///
/// ```text
/// <root>/<file_id>/oplogs/{seq}_{client}.oplog
/// <root>/<file_id>/baselines/baseline_<seq>.zst
/// <root>/packs/<pack_id>
/// <root>/pack-indexes/<pack_id>
/// <root>/clients_status/<client>.status
/// ```
#[derive(uniffi::Object)]
pub struct LocalFolderRemote {
    /// Root of the remote directory tree.
    root: PathBuf,
}

/// Map any `io::Error` into a [`SyncError::IoError`].
fn io<E: std::fmt::Display>(e: E) -> SyncError {
    SyncError::IoError { msg: e.to_string() }
}

impl LocalFolderRemote {
    /// Open (creating if needed) a local-folder remote rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, SyncError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(io)?;
        Ok(Self { root })
    }

    /// `<root>/<file_id>/oplogs`
    fn oplog_dir(&self, file_id: &str) -> PathBuf {
        self.root.join(file_id).join("oplogs")
    }

    /// `<root>/<file_id>/baselines`
    fn baseline_dir(&self, file_id: &str) -> PathBuf {
        self.root.join(file_id).join("baselines")
    }

    /// `<root>/packs`
    fn pack_dir(&self) -> PathBuf {
        self.root.join("packs")
    }

    /// `<root>/pack-indexes`
    fn pack_index_dir(&self) -> PathBuf {
        self.root.join("pack-indexes")
    }

    /// `<root>/clients_status`
    fn status_dir(&self) -> PathBuf {
        self.root.join("clients_status")
    }

    /// Serialize and write an oplog entry to its object path.
    fn write_oplog(&self, file_id: &str, entry: &OpLogEntry) -> Result<(), SyncError> {
        let dir = self.oplog_dir(file_id);
        fs::create_dir_all(&dir).map_err(io)?;
        let bytes =
            serde_json::to_vec(entry).map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
        let path = dir.join(format_oplog_name(entry.sequence, &entry.client_id));
        fs::write(path, bytes).map_err(io)
    }
}

impl RemoteStorage for LocalFolderRemote {
    fn list_files(&self) -> Result<Vec<String>, SyncError> {
        // Every top-level dir is a file id except the three reserved global
        // stores. A dir counts as a file only if it actually has an `oplogs`
        // subdir, so a stray/empty dir never masquerades as a file.
        const RESERVED: [&str; 3] = ["packs", "pack-indexes", "clients_status"];
        let mut out = Vec::new();
        for name in list_dir_names(&self.root)? {
            if RESERVED.contains(&name.as_str()) {
                continue;
            }
            if self.oplog_dir(&name).exists() {
                out.push(name);
            }
        }
        Ok(out)
    }

    fn list_oplogs(&self, file_id: String) -> Result<Vec<RemoteLogItem>, SyncError> {
        let dir = self.oplog_dir(&file_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(&format!(".{OPLOG_EXT}")) {
                continue;
            }
            let (sequence, client_id) = parse_oplog_name(&name)?;
            out.push(RemoteLogItem {
                sequence,
                client_id,
                remote_path: name,
            });
        }
        Ok(out)
    }

    fn put_oplog(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        self.write_oplog(&file_id, &entry)
    }

    fn put_oplog_cas(&self, file_id: String, entry: OpLogEntry) -> Result<(), SyncError> {
        // A dumb remote can't offer a true atomic CAS; we approximate it by
        // rejecting the append if any client already claimed this sequence.
        let taken = self
            .list_oplogs(file_id.clone())?
            .iter()
            .any(|i| i.sequence == entry.sequence);
        if taken {
            return Err(SyncError::Conflict {
                sequence: entry.sequence,
            });
        }
        self.write_oplog(&file_id, &entry)
    }

    fn get_oplog(&self, file_id: String, remote_path: String) -> Result<Vec<u8>, SyncError> {
        fs::read(self.oplog_dir(&file_id).join(remote_path)).map_err(io)
    }

    fn delete_oplog(&self, file_id: String, remote_path: String) -> Result<(), SyncError> {
        let path = self.oplog_dir(&file_id).join(remote_path);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io(e)),
        }
    }

    fn put_pack(&self, pack_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        write_addressed(&self.pack_dir(), &pack_id, data)
    }

    fn get_pack_range(
        &self,
        pack_id: String,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, SyncError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = fs::File::open(self.pack_dir().join(pack_id)).map_err(io)?;
        f.seek(SeekFrom::Start(offset)).map_err(io)?;
        let mut buf = vec![0u8; length as usize];
        f.read_exact(&mut buf).map_err(io)?;
        Ok(buf)
    }

    fn list_packs(&self) -> Result<Vec<String>, SyncError> {
        list_dir_names(&self.pack_dir())
    }

    fn delete_pack(&self, pack_id: String) -> Result<(), SyncError> {
        delete_if_present(&self.pack_dir().join(pack_id))
    }

    fn put_pack_index(&self, index_id: String, data: Vec<u8>) -> Result<(), SyncError> {
        write_addressed(&self.pack_index_dir(), &index_id, data)
    }

    fn get_pack_index(&self, index_id: String) -> Result<Vec<u8>, SyncError> {
        fs::read(self.pack_index_dir().join(index_id)).map_err(io)
    }

    fn list_pack_indexes(&self) -> Result<Vec<String>, SyncError> {
        list_dir_names(&self.pack_index_dir())
    }

    fn delete_pack_index(&self, index_id: String) -> Result<(), SyncError> {
        delete_if_present(&self.pack_index_dir().join(index_id))
    }

    fn put_baseline(&self, file_id: String, seq: u64, data: Vec<u8>) -> Result<(), SyncError> {
        let dir = self.baseline_dir(&file_id);
        fs::create_dir_all(&dir).map_err(io)?;
        fs::write(dir.join(baseline_name(seq)), data).map_err(io)
    }

    fn get_baseline(&self, file_id: String, seq: u64) -> Result<Option<Vec<u8>>, SyncError> {
        let path = self.baseline_dir(&file_id).join(baseline_name(seq));
        match fs::read(&path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    fn list_baselines(&self, file_id: String) -> Result<Vec<u64>, SyncError> {
        let dir = self.baseline_dir(&file_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            // `baseline_<seq>.zst` -> seq
            if let Some(seq) = name
                .strip_prefix("baseline_")
                .and_then(|s| s.strip_suffix(".zst"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                out.push(seq);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    fn put_status(&self, client_id: String, last_synced_sequence: u64) -> Result<(), SyncError> {
        let dir = self.status_dir();
        fs::create_dir_all(&dir).map_err(io)?;
        let body = serde_json::json!({ "last_synced_sequence": last_synced_sequence });
        let bytes =
            serde_json::to_vec(&body).map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
        fs::write(dir.join(format!("{client_id}.status")), bytes).map_err(io)
    }

    fn list_statuses(&self) -> Result<Vec<ClientStatus>, SyncError> {
        let dir = self.status_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(client) = name.strip_suffix(".status") else {
                continue;
            };
            let bytes = fs::read(entry.path()).map_err(io)?;
            let val: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::SerdeError { msg: e.to_string() })?;
            let seq = val
                .get("last_synced_sequence")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| SyncError::SerdeError {
                    msg: format!("bad status file: {name}"),
                })?;
            out.push(ClientStatus {
                client_id: client.to_string(),
                last_synced_sequence: seq,
            });
        }
        Ok(out)
    }
}

/// `baseline_<seq>.zst`
fn baseline_name(seq: u64) -> String {
    format!("baseline_{seq}.zst")
}

/// Write `data` to `dir/name`, creating `dir`. Content-addressed: an existing
/// object is left as-is (identical bytes), making the write idempotent.
fn write_addressed(dir: &std::path::Path, name: &str, data: Vec<u8>) -> Result<(), SyncError> {
    fs::create_dir_all(dir).map_err(io)?;
    let path = dir.join(name);
    if path.exists() {
        return Ok(());
    }
    fs::write(path, data).map_err(io)
}

/// List the file names directly under `dir` (empty if `dir` is absent).
fn list_dir_names(dir: &std::path::Path) -> Result<Vec<String>, SyncError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(io)? {
        let entry = entry.map_err(io)?;
        out.push(entry.file_name().to_string_lossy().into_owned());
    }
    Ok(out)
}

/// Delete `path`, treating an already-absent file as success.
fn delete_if_present(path: &std::path::Path) -> Result<(), SyncError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChangeType;
    use tempfile::TempDir;

    fn entry(seq: u64, client: &str) -> OpLogEntry {
        OpLogEntry {
            sequence: seq,
            client_id: client.to_string(),
            timestamp: 0,
            change_type: ChangeType::TextDelta {
                delta: vec![u8::try_from(seq & 0xff).unwrap_or(0)],
            },
        }
    }

    #[test]
    fn oplog_round_trips() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        let e = entry(1, "a");
        remote.put_oplog("f1".into(), e.clone()).unwrap();

        let items = remote.list_oplogs("f1".into()).unwrap();
        assert_eq!(items.len(), 1);
        let raw = remote
            .get_oplog("f1".into(), items[0].remote_path.clone())
            .unwrap();
        let decoded: OpLogEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded, e);
    }

    #[test]
    fn cas_rejects_duplicate_sequence_from_other_client() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        remote.put_oplog_cas("f1".into(), entry(5, "a")).unwrap();

        let err = remote
            .put_oplog_cas("f1".into(), entry(5, "b"))
            .unwrap_err();
        assert!(matches!(err, SyncError::Conflict { sequence: 5 }));
        // The loser must not have overwritten the winner.
        assert_eq!(remote.list_oplogs("f1".into()).unwrap().len(), 1);
    }

    #[test]
    fn cas_allows_distinct_sequences() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        remote.put_oplog_cas("f1".into(), entry(1, "a")).unwrap();
        remote.put_oplog_cas("f1".into(), entry(2, "a")).unwrap();
        assert_eq!(remote.list_oplogs("f1".into()).unwrap().len(), 2);
    }

    #[test]
    fn packs_round_trip_range_read_and_list() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        let data = b"HELLOWORLD".to_vec();
        remote.put_pack("p1".into(), data.clone()).unwrap();
        remote.put_pack("p1".into(), data).unwrap(); // idempotent
        // Range read pulls a chunk-sized slice out of the pack.
        assert_eq!(remote.get_pack_range("p1".into(), 5, 5).unwrap(), b"WORLD");
        assert_eq!(remote.get_pack_range("p1".into(), 0, 5).unwrap(), b"HELLO");
        assert_eq!(remote.list_packs().unwrap(), vec!["p1".to_string()]);
        remote.delete_pack("p1".into()).unwrap();
        assert!(remote.list_packs().unwrap().is_empty());
        // Deleting an absent pack is a no-op success.
        remote.delete_pack("p1".into()).unwrap();
    }

    #[test]
    fn pack_indexes_round_trip_and_list() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        remote.put_pack_index("p1".into(), b"idx".to_vec()).unwrap();
        assert_eq!(remote.get_pack_index("p1".into()).unwrap(), b"idx");
        assert_eq!(remote.list_pack_indexes().unwrap(), vec!["p1".to_string()]);
        remote.delete_pack_index("p1".into()).unwrap();
        assert!(remote.list_pack_indexes().unwrap().is_empty());
    }

    #[test]
    fn baselines_round_trip() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        remote
            .put_baseline("f1".into(), 110, b"snap".to_vec())
            .unwrap();
        assert_eq!(
            remote.get_baseline("f1".into(), 110).unwrap().unwrap(),
            b"snap"
        );
        assert!(remote.get_baseline("f1".into(), 999).unwrap().is_none());
        assert_eq!(remote.list_baselines("f1".into()).unwrap(), vec![110]);
    }

    #[test]
    fn statuses_round_trip_and_min() {
        let dir = TempDir::new().unwrap();
        let remote = LocalFolderRemote::new(dir.path()).unwrap();
        remote.put_status("a".into(), 120).unwrap();
        remote.put_status("b".into(), 115).unwrap();
        let mut statuses = remote.list_statuses().unwrap();
        statuses.sort_by(|x, y| x.client_id.cmp(&y.client_id));
        assert_eq!(statuses[0].client_id, "a");
        assert_eq!(statuses[0].last_synced_sequence, 120);
        assert_eq!(statuses[1].client_id, "b");
        let min = statuses
            .iter()
            .map(|s| s.last_synced_sequence)
            .min()
            .unwrap();
        assert_eq!(min, 115);
    }
}
