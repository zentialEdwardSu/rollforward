//! Atomic durable state for the v2 runtime.

use std::path::Path;
use std::sync::{Arc, RwLock};

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::types::SyncError;

#[uniffi::export(with_foreign)]
pub trait RuntimeStore: Send + Sync {
    fn load_runtime_state(&self) -> Result<Option<Vec<u8>>, SyncError>;
    fn save_runtime_state(&self, state: Vec<u8>) -> Result<(), SyncError>;
    fn close(&self) -> Result<(), SyncError>;
}

const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("runtime_state_v2");
const STATE_KEY: &str = "current";

fn db_err(error: impl std::fmt::Display) -> SyncError {
    SyncError::IoError {
        msg: error.to_string(),
    }
}

#[derive(uniffi::Object)]
pub struct RedbRuntimeStore {
    db: RwLock<Option<Database>>,
}

impl RedbRuntimeStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SyncError> {
        let db = Database::create(path).map_err(db_err)?;
        let transaction = db.begin_write().map_err(db_err)?;
        {
            transaction.open_table(STATE).map_err(db_err)?;
        }
        transaction.commit().map_err(db_err)?;
        Ok(Self {
            db: RwLock::new(Some(db)),
        })
    }

    fn with_db<T>(
        &self,
        action: impl FnOnce(&Database) -> Result<T, SyncError>,
    ) -> Result<T, SyncError> {
        let guard = self.db.read().expect("v2 store lock poisoned");
        action(guard.as_ref().ok_or_else(|| SyncError::IoError {
            msg: "runtime store is closed".into(),
        })?)
    }
}

#[uniffi::export]
impl RedbRuntimeStore {
    #[uniffi::constructor]
    pub fn new(path: String) -> Result<Arc<Self>, SyncError> {
        Ok(Arc::new(Self::open(path)?))
    }
}

impl RuntimeStore for RedbRuntimeStore {
    fn load_runtime_state(&self) -> Result<Option<Vec<u8>>, SyncError> {
        self.with_db(|db| {
            let transaction = db.begin_read().map_err(db_err)?;
            let table = transaction.open_table(STATE).map_err(db_err)?;
            Ok(table
                .get(STATE_KEY)
                .map_err(db_err)?
                .map(|value| value.value().to_vec()))
        })
    }

    fn save_runtime_state(&self, state: Vec<u8>) -> Result<(), SyncError> {
        self.with_db(|db| {
            let transaction = db.begin_write().map_err(db_err)?;
            {
                transaction
                    .open_table(STATE)
                    .map_err(db_err)?
                    .insert(STATE_KEY, state.as_slice())
                    .map_err(db_err)?;
            }
            transaction.commit().map_err(db_err)
        })
    }

    fn close(&self) -> Result<(), SyncError> {
        drop(self.db.write().expect("v2 store lock poisoned").take());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_atomically() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = RedbRuntimeStore::open(dir.path().join("store-v2.redb")).unwrap();
        assert!(store.load_runtime_state().unwrap().is_none());
        store.save_runtime_state(b"state".to_vec()).unwrap();
        assert_eq!(store.load_runtime_state().unwrap(), Some(b"state".to_vec()));
    }
}
