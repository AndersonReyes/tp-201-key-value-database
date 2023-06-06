use crate::result::DBResult;
use crate::storage::{InMemoryStorage, Storage};

/// key value store, uses in memory storage for now
pub struct KvStore {
    storage: InMemoryStorage,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// Create new store, in memory by default
    pub fn new() -> Self {
        Self {
            storage: InMemoryStorage::new(),
        }
    }

    /// Get value for key
    pub fn get(&self, key: String) -> Option<String> {
        self.storage.get(&key)
    }

    /// set value with key
    pub fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.storage.set(key, value)
    }

    /// set value with key
    pub fn remove(&mut self, key: String) -> DBResult<()> {
        self.storage.remove(&key)
    }
}
