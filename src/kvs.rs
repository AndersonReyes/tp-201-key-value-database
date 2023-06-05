use crate::result::DBResult;
use crate::storage::{InMemoryStorage, Storage};

pub struct KvStore {
    storage: InMemoryStorage,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            storage: InMemoryStorage::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.storage.get(&key)
    }

    pub fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.storage.set(key, value)
    }

    pub fn remove(&mut self, key: String) -> DBResult<()> {
        self.storage.remove(&key)
    }
}
