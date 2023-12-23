use std::collections::HashMap;

use crate::result::DBResult;
use crate::storage::Storage;

/// In memory db storage. Good for testing only
pub struct InMemoryStorage {
    storage: HashMap<String, String>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// Create new in memory storage using raw hashmap
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    fn get(&self, key: &str) -> Option<String> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.storage.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> DBResult<()> {
        self.storage.remove(key);
        Ok(())
    }

    fn open(_path: &std::path::Path) -> DBResult<Self> {
        unimplemented!()
    }
}
