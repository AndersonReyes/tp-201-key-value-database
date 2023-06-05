use std::collections::HashMap;

use crate::result::DBResult;

pub trait Storage<K, V> {
    fn get(&self, key: &K) -> Option<V>;
    fn set(&mut self, key: K, value: V) -> DBResult<()>;
    fn remove(&mut self, key: &K) -> DBResult<()>;
}

pub struct InMemoryStorage {
    storage: HashMap<String, String>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }
}

impl Storage<String, String> for InMemoryStorage {
    fn get(&self, key: &String) -> Option<String> {
        self.storage.get(key).cloned()
    }

    fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.storage.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &String) -> DBResult<()> {
        self.storage.remove(key);
        Ok(())
    }
}
