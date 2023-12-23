use crate::result::DBResult;
use crate::Storage;
use std::collections::HashMap;

/// key value store, uses in memory storage for now
pub struct KvStore<T> {
    durable_storage: T,
    value_index: HashMap<String, String>,
}

impl<T> KvStore<T>
where
    T: Storage,
{
    /// Create new store from file
    pub fn open(path: &std::path::Path) -> DBResult<KvStore<T>> {
        let storage = T::open(path)?;
        Ok(Self {
            durable_storage: storage,
            value_index: HashMap::new(),
        })
    }

    /// Get value for key
    /// TODO: why does self need to be mut here?
    pub fn get(&mut self, key: &str) -> Option<String> {
        // check value index or else get from durable storage and store in index
        self.value_index.get(key).cloned().or_else(|| {
            self.durable_storage.get(key).map(|v| {
                self.value_index.insert(key.to_string(), v.clone());
                v
            })
        })
    }

    /// set value with key
    pub fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.durable_storage.set(key.clone(), value.clone())?;
        self.value_index.insert(key, value);
        Ok(())
    }

    /// set value with key
    pub fn remove(&mut self, key: &str) -> DBResult<()> {
        self.value_index.remove(key);
        self.durable_storage.remove(key)
    }
}
