use crate::result::DBResult;
use crate::{InMemoryStorage, Storage};

/// key value store, uses in memory storage for now
pub struct KvStore<T> {
    storage: T,
}

impl<T> KvStore<T>
where
    T: Storage,
{
    /// Create new store from file
    pub fn open(path: &std::path::Path) -> DBResult<KvStore<T>> {
        let storage = T::open(path)?;
        Ok(Self { storage })
    }

    /// Get value for key
    pub fn get(&mut self, key: T::Key) -> Option<T::Value> {
        self.storage.get(&key)
    }

    /// set value with key
    pub fn set(&mut self, key: T::Key, value: T::Value) -> DBResult<()> {
        self.storage.set(key, value)
    }

    /// set value with key
    pub fn remove(&mut self, key: T::Key) -> DBResult<()> {
        self.storage.remove(&key)
    }
}
