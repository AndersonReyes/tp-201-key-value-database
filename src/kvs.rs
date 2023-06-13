use crate::result::DBResult;
use crate::storage::{InMemoryStorage, Storage};

/// key value store, uses in memory storage for now
pub struct KvStore<T> {
    storage: T,
}

impl<T> Default for KvStore<T>
where
    T: Storage + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> KvStore<T>
where
    T: Storage,
{
    /// Create new store, in memory by default
    pub fn new() -> Self
    where
        T: Default,
    {
        Self {
            storage: T::default(),
        }
    }

    /// Get value for key
    pub fn get(&self, key: T::Key) -> Option<T::Value> {
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
