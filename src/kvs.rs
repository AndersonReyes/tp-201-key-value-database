use crate::result::DBResult;
use crate::Storage;

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
    pub fn get(&mut self, key: &str) -> Option<String> {
        self.storage.get(key)
    }

    /// set value with key
    pub fn set(&mut self, key: String, value: String) -> DBResult<()> {
        self.storage.set(key, value)
    }

    /// set value with key
    pub fn remove(&mut self, key: &str) -> DBResult<()> {
        self.storage.remove(key)
    }
}
