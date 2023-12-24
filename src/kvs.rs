use crate::Storage;

/// key value store, uses in memory storage for now
pub struct KvStore<T> {
    durable_storage: T,
}

impl<T> KvStore<T>
where
    T: Storage,
{
    /// Create new store from file
    pub fn open(path: &std::path::Path) -> anyhow::Result<KvStore<T>> {
        let storage = T::open(path)?;
        Ok(Self {
            durable_storage: storage,
        })
    }

    /// Get value for key
    /// TODO: why does self need to be mut here?
    pub fn get(&mut self, key: &str) -> Option<String> {
        self.durable_storage.get(key)
    }

    /// set value with key
    pub fn set(&mut self, key: String, value: String) -> anyhow::Result<()> {
        self.durable_storage.set(key.clone(), value.clone())?;
        Ok(())
    }

    /// set value with key
    pub fn remove(&mut self, key: &str) -> anyhow::Result<()> {
        self.durable_storage.remove(key)
    }
}
