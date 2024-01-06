use anyhow::Ok;

use crate::Engine;

/// key value store, uses in memory storage for now
pub struct KvStore<T: Engine> {
    engine: T,
}

impl<T: Engine> KvStore<T> {
    /// Create new store using the specified engine
    pub fn new(engine: T) -> anyhow::Result<Self> {
        Ok(Self { engine })
    }

    /// Create new store from file
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let engine = T::open(path)?;
        Ok(Self { engine })
    }

    /// Get value for key
    /// TODO: why does self need to be mut here?
    pub fn get(&self, key: &str) -> Option<String> {
        self.engine.get(key)
    }

    /// set value with key
    pub fn set(&mut self, key: String, value: String) -> anyhow::Result<()> {
        self.engine.set(key.clone(), value.clone())?;
        Ok(())
    }

    /// set value with key
    pub fn remove(&mut self, key: &str) -> anyhow::Result<()> {
        self.engine.remove(key)
    }
}
