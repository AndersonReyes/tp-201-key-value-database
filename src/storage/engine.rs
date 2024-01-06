/// Generic trait that abstracts over the db storage
pub trait Engine {
    /// Get value for key
    fn get(&self, key: &str) -> Option<String>;
    /// set value with key
    fn set(&mut self, key: String, value: String) -> anyhow::Result<()>;
    /// remove entry with key
    fn remove(&mut self, key: &str) -> anyhow::Result<()>;

    /// open storage from file if supported
    fn open(path: &std::path::Path) -> anyhow::Result<Self>
    where
        Self: Sized;
}
