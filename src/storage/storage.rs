use crate::result::DBResult;

/// Generic trait that abstracts over the db storage
pub trait Storage {
    /// Get value for key
    fn get(&mut self, key: &str) -> Option<String>;
    /// set value with key
    fn set(&mut self, key: String, value: String) -> DBResult<()>;
    /// remove entry with key
    fn remove(&mut self, key: &str) -> DBResult<()>;

    /// open storage from file if supported
    fn open(path: &std::path::Path) -> DBResult<Self>
    where
        Self: Sized;
}
