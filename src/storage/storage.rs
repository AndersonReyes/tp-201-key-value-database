use crate::result::DBResult;

/// Generic trait that abstracts over the db storage
pub trait Storage {
    /// typf of key
    type Key;
    /// Type of value
    type Value;

    /// Get value for key
    fn get(&mut self, key: &Self::Key) -> Option<Self::Value>;
    /// set value with key
    fn set(&mut self, key: Self::Key, value: Self::Value) -> DBResult<()>;
    /// remove entry with key
    fn remove(&mut self, key: &Self::Key) -> DBResult<()>;

    /// open storage from file if supported
    fn open(path: &std::path::Path) -> DBResult<Self>
    where
        Self: Sized;
}
