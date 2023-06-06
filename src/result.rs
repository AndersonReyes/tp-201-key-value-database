/// DB Errors
#[derive(Debug, Clone)]
pub enum Error {
    /// Storage error
    Storage(String),
}

/// DB Result type
pub type DBResult<T> = Result<T, Error>;
