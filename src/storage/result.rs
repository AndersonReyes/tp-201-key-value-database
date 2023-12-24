use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Key not found [{0}]")]
    KeyNotFound(String),
}
