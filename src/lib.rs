#![deny(missing_docs)]
//! Talent plan key value store course work
pub(crate) mod kvs;
/// `
pub mod result;

/// `
pub mod storage;

pub use kvs::KvStore;
pub use result::{DBResult, Error};
pub use storage::{InMemoryStorage, LogStructured, Storage};
