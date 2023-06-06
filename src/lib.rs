#![deny(missing_docs)]
//! Talent plan key value store course work
pub(crate) mod kvs;
pub(crate) mod result;
pub(crate) mod storage;

pub use kvs::KvStore;
pub use result::{DBResult, Error};
pub use storage::{InMemoryStorage, Storage};
