mod in_memory_basic;
mod log_structured;
mod result;
mod storage;

pub use in_memory_basic::InMemoryStorage;
pub use log_structured::LogStructured;
pub use storage::Storage;
