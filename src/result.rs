#[derive(Debug, Clone)]
pub enum Error {
    Storage(String),
}

pub type DBResult<T> = Result<T, Error>;
