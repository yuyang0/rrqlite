use thiserror::Error;

// represent network related errors
#[derive(Error, Debug)]
pub enum DBError {
    #[error(transparent)]
    RusqliteError(#[from] rusqlite::Error),
    #[error(transparent)]
    ConnPoolError(#[from] r2d2::Error),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DBError>;
