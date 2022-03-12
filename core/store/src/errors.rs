use anyerror;
use core_exception::ErrorCode;
use core_sled::SledStorageError;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

// represent network related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum StoreError {
    #[error("{0}")]
    InvalidConfig(String),
    #[error("{0}")]
    GetNodeAddrError(String),
    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),
    #[error("raft state absent, can not open")]
    MetaStoreNotFound,
    #[error("SledStorageError: {0}")]
    SledStorageError(#[from] SledStorageError),
    #[error("FSMError: {0}")]
    FSMError(#[from] anyerror::AnyError),
    #[error("APIError: {0}")]
    APIError(String),
    // #[error(transparent)]
    // Other(#[from] anyhow::Error),
}
pub type StoreResult<T> = std::result::Result<T, StoreError>;

impl From<StoreError> for ErrorCode {
    fn from(e: StoreError) -> Self {
        ErrorCode::StoreError(format!("Store Error: {}", e))
    }
}

// impl From<SledStorageError> for StoreError {

// }
