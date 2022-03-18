use crate::types::openraft::{ChangeMembershipError, Fatal, ForwardToLeader};
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
    #[error(transparent)]
    RaftError(#[from] RaftError),
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
    APIError(#[from] APIError),
    #[error(transparent)]
    Other(anyerror::AnyError),
}
pub type StoreResult<T> = std::result::Result<T, StoreError>;

impl From<StoreError> for ErrorCode {
    fn from(e: StoreError) -> Self {
        ErrorCode::StoreError(format!("Store Error: {}", e))
    }
}

// impl From<SledStorageError> for StoreError {

// }

// represent raft related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RaftError {
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error("{0}")]
    ConsistentReadError(String),

    #[error("{0}")]
    RaftFatal(#[from] Fatal),

    #[error("{0}")]
    ForwardRequestError(String),

    #[error("{0}")]
    NoLeaderError(String),

    #[error("{0}")]
    JoinClusterFail(String),

    #[error("{0}")]
    RequestNotForwardToLeaderError(String),
}

// represent raft related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum APIError {
    #[error("{0}")]
    StaleRead(String),

    #[error("{0}")]
    Query(String),

    #[error("{0}")]
    Execute(String),

    #[error("{0}")]
    NotLeader(String),

    #[error("{0}")]
    AddLearner(String),

    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error("{0}")]
    ConsistentReadError(String),

    #[error("{0}")]
    RaftFatal(#[from] Fatal),

    #[error("{0}")]
    ForwardRequestError(String),

    #[error("{0}")]
    NoLeaderError(String),

    #[error("{0}")]
    JoinClusterFail(String),

    #[error("{0}")]
    RequestNotForwardToLeaderError(String),
}
