// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use anyerror::AnyError;
use core_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;
use sled::transaction::UnabortableTransactionError;
use thiserror::Error;

use crate::error_context::ErrorWithContext;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
pub enum SledStorageError {
    // type to represent bytes format errors
    #[error("{0}")]
    BytesError(String),

    // type to represent serialize/deserialize errors
    #[error("{0}")]
    SerdeError(String),

    /// An AnyError built from sled::Error.
    #[error(transparent)]
    SledError(AnyError),

    #[error(transparent)]
    Damaged(AnyError),

    /// Error that is related to snapshot
    #[error(transparent)]
    SnapshotError(AnyError),

    /// An internal error that inform txn to retry.
    #[error("Conflict when execute transaction, just retry")]
    TransactionConflict,

    /// An application error that cause transaction to abort.
    #[error("{0}")]
    AppError(#[from] AppError),
}

/// Output message for end users, with sensitive info stripped.
pub trait AppErrorMessage: Display {
    fn message(&self) -> String {
        format!("{}", self)
    }
}

// TODO: remove UnknownDatabase
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("UnknownDatabase: `{db_name}` while `{context}`")]
pub struct UnknownDatabase {
    db_name: String,
    context: String,
}

impl UnknownDatabase {
    pub fn new(db_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AppError {
    #[error(transparent)]
    UnknownDatabase(#[from] UnknownDatabase),
}

impl AppErrorMessage for UnknownDatabase {
    fn message(&self) -> String {
        self.db_name.to_string()
    }
}

impl From<AppError> for ErrorCode {
    fn from(app_err: AppError) -> Self {
        match app_err {
            AppError::UnknownDatabase(err) => ErrorCode::StoreError(err.message()),
        }
    }
}

pub type SledStorageResult<T> = std::result::Result<T, SledStorageError>;

impl From<SledStorageError> for ErrorCode {
    fn from(e: SledStorageError) -> Self {
        match e {
            SledStorageError::AppError(app_err) => app_err.into(),
            _ => ErrorCode::SledStorageError(e.to_string()),
        }
    }
}

impl From<std::string::FromUtf8Error> for SledStorageError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        SledStorageError::BytesError(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

// from serde error to MetaStorageError::SerdeError
impl From<serde_json::Error> for SledStorageError {
    fn from(error: serde_json::Error) -> SledStorageError {
        SledStorageError::SerdeError(format!("serde json se/de error: {:?}", error))
    }
}

impl From<sled::Error> for SledStorageError {
    fn from(e: sled::Error) -> SledStorageError {
        SledStorageError::SledError(AnyError::new(&e))
    }
}

impl From<ErrorWithContext<sled::Error>> for SledStorageError {
    fn from(e: ErrorWithContext<sled::Error>) -> SledStorageError {
        SledStorageError::SledError(AnyError::new(&e.err).add_context(|| e.context))
    }
}

pub trait ToMetaStorageError<T, E, CtxFn>
where
    E: Display + Send + Sync + 'static,
{
    /// Wrap the error value with MetaError. It is lazily evaluated:
    /// only when an error does occur.
    ///
    /// `err_code_fn` is one of the MetaError builder function such as `MetaError::Ok`.
    /// `context_fn` builds display_text for the MetaError.
    fn map_error_to_meta_storage_error<ErrFn, D>(
        self,
        err_code_fn: ErrFn,
        context_fn: CtxFn,
    ) -> SledStorageResult<T>
    where
        ErrFn: FnOnce(String) -> SledStorageError,
        D: Display,
        CtxFn: FnOnce() -> D;
}

impl<T, E, CtxFn> ToMetaStorageError<T, E, CtxFn> for std::result::Result<T, E>
where
    E: Display + Send + Sync + 'static,
{
    fn map_error_to_meta_storage_error<ErrFn, D>(
        self,
        make_exception: ErrFn,
        context_fn: CtxFn,
    ) -> SledStorageResult<T>
    where
        ErrFn: FnOnce(String) -> SledStorageError,
        D: Display,
        CtxFn: FnOnce() -> D,
    {
        self.map_err(|error| {
            let err_text = format!("meta storage error: {}, cause: {}", context_fn(), error);
            make_exception(err_text)
        })
    }
}

impl From<UnabortableTransactionError> for SledStorageError {
    fn from(error: UnabortableTransactionError) -> Self {
        match error {
            UnabortableTransactionError::Storage(e) => {
                SledStorageError::SledError(AnyError::new(&e))
            }
            UnabortableTransactionError::Conflict => SledStorageError::TransactionConflict,
        }
    }
}
