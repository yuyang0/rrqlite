#![allow(non_snake_case)]

use std::sync::Arc;

use backtrace::Backtrace;

use crate::exception::ErrorCodeBacktrace;
use crate::ErrorCode;

// pub static ABORT_SESSION: u16 = 1042;
// pub static ABORT_QUERY: u16 = 1043;

macro_rules! build_exceptions {
    ($($body:ident($code:expr)),*$(,)*) => {
            impl ErrorCode {
                $(
                pub fn $body(display_text: impl Into<String>) -> ErrorCode {
                    ErrorCode::create(
                        $code,
                        display_text.into(),
                        None,
                        Some(ErrorCodeBacktrace::Origin(Arc::new(Backtrace::new())))
                    )
                }
                paste::item! {
                    pub fn [< $body:snake _ code >] ()  -> u16{
                        $code
                    }

                    pub fn [< $body  Code >] ()  -> u16{
                        $code
                    }
                }
                )*
            }
    }
}

// Internal errors [0, 2000].
build_exceptions! {
    Ok(0),
    UnknownTypeOfQuery(1001),
    UnImplement(1002),

    DBQueryError(1003),
    DBExecuteError(1004),
    DBOtherError(1005),

    GrpcError(1006),
    RaftError(1007),
    RaftStorageError(1008),
    JoinError(1009),
    NotLeaderError(1010),

    StaleRead(1011),
    ConfigError(1012),

    StoreError(1021),
    SledStorageError(1022),

    BadAddressFormat(1036),
    BadBytes(1046),
    BadRequest(1047),

    // Uncategorized error codes.
    UnexpectedResponseType(1066),
    UnknownException(1067),
    TokioError(1068),
     // Http query error codes.
    HttpNotFound(1072),

    // Network error codes.
    NetworkRequestError(1073),

}
