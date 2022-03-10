use core_command::command;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum AppRequest {
    Query(command::QueryRequest),
    Execute(command::ExecuteRequest),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum AppResponse {
    Query(command::QueryResult),
    Execute(command::ExecuteResult),

    // #[try_into(ignore)]
    None,
}
