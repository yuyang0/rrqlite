use std::fmt;

use crate::types::openraft::{Node, NodeId};
use core_command::command;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum AppRequest {
    // /// Add node if absent
    // AddNode {
    //     node_id: NodeId,
    //     node: Node,
    // },
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

impl fmt::Display for AppRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Cmd::IncrSeq { key } => {
            //     write!(f, "incr_seq:{}", key)
            // }
            // AppRequest::AddNode { node_id, node } => {
            //     write!(f, "add_node:{}={}", node_id, node)
            // }
            AppRequest::Execute(req) => {
                write!(f, "Execute:{:?}", req)
            }
            AppRequest::Query(req) => {
                write!(f, "Query:{:?}", req)
            }
        }
    }
}

impl fmt::Display for AppResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Cmd::IncrSeq { key } => {
            //     write!(f, "incr_seq:{}", key)
            // }
            // Cmd::AddNode { node_id, node } => {
            //     write!(f, "add_node:{}={}", node_id, node)
            // }
            AppResponse::None => write!(f, "None"),
            AppResponse::Execute(resp) => {
                write!(f, "Execute:{:?}", resp)
            }
            AppResponse::Query(resp) => {
                write!(f, "Query:{:?}", resp)
            }
        }
    }
}
