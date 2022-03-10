use std::fmt;

use serde::Deserialize;
use serde::Serialize;

// use crate::Node;
use core_command::command;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Cmd {
    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq {
        key: String,
    },

    // /// Add node if absent
    // AddNode {
    //     node_id: NodeId,
    //     node: Node,
    // },
    Execute {
        req: command::ExecuteRequest,
    },
    Query {
        req: command::QueryRequest,
    },
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::IncrSeq { key } => {
                write!(f, "incr_seq:{}", key)
            }
            // Cmd::AddNode { node_id, node } => {
            //     write!(f, "add_node:{}={}", node_id, node)
            // }
            Cmd::Execute { req } => {
                write!(f, "Execute:{}", req)
            }
            Cmd::Query { req } => {
                write!(f, "Query:{}", req)
            }
        }
    }
}
