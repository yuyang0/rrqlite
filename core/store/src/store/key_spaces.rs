use core_sled::SledKeySpace;

use super::kv_types::{
    FSMMetaKey, LogMetaKey, LogMetaValue, RaftStateKey, RaftStateValue, StateMachineMetaValue,
};
use crate::types::openraft::{Entry, LogIndex};

/// Types for raft log in SledTree
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry;
}

/// Types for raft log meta data in SledTree
pub struct LogMeta {}
impl SledKeySpace for LogMeta {
    const PREFIX: u8 = 13;
    const NAME: &'static str = "log-meta";
    type K = LogMetaKey;
    type V = LogMetaValue;
}

/// Key-Value Types for storing meta data of a raft state machine in sled::Tree,
/// e.g. the last applied log id.
pub struct FSMMeta {}
impl SledKeySpace for FSMMeta {
    const PREFIX: u8 = 3;
    const NAME: &'static str = "sm-meta";
    type K = FSMMetaKey;
    type V = StateMachineMetaValue;
}

/// Key-Value Types for storing meta data of a raft in sled::Tree:
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
pub struct RaftStateKV {}
impl SledKeySpace for RaftStateKV {
    const PREFIX: u8 = 4;
    const NAME: &'static str = "raft-state";
    type K = RaftStateKey;
    type V = RaftStateValue;
}
