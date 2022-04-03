mod service;
mod protobuf {
    tonic::include_proto!("raftservice");
}
pub mod config;
pub mod errors;
pub mod fsm;
pub mod network;
mod raft_node;
pub mod store;
pub mod types;

use std::sync::Arc;

use core_sled::openraft;
use openraft::Raft;
pub use raft_node::RqliteNode;
use types::{AppRequest, AppResponse};

use crate::network::AppNetwork;
use crate::store::SledRaftStore;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub RqliteTypeConfig: D = AppRequest, R = AppResponse, NodeId = types::openraft::NodeId
);

pub type RqliteRaft = Raft<RqliteTypeConfig, AppNetwork, Arc<SledRaftStore>>;
