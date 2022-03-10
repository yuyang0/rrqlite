mod service;
mod protobuf {
    tonic::include_proto!("raftservice");
}
pub mod config;
pub mod errors;
mod fsm;
pub mod network;
pub mod store;
mod types;

use crate::network::AppNetwork;
use crate::store::SledRaftStore;
use core_sled::openraft;
use openraft::Raft;
use std::sync::Arc;
use types::{AppRequest, AppResponse};

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub RqliteTypeConfig: D = AppRequest, R = AppResponse, NodeId = types::openraft::NodeId
);

pub type RqliteRaft = Raft<RqliteTypeConfig, AppNetwork, Arc<SledRaftStore>>;
