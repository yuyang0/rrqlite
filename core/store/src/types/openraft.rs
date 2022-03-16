use crate::protobuf::RaftRequest;
use crate::RqliteTypeConfig;
use core_sled::openraft;
use serde::{Deserialize, Serialize};

pub use openraft::Node;
pub type LogIndex = u64;
pub type Term = u64;
pub type NodeId = u64;

pub type LogId = openraft::LogId<NodeId>;
pub type Entry = openraft::raft::Entry<RqliteTypeConfig>;
pub type EntryPayload = openraft::raft::EntryPayload<RqliteTypeConfig>;
pub type EffectiveMembership = openraft::EffectiveMembership<RqliteTypeConfig>;

pub type LogState = openraft::storage::LogState<RqliteTypeConfig>;
pub type Snapshot<S> = openraft::storage::Snapshot<RqliteTypeConfig, S>;
pub type StorageError = openraft::StorageError<RqliteTypeConfig>;
pub type ErrorSubject = openraft::ErrorSubject<RqliteTypeConfig>;
pub type ErrorVerb = openraft::ErrorVerb;
// pub type RaftLogReader = openraft::RaftLogReader<RqliteTypeConfig>;
// pub type RaftSnapshotBuilder<SD> = openraft::RaftSnapshotBuilder<RqliteTypeConfig, SD>;
// pub type RaftStorage = openraft::RaftStorage<RqliteTypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<RqliteTypeConfig>;
pub type StorageIOError = openraft::StorageIOError<RqliteTypeConfig>;
pub type Vote = openraft::Vote<RqliteTypeConfig>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<RqliteTypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<RqliteTypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<RqliteTypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<RqliteTypeConfig>;
pub type VoteRequest = openraft::raft::VoteRequest<RqliteTypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<RqliteTypeConfig>;

pub type ClientWriteRequest = openraft::raft::ClientWriteRequest<RqliteTypeConfig>;
pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<RqliteTypeConfig>;
pub type ClientWriteError = openraft::error::ClientWriteError<RqliteTypeConfig>;

pub type ForwardToLeader = openraft::error::ForwardToLeader<RqliteTypeConfig>;
pub type Fatal = openraft::error::Fatal<RqliteTypeConfig>;
pub type ChangeMembershipError = openraft::error::ChangeMembershipError<RqliteTypeConfig>;
// pub type RqliteRaft = Raft<RqliteTypeConfig, Network, Arc<SledRaftStore>>;

/// A record holding the hard state of a Raft node.
///
/// This model derives serde's traits for easily (de)serializing this
/// model for storage & retrieval.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct HardState {
    /// The last recorded term observed by this system.
    pub current_term: u64,
    /// The ID of the node voted for in the `current_term`.
    pub voted_for: Option<NodeId>,
}

impl tonic::IntoRequest<RaftRequest> for AppendEntriesRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}
