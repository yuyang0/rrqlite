use crate::RqliteTypeConfig;
use core_sled::openraft;
use serde::{Deserialize, Serialize};

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
