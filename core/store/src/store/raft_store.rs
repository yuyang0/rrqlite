use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::types::openraft::{EffectiveMembership, Entry, LogId, NodeId};
pub use core_sled::{openraft, sled};
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StateMachineChanges;
use openraft::StorageError;
use openraft::Vote;
use tokio::sync::RwLock;

use super::log::RaftLog;
use super::state::RaftState;
use super::ToStorageError;
use crate::config::RaftConfig;
use crate::errors::StoreResult;
use crate::fsm::{FSMSnapshot, FSM};
use crate::types::AppResponse;
use crate::RqliteTypeConfig;
use core_sled::get_sled_db;

pub struct SledRaftStore {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from disk) or created.
    is_opened: bool,

    /// The sled db for log and raft_state.
    /// state machine is stored in another sled db since it contains user data and needs to be export/import as a whole.
    /// This db is also used to generate a locally unique id.
    /// Currently the id is used to create a unique snapshot id.
    _db: sled::Db,

    // Raft state includes:
    // id: NodeId,
    //     current_term,
    //     voted_for
    pub raft_state: RaftState,

    pub log: RaftLog,

    /// The Raft state machine.
    ///
    /// sled db has its own concurrency control, e.g., batch or transaction.
    /// But we still need a lock, when installing a snapshot, which is done by replacing the state machine:
    ///
    /// - Acquire a read lock to WRITE or READ. Transactional RW relies on sled concurrency control.
    /// - Acquire a write lock before installing a snapshot, to prevent any write to the db.
    pub state_machine: Arc<dyn FSM>,

    /// The current snapshot.
    pub current_snapshot: RwLock<Option<FSMSnapshot>>,
}

impl SledRaftStore {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[tracing::instrument(level = "debug", skip(config,fsm,open,create), fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        fsm: Arc<dyn FSM>,
        open: Option<()>,
        create: Option<()>,
    ) -> StoreResult<SledRaftStore> {
        tracing::info!("open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        tracing::info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        tracing::info!("RaftLog opened");

        // let (sm_id, prev_sm_id) = raft_state.read_state_machine_id()?;

        // // There is a garbage state machine need to be cleaned.
        // if sm_id != prev_sm_id {
        //     StateMachine::clean(config, prev_sm_id)?;
        //     raft_state.write_state_machine_id(&(sm_id, sm_id)).await?;
        // }

        let current_snapshot = RwLock::new(None);

        Ok(Self {
            id: raft_state.id,
            config: config.clone(),
            is_opened: is_open,
            _db: db,
            raft_state,
            log,
            state_machine: fsm.clone(),
            current_snapshot,
        })
    }

    // pub async fn get_node_endpoint(&self, node_id: &NodeId) -> StoreResult<Endpoint> {
    //     let endpoint = self
    //         .get_node(node_id)
    //         .await?
    //         .map(|n| n.endpoint)
    //         .ok_or_else(|| StoreError::GetNodeAddrError(format!("node id: {}", node_id)))?;

    //     Ok(endpoint)
    // }

    pub fn is_opened(&self) -> bool {
        self.is_opened
    }
}

#[async_trait]
impl RaftLogReader<RqliteTypeConfig> for Arc<SledRaftStore> {
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RqliteTypeConfig>, StorageError<RqliteTypeConfig>> {
        let last = self
            .log
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;
        let last_purged = self
            .log
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        let last = match last {
            None => last_purged,
            Some((_, v)) => Some(v.log_id),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<RqliteTypeConfig>> {
        let entries = self
            .log
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        Ok(entries)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RqliteTypeConfig, Cursor<Vec<u8>>> for Arc<SledRaftStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RqliteTypeConfig, Cursor<Vec<u8>>>, StorageError<RqliteTypeConfig>> {
        let (data, last_applied_log, snapshot_id) = self
            .state_machine
            .snapshot()
            .await
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;

        let snap_meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = FSMSnapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };

        // Update the snapshot first.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        let snapshot_size = data.len();
        tracing::debug!(snapshot_size = snapshot_size, "log compaction complete");

        Ok(openraft::storage::Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<RqliteTypeConfig> for Arc<SledRaftStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<RqliteTypeConfig>,
    ) -> Result<(), StorageError<RqliteTypeConfig>> {
        self.raft_state
            .write_vote(vote)
            .await
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Write)?;
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<RqliteTypeConfig>>, StorageError<RqliteTypeConfig>> {
        let vt = self
            .raft_state
            .read_vote()
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Read)?;
        Ok(vt)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry],
    ) -> Result<(), StorageError<RqliteTypeConfig>> {
        let entries = entries.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
        self.log
            .append(&entries)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId,
    ) -> Result<(), StorageError<RqliteTypeConfig>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        self.log
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId,
    ) -> Result<(), StorageError<RqliteTypeConfig>> {
        self.log
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)?;
        self.log
            .range_remove(..=log_id.index)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, Option<EffectiveMembership>), StorageError<RqliteTypeConfig>> {
        let last_applied = self
            .state_machine
            .get_last_applied()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;
        let last_membership = self
            .state_machine
            .get_membership()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;
        Ok((last_applied, last_membership))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry],
    ) -> Result<Vec<AppResponse>, StorageError<RqliteTypeConfig>> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = &self.state_machine;
        for entry in entries {
            let r = sm
                .apply(*entry)
                .await
                .map_to_sto_err(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)?;
            res.push(r);
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<RqliteTypeConfig>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RqliteTypeConfig>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<RqliteTypeConfig>, StorageError<RqliteTypeConfig>> {
        // TODO(xp): disallow installing a snapshot with smaller last_applied.

        tracing::debug!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = FSMSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        tracing::debug!("SNAP META:{:?}", meta);

        // Update the state machine.
        let res = self.state_machine.restore(&new_snapshot.data).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("error: {:?} when install_snapshot", e);
            }
        };

        // Update current snapshot.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<RqliteTypeConfig, Self::SnapshotData>>,
        StorageError<RqliteTypeConfig>,
    > {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
