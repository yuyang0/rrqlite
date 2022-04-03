// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::RangeBounds;

use core_sled::{sled, AsKeySpace, SledStorageResult, SledTree};
use core_tracing::tracing;

use crate::config::RaftConfig;
use crate::errors::StoreResult;
use crate::store::key_spaces::{LogMeta, Logs};
use crate::store::kv_types::{LogMetaKey, LogMetaValue};
use crate::types::openraft::{Entry, LogId, LogIndex};

const TREE_RAFT_LOG: &str = "raft_log";

/// RaftLog stores the logs of a raft node.
/// It is part of MetaStore.
pub struct RaftLog {
    pub inner: SledTree,
}

impl RaftLog {
    /// Open RaftLog
    #[tracing::instrument(level = "debug", skip(db,config), fields(config_id=%config.config_id))]
    pub async fn open(db: &sled::Db, config: &RaftConfig) -> StoreResult<RaftLog> {
        tracing::info!(?config);

        let tree_name = config.tree_name(TREE_RAFT_LOG);
        let inner = SledTree::open(db, &tree_name, config.is_sync())?;
        let rl = RaftLog { inner };
        Ok(rl)
    }

    pub fn contains_key(&self, key: &LogIndex) -> StoreResult<bool> {
        let res = self.logs().contains_key(key)?;
        Ok(res)
    }

    pub fn get(&self, key: &LogIndex) -> StoreResult<Option<Entry>> {
        let res = self.logs().get(key)?;
        Ok(res)
    }

    pub fn last(&self) -> StoreResult<Option<(LogIndex, Entry)>> {
        let res = self.logs().last()?;
        Ok(res)
    }

    pub async fn set_last_purged(&self, log_id: LogId) -> StoreResult<()> {
        self.log_meta()
            .insert(&LogMetaKey::LastPurged, &LogMetaValue::LogId(log_id))
            .await?;
        Ok(())
    }

    pub fn get_last_purged(&self) -> StoreResult<Option<LogId>> {
        let res = self.log_meta().get(&LogMetaKey::LastPurged)?;
        match res {
            None => Ok(None),
            Some(l) => {
                let log_id: LogId = l.try_into().unwrap();
                Ok(Some(log_id))
            }
        }
    }

    /// Delete logs that are in `range`.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    ///
    /// TODO(xp): in raft deleting logs may not need to be fsync-ed.
    ///
    /// 1. Deleting happens when cleaning applied logs, in which case, these
    /// logs will never be read:    The logs to clean are all included in a
    /// snapshot and state machine.    Replication will use the snapshot for
    /// sync, or create a new snapshot from the state machine for sync.
    ///    Thus these logs will never be read. If an un-fsync-ed delete is lost
    /// during server crash, it just wait for next delete to clean them up.
    ///
    /// 2. Overriding uncommitted logs of an old term by some new leader that
    /// did not see these logs:    In this case, atomic delete is quite
    /// enough(to not leave a hole).    If the system allows logs hole,
    /// non-atomic delete is quite enough(depends on the upper layer).
    pub async fn range_remove<R>(&self, range: R) -> StoreResult<()>
    where
        R: RangeBounds<LogIndex>,
    {
        self.logs().range_remove(range, true).await?;
        Ok(())
    }

    /// Returns an iterator of logs
    pub fn range<R>(
        &self,
        range: R,
    ) -> StoreResult<impl DoubleEndedIterator<Item = SledStorageResult<(LogIndex, Entry)>>>
    where
        R: RangeBounds<LogIndex>,
    {
        let res = self.logs().range(range)?;
        Ok(res)
    }

    pub fn range_keys<R>(&self, range: R) -> StoreResult<Vec<LogIndex>>
    where
        R: RangeBounds<LogIndex>,
    {
        let res = self.logs().range_keys(range)?;
        Ok(res)
    }

    pub fn range_values<R>(&self, range: R) -> StoreResult<Vec<Entry>>
    where
        R: RangeBounds<LogIndex>,
    {
        let res = self.logs().range_values(range)?;
        Ok(res)
    }

    /// Append logs into RaftLog.
    /// There is no consecutiveness check. It is the caller's responsibility to
    /// leave no holes(if it runs a standard raft:DDD). There is no
    /// overriding check either. It always overrides the existent ones.
    ///
    /// When this function returns the logs are guaranteed to be fsync-ed.
    pub async fn append(&self, logs: &[Entry]) -> StoreResult<()> {
        let res = self.logs().append_values(logs).await?;
        Ok(res)
    }

    /// Insert a single log.
    #[tracing::instrument(level = "debug", skip(self, log), fields(log_id=format!("{}",log.log_id).as_str()))]
    pub async fn insert(&self, log: &Entry) -> StoreResult<Option<Entry>> {
        let res = self.logs().insert_value(log).await?;
        Ok(res)
    }

    /// Returns a borrowed key space in sled::Tree for logs
    fn logs(&self) -> AsKeySpace<Logs> {
        self.inner.key_space()
    }

    /// Returns a borrowed key space in sled::Tree for logs
    fn log_meta(&self) -> AsKeySpace<LogMeta> {
        self.inner.key_space()
    }
}
