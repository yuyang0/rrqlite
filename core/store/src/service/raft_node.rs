use std::sync::Arc;

use crate::store::SledRaftStore;
use crate::RqliteRaft;
use core_exception::Result;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

// RqliteNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct RqliteNode {
    pub sto: Arc<SledRaftStore>,
    pub raft: RqliteRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,
}
