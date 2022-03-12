use std::sync::Arc;

use crate::errors::{StoreError, StoreResult};
use crate::store::SledRaftStore;
use crate::types::openraft::{ClientWriteRequest, EntryPayload};
use crate::types::{AppRequest, AppResponse};
use crate::RqliteRaft;
use core_command::command;
use core_exception::{ErrorCode, Result};
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

impl RqliteNode {
    // pub fn new(_conf: &StoreConfig) -> Result<Store> {
    //     let fsm = SQLFsm::new()?;

    //     let raft = Raft::new(_conf.raft_addr, logger.clone());

    //     let raft_handle = match _conf.peer_addr {
    //         Some(addr) => {
    //             info!("running in follower mode");
    //             let handle = tokio::spawn(raft.join(addr, fsm.clone()));
    //             handle
    //         }
    //         None => {
    //             info!("running in leader mode");
    //             let handle = tokio::spawn(raft.lead(fsm.clone()));
    //             handle
    //         }
    //     };

    //     let store = Store {
    //         fsm: fsm.clone(),
    //         r: raft,
    //     };
    //     // let result = tokio::try_join!(raft_handle)?;
    //     // result.0?;
    //     Ok(store)
    // }

    pub async fn execute(
        &self,
        er: command::ExecuteRequest,
    ) -> StoreResult<command::ExecuteResult> {
        //check leader
        self.raft
            .is_leader()
            .await
            .map_err(|e| StoreError::APIError(format!("Not a leader {}", e)))?;
        let rpc = ClientWriteRequest::new(EntryPayload::Normal(AppRequest::Execute(er)));
        let resp = self
            .raft
            .client_write(rpc)
            .await
            .map_err(|e| StoreError::APIError(format!("{}", e)))?;
        match resp.data {
            AppResponse::Execute(res) => Ok(res),
            _ => Err(StoreError::APIError(format!(
                "Need ExecuteResult, but got {}",
                resp.data
            ))),
        }
    }

    async fn query(&self, qr: command::QueryRequest) -> StoreResult<command::QueryResult> {
        let level = command::query_request::Level::from_i32(qr.level);
        let Some(level) = level;
        let res = match level {
            command::query_request::Level::QueryRequestLevelStrong => {
                //check leader
                self.raft
                    .is_leader()
                    .await
                    .map_err(|e| StoreError::APIError(format!("Not a leader {}", e)))?;
                let rpc = ClientWriteRequest::new(EntryPayload::Normal(AppRequest::Query(qr)));
                let resp = self
                    .raft
                    .client_write(rpc)
                    .await
                    .map_err(|e| StoreError::APIError(format!("{}", e)))?;
                match resp.data {
                    AppResponse::Query(res) => Ok(res),
                    _ => Err(StoreError::APIError(format!(
                        "Need QueryResult, but got {}",
                        resp.data
                    ))),
                }
            }
            command::query_request::Level::QueryRequestLevelWeak => {
                self.raft
                    .is_leader()
                    .await
                    .map_err(|e| StoreError::APIError(format!("Not a leader {}", e)))?;
                None
            }
            command::query_request::Level::QueryRequestLevelNone => {
                let elapsed = self.r.last_contact_time().elapsed();

                if qr.freshness > 0 && elapsed.as_nanos() > (qr.freshness as u128) {
                    return Err(ErrorCode::StaleRead(""));
                } else {
                    None
                }
            }
            _ => None,
        };
        match res {
            Some(q_res) => Ok(q_res),
            None => {
                //TODO after remove rwlock in FSM, here we need to acquire read lock when req.transaction is true.
                let mut db = self.fsm.db.write().unwrap();
                let Some(ref req) = qr.request;
                let res = db.query(req)?;
                Ok(res)
            }
        }
    }

    // // join the node with the given ID, reachable at addr, to this node.
    // fn join(id: &str, addr: &str, voter: bool) -> Result<()> {
    //     Ok(())
    // }

    // // notify this node that a node is available at addr.
    // fn notify(id: &str, addr: &str) -> Result<()> {
    //     Ok(())
    // }

    // // remove the node, specified by id, from the cluster.
    // fn remove(id: &str) -> Result<()> {
    //     Ok(())
    // }

    // // leader_addr returns the Raft address of the leader of the cluster.
    // fn leader_addr() -> Result<String> {
    //     Ok(String::from(""))
    // }

    // fn is_leader(&self) -> bool {
    //     self.r.is_leader()
    // }

    // // // Stats returns stats on the Store.
    // // Stats() (map[string]interface{}, error)

    // // // Nodes returns the slice of store.Servers in the cluster
    // // Nodes() ([]*store.Server, error)

    // // // Backup wites backup of the node state to dst
    // // Backup(leader bool, f store.BackupFormat, dst io.Writer) error
}
