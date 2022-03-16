use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::RwLock;

use crate::errors::{APIError, RaftError, StoreResult};
use crate::fsm::FSM;
use crate::store::SledRaftStore;
use crate::types::openraft::{ClientWriteError, ClientWriteRequest, EntryPayload, NodeId};
use crate::types::{AppRequest, AppResponse};
use crate::RqliteRaft;
use core_command::command;
use core_exception::Result;
use std::time::Instant;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;

// RqliteNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct RqliteNode {
    pub sto: Arc<SledRaftStore>,
    pub raft: RqliteRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,

    last_contact_time: RwLock<Instant>,
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

    fn state_machine(&self) -> Arc<dyn FSM> {
        return self.sto.state_machine.clone();
    }

    pub async fn execute(
        &self,
        er: command::ExecuteRequest,
    ) -> StoreResult<command::ExecuteResult> {
        //check leader
        self.raft
            .is_leader()
            .await
            .map_err(|e| APIError::NotLeader(format!("Not a leader {}", e)))?;
        let app_resp = self.write_to_leader(AppRequest::Execute(er)).await?;

        match app_resp {
            AppResponse::Execute(res) => Ok(res),
            _ => Err(APIError::Execute(format!("Need ExecuteResult, but got {}", app_resp)).into()),
        }
    }

    pub async fn query(&self, qr: command::QueryRequest) -> StoreResult<command::QueryResult> {
        let level = command::query_request::Level::from_i32(qr.level)
            .ok_or(APIError::Query(format!("invalid query level {}", qr.level)))?;
        match level {
            command::query_request::Level::QueryRequestLevelStrong => {
                //check leader
                self.raft
                    .is_leader()
                    .await
                    .map_err(|e| APIError::NotLeader(format!("Not a leader {}", e)))?;
                let rpc = ClientWriteRequest::new(EntryPayload::Normal(AppRequest::Query(qr)));
                let resp = self
                    .raft
                    .client_write(rpc)
                    .await
                    .map_err(|e| APIError::Query(format!("{}", e)))?;
                match resp.data {
                    AppResponse::Query(res) => Ok(res),
                    _ => {
                        return Err(APIError::Query(format!(
                            "Need QueryResult, but got {}",
                            resp.data
                        ))
                        .into())
                    }
                }
            }
            command::query_request::Level::QueryRequestLevelWeak => {
                self.raft
                    .is_leader()
                    .await
                    .map_err(|e| APIError::NotLeader(format!("Not a leader {}", e)))?;
                //TODO after remove rwlock in FSM, here we need to acquire read lock when req.transaction is true.
                let res = self.state_machine().query(&qr).await?;
                Ok(res)
            }
            command::query_request::Level::QueryRequestLevelNone => {
                let elapsed = self
                    .last_contact_time
                    .read()
                    .map_err(|e| APIError::Query(format!("{}", e)))?
                    .elapsed();

                if qr.freshness > 0 && elapsed.as_nanos() > (qr.freshness as u128) {
                    return Err(APIError::StaleRead(String::from("")).into());
                }

                //TODO after remove rwlock in FSM, here we need to acquire read lock when req.transaction is true.
                let res = self.state_machine().query(&qr).await?;
                Ok(res)
            }
        }
    }

    // join the node with the given ID, reachable at addr, to this node.
    pub async fn join(&self, node_id: NodeId, addr: String, voter: bool) -> StoreResult<()> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.get_configs();

        // TODO(xp): deal with joint config
        assert!(membership.get(1).is_none());

        // safe unwrap: if the first config is None, panic is the expected behavior here.
        let mut membership = membership.get(0).unwrap().clone();

        if membership.contains(&node_id) {
            return Ok(());
        }
        membership.insert(node_id);

        // let req = AppRequest::AddNode {
        //     node_id,
        //     node: Node {
        //         addr: addr,
        //         ..Default::default()
        //     },
        // };

        // self.write_to_leader(req.clone()).await?;

        self.change_membership(membership).await
    }

    // notify this node that a node is available at addr.
    pub async fn notify(id: &str, addr: &str) -> StoreResult<()> {
        Ok(())
    }

    // remove the node, specified by id, from the cluster.
    pub async fn remove(&self, node_id: NodeId) -> StoreResult<()> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.get_configs();

        // if !membership.contains(&node_id) {
        //     return Ok(());
        // }

        // TODO(xp): deal with joint config
        assert!(membership.get(1).is_none());

        // safe unwrap: if the first config is None, panic is the expected behavior here.
        let mut membership = membership.get(0).unwrap().clone();

        membership.remove(&node_id);
        if !membership.contains(&node_id) {
            return Ok(());
        }

        // let req = AppRequest::AddNode {
        //     node_id,
        //     node: Node {
        //         // addr: addr,
        //         ..Default::default()
        //     },
        // };

        // self.write_to_leader(req.clone()).await?;

        self.change_membership(membership).await
    }

    // leader_addr returns the Raft address of the leader of the cluster.
    pub async fn leader_addr(&self) -> StoreResult<String> {
        let leader_node_id = self
            .raft
            .current_leader()
            .await
            .ok_or(APIError::NotLeader(format!("not found leader")))?;

        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = metrics.membership_config.get_node(leader_node_id);
        let node =
            leader_node.ok_or(APIError::NoLeaderError(format!("leader addr is not found")))?;
        Ok(String::from(&node.addr))
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.is_leader().await.is_ok()
    }

    // // stats returns stats on the Store.
    // pub async stats() (map[string]interface{}, error)

    // // nodes returns the slice of store.Servers in the cluster
    // pub async nodes() ([]*store.Server, error)

    // // backup wites backup of the node state to dst
    // pub async fn backup(leader:bool, f store.BackupFormat, dst io.Writer) ->StoreResult<()> {

    // }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn change_membership(&self, membership: BTreeSet<NodeId>) -> StoreResult<()> {
        let res = self.raft.change_membership(membership, true, false).await;

        let err = match res {
            Ok(_) => return Ok(()),
            Err(e) => e,
        };

        match err {
            ClientWriteError::ChangeMembershipError(e) => {
                Err(RaftError::ChangeMembershipError(e).into())
            }
            // TODO(xp): enable MetaNode::RaftError when RaftError impl Serialized
            ClientWriteError::Fatal(fatal) => Err(RaftError::RaftFatal(fatal).into()),
            ClientWriteError::ForwardToLeader(to_leader) => {
                Err(RaftError::ForwardToLeader(to_leader).into())
            }
        }
    }

    /// Write a log through local raft node and return the states before and after applying the log.
    ///
    /// If the raft node is not a leader, it returns MetaRaftError::ForwardToLeader.
    /// If the leadership is lost during writing the log, it returns an UnknownError.
    /// TODO(xp): elaborate the UnknownError, e.g. LeaderLostError
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn write_to_leader(&self, app_req: AppRequest) -> StoreResult<AppResponse> {
        let write_rst = self
            .raft
            .client_write(ClientWriteRequest::new(EntryPayload::Normal(app_req)))
            .await;

        tracing::debug!("raft.client_write rst: {:?}", write_rst);

        match write_rst {
            Ok(resp) => {
                let data = resp.data;
                match data {
                    // AppliedState::AppError(ae) => Err(StoreError::from(ae)),
                    _ => Ok(data),
                }
            }

            Err(cli_write_err) => match cli_write_err {
                // fatal error
                ClientWriteError::Fatal(fatal) => Err(RaftError::RaftFatal(fatal).into()),
                // retryable error
                ClientWriteError::ForwardToLeader(to_leader) => {
                    Err(RaftError::ForwardToLeader(to_leader).into())
                }
                ClientWriteError::ChangeMembershipError(_) => {
                    unreachable!("there should not be a ChangeMembershipError for client_write")
                }
            },
        }
    }
}
