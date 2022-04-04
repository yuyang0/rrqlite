use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use core_command::command;
use core_sled::openraft;
use core_sled::openraft::error::CheckIsLeaderError;
use core_tracing::tracing_futures::Instrument;
use openraft::{Config, SnapshotPolicy};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tonic::Status;

use crate::config::{RaftConfig, StoreConfig};
use crate::errors::{APIError, RaftError, StoreError, StoreResult};
use crate::fsm::{SQLFsm, FSM};
use crate::network::AppNetwork;
use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::protobuf::raft_service_server::RaftServiceServer;
use crate::protobuf::RaftReply;
use crate::service::RaftServiceImpl;
use crate::store::SledRaftStore;
use crate::types::openraft::{
    ClientWriteError, ClientWriteRequest, EntryPayload, ForwardToLeader, Node, NodeId, RaftMetrics,
};
use crate::types::{
    AppRequest, AppResponse, BackupFormat, ForwardRequest, ForwardRequestBody, ForwardResponse,
    JoinRequest, RemoveNodeRequest,
};
use crate::RqliteRaft;

// RqliteNode is the container of meta data related components and threads, such
// as storage, the raft node and a raft-state monitor.
pub struct RqliteNode {
    cfg: StoreConfig,
    fsm: Arc<SQLFsm>,
    pub sto: Arc<SledRaftStore>,
    pub raft: RqliteRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<StoreResult<()>>>>,

    last_contact_time: RwLock<Instant>,
}

impl RqliteNode {
    pub fn is_opened(&self) -> bool {
        self.sto.is_opened()
    }

    pub async fn set_last_contact_time(&self) {
        let mut _g = self.last_contact_time.write().await;
        *_g = std::time::Instant::now();
    }

    pub fn new_raft_config(config: &RaftConfig) -> Config {
        // TODO(xp): configure cluster name.

        let hb = config.raft_heartbeat_timeout * 1000;

        Config {
            cluster_name: "rqlited_cluster".to_string(),
            heartbeat_interval: hb,
            election_timeout_min: hb * 8,
            election_timeout_max: hb * 12,
            install_snapshot_timeout: config.install_snapshot_timeout * 1000,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snap_threshold),
            max_applied_log_to_keep: config.max_applied_log_to_keep,
            ..Default::default()
        }
        .validate()
        .expect("building raft Config from databend-metasrv config")
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[tracing::instrument(level = "debug", skip(mn))]
    pub async fn start_grpc(mn: Arc<RqliteNode>, addr: &str) -> StoreResult<()> {
        let mut rx = mn.running_rx.clone();

        let meta_srv_impl = RaftServiceImpl::create(mn.clone());
        let meta_srv = RaftServiceServer::new(meta_srv_impl);

        tracing::info!("about to start raft grpc on resolved addr {}", addr);

        let addr_str = addr.to_string();
        let ret = addr.parse::<std::net::SocketAddr>();
        let addr = match ret {
            Ok(addr) => addr,
            Err(e) => {
                return Err(StoreError::Other(anyerror::AnyError::new(&e)));
            }
        };
        let node_id = mn.sto.id;

        let srv = tonic::transport::Server::builder().add_service(meta_srv);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                tracing::info!(
                    "signal received, shutting down: id={} {} ",
                    node_id,
                    addr_str
                );
            })
            .await
            .map_err(|e| StoreError::Other(anyerror::AnyError::new(&e)))?;

            Ok::<(), StoreError>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Open or create a metasrv node.
    /// Optionally boot a single node cluster.
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create an one in non-voter mode.
    /// 3. If `init_cluster` is `Some` and it is just created, try to initialize
    /// a single-node cluster.
    ///
    /// TODO(xp): `init_cluster`: pass in a Map<id, address> to initialize the
    /// cluster.
    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.raft.config_id.as_str()))]
    pub async fn open_create_boot(
        config: &StoreConfig,
        open: Option<()>,
        create: Option<()>,
        init_cluster: Option<Vec<String>>,
        monitor_metrics: bool,
    ) -> StoreResult<Arc<RqliteNode>> {
        let mut config = config.clone();

        // Always disable fsync on mac.
        // Because there are some integration tests running on mac VM.
        //
        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very
        // likely to fail a test.
        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.raft.no_sync = true;
        }

        let fsm = SQLFsm::new_with_config(&config.fsm)?;
        let fsm = Arc::new(fsm);

        let sto = SledRaftStore::open_create(&config.raft, fsm.clone(), open, create).await?;
        let is_open = sto.is_opened();
        let sto = Arc::new(sto);

        // // config.id only used for the first time
        // let self_node_id = if is_open { sto.id } else { config.raft.id };

        let rc = Self::new_raft_config(&config.raft);
        let network = AppNetwork::new(sto.clone());
        let raft = RqliteRaft::new(config.raft.id, Arc::new(rc), network, sto.clone());
        let metrics_rx = raft.metrics();

        let (tx, rx) = watch::channel::<()>(());

        let node = RqliteNode {
            cfg: config.clone(),
            fsm: fsm,
            sto: sto,
            raft: raft,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
            last_contact_time: RwLock::new(std::time::Instant::now()),
        };
        let node = Arc::new(node);

        if monitor_metrics {
            tracing::info!("about to subscribe raft metrics");
            Self::subscribe_metrics(node.clone(), metrics_rx).await;
        }

        Self::start_grpc(node.clone(), &config.raft.raft_addr).await?;

        tracing::info!("Node started: {:?}", config);

        // init_cluster with advertise_host other than listen_host
        if !is_open {
            if let Some(_addrs) = init_cluster {
                node.init_cluster().await?;
            }
        }
        Ok(node)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn stop(&self) -> StoreResult<i32> {
        // TODO(xp): need to be reentrant.

        let mut rx = self.raft.metrics();

        self.raft
            .shutdown()
            .await
            .map_err(|_e| StoreError::Other(anyerror::AnyError::error("failed to stop raft")))?;
        // safe unwrap: receiver wait for change.
        self.running_tx.send(()).unwrap();

        // wait for raft to close the metrics tx
        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
            tracing::info!("waiting for raft to shutdown, metrics: {:?}", rx.borrow());
        }
        tracing::info!("shutdown raft");

        // raft counts 1
        let mut joined = 1;
        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j
                .await
                .map_err(|_e| StoreError::Other(anyerror::AnyError::error("failed to join")))?;
            joined += 1;
        }

        tracing::info!("shutdown: id={}", self.sto.id);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        // TODO: return a handle for join
        // TODO: every state change triggers add_non_voter!!!
        let mut running_rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;

        // TODO: reduce dependency: it does not need all of the fields in MetaNode
        let mn = mn.clone();

        let span = tracing::span!(tracing::Level::INFO, "watch-metrics");

        let h = tokio::task::spawn(
            {
                async move {
                    loop {
                        let changed = tokio::select! {
                            _ = running_rx.changed() => {
                               return Ok::<(), StoreError>(());
                            }
                            changed = metrics_rx.changed() => {
                                changed
                            }
                        };
                        if changed.is_ok() {
                            let mm = metrics_rx.borrow().clone();
                            if let Some(cur) = mm.current_leader {
                                if cur == mn.sto.id {
                                    // TODO: check result
                                    let _rst = mn.add_configured_non_voters().await;

                                    if _rst.is_err() {
                                        tracing::info!(
                                            "fail to add non-voter: my id={}, rst:{:?}",
                                            mn.sto.id,
                                            _rst
                                        );
                                    }
                                }
                            }
                        } else {
                            // shutting down
                            break;
                        }
                    }

                    Ok::<(), StoreError>(())
                }
            }
            .instrument(span),
        );
        jh.push(h);
    }

    /// Start MetaNode in either `boot`, `single`, `join` or `open` mode,
    /// according to config.
    #[tracing::instrument(level = "debug", skip(config))]
    pub async fn start(config: &StoreConfig) -> StoreResult<Arc<RqliteNode>> {
        tracing::info!(?config, "start()");
        let mn = Self::do_start(config).await?;
        tracing::info!("Done starting MetaNode: {:?}", config);
        Ok(mn)
    }

    #[tracing::instrument(level = "info", skip(conf, self))]
    pub async fn join_cluster(&self, conf: &RaftConfig) -> StoreResult<()> {
        if conf.join.is_empty() {
            tracing::info!("--join config is empty");
            return Ok(());
        }

        // Try to join a cluster only when this node is just created.
        // Joining a node with log has risk messing up the data in this node and in the
        // target cluster.
        if self.is_opened() {
            tracing::info!("has opened");
            return Ok(());
        }

        let addrs = &conf.join;
        for _ in 0..self.cfg.raft.join_attempts {
            #[allow(clippy::never_loop)]
            for addr in addrs {
                tracing::info!("try to join cluster accross {}...", addr);

                let mut client = match RaftServiceClient::connect(format!("http://{}", addr)).await
                {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("connect to {} join cluster fail: {:?}", addr, e);
                        continue;
                    }
                };

                let admin_req = ForwardRequest {
                    forward_to_leader: 1,
                    body: ForwardRequestBody::Join(JoinRequest {
                        node_id: conf.id,
                        addr: String::from(&conf.join_src_ip),
                        voter: true,
                    }),
                };

                let result: std::result::Result<RaftReply, Status> =
                    match client.forward(admin_req.clone()).await {
                        Ok(r) => Ok(r.into_inner()),
                        Err(s) => {
                            tracing::error!("join cluster accross {} fail: {:?}", addr, s);
                            continue;
                        }
                    };

                match result {
                    Ok(reply) => {
                        if !reply.data.is_empty() {
                            tracing::info!(
                                "join cluster accross {} success: {:?}",
                                addr,
                                reply.data
                            );
                            return Ok(());
                        } else {
                            tracing::error!(
                                "join cluster accross {} fail: {:?}",
                                addr,
                                reply.error
                            );
                        }
                    }
                    Err(s) => {
                        tracing::error!("join cluster accross {} fail: {:?}", addr, s);
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(self.cfg.raft.join_interval)).await;
        }
        Err(
            RaftError::JoinClusterFail(format!("join cluster accross addrs {:?} fail", addrs))
                .into(),
        )
    }

    async fn do_start(conf: &StoreConfig) -> StoreResult<Arc<Self>> {
        if conf.raft.single {
            let mn = Self::open_create_boot(conf, Some(()), Some(()), Some(vec![]), true).await?;
            return Ok(mn);
        }

        if !conf.raft.join.is_empty() {
            // Bring up a new node, join it into a cluster
            let mn = Self::open_create_boot(conf, Some(()), Some(()), None, true).await?;

            if mn.is_opened() {
                return Ok(mn);
            }
            return Ok(mn);
        }
        // open mode
        let mn = Self::open_create_boot(conf, Some(()), None, None, true).await?;
        Ok(mn)
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    #[tracing::instrument(level = "debug", skip(config), fields(config_id=config.raft.config_id.as_str()))]
    pub async fn boot(config: &StoreConfig) -> StoreResult<Arc<Self>> {
        // 1. Bring a node up as non voter, start the grpc service for raft
        // communication. 2. Initialize itself as leader, because it is the only
        // one in the new cluster. 3. Add itself to the cluster storage by
        // committing an `add-node` log so that the cluster members(only this
        // node) is persisted.

        let node = Self::open_create_boot(config, None, Some(()), Some(vec![]), true).await?;

        Ok(node)
    }

    // Initialized a single node cluster by:
    // - Initializing raft membership.
    // - Adding current node into the meta data.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn init_cluster(&self) -> StoreResult<()> {
        let node_id = self.sto.id;

        // let mut cluster_node_ids = BTreeSet::new();
        // cluster_node_ids.insert(node_id);
        let mut cluster_nodes = BTreeMap::new();
        cluster_nodes.insert(
            node_id,
            Node {
                addr: String::from(&self.cfg.raft.raft_addr),
                data: Default::default(),
            },
        );

        let rst = self
            .raft
            .initialize(cluster_nodes)
            .await
            .map_err(|x| RaftError::InitializeError(format!("{:?}", x)))?;

        tracing::info!("initialized cluster, rst: {:?}", rst);

        // self.add_node(node_id, endpoint).await?;

        Ok(())
    }

    /// When a leader is established, it is the leader's responsibility to setup
    /// replication from itself to non-voters, AKA learners. openraft does
    /// not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_configured_non_voters(&self) -> StoreResult<()> {
        // TODO after leader established, add non-voter through apis
        // let node_ids = self.sto.list_non_voters().await;
        // for i in node_ids.iter() {
        //     let node = Some(Node {
        //         addr: req.addr,
        //         ..Default::default()
        //     });
        //     let x = self.raft.add_learner(*i, node, true).await;

        //     tracing::info!("add_non_voter result: {:?}", x);
        //     if x.is_ok() {
        //         tracing::info!("non-voter is added: {}", i);
        //     } else {
        //         tracing::info!("non-voter already exist: {}", i);
        //     }
        // }
        Ok(())
    }

    fn state_machine(&self) -> Arc<dyn FSM> {
        return self.sto.state_machine.clone();
    }

    pub async fn redirect_if_needed<T: Into<ForwardRequest>>(
        &self,
        req: T,
        redirect: bool,
    ) -> StoreResult<Option<ForwardResponse>> {
        // check leader
        if let Err(e) = self.raft.is_leader().await {
            match e {
                CheckIsLeaderError::ForwardToLeader(info) => {
                    if !redirect {
                        return Err(APIError::NotLeader(format!("Not a leader {}", info)).into());
                    }
                    let leader_id = match info.leader_id {
                        Some(id) => id,
                        None => self.leader_id().await?,
                    };
                    let resp = self.forward(&leader_id, req.into()).await?;
                    return Ok(Some(resp));
                }
                _ => return Err(APIError::NotLeader(format!("Not a leader {}", e)).into()),
            }
        }
        Ok(None)
    }

    pub async fn execute(
        &self,
        er: command::ExecuteRequest,
        redirect: bool,
    ) -> StoreResult<command::ExecuteResult> {
        // check leader
        let resp = self.redirect_if_needed(&er, redirect).await?;
        if let Some(forward_resp) = resp {
            match forward_resp {
                ForwardResponse::AppResponse(app_resp) => match app_resp {
                    AppResponse::Execute(es) => return Ok(es),
                    _ => panic!("BUG: need ExecuteResult"),
                },
                _ => panic!("BUG: need AppRsponse"),
            }
        }
        let l = self.as_leader().await;
        let app_resp = match l {
            Ok(l) => l.write(AppRequest::Execute(er)).await?,
            Err(e) => return Err(RaftError::ForwardToLeader(e).into()),
        };

        match app_resp {
            AppResponse::Execute(res) => Ok(res),
            _ => Err(APIError::Execute(format!("Need ExecuteResult, but got {}", app_resp)).into()),
        }
    }

    // Query the database, the meaning of level:
    // Strong: auto forward the request to leader if current node is follower, and
    // the leader will use RAFT alog to query the database. Weak: auto forward
    // request to leader if current node is follower and the leader will
    // query the local SQLite directly. None: just query the local SQLite directly.
    pub async fn query(
        &self,
        qr: command::QueryRequest,
        redirect: bool,
    ) -> StoreResult<command::QueryResult> {
        let level = command::query_request::Level::from_i32(qr.level)
            .ok_or(APIError::Query(format!("invalid query level {}", qr.level)))?;
        match level {
            command::query_request::Level::QueryRequestLevelStrong => {
                // check leader
                let resp = self.redirect_if_needed(&qr, redirect).await?;
                if let Some(forward_resp) = resp {
                    match forward_resp {
                        ForwardResponse::AppResponse(app_resp) => match app_resp {
                            AppResponse::Query(qs) => return Ok(qs),
                            _ => panic!("BUG: need QueryResult"),
                        },
                        _ => panic!("BUG: need AppRsponse"),
                    }
                }

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
                // TODO after remove rwlock in FSM, here we need to acquire read lock when
                // req.transaction is true.
                let res = self.state_machine().query(&qr).await?;
                Ok(res)
            }
            command::query_request::Level::QueryRequestLevelNone => {
                let elapsed = self.last_contact_time.read().await.elapsed();
                // .map_err(|e| APIError::Query(format!("{}", e)))?

                if qr.freshness > 0 && elapsed.as_nanos() > (qr.freshness as u128) {
                    return Err(APIError::StaleRead(String::from("")).into());
                }

                // TODO after remove rwlock in FSM, here we need to acquire read lock when
                // req.transaction is true.
                let res = self.state_machine().query(&qr).await?;
                Ok(res)
            }
        }
    }

    // join the node with the given ID, reachable at addr, to this node.
    pub async fn join(&self, req: JoinRequest) -> StoreResult<()> {
        // TODO: check if this node is already in learner list.
        let resp = self.redirect_if_needed(&req, true).await?;

        if let Some(forward_resp) = resp {
            match forward_resp {
                ForwardResponse::Join(_) => return Ok(()),
                _ => panic!("BUG: need JoinResponse"),
            }
        }

        let l = self.as_leader().await;
        match l {
            Ok(l) => l.join(req).await,
            Err(e) => return Err(RaftError::ForwardToLeader(e).into()),
        }
    }

    // notify this node that a node is available at addr.
    pub async fn notify(id: &str, addr: &str) -> StoreResult<()> {
        // TODO: implement this
        (id, addr);
        Ok(())
    }

    // remove the node, specified by id, from the cluster.
    pub async fn remove(&self, req: RemoveNodeRequest) -> StoreResult<()> {
        let resp = self.redirect_if_needed(&req, true).await?;

        if let Some(forward_resp) = resp {
            match forward_resp {
                ForwardResponse::RemoveNode(_) => return Ok(()),
                _ => panic!("BUG: need JoinResponse"),
            }
        }
        let l = self.as_leader().await;
        match l {
            Ok(l) => l.remove(req.node_id).await,
            Err(e) => return Err(RaftError::ForwardToLeader(e).into()),
        }
    }

    pub async fn leader_id(&self) -> StoreResult<NodeId> {
        let leader_node_id = self
            .raft
            .current_leader()
            .await
            .ok_or(APIError::NotLeader(format!("not found leader")))?;
        Ok(leader_node_id)
    }

    // leader_addr returns the Raft address of the leader of the cluster.
    pub async fn leader_addr(&self) -> StoreResult<String> {
        let leader_node_id = self
            .raft
            .current_leader()
            .await
            .ok_or(APIError::NotLeader(format!("not found leader")))?;

        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = metrics.membership_config.get_node(&leader_node_id);
        let node =
            leader_node.ok_or(APIError::NoLeaderError(format!("leader addr is not found")))?;
        Ok(String::from(&node.addr))
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> StoreResult<String> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = metrics.membership_config.get_node(node_id);
        let node = leader_node.ok_or(APIError::GetNodeAddrError(format!(
            "Node({})'s addr is not found",
            node_id
        )))?;
        Ok(String::from(&node.addr))
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.is_leader().await.is_ok()
    }

    // // stats returns stats on the Store.
    // pub async stats() (map[string]interface{}, error)

    // // nodes returns the slice of store.Servers in the cluster
    // pub async nodes() ([]*store.Server, error)

    // backup wites backup of the node state to dst
    pub async fn backup(&self, leader: bool, f: BackupFormat) -> StoreResult<Vec<u8>> {
        if leader {
            let req = ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Backup(f.clone()),
            };
            let resp = self.redirect_if_needed(req, leader).await?;
            let data = match resp {
                Some(forward_resp) => match forward_resp {
                    ForwardResponse::Backup(data) => data,
                    _ => panic!("BUG: need AppRsponse"),
                },
                None => {
                    let data = self.fsm.backup(f)?;
                    data
                }
            };
            return Ok(data);
        } else {
            let data = self.fsm.backup(f)?;
            return Ok(data);
        }
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is
    /// set.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_leader(&self) -> NodeId {
        // fast path: there is a known leader

        if let Some(l) = self.raft.metrics().borrow().current_leader {
            return l;
        }

        // slow path: wait loop

        // Need to clone before calling changed() on it.
        // Otherwise other thread waiting on changed() may not receive the change event.
        let mut rx = self.raft.metrics();

        loop {
            // NOTE:
            // The metrics may have already changed before we cloning it.
            // Thus we need to re-check the cloned rx.
            if let Some(l) = rx.borrow().current_leader {
                return l;
            }

            let changed = rx.changed().await;
            if changed.is_err() {
                tracing::info!("raft metrics tx closed");
                return 0;
            }
        }
    }

    pub async fn handle_forwardable_request(
        &self,
        req: ForwardRequest,
    ) -> StoreResult<ForwardResponse> {
        tracing::debug!("handle_forwardable_request: {:?}", req);
        let curr_leader = self.get_leader().await;
        // current is leader.
        if curr_leader == self.sto.id {
            let l = self.as_leader().await;
            let resp = match l {
                Ok(l) => l.handle_forwardable_request(req).await?,
                Err(e) => return Err(RaftError::ForwardToLeader(e).into()),
            };
            return Ok(resp);
        }

        // if current node is not leader, then forward to real leader.
        let forward = req.forward_to_leader;
        if forward == 0 {
            return Err(APIError::RequestNotForwardToLeaderError(
                "req not forward to leader".to_string(),
            )
            .into());
        }

        let mut r2 = req.clone();
        // Avoid infinite forward
        r2.decr_forward();

        let res: ForwardResponse = self.forward(&curr_leader, r2).await?;

        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn forward(
        &self,
        node_id: &NodeId,
        req: ForwardRequest,
    ) -> StoreResult<ForwardResponse> {
        let addr = self.get_node_addr(node_id).await?;
        let mut client = RaftServiceClient::connect(format!("http://{}", addr))
            .await
            .map_err(|e| APIError::ConnectionError(format!("address: {}, {}", addr, e)))?;

        let resp = client
            .forward(req)
            .await
            .map_err(|e| APIError::ForwardRequestError(e.to_string()))?;
        let raft_mes = resp.into_inner();

        let res: StoreResult<ForwardResponse> = raft_mes.into();
        res
    }

    /// Return a AppLeader if `self` believes it is the leader.
    ///
    /// Otherwise it returns the leader in a ForwardToLeader error.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn as_leader(&self) -> Result<AppLeader<'_>, ForwardToLeader> {
        let curr_leader = self.get_leader().await;
        if curr_leader == self.sto.id {
            return Ok(AppLeader::new(self));
        }
        Err(ForwardToLeader {
            leader_id: Some(curr_leader),
            // TODO: add address information
            leader_node: Default::default(),
        })
    }

    pub fn get_metrics(&self) -> RaftMetrics {
        self.raft.metrics().borrow().clone()
    }
}

pub struct AppLeader<'a> {
    app_node: &'a RqliteNode,
}

impl<'a> AppLeader<'a> {
    pub fn new(app_node: &'a RqliteNode) -> AppLeader {
        AppLeader { app_node }
    }

    // join the node with the given ID, reachable at addr, to this node.
    pub async fn join(&self, req: JoinRequest) -> StoreResult<()> {
        // TODO: check if this node is already in learner list.
        let node = Some(Node {
            addr: req.addr,
            ..Default::default()
        });
        self.app_node
            .raft
            .add_learner(req.node_id, node, true)
            .await
            .map_err(|e| APIError::AddLearner(format!("{}", e)))?;

        if !req.voter {
            return Ok(());
        }
        let metrics = self.app_node.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.get_configs();

        // TODO(xp): deal with joint config
        assert!(membership.get(1).is_none());

        // safe unwrap: if the first config is None, panic is the expected behavior
        // here.
        let mut membership = membership.get(0).unwrap().clone();

        if membership.contains(&req.node_id) {
            return Ok(());
        }
        membership.insert(req.node_id);

        self.change_membership(membership).await
    }

    // remove the node, specified by id, from the cluster.
    pub async fn remove(&self, node_id: NodeId) -> StoreResult<()> {
        let metrics = self.app_node.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.get_configs();

        // if !membership.contains(&node_id) {
        //     return Ok(());
        // }

        // TODO(xp): deal with joint config
        assert!(membership.get(1).is_none());

        // safe unwrap: if the first config is None, panic is the expected behavior
        // here.
        let mut membership = membership.get(0).unwrap().clone();

        membership.remove(&node_id);
        if !membership.contains(&node_id) {
            return Ok(());
        }
        self.change_membership(membership).await
    }

    /// Write a log through local raft node and return the states before and
    /// after applying the log.
    ///
    /// If the raft node is not a leader, it returns
    /// MetaRaftError::ForwardToLeader. If the leadership is lost during
    /// writing the log, it returns an UnknownError. TODO(xp): elaborate the
    /// UnknownError, e.g. LeaderLostError
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn write(&self, app_req: AppRequest) -> StoreResult<AppResponse> {
        let write_rst = self
            .app_node
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn change_membership(&self, membership: BTreeSet<NodeId>) -> StoreResult<()> {
        let res = self
            .app_node
            .raft
            .change_membership(membership, true, false)
            .await;

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

    pub async fn handle_forwardable_request(
        &self,
        req: ForwardRequest,
    ) -> StoreResult<ForwardResponse> {
        tracing::debug!("handle_forwardable_request: {:?}", req);
        let resp = match req.body {
            ForwardRequestBody::Join(join_req) => {
                self.join(join_req).await?;
                ForwardResponse::Join(())
            }
            ForwardRequestBody::RemoveNode(req) => {
                self.remove(req.node_id).await?;
                ForwardResponse::RemoveNode(())
            }
            ForwardRequestBody::App(app_req) => {
                let app_resp = self.write(app_req).await?;
                ForwardResponse::AppResponse(app_resp)
            }
            ForwardRequestBody::Backup(f) => {
                let data = self.app_node.fsm.backup(f)?;
                ForwardResponse::Backup(data)
            }
        };
        return Ok(resp);
    }
}
