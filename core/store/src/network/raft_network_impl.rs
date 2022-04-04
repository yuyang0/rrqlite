use std::sync::Arc;
use std::time::Duration;

use core_sled::openraft;
use core_tracing::tracing;
use core_util_containers::{ItemManager, Pool};
// use openraft::error::RemoteError;
use openraft::{
    async_trait::async_trait,
    error::{AppendEntriesError, InstallSnapshotError, NetworkError, RPCError, VoteError},
    MessageSummary, Node, RaftNetwork, RaftNetworkFactory,
};
use serde::de::DeserializeOwned;
// use serde::Serialize;
use tonic::{client::GrpcService, transport::channel::Channel};

use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::store::SledRaftStore;
use crate::types::openraft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    NodeId, VoteRequest, VoteResponse,
};
use crate::RqliteTypeConfig;

struct ChannelManager {}

#[async_trait]
impl ItemManager for ChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = tonic::transport::Error;

    async fn build(&self, addr: &Self::Key) -> Result<Channel, tonic::transport::Error> {
        tonic::transport::Endpoint::new(addr.clone())?
            .connect()
            .await
    }

    async fn check(&self, mut ch: Channel) -> Result<Channel, tonic::transport::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx)).await?;
        Ok(ch)
    }
}

#[derive(Clone)]
pub struct AppNetwork {
    sto: Arc<SledRaftStore>,
    conn_pool: Arc<Pool<ChannelManager>>,
}

impl AppNetwork {
    pub fn new(sto: Arc<SledRaftStore>) -> AppNetwork {
        let mgr = ChannelManager {};
        AppNetwork {
            sto,
            conn_pool: Arc::new(Pool::new(mgr, Duration::from_millis(50))),
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.sto.id))]
    pub async fn make_client<Err>(
        &self,
        target: &NodeId,
        target_node: Option<&Node>,
    ) -> Result<RaftServiceClient<Channel>, RPCError<RqliteTypeConfig, Err>>
    where
        Err: std::error::Error + DeserializeOwned,
    {
        // let endpoint = self
        //     .sto
        //     .get_node_endpoint(target)
        //     .await
        //     .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let addr = target_node.map(|x| &x.addr).unwrap();
        let url = format!("http://{}", addr);

        tracing::debug!("connect: target={}: {}", target, url);

        let channel = self
            .conn_pool
            .get(&url)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let client = RaftServiceClient::new(channel);

        tracing::info!("connected: target={}: {}", target, url);

        Ok(client)
    }
}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's
// empty, implemented directly.
#[async_trait]
impl RaftNetworkFactory<RqliteTypeConfig> for AppNetwork {
    type Network = AppNetworkConnection;

    async fn connect(&mut self, target: NodeId, node: Option<&Node>) -> Self::Network {
        AppNetworkConnection {
            owner: self.clone(),
            target,
            target_node: node.cloned(),
        }
    }
}

pub struct AppNetworkConnection {
    owner: AppNetwork,
    target: NodeId,
    target_node: Option<Node>,
}

#[async_trait]
impl RaftNetwork<RqliteTypeConfig> for AppNetworkConnection {
    #[tracing::instrument(level = "debug", skip(self), fields(id=self.owner.sto.id, rpc=%rpc.summary()))]
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
    ) -> Result<
        AppendEntriesResponse,
        RPCError<RqliteTypeConfig, AppendEntriesError<RqliteTypeConfig>>,
    > {
        tracing::debug!("append_entries req to: id={}: {:?}", self.target, rpc);

        let mut client = self
            .owner
            .make_client(&self.target, self.target_node.as_ref())
            .await?;

        let req = core_tracing::inject_span_to_tonic_request(rpc);

        let resp = client.append_entries(req).await;
        tracing::debug!("append_entries resp from: id={}: {:?}", self.target, resp);

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.owner.sto.id, rpc=%rpc.summary()))]
    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
    ) -> Result<
        InstallSnapshotResponse,
        RPCError<RqliteTypeConfig, InstallSnapshotError<RqliteTypeConfig>>,
    > {
        tracing::debug!("install_snapshot req to: id={}", self.target);

        let mut client = self
            .owner
            .make_client(&self.target, self.target_node.as_ref())
            .await?;
        let req = core_tracing::inject_span_to_tonic_request(rpc);
        let resp = client.install_snapshot(req).await;
        tracing::debug!("install_snapshot resp from: id={}: {:?}", self.target, resp);

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.owner.sto.id))]
    async fn send_vote(
        &mut self,
        rpc: VoteRequest,
    ) -> Result<VoteResponse, RPCError<RqliteTypeConfig, VoteError<RqliteTypeConfig>>> {
        tracing::debug!("vote: req to: target={} {:?}", self.target, rpc);

        let mut client = self
            .owner
            .make_client(&self.target, self.target_node.as_ref())
            .await?;
        let req = core_tracing::inject_span_to_tonic_request(rpc);
        let resp = client.vote(req).await;
        tracing::info!("vote: resp from target={} {:?}", self.target, resp);

        let resp = resp.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(resp)
    }
}
