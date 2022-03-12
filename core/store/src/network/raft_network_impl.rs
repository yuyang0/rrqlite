use std::sync::Arc;
use std::time::Duration;

use crate::protobuf::raft_service_client::RaftServiceClient;
use crate::types::openraft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use core_sled::openraft;
use core_tracing::tracing;
use core_util_containers::{ItemManager, Pool};
use openraft::async_trait::async_trait;
use openraft::error::AppendEntriesError;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
// use openraft::error::RemoteError;
use openraft::error::VoteError;
use openraft::MessageSummary;
use openraft::Node;
use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
// use serde::Serialize;
use tonic::client::GrpcService;
use tonic::transport::channel::Channel;

use crate::store::SledRaftStore;
use crate::types::openraft::NodeId;
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

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
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

// use async_trait::async_trait;
// use openraft::error::AppendEntriesError;
// use openraft::error::InstallSnapshotError;
// use openraft::error::NetworkError;
// use openraft::error::RPCError;
// use openraft::error::RemoteError;
// use openraft::error::VoteError;
// use openraft::raft::AppendEntriesRequest;
// use openraft::raft::AppendEntriesResponse;
// use openraft::raft::InstallSnapshotRequest;
// use openraft::raft::InstallSnapshotResponse;
// use openraft::raft::VoteRequest;
// use openraft::raft::VoteResponse;
// use openraft::Node;
// use openraft::RaftNetwork;
// use openraft::RaftNetworkFactory;
// use serde::de::DeserializeOwned;
// use serde::Serialize;

// use crate::types::openraft::NodeId;
// use crate::RqliteTypeConfig;

// pub struct AppNetwork {}

// impl AppNetwork {
//     pub async fn send_rpc<Req, Resp, Err>(
//         &self,
//         target: ExampleNodeId,
//         target_node: Option<&Node>,
//         uri: &str,
//         req: Req,
//     ) -> Result<Resp, RPCError<RqliteTypeConfig, Err>>
//     where
//         Req: Serialize,
//         Err: std::error::Error + DeserializeOwned,
//         Resp: DeserializeOwned,
//     {
//         let addr = target_node.map(|x| &x.addr).unwrap();

//         let url = format!("http://{}/{}", addr, uri);
//         let client = reqwest::Client::new();

//         let resp = client
//             .post(url)
//             .json(&req)
//             .send()
//             .await
//             .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

//         let res: Result<Resp, Err> = resp
//             .json()
//             .await
//             .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

//         res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
//     }
// }

// // NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
// #[async_trait]
// impl RaftNetworkFactory<RqliteTypeConfig> for AppNetwork {
//     type Network = ExampleNetworkConnection;

//     async fn connect(&mut self, target: ExampleNodeId, node: Option<&Node>) -> Self::Network {
//         ExampleNetworkConnection {
//             owner: AppNetwork {},
//             target,
//             target_node: node.cloned(),
//         }
//     }
// }

// pub struct ExampleNetworkConnection {
//     owner: AppNetwork,
//     target: ExampleNodeId,
//     target_node: Option<Node>,
// }

// #[async_trait]
// impl RaftNetwork<RqliteTypeConfig> for ExampleNetworkConnection {
//     async fn send_append_entries(
//         &mut self,
//         req: AppendEntriesRequest<RqliteTypeConfig>,
//     ) -> Result<
//         AppendEntriesResponse<RqliteTypeConfig>,
//         RPCError<RqliteTypeConfig, AppendEntriesError<ExampleTypeConfig>>,
//     > {
//         self.owner
//             .send_rpc(self.target, self.target_node.as_ref(), "raft-append", req)
//             .await
//     }

//     async fn send_install_snapshot(
//         &mut self,
//         req: InstallSnapshotRequest<RqliteTypeConfig>,
//     ) -> Result<
//         InstallSnapshotResponse<RqliteTypeConfig>,
//         RPCError<RqliteTypeConfig, InstallSnapshotError<RqliteTypeConfig>>,
//     > {
//         self.owner
//             .send_rpc(self.target, self.target_node.as_ref(), "raft-snapshot", req)
//             .await
//     }

//     async fn send_vote(
//         &mut self,
//         req: VoteRequest<RqliteTypeConfig>,
//     ) -> Result<
//         VoteResponse<RqliteTypeConfig>,
//         RPCError<RqliteTypeConfig, VoteError<RqliteTypeConfig>>,
//     > {
//         self.owner
//             .send_rpc(self.target, self.target_node.as_ref(), "raft-vote", req)
//             .await
//     }
// }
