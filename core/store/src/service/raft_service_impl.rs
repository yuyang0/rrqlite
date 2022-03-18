// use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
// use std::time::Duration;

use crate::protobuf::raft_service_server::RaftService;
// use crate::protobuf::GetReply;
// use crate::protobuf::GetRequest;
use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;
use crate::RqliteNode;

// use bincode::serialize;
// use tokio::sync::mpsc;
// use tokio::sync::oneshot;
// use tokio::time::timeout;
// use tonic::transport::Server;
// use tonic::{Request, Response, Status};
// use tracing::{error, info, warn};

pub struct RaftServiceImpl {
    pub rqlite_node: Arc<RqliteNode>,
}

impl RaftServiceImpl {
    pub fn create(rqlite_node: Arc<RqliteNode>) -> Self {
        Self { rqlite_node }
    }
    // pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message>, addr: A) -> Self {
    //     let addr = addr.to_socket_addrs().unwrap().next().unwrap();
    //     RaftServiceImpl { snd, addr }
    // }

    // pub async fn run(self) {
    //     let addr = self.addr;
    //     info!("listening gRPC requests on: {}", addr);
    //     let svc = RaftServiceServer::new(self);
    //     Server::builder()
    //         .add_service(svc)
    //         .serve(addr)
    //         .await
    //         .expect("error running server");
    //     warn!("server has quit");
    // }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn forward(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        core_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let admin_req: ForwardRequest = serde_json::from_str(&req.data)
            .map_err(|x| tonic::Status::invalid_argument(x.to_string()))?;

        let res = self.rqlite_node.handle_forwardable_request(admin_req).await;

        let raft_mes: RaftReply = res.into();

        Ok(tonic::Response::new(raft_mes))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        core_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let ae_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .rqlite_node
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        core_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let is_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .rqlite_node
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        core_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let v_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .rqlite_node
            .raft
            .vote(v_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
}
