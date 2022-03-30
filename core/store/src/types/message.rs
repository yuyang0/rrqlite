use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use super::app_data::{AppRequest, AppResponse};
use super::openraft::{Entry, NodeId};
use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum BackupFormat {
    Binary,
    SQL,
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub voter: bool,
    pub addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RemoveNodeRequest {
    pub node_id: NodeId,
}

#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::From, derive_more::TryInto,
)]
pub enum ForwardRequestBody {
    Join(JoinRequest),
    RemoveNode(RemoveNodeRequest),
    App(AppRequest),
    Backup(BackupFormat),
}

/// A request that is forwarded from one raft node to another
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ForwardRequest {
    /// Forward the request to leader if the node received this request is not leader.
    pub forward_to_leader: u64,

    pub body: ForwardRequestBody,
}

impl ForwardRequest {
    pub fn decr_forward(&mut self) {
        self.forward_to_leader -= 1;
    }
}

impl From<&JoinRequest> for ForwardRequest {
    fn from(jr: &JoinRequest) -> Self {
        ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::Join(jr.clone()),
        }
    }
}

impl From<&RemoveNodeRequest> for ForwardRequest {
    fn from(req: &RemoveNodeRequest) -> Self {
        ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::RemoveNode(req.clone()),
        }
    }
}

impl From<&core_command::command::ExecuteRequest> for ForwardRequest {
    fn from(er: &core_command::command::ExecuteRequest) -> Self {
        ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::App(AppRequest::Execute(er.clone())),
        }
    }
}

impl From<&core_command::command::QueryRequest> for ForwardRequest {
    fn from(qr: &core_command::command::QueryRequest) -> Self {
        ForwardRequest {
            forward_to_leader: 1,
            body: ForwardRequestBody::App(AppRequest::Query(qr.clone())),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::TryInto)]
#[allow(clippy::large_enum_variant)]
pub enum ForwardResponse {
    Join(()),
    RemoveNode(()),
    AppResponse(AppResponse),
    Backup(Vec<u8>),
}

impl tonic::IntoRequest<RaftRequest> for ForwardRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for ForwardRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}

impl tonic::IntoRequest<RaftRequest> for Entry {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for Entry {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req: Entry = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}

impl From<RetryableError> for RaftReply {
    fn from(err: RetryableError) -> Self {
        let error = serde_json::to_string(&err).expect("fail to serialize");
        RaftReply {
            data: "".to_string(),
            error,
        }
    }
}

impl From<AppResponse> for RaftReply {
    fn from(msg: AppResponse) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftReply {
            data,
            error: "".to_string(),
        }
    }
}

impl<T, E> From<RaftReply> for Result<T, E>
where
    T: DeserializeOwned,
    E: DeserializeOwned,
{
    fn from(msg: RaftReply) -> Self {
        if !msg.data.is_empty() {
            let resp: T = serde_json::from_str(&msg.data).expect("fail to deserialize");
            Ok(resp)
        } else {
            let err: E = serde_json::from_str(&msg.error).expect("fail to deserialize");
            Err(err)
        }
    }
}

impl<T, E> From<Result<T, E>> for RaftReply
where
    T: Serialize,
    E: Serialize,
{
    fn from(r: Result<T, E>) -> Self {
        match r {
            Ok(x) => {
                let data = serde_json::to_string(&x).expect("fail to serialize");
                RaftReply {
                    data,
                    error: Default::default(),
                }
            }
            Err(e) => {
                let error = serde_json::to_string(&e).expect("fail to serialize");
                RaftReply {
                    data: Default::default(),
                    error,
                }
            }
        }
    }
}
