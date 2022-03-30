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

use std::fmt;

use crate::types::openraft::{EffectiveMembership, LogId, NodeId, Vote};
use anyerror::AnyError;
use core_sled::{sled, SledOrderedSerde, SledStorageError};
use serde::Deserialize;
use serde::Serialize;
use sled::IVec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogMetaKey {
    /// The last purged log id in the log.
    ///
    /// Log entries are purged after being applied to state machine.
    /// Even when all logs are purged the last purged log id has to be stored.
    /// Because raft replication requires logs to be consecutive.
    LastPurged,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, derive_more::TryInto)]
pub enum LogMetaValue {
    LogId(LogId),
}

impl fmt::Display for LogMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogMetaKey::LastPurged => {
                write!(f, "last-purged")
            }
        }
    }
}

impl SledOrderedSerde for LogMetaKey {
    fn ser(&self) -> Result<IVec, SledStorageError> {
        let i = match self {
            LogMetaKey::LastPurged => 1,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledStorageError>
    where
        Self: Sized,
    {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(LogMetaKey::LastPurged);
        }

        Err(SledStorageError::SledError(AnyError::error(
            "invalid key IVec",
        )))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftStateKey {
    /// The node id.
    Id,

    Vote,

    /// The id of the only active state machine.
    /// When installing a state machine snapshot:
    /// 1. A temp state machine is written into a new sled::Tree.
    /// 2. Update this field to point to the new state machine.
    /// 3. Cleanup old state machine.
    StateMachineId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStateValue {
    NodeId(NodeId),
    Vote(Vote),
    /// active state machine, previous state machine
    StateMachineId((u64, u64)),
}

impl fmt::Display for RaftStateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftStateKey::Id => {
                write!(f, "Id")
            }
            RaftStateKey::Vote => {
                write!(f, "Vote")
            }
            RaftStateKey::StateMachineId => {
                write!(f, "StateMachineId")
            }
        }
    }
}

impl SledOrderedSerde for RaftStateKey {
    fn ser(&self) -> Result<IVec, SledStorageError> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::Vote => 2,
            RaftStateKey::StateMachineId => 3,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledStorageError>
    where
        Self: Sized,
    {
        let slice = v.as_ref();
        match slice[0] {
            1 => Ok(RaftStateKey::Id),
            2 => Ok(RaftStateKey::Vote),
            3 => Ok(RaftStateKey::StateMachineId),
            _ => Err(SledStorageError::SledError(AnyError::error(
                "invalid key IVec",
            ))),
        }
    }
}

impl From<RaftStateValue> for NodeId {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::NodeId(x) => x,
            _ => panic!("expect NodeId"),
        }
    }
}

impl From<RaftStateValue> for Vote {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::Vote(vt) => vt,
            _ => panic!("expect Vote"),
        }
    }
}

impl From<RaftStateValue> for (u64, u64) {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::StateMachineId(x) => x,
            _ => panic!("expect StateMachineId"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FSMMetaKey {
    /// The last applied log id in the state machine.
    LastApplied,

    /// Whether the state machine is initialized.
    Initialized,

    /// The last membership config
    LastMembership,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, derive_more::TryInto)]
pub enum StateMachineMetaValue {
    LogId(LogId),
    Bool(bool),
    Membership(EffectiveMembership),
}

impl fmt::Display for FSMMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FSMMetaKey::LastApplied => {
                write!(f, "last-applied")
            }
            FSMMetaKey::Initialized => {
                write!(f, "initialized")
            }
            FSMMetaKey::LastMembership => {
                write!(f, "last-membership")
            }
        }
    }
}

impl SledOrderedSerde for FSMMetaKey {
    fn ser(&self) -> Result<IVec, SledStorageError> {
        let i = match self {
            FSMMetaKey::LastApplied => 1,
            FSMMetaKey::Initialized => 2,
            FSMMetaKey::LastMembership => 3,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledStorageError>
    where
        Self: Sized,
    {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(FSMMetaKey::LastApplied);
        } else if slice[0] == 2 {
            return Ok(FSMMetaKey::Initialized);
        } else if slice[0] == 3 {
            return Ok(FSMMetaKey::LastMembership);
        }

        Err(SledStorageError::SledError(AnyError::error(
            "invalid key IVec",
        )))
    }
}
