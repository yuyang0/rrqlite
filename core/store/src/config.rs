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

use clap::Parser;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::errors::{StoreError, StoreResult};
use crate::types::openraft::NodeId;

pub static DATABEND_COMMIT_VERSION: Lazy<String> = Lazy::new(|| {
    let build_semver = option_env!("VERGEN_BUILD_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
        #[cfg(not(feature = "simd"))]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
        #[cfg(feature = "simd")]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => {
            format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
        }
        _ => String::new(),
    };
    ver
});

pub const KVSRV_LISTEN_HOST: &str = "KVSRV_LISTEN_HOST";
pub const KVSRV_ADVERTISE_HOST: &str = "KVSRV_ADVERTISE_HOST";
pub const KVSRV_API_PORT: &str = "KVSRV_API_PORT";
pub const KVSRV_RAFT_DIR: &str = "KVSRV_RAFT_DIR";
pub const KVSRV_NO_SYNC: &str = "KVSRV_NO_SYNC";
pub const KVSRV_SNAPSHOT_LOGS_SINCE_LAST: &str = "KVSRV_SNAPSHOT_LOGS_SINCE_LAST";
pub const KVSRV_HEARTBEAT_INTERVAL: &str = "KVSRV_HEARTBEAT_INTERVAL";
pub const KVSRV_INSTALL_SNAPSHOT_TIMEOUT: &str = "KVSRV_INSTALL_SNAPSHOT_TIMEOUT";
pub const KVSRV_BOOT: &str = "KVSRV_BOOT";
pub const KVSRV_SINGLE: &str = "KVSRV_SINGLE";
pub const KVSRV_ID: &str = "KVSRV_ID";

pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Parser)]
#[serde(default)]
pub struct FSMConfig {
    // OnDisk enables on-disk mode.
    #[clap(long = "on-disk", env = "RQLITED_ON_DISK")]
    pub on_disk: bool,

    // OnDiskPath sets the path to the SQLite file. May not be set.
    #[clap(
        long = "on-disk-path",
        env = "RQLITED_ON_DISK_PATH",
        default_value = ""
    )]
    pub on_disk_path: String,

    // OnDiskStartup disables the in-memory on-disk startup optimization.
    #[clap(long = "on-disk-startup", env = "RQLITED_ON_DISK_STARTUP")]
    pub on_disk_startup: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, Parser)]
#[serde(default)]
pub struct StoreConfig {
    #[clap(flatten)]
    pub fsm: FSMConfig,

    #[clap(flatten)]
    pub raft: RaftConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser, Default)]
#[serde(default)]
pub struct NodeConfig {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser, Default)]
#[serde(default)]
pub struct RaftConfig {
    /// Identify a config.
    /// This is only meant to make debugging easier with more than one Config
    /// involved.
    #[clap(long, default_value = "")]
    pub config_id: String,

    // RaftAddr is the bind network address for the Raft server.
    #[clap(
        long = "raft-addr",
        env = "RQLITED_RAFT_ADDR",
        default_value = "localhost:4002"
    )]
    pub raft_addr: String,

    // RaftAdv is the advertised Raft server address.
    #[clap(
        long = "raft-adv-addr",
        env = "RQLITED_RAFT_ADV_ADDR",
        default_value = ""
    )]
    pub raft_adv_addr: String,

    // JoinSrcIP sets the source IP address during Join request. May not be set.
    #[clap(
        long = "join-source-ip",
        env = "RQLITED_JOIN_SOURCE_IP",
        default_value = ""
    )]
    pub join_src_ip: String,

    /// Bring up a metasrv node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which
    /// this node sends a `join` request.
    #[clap(
        long = "join",
        env = "RQLITED_JOIN",
        multiple_occurrences = true,
        multiple_values = true
    )]
    pub join: Vec<String>,

    // JoinAs sets the user join attempts should be performed as. May not be set.
    #[clap(long = "join-as", env = "RQLITED_JOIN_AS", default_value = "")]
    pub join_as: String,

    // JoinAttempts is the number of times a node should attempt to join using a
    // given address.
    #[clap(
        long = "join-attempts",
        env = "RQLITED_JOIN_ATTEMPTS",
        default_value_t = 5
    )]
    pub join_attempts: u64,

    // JoinInterval is the time between retrying failed join operations.
    #[clap(
        long = "join-interval",
        env = "RQLITED_JOIN_INTERVAL",
        default_value_t = 3
    )]
    pub join_interval: u64,

    // // RaftLogLevel sets the minimum logging level for the Raft subsystem.
    // #[clap(
    //     long = "raft-log-level",
    //     env = "RQLITED_RAFT_LOG_LEVEL",
    //     default_value = "INFO"
    // )]
    // log_level: String,

    // RaftNonVoter controls whether this node is a voting, read-only node.
    #[clap(long = "raft-no-voter", env = "RQLITED_RAFT_NO_VOTER")]
    pub non_voter: bool,

    // RaftSnapThreshold is the number of outstanding log entries that trigger snapshot.
    #[clap(long = "raft-snap", env = "RQLITED_RAFT_SNAP", default_value_t = 8192)]
    pub snap_threshold: u64,

    // RaftSnapInterval sets the threshold check interval.
    #[clap(
        long = "raft-snap-int",
        env = "RQLITED_RAFT_SNAP_INT",
        default_value_t = 30
    )]
    pub snap_interval: u64,

    // RaftLeaderLeaseTimeout sets the leader lease timeout.
    #[clap(
        long = "raft-leader-lease-timeout",
        env = "RQLITED_RAFT_LEADER_LEASE_TIMEOUT",
        default_value_t = 0
    )]
    pub raft_leader_lease_timeout: u64,

    // RaftHeartbeatTimeout sets the heartbeast timeout.
    #[clap(
        long = "raft-timeout",
        env = "RQLITED_RAFT_TIMEOUT",
        default_value_t = 1
    )]
    pub raft_heartbeat_timeout: u64,

    // // RaftElectionTimeout sets the election timeout.
    // RaftElectionTimeout time.Duration

    // // RaftApplyTimeout sets the Log-apply timeout.
    // RaftApplyTimeout time.Duration

    // // RaftShutdownOnRemove sets whether Raft should be shutdown if the node is removed
    // RaftShutdownOnRemove bool

    // // RaftNoFreelistSync disables syncing Raft database freelist to disk. When true,
    // // it improves the database write performance under normal operation, but requires
    // // a full database re-sync during recovery.
    // RaftNoFreelistSync bool
    /// The max time in seconds that a leader wait for install-snapshot ack from
    /// a follower or non-voter.
    #[clap(long, env = "RAFT_INSTALL_SNAPSHOT_TIMEOUT", default_value = "4")]
    pub install_snapshot_timeout: u64,

    /// The maximum number of applied logs to keep before purging
    #[clap(long, env = "RAFT_MAX_APPLIED_LOG_TO_KEEP", default_value = "1000")]
    pub max_applied_log_to_keep: u64,

    /// The node id. Only used when this server is not initialized,
    ///  e.g. --boot or --single for the first time.
    ///  Otherwise this argument is ignored.
    #[clap(long, env = "RQLITED_ID", default_value = "0")]
    pub id: NodeId,

    /// Whether to fsync meta to disk for every meta write(raft log, state
    /// machine etc). No-sync brings risks of data loss during a crash.
    /// You should only use this in a testing environment, unless YOU KNOW WHAT
    /// YOU ARE DOING.
    #[clap(long, env = "RQLITED_NO_SYNC")]
    pub no_sync: bool,

    /// Single node metasrv. It creates a single node cluster if meta data is
    /// not initialized. Otherwise it opens the previous one.
    /// This is mainly for testing purpose.
    #[clap(long, env = "RQLITED_SINGLE")]
    pub single: bool,

    /// For test only: specifies the tree name prefix
    #[clap(long, default_value = "")]
    pub sled_tree_prefix: String,
}

pub fn get_default_raft_advertise_host() -> String {
    match hostname::get() {
        Ok(h) => match h.into_string() {
            Ok(h) => h,
            _ => "UnknownHost".to_string(),
        },
        _ => "UnknownHost".to_string(),
    }
}

pub fn get_default_raft_listen_host() -> String {
    DEFAULT_LISTEN_HOST.to_string()
}

impl RaftConfig {
    pub fn validate(&mut self) -> StoreResult<()> {
        if self.raft_addr.is_empty() {
            return Err(StoreError::InvalidConfig(format!(
                "raft address is required."
            )));
        }
        if self.join_src_ip.is_empty() {
            self.join_src_ip = String::from(&self.raft_addr);
        }
        Ok(())
    }
}

// impl Default for RaftConfig {
//     fn default() -> Self {
//         Self {
//             config_id: "".to_string(),
//             raft_addr: get_default_raft_listen_host(),
//             raft_adv_addr: get_default_raft_advertise_host(),
//             no_sync: false,
//             snap_threshold: 1024,
//             install_snapshot_timeout: 4000,
//             max_applied_log_to_keep: 1000,
//             single: false,
//             join: vec![],
//             id: 0,
//             sled_tree_prefix: "".to_string(),
//         }
//     }
// }

impl RaftConfig {
    /// StructOptToml provides a default Default impl that loads config from cli
    /// args, which conflicts with unit test if case-filter arguments
    /// passed, e.g.: `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as Parser>::parse_from(&Vec::<&'static str>::new())
    }

    /// Returns true to fsync after a write operation to meta.
    pub fn is_sync(&self) -> bool {
        !self.no_sync
    }

    pub fn check(&self) -> StoreResult<()> {
        if !self.join.is_empty() && self.single {
            return Err(StoreError::InvalidConfig(String::from(
                "--join and --single can not be both set",
            )));
        }
        if self.join.contains(&self.raft_addr) {
            return Err(StoreError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )));
        }
        Ok(())
    }

    /// Create a unique sled::Tree name by prepending a unique prefix.
    /// So that multiple instance that depends on a sled::Tree can be used in
    /// one process. sled does not allow to open multiple `sled::Db` in one
    /// process.
    pub fn tree_name(&self, name: impl std::fmt::Display) -> String {
        format!("{}{}", self.sled_tree_prefix, name)
    }
}
