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

//! sled_store implement a key-value like store backed by sled::Tree.
//!
//! It is used by raft for log and state machine storage.
pub use db::{get_sled_db, init_sled_db, init_temp_sled_db};
pub use errors::{SledStorageError, SledStorageResult};
pub use sled_key_space::SledKeySpace;
pub use sled_serde::{SledOrderedSerde, SledRangeSerde, SledSerde};
pub use sled_tree::{AsKeySpace, AsTxnKeySpace, SledTree, SledValueToKey, TransactionSledTree};
pub use store::Store;
pub use {openraft, sled};

mod db;
mod error_context;
mod errors;
mod sled_key_space;
mod sled_serde;
mod sled_tree;
mod store;
