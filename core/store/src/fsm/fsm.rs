use anyerror::AnyError;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use core_sled::openraft;
use openraft::MessageSummary;
use serde::{Deserialize, Serialize};

use crate::errors::{StoreError, StoreResult};
use crate::types::openraft::{EffectiveMembership, Entry, EntryPayload, LogId, SnapshotMeta};
use crate::types::{AppRequest, AppResponse};
use core_command::command;
use core_db::{Context, DB};
use core_tracing::tracing;
use std::sync::Arc;

const LAST_APPLIED_KEY: &'static str = "last-applied-key";
const LAST_MEMBERSHIP_KEY: &'static str = "last-membership-key";

#[async_trait]
pub trait FSM: Send + Sync {
    async fn apply(&self, entry: &Entry) -> StoreResult<AppResponse>;
    async fn snapshot(&self) -> StoreResult<(Vec<u8>, LogId, String)>;
    async fn restore(&self, snapshot: &[u8]) -> StoreResult<()>;
    async fn query(&self, qr: &command::QueryRequest) -> StoreResult<command::QueryResult>;
    fn get_membership(&self) -> StoreResult<Option<EffectiveMembership>>;
    fn get_last_applied(&self) -> StoreResult<Option<LogId>>;
}

/// The application snapshot type which the `MetaStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FSMSnapshot {
    pub meta: SnapshotMeta,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

pub struct SQLFsm {
    db: Arc<DB>,
}

impl SQLFsm {
    pub fn new() -> StoreResult<Self> {
        let db = DB::new_mem_db().map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        let init_sql = "
            BEGIN;
            CREATE TABLE IF NOT EXISTS rqlite_meta(
                key TEXT PRIMARY KEY, 
                value TEXT NOT NULL
            );
            COMMIT;
        ";
        db.execute_batch(init_sql)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_meta_val(&self, ctx: &Context, key: &str) -> StoreResult<String> {
        let sql = format!("SELECT value from rqlite WHERE key={}", key);
        let res = self
            .db
            .query_str_stmt(ctx, &sql)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        if res.results.len() == 0 {
            return Ok(String::from(""));
        }
        let res = &res.results[0];
        if res.values.len() == 0 {
            return Ok(String::from(""));
        }
        let val = &res.values[0];
        if val.parameters.len() == 0 {
            return Ok(String::from(""));
        }
        let p = &val.parameters[0];
        let last_applied_str = match &p.value {
            None => return Ok(String::from("")),
            Some(v) => match v {
                // command::parameter::Value::Y(bv) => ToSqlOutput::Borrowed(ValueRef::Blob(bv)),
                command::parameter::Value::S(sv) => String::from(sv),
                _ => {
                    return Err(StoreError::FSMError(AnyError::error(
                        "invalid value of last_applied_log",
                    )))
                }
            },
        };
        Ok(last_applied_str)
    }

    fn set_meta_val(&self, ctx: &Context, key: &str, val: &str) -> StoreResult<()> {
        let sql = format!(
            "
            insert or replace into rqlite_meta(key, value) values({}, {});
        ",
            key, val
        );
        self.db
            .execute_str_stmt(ctx, &sql)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        Ok(())
    }

    fn get_last_applied_with_ctx(&self, ctx: &Context) -> StoreResult<Option<LogId>> {
        let last_applied_str = self.get_meta_val(ctx, LAST_APPLIED_KEY)?;
        let last_applied: LogId = serde_json::from_str(&last_applied_str)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;

        Ok(Some(last_applied))
    }

    fn set_last_applied_with_ctx(&self, ctx: &Context, log_id: &LogId) -> StoreResult<()> {
        let v_str =
            serde_json::to_string(log_id).map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        self.set_meta_val(ctx, LAST_APPLIED_KEY, &v_str)?;
        Ok(())
    }

    pub fn set_last_applied(&self, log_id: &LogId) -> StoreResult<()> {
        let ctx = Context::default();
        self.set_last_applied_with_ctx(&ctx, log_id)
    }

    fn get_membership_with_ctx(&self, ctx: &Context) -> StoreResult<Option<EffectiveMembership>> {
        let mem_str = self.get_meta_val(ctx, LAST_MEMBERSHIP_KEY)?;
        let mem: EffectiveMembership =
            serde_json::from_str(&mem_str).map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        Ok(Some(mem))
    }

    fn set_membership_with_ctx(&self, ctx: &Context, mem: &EffectiveMembership) -> StoreResult<()> {
        let v_str =
            serde_json::to_string(mem).map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        self.set_meta_val(ctx, LAST_MEMBERSHIP_KEY, &v_str)?;
        Ok(())
    }

    pub fn set_membership(&self, mem: &EffectiveMembership) -> StoreResult<()> {
        let ctx = Context::default();
        self.set_membership_with_ctx(&ctx, mem)
    }

    pub fn apply_cmd(&self, ctx: &Context, app_req: &AppRequest) -> StoreResult<AppResponse> {
        let msg = match app_req {
            AppRequest::Query(qr) => {
                let req = qr.request.as_ref().unwrap();
                let res = self
                    .db
                    .query(ctx, req)
                    .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
                AppResponse::Query(res)
            }
            AppRequest::Execute(er) => {
                let req = er.request.as_ref().unwrap();
                let res = self
                    .db
                    .execute(ctx, req)
                    .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
                AppResponse::Execute(res)
            } // AppRequest::AddNode { node_id, node } => {}
        };
        Ok(msg)
    }
}

#[async_trait]
impl FSM for SQLFsm {
    async fn apply(&self, entry: &Entry) -> StoreResult<AppResponse> {
        tracing::debug!("apply: summary: {}", entry.summary());
        tracing::debug!("apply: payload: {:?}", entry.payload);

        let log_id = &entry.log_id;

        tracing::debug!("sled tx start: {:?}", entry);

        let mut conn = self
            .db
            .get_conn()
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        let ctx =
            Context::new(&mut conn, true).map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;
        self.set_last_applied_with_ctx(&ctx, log_id)?;

        let res = match entry.payload {
            EntryPayload::Blank => AppResponse::None,
            EntryPayload::Normal(ref app_req) => {
                let res = self.apply_cmd(&ctx, app_req)?;
                res
            }
            EntryPayload::Membership(ref mem) => {
                let e_mem = EffectiveMembership::new(*log_id, mem.clone());
                self.set_membership_with_ctx(&ctx, &e_mem)?;
                AppResponse::None
            }
        };
        tracing::debug!("sled tx done: {:?}", entry);

        Ok(res)
    }

    async fn snapshot(&self) -> StoreResult<(Vec<u8>, LogId, String)> {
        let last_applied = self.get_last_applied()?;

        // NOTE: An initialize node/cluster always has the first log contains membership config.

        let last_applied =
            last_applied.expect("not allowed to build snapshot with empty state machine");

        let snapshot_idx = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied.leader_id.term, last_applied.index, snapshot_idx
        );

        let b = self
            .db
            .serialize()
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))?;

        Ok((b, last_applied, snapshot_id))
    }

    async fn restore(&self, snapshot: &[u8]) -> StoreResult<()> {
        self.db
            .deserialize(snapshot)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))
    }

    async fn query(&self, qr: &command::QueryRequest) -> StoreResult<command::QueryResult> {
        let ctx = Context::default();
        let req = qr
            .request
            .as_ref()
            .ok_or(StoreError::FSMError(AnyError::error(
                "request field is empty",
            )))?;
        self.db
            .query(&ctx, &req)
            .map_err(|e| StoreError::FSMError(AnyError::new(&e)))
    }

    fn get_membership(&self) -> StoreResult<Option<EffectiveMembership>> {
        let ctx = Context::default();
        self.get_membership_with_ctx(&ctx)
    }

    fn get_last_applied(&self) -> StoreResult<Option<LogId>> {
        let ctx = Context::default();
        self.get_last_applied_with_ctx(&ctx)
    }
}
