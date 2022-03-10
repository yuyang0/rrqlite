use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use core_sled::openraft;
use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::types::openraft::{EffectiveMembership, Entry, EntryPayload, LogId};
use crate::types::{AppRequest, AppResponse};
use crate::RqliteTypeConfig;
use core_command::command;
use core_db::{Context, DB};
use core_exception::{ErrorCode, Result};
use core_tracing::tracing;

const LAST_APPLIED_KEY: &'static str = "last-applied-key";
const LAST_MEMBERSHIP_KEY: &'static str = "last-membership-key";

#[async_trait]
pub trait FSM: Send + Sync {
    async fn apply(&mut self, entry: Entry) -> Result<AppResponse>;
    async fn snapshot(&self) -> Result<(Vec<u8>, LogId, String)>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
    fn get_membership(&self) -> Result<Option<EffectiveMembership>>;
    fn get_last_applied(&self) -> Result<Option<LogId>>;
}

/// The application snapshot type which the `MetaStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FSMSnapshot {
    pub meta: SnapshotMeta<RqliteTypeConfig>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct SQLFsm {
    db: Arc<DB>,
}

impl SQLFsm {
    fn new() -> Result<Self> {
        let db = DB::new_mem_db()?;
        let init_sql = "
            BEGIN;
            CREATE TABLE IF NOT EXISTS rqlite_meta(
                key TEXT PRIMARY KEY, 
                value TEXT NOT NULL
            );
            COMMIT;
        ";
        db.execute_batch(init_sql)?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_meta_val(&self, ctx: &Context, key: &str) -> Result<String> {
        let sql = format!("SELECT value from rqlite WHERE key={}", key);
        let res = self.db.query_str_stmt(ctx, &sql)?;
        if res.results.len() == 0 {
            return Ok(String::from(""));
        }
        let res = res.results[0];
        if res.values.len() == 0 {
            return Ok(String::from(""));
        }
        let val = res.values[0];
        if val.parameters.len() == 0 {
            return Ok(String::from(""));
        }
        let p = val.parameters[0];
        let last_applied_str = match p.value {
            None => return Ok(String::from("")),
            Some(v) => match v {
                // command::parameter::Value::Y(bv) => ToSqlOutput::Borrowed(ValueRef::Blob(bv)),
                command::parameter::Value::S(sv) => sv,
                _ => return Err(ErrorCode::StoreError("invalid value of last_applied_log")),
            },
        };
        Ok(last_applied_str)
    }

    fn set_meta_val(&self, ctx: &Context, key: &str, val: &str) -> Result<()> {
        let sql = format!(
            "
            insert or replace into rqlite_meta(key, value) values({}, {});
        ",
            key, val
        );
        let res = self.db.execute_str_stmt(ctx, &sql)?;
        Ok(())
    }

    fn get_last_applied_with_ctx(&self, ctx: &Context) -> Result<Option<LogId>> {
        let last_applied_str = self.get_meta_val(ctx, LAST_APPLIED_KEY)?;
        let last_applied: LogId = serde_json::from_str(&last_applied_str)?;

        Ok(Some(last_applied))
    }

    fn set_last_applied_with_ctx(&self, ctx: &Context, log_id: &LogId) -> Result<()> {
        let v_str = serde_json::to_string(log_id)?;
        self.set_meta_val(ctx, LAST_APPLIED_KEY, &v_str)?;
        Ok(())
    }

    fn set_last_applied(&self, log_id: &LogId) -> Result<()> {
        let ctx = Context::default();
        self.set_last_applied_with_ctx(&ctx, log_id)
    }

    fn get_membership_with_ctx(&self, ctx: &Context) -> Result<Option<EffectiveMembership>> {
        let mem_str = self.get_meta_val(ctx, LAST_MEMBERSHIP_KEY)?;
        let mem: EffectiveMembership = serde_json::from_str(&mem_str)?;
        Ok(Some(mem))
    }

    fn set_membership_with_ctx(&self, ctx: &Context, mem: &EffectiveMembership) -> Result<()> {
        let v_str = serde_json::to_string(mem)?;
        self.set_meta_val(ctx, LAST_MEMBERSHIP_KEY, &v_str)?;
        Ok(())
    }

    fn set_membership(&self, mem: &EffectiveMembership) -> Result<()> {
        let ctx = Context::default();
        self.set_membership_with_ctx(&ctx, mem)
    }

    pub fn apply_cmd(&self, ctx: &Context, app_req: &AppRequest) -> Result<AppResponse> {
        let msg = match app_req {
            AppRequest::Query(qr) => {
                let Some(req) = qr.request;
                let res = self.db.query(ctx, &req)?;
                AppResponse::Query(res)
            }
            AppRequest::Execute(er) => {
                let Some(req) = er.request;
                let res = self.db.execute(ctx, &req)?;
                AppResponse::Execute(res)
            }
        };
        Ok(msg)
    }
}

#[async_trait]
impl FSM for SQLFsm {
    async fn apply(&mut self, entry: Entry) -> Result<AppResponse> {
        tracing::debug!("apply: summary: {}", entry.summary());
        tracing::debug!("apply: payload: {:?}", entry.payload);

        let log_id = &entry.log_id;

        tracing::debug!("sled tx start: {:?}", entry);

        let conn = self.db.get_conn()?;
        let ctx = Context::new(&mut conn, true)?;
        self.set_last_applied_with_ctx(&ctx, log_id)?;

        let res = match entry.payload {
            EntryPayload::Blank => AppResponse::None,
            EntryPayload::Normal(ref app_req) => {
                let res = self.apply_cmd(&ctx, app_req)?;
                res
            }
            EntryPayload::Membership(ref mem) => {
                let e_mem = EffectiveMembership {
                    log_id: *log_id,
                    membership: mem.clone(),
                };
                self.set_membership_with_ctx(&ctx, &e_mem)?;
                AppResponse::None
            }
        };
        tracing::debug!("sled tx done: {:?}", entry);

        Ok(res)
    }

    async fn snapshot(&self) -> Result<(Vec<u8>, LogId, String)> {
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

        let b = self.db.serialize()?;

        Ok((b, last_applied, snapshot_id))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        self.db.deserialize(snapshot)
    }

    fn get_membership(&self) -> Result<Option<EffectiveMembership>> {
        let ctx = Context::default();
        self.get_membership_with_ctx(&ctx)
    }

    fn get_last_applied(&self) -> Result<Option<LogId>> {
        let ctx = Context::default();
        self.get_last_applied_with_ctx(&ctx)
    }
}
