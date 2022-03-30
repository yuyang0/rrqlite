use async_trait::async_trait;
use core_sled::{init_temp_sled_db, openraft};
use core_store::config::RaftConfig;
use core_store::fsm::SQLFsm;
use core_store::store::SledRaftStore;
use core_store::RqliteTypeConfig;
use core_util_misc::GlobalSequence;
use openraft::testing::StoreBuilder;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()` and keeps the guard held.
// #[macro_export]
macro_rules! init_meta_ut {
    () => {{
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        init_temp_sled_db(t);

        // common_tracing::init_tracing(&format!("ut-{}", name), "./_logs")
        core_tracing::init_meta_ut_tracing();

        let name = core_tracing::func_name!();
        let span = core_tracing::tracing::debug_span!("ut", "{}", name.split("::").last().unwrap());
        ((), span)
    }};
}

pub fn next_port() -> u32 {
    29000u32 + (GlobalSequence::next() as u32)
}

#[derive(Default)]
struct SledStoreBuilder {}

#[async_trait]
impl StoreBuilder<RqliteTypeConfig, Arc<SledRaftStore>> for SledStoreBuilder {
    async fn build(&self) -> Arc<SledRaftStore> {
        async fn create_store<P: AsRef<Path>>(db_path: Option<P>) -> anyhow::Result<SledRaftStore> {
            let fsm = match db_path {
                Some(p) => SQLFsm::new(Some(p)).map_err(|e| anyhow::format_err!("{}", e))?,
                None => SQLFsm::new::<String>(None).map_err(|e| anyhow::format_err!("{}", e))?,
            };
            let new_id = next_port();
            let cfg = RaftConfig {
                id: 1,
                config_id: format!("{}", new_id),
                sled_tree_prefix: format!("test-{}", new_id),
                ..Default::default()
            };
            let sto = SledRaftStore::open_create(&cfg, Arc::new(fsm), None, Some(()))
                .await
                .map_err(|e| anyhow::format_err!("{}", e))?;
            Ok(sto)
        }

        let db_path = String::from("/tmp/test-rrqlite.db");
        let _ = fs::remove_file(&db_path);
        let db_path_arg = Some(db_path);
        // let db_path_arg = None;
        let sto = create_store::<String>(db_path_arg)
            .await
            .expect("failed to create store");
        Arc::new(sto)
    }
}
#[test]
pub fn test_sled_raft_store() -> anyhow::Result<()> {
    // let _guards = init_global_tracing("rqlited", "_logs", "debug");
    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    openraft::testing::Suite::test_all(SledStoreBuilder::default())
        .map_err(|e| anyhow::format_err!("{}", e))
}
