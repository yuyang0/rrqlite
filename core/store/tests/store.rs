use async_trait::async_trait;
use core_sled::{init_temp_sled_db, openraft};
use core_store::config::RaftConfig;
use core_store::fsm::SQLFsm;
use core_store::store::SledRaftStore;
use core_store::RqliteTypeConfig;
use core_tracing::init_global_tracing;
use openraft::testing::StoreBuilder;
use std::sync::Arc;

struct SledStoreBuilder {}

#[async_trait]
impl StoreBuilder<RqliteTypeConfig, Arc<SledRaftStore>> for SledStoreBuilder {
    async fn build(&self) -> Arc<SledRaftStore> {
        async fn create_store() -> anyhow::Result<SledRaftStore> {
            let d = tempfile::tempdir()
                .map_err(|e| anyhow::format_err!("failed to create temporary dir {}", e))?;
            init_temp_sled_db(d);
            let fsm = SQLFsm::new().map_err(|e| anyhow::format_err!("{}", e))?;
            let cfg = RaftConfig {
                id: 1,
                sled_tree_prefix: String::from("test"),
                ..Default::default()
            };
            let sto = SledRaftStore::open_create(&cfg, Arc::new(fsm), None, Some(()))
                .await
                .map_err(|e| anyhow::format_err!("{}", e))?;
            Ok(sto)
        }
        let sto = create_store().await.expect("failed to create store");
        // match &res {
        //     Err(e) => {
        //         println!("===== {}", e);
        //         ()
        //     }
        //     _ => (),
        // }
        // println!("++++++++ {:?}", res.is_err());
        // assert!(res.is_ok());
        Arc::new(sto)
    }
}
#[test]
pub fn test_sled_raft_store() -> anyhow::Result<()> {
    let _guards = init_global_tracing("rqlited", "_logs", "debug");
    openraft::testing::Suite::test_all(SledStoreBuilder {})
        .map_err(|e| anyhow::format_err!("{}", e))
}
