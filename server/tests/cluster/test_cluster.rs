use std::time::Duration;

use anyerror::AnyError;
use client::URLParams;
use core_command::command;
use core_exception::Result;
use core_sled::{init_temp_sled_db, openraft};
use core_store::config::{FSMConfig, RaftConfig, StoreConfig};
use core_store::{RqliteNode, RqliteTypeConfig};
use core_tracing::tracing;
use core_util_misc::{StopHandle, Stoppable};
use openraft::error::NodeNotFound;
use rqlited::api::http;
use rqlited::config::{Config, HttpConfig};

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()`
/// and keeps the guard held.
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

async fn start_node(_conf: Config, stop_handler: &mut StopHandle) -> Result<()> {
    // let _conf = Config::load()?;
    // let _guards = init_global_tracing("rqlited", &_conf.log_dir,
    // &_conf.log_level);

    let node = RqliteNode::start(&_conf.store).await?;

    // HTTP API service.
    {
        let mut srv = http::Server::new(&_conf, node.clone())?;
        tracing::info!("Starting HTTP API server at {}", _conf.http.http_addr);
        srv.start().await.expect("Failed to start http server");
        stop_handler.push(Box::new(srv));
    }
    // join raft cluster after all service started
    node.join_cluster(&_conf.store.raft).await?;

    Ok(())
}

/// Setup a cluster of 3 nodes.
/// Write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster() -> anyhow::Result<()> {
    // --- The client itself does not store addresses for all nodes, but just node
    // id.     Thus we need a supporting component to provide mapping from node
    // id to node address.     This is only used by the client. A raft node in
    // this example stores node addresses in its store.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let get_api_addr = |node_id| {
        let addr = match node_id {
            1 => "127.0.0.1:21011".to_string(),
            2 => "127.0.0.1:21012".to_string(),
            3 => "127.0.0.1:21013".to_string(),
            _ => {
                return Err(NodeNotFound::<RqliteTypeConfig> {
                    node_id,
                    source: AnyError::error("node not found"),
                });
            }
        };
        Ok(addr)
    };

    let get_raft_addr = |node_id| -> anyhow::Result<String> {
        let addr = match node_id {
            1 => "127.0.0.1:21001".to_string(),
            2 => "127.0.0.1:21002".to_string(),
            3 => "127.0.0.1:21003".to_string(),
            _ => {
                return Err(NodeNotFound::<RqliteTypeConfig> {
                    node_id,
                    source: AnyError::error("node not found"),
                }
                .into());
            }
        };
        Ok(addr)
    };

    let get_cfg = |node_id| -> anyhow::Result<Config> {
        let leader_addr = "127.0.0.1:21001";
        let http_addr = get_api_addr(node_id)?;
        let raft_addr = get_raft_addr(node_id)?;

        let mut cfg = Config {
            http: HttpConfig {
                http_addr: http_addr,
                ..Default::default()
            },
            store: StoreConfig {
                raft: RaftConfig {
                    config_id: format!("{}", node_id),
                    id: node_id,
                    raft_addr: String::from(&raft_addr),
                    join_src_ip: String::from(&raft_addr),
                    raft_heartbeat_timeout: 1,
                    join_attempts: 5,
                    join_interval: 3,
                    install_snapshot_timeout: 4,
                    snap_threshold: 8192,
                    max_applied_log_to_keep: 1000,
                    sled_tree_prefix: format!("test-cluster-{}", node_id),
                    ..Default::default()
                },
                fsm: FSMConfig {
                    ..Default::default()
                },
            },
            ..Default::default()
        };
        if node_id == 1 {
            cfg.store.raft.single = true;
        }
        if node_id > 1 {
            cfg.store.raft.join.push(String::from(leader_addr));
        }
        Ok(cfg)
    };

    let mut stop_handler = StopHandle::create();
    let stop_tx = StopHandle::install_termination_handle();

    // --- Start 3 raft node in 3 threads.
    {
        let cfg = get_cfg(1).expect("failed get store config");
        start_node(cfg, &mut stop_handler).await?;
    }
    {
        let cfg = get_cfg(2).expect("failed get store config");
        start_node(cfg, &mut stop_handler).await?;
    }
    {
        let cfg = get_cfg(3).expect("failed get store config");
        start_node(cfg, &mut stop_handler).await?;
    }

    let _tx = stop_tx.clone();
    let join_handle = tokio::spawn(async {
        tracing::info!("Wating rqlited to shutdown");
        stop_handler.wait_to_terminate(_tx).await;
        tracing::info!("Rqlited is done shutting down");
    });

    let client1 = client::RqliteClient::new(get_api_addr(1)?);
    let client2 = client::RqliteClient::new(get_api_addr(2)?);
    let client3 = client::RqliteClient::new(get_api_addr(3)?);

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let default_params = URLParams {
        transaction: true,
        timings: true,
        level: String::from("none"),
        ..Default::default()
    };
    // --- Try to write some application data through the leader.
    // 1 create table
    let exec_sql = "
        CREATE TABLE contacts (
            contact_id INTEGER PRIMARY KEY,
            name text NOT NULL,
            email TEXT NOT NULL UNIQUE,
            data BLOB
        );
    ";

    let exec_params = default_params.clone();
    let res = client1
        .execute(&vec![exec_sql.to_string()], &exec_params)
        .await
        .unwrap();
    println!("~~~~ execute result {:?}", res);

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Read it on every node.
    fn check_table(res: command::QueryResult) {
        let res = res.results;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].values.len(), 4);
        assert_eq!(
            res[0].values[0].parameters[1].value,
            Some(command::parameter::Value::S(String::from("contact_id")))
        );
        assert_eq!(
            res[0].values[1].parameters[1].value,
            Some(command::parameter::Value::S(String::from("name")))
        );
        assert_eq!(
            res[0].values[2].parameters[1].value,
            Some(command::parameter::Value::S(String::from("email")))
        );
        assert_eq!(
            res[0].values[3].parameters[1].value,
            Some(command::parameter::Value::S(String::from("data")))
        );
    }

    let tables_sql = "
        pragma table_info('contacts')
    ";

    let query_params = default_params.clone();
    println!("=== check tables on node 1");
    let res = client1.query_get(tables_sql, &query_params).await.unwrap();
    println!("~~~~ node1 query result {:?}", res);
    check_table(res);

    println!("=== check table on node 2");
    let res = client2.query_get(tables_sql, &query_params).await.unwrap();
    println!("~~~~ node2 query result {:?}", res);
    check_table(res);

    println!("=== check table on node 3");
    let res = client3.query_get(tables_sql, &query_params).await.unwrap();
    check_table(res);

    // 2 write data to table
    let exec_sql = "
    INSERT INTO contacts (contact_id, name, email, data) VALUES (1, 'Jim', 'haha@qq.com', '37e79');
    ";
    let exec_params = default_params.clone();
    let res = client1
        .execute(&vec![exec_sql.to_string()], &exec_params)
        .await?;
    println!("~~~~ execute result {:?}", res);

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Read it on every node.
    let query_sql = "
    SELECT name FROM contacts
    ";
    let query_params = default_params.clone();
    println!("=== read record1 on node 1");
    let res = client1.query_get(query_sql, &query_params).await?;
    println!("~~~~ node1 query result {:?}", res);
    fn check_record1(res: command::QueryResult) {
        assert_eq!(res.results.len(), 1);
        assert_eq!(res.results[0].values.len(), 1);
        assert_eq!(res.results[0].values[0].parameters.len(), 1);
        let expect_p = command::Parameter {
            name: String::from("name"),
            value: Some(command::parameter::Value::S(String::from("Jim"))),
        };
        assert_eq!(res.results[0].values[0].parameters[0], expect_p);
    }
    check_record1(res);

    println!("=== read record1 on node 2");
    let res = client2.query_get(query_sql, &query_params).await?;
    check_record1(res);

    println!("=== read record1 on node 3");
    let res = client3.query_get(query_sql, &query_params).await?;
    check_record1(res);

    // --- A write to non-leader will be automatically forwarded to a known leader
    let exec_sql = "
        INSERT INTO contacts (contact_id, name, email) VALUES (2, 'Lucy', 'kaka@qq.com');
    ";
    let mut exec_params = default_params.clone();
    exec_params.redirect = true;

    let res = client2
        .execute(&vec![exec_sql.to_string()], &exec_params)
        .await?;
    println!("~~~~ execute result {:?}", res);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Read it on every node.
    let query_sql = "
    SELECT name FROM contacts WHERE contact_id = 2
    ";
    let query_params = default_params.clone();
    println!("=== read record2 on node 1");
    let res = client1.query_get(query_sql, &query_params).await?;
    println!("~~~~ node1 query result {:?}", res);
    fn check_record2(res: command::QueryResult) {
        assert_eq!(res.results.len(), 1);
        assert_eq!(res.results[0].values.len(), 1);
        assert_eq!(res.results[0].values[0].parameters.len(), 1);
        let expect_p = command::Parameter {
            name: String::from("name"),
            value: Some(command::parameter::Value::S(String::from("Lucy"))),
        };
        assert_eq!(res.results[0].values[0].parameters[0], expect_p);
    }
    check_record2(res);

    println!("=== read record2 on node 2");
    let res = client2.query_get(query_sql, &query_params).await?;
    check_record2(res);

    println!("=== read record2 on node 3");
    let res = client3.query_get(query_sql, &query_params).await?;
    check_record2(res);

    // TODO consistent read
    let exec_sql = "
        INSERT INTO contacts (contact_id, name, email) VALUES (3, 'Tom', 'tom@qq.com');
    ";
    let mut exec_params = default_params.clone();
    exec_params.redirect = true;
    let res = client2
        .execute(&vec![exec_sql.to_string()], &exec_params)
        .await?;
    println!("~~~~ execute result {:?}", res);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Read it on every node.
    let query_sql = "
    SELECT name FROM contacts WHERE contact_id = 3
    ";
    let mut query_params = default_params.clone();
    query_params.level = "strong".to_string();

    fn check_record3(res: command::QueryResult) {
        assert_eq!(res.results.len(), 1);
        assert_eq!(res.results[0].values.len(), 1);
        assert_eq!(res.results[0].values[0].parameters.len(), 1);
        let expect_p = command::Parameter {
            name: String::from("name"),
            value: Some(command::parameter::Value::S(String::from("Tom"))),
        };
        assert_eq!(res.results[0].values[0].parameters[0], expect_p);
    }
    println!("=== read record3 on node 1");
    let res = client1.query_get(query_sql, &query_params).await?;
    println!("~~~~ node1 query result {:?}", res);
    check_record3(res);

    println!("=== read record3 on node 2");
    query_params.redirect = true;
    let res = client2.query_get(query_sql, &query_params).await?;
    println!("~~~~ node2 query result {:?}", res);
    check_record3(res);

    println!("=== read record3 on node 3");
    let res = client3.query_get(query_sql, &query_params).await?;
    println!("~~~~ node3 query result {:?}", res);
    check_record3(res);

    stop_tx.send(())?;
    join_handle.await?;
    Ok(())
}
