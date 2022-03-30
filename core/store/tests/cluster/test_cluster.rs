use std::time::Duration;

use anyerror::AnyError;
use core_command::command;
use core_sled::init_temp_sled_db;
use core_sled::openraft;
use core_store::config::{FSMConfig, RaftConfig, StoreConfig};
use core_store::{RqliteNode, RqliteTypeConfig};
use maplit::btreeset;
use openraft::error::NodeNotFound;

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

/// Setup a cluster of 3 nodes.
/// Write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster() -> anyhow::Result<()> {
    // --- The client itself does not store addresses for all nodes, but just node id.
    //     Thus we need a supporting component to provide mapping from node id to node address.
    //     This is only used by the client. A raft node in this example stores node addresses in its store.

    let (_log_guards, ut_span) = init_meta_ut!();
    let _ent = ut_span.enter();

    let get_cfg = |node_id| {
        let leader_addr = "127.0.0.1:21001";
        let addr = match node_id {
            1 => "127.0.0.1:21001".to_string(),
            2 => "127.0.0.1:21002".to_string(),
            3 => "127.0.0.1:21003".to_string(),
            _ => {
                return Err(NodeNotFound::<RqliteTypeConfig> {
                    node_id,
                    source: AnyError::error("node not found"),
                });
            }
        };

        let mut cfg = StoreConfig {
            raft: RaftConfig {
                config_id: format!("{}", node_id),
                id: node_id,
                raft_addr: String::from(&addr),
                join_src_ip: String::from(&addr),
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
        };
        if node_id == 1 {
            cfg.raft.single = true;
        }
        if node_id > 1 {
            cfg.raft.join.push(String::from(leader_addr));
        }
        Ok(cfg)
    };
    // --- Start 3 raft node in 3 threads.
    let node1 = {
        let cfg = get_cfg(1).expect("failed get store config");
        let node = RqliteNode::start(&cfg)
            .await
            .expect("failed to run rqlite node");

        // join raft cluster after all service started
        node.join_cluster(&cfg.raft).await?;
        node
    };

    println!("=== metrics after init");
    assert_eq!(node1.is_leader().await, true);
    assert_eq!(node1.leader_addr().await?, "127.0.0.1:21001");

    let _x1 = node1.get_metrics();

    let node2 = {
        let cfg = get_cfg(2).expect("failed get store config");
        let node = RqliteNode::start(&cfg)
            .await
            .expect("failed to run rqlite node");

        // join raft cluster after all service started
        node.join_cluster(&cfg.raft).await?;
        node
    };
    let node3 = {
        let cfg = get_cfg(3).expect("failed get store config");
        let node = RqliteNode::start(&cfg)
            .await
            .expect("failed to run rqlite node");

        node.join_cluster(&cfg.raft).await?;
        node
    };

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- After change-membership, some cluster state will be seen in the metrics.
    //
    // ```text
    // metrics: RaftMetrics {
    //   current_leader: Some(1),
    //   membership_config: EffectiveMembership {
    //        log_id: LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 },
    //        membership: Membership { learners: {}, configs: [{1, 2, 3}] }
    //   },
    //   leader_metrics: Some(LeaderMetrics { replication: {
    //     2: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 7 }) },
    //     3: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 }) }} })
    // }
    // ```

    println!("=== metrics after change-member");
    let x = node1.get_metrics();
    assert_eq!(&vec![btreeset! {1,2,3}], x.membership_config.get_configs());

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
    let er = command::ExecuteRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(exec_sql),
                ..Default::default()
            }],
        }),
        timings: false,
    };
    let res = node1.execute(er, false).await?;
    println!("~~~~ execute result {:?}", res);

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Read it on every node.
    let tables_sql = "
    SELECT name FROM contacts
    ";
    let qr = command::QueryRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(tables_sql),
                ..Default::default()
            }],
        }),
        level: command::query_request::Level::QueryRequestLevelNone as i32,
        // freshness: 1000000,
        ..Default::default()
    };
    println!("=== check tables on node 1");
    let res = node1.query(qr.clone(), false).await?;
    println!("~~~~ node1 query result {:?}", res);
    assert_eq!(res.results.len(), 1);
    assert_eq!(res.results[0].values.len(), 0);

    println!("=== check table on node 2");
    let res = node2.query(qr.clone(), false).await?;
    assert_eq!(res.results.len(), 1);
    assert_eq!(res.results[0].values.len(), 0);

    println!("=== check table on node 3");
    let res = node3.query(qr.clone(), false).await?;
    assert_eq!(res.results.len(), 1);
    assert_eq!(res.results[0].values.len(), 0);

    // 2 write data to table
    let exec_sql = "
    INSERT INTO contacts (contact_id, name, email, data) VALUES (1, 'Jim', 'haha@qq.com', '37e79');
    ";
    let er = command::ExecuteRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(exec_sql),
                ..Default::default()
            }],
        }),
        timings: false,
    };
    let res = node1.execute(er, false).await?;
    println!("~~~~ execute result {:?}", res);

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Read it on every node.
    let tables_sql = "
    SELECT name FROM contacts
    ";
    let qr = command::QueryRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(tables_sql),
                ..Default::default()
            }],
        }),
        level: command::query_request::Level::QueryRequestLevelNone as i32,
        // freshness: 1000000,
        ..Default::default()
    };
    println!("=== read record on node 1");
    let res = node1.query(qr.clone(), false).await?;
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

    println!("=== read record on node 2");
    let res = node2.query(qr.clone(), false).await?;
    check_record1(res);

    println!("=== read record on node 3");
    let res = node3.query(qr.clone(), false).await?;
    check_record1(res);

    // --- A write to non-leader will be automatically forwarded to a known leader
    let exec_sql = "
        INSERT INTO contacts (contact_id, name, email) VALUES (2, 'Lucy', 'kaka@qq.com');
    ";
    let er = command::ExecuteRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(exec_sql),
                ..Default::default()
            }],
        }),
        timings: false,
    };
    let res = node2.execute(er, true).await?;
    println!("~~~~ execute result {:?}", res);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Read it on every node.
    let tables_sql = "
    SELECT name FROM contacts WHERE contact_id = 2
    ";
    let qr = command::QueryRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(tables_sql),
                ..Default::default()
            }],
        }),
        level: command::query_request::Level::QueryRequestLevelNone as i32,
        // freshness: 1000000,
        ..Default::default()
    };
    println!("=== read record on node 1");
    let res = node1.query(qr.clone(), false).await?;
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

    println!("=== read record on node 2");
    let res = node2.query(qr.clone(), false).await?;
    check_record2(res);

    println!("=== read record on node 3");
    let res = node3.query(qr.clone(), false).await?;
    check_record2(res);

    //TODO consistent read
    let exec_sql = "
        INSERT INTO contacts (contact_id, name, email) VALUES (3, 'Tom', 'tom@qq.com');
    ";
    let er = command::ExecuteRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(exec_sql),
                ..Default::default()
            }],
        }),
        timings: false,
    };
    let res = node2.execute(er, true).await?;
    println!("~~~~ execute result {:?}", res);

    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Read it on every node.
    let tables_sql = "
    SELECT name FROM contacts WHERE contact_id = 3
    ";
    let qr = command::QueryRequest {
        request: Some(command::Request {
            transaction: true,
            statements: vec![command::Statement {
                sql: String::from(tables_sql),
                ..Default::default()
            }],
        }),
        level: command::query_request::Level::QueryRequestLevelStrong as i32,
        // freshness: 1000000,
        ..Default::default()
    };
    println!("=== read record on node 1");
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
    let res = node1.query(qr.clone(), false).await?;
    println!("~~~~ node1 query result {:?}", res);
    check_record3(res);
    let res = node2.query(qr.clone(), true).await?;
    println!("~~~~ node2 query result {:?}", res);
    check_record3(res);

    let res = node3.query(qr.clone(), true).await?;
    println!("~~~~ node3 query result {:?}", res);
    check_record3(res);
    Ok(())
}
