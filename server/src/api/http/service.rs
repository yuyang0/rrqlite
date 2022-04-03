use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{delete, get, post, web, HttpRequest, HttpResponse, Responder};
use core_command::command;
use core_exception::Result;
use core_store::errors::{RaftError, StoreError};
use core_store::types::{BackupFormat, JoinRequest, RemoveNodeRequest};
use core_store::RqliteNode;
use core_tracing::tracing;
use qstring::QString;
use serde::{Deserialize, Serialize};

// use serde::{Deserialize, Serialize};
use super::util::parse_sql_stmts;

fn qs_get_i64(qs: &QString, name: &str, dft: i64) -> Result<i64> {
    match qs.get(name) {
        Some(sv) => {
            let iv = sv.parse::<i64>()?;
            Ok(iv)
        }
        None => Ok(dft),
    }
}

fn store_err_to_resp(e: StoreError) -> HttpResponse {
    match e {
        StoreError::RaftError(re) => match re {
            RaftError::NoLeaderError(_e) => {
                HttpResponse::ServiceUnavailable().body("no enough nodes for RAFT group")
            }
            _ => HttpResponse::InternalServerError()
                .body("internel error, please contact administrator."),
        },
        _ => HttpResponse::InternalServerError()
            .body("internel error, please contact administrator."),
    }
}

#[post("/db/execute")]
async fn handle_db_execute(
    _req: HttpRequest,
    req_body: String,
    node: web::Data<Arc<RqliteNode>>,
) -> impl Responder {
    let stmts_opt = parse_sql_stmts(&req_body);

    // extract query arguments
    let query_str = _req.query_string();
    let qs = QString::from(query_str);
    let is_txn = qs.get("transaction").is_some();
    let timings = qs.get("timings").is_some();
    let redirect = qs.get("redirect").is_some();
    let timeout = match qs_get_i64(&qs, "timeout", 50) {
        Err(_e) => return HttpResponse::BadRequest().body("invalid timeout argument"),
        Ok(v) => v,
    };

    let er = match stmts_opt {
        Err(e) => {
            let msg = HashMap::from([("error", format!("{}", e))]);
            return HttpResponse::BadRequest().json(msg);
        }
        Ok(stmts) => {
            let er = command::ExecuteRequest {
                request: Some(command::Request {
                    transaction: is_txn,
                    statements: stmts,
                }),
                timings: timings,
            };
            er
        }
    };
    let res = tokio::time::timeout(
        Duration::from_secs(timeout as u64),
        node.execute(er, redirect),
    )
    .await;
    let es = match res {
        Ok(v) => match v {
            Ok(resp) => resp,
            Err(e) => return HttpResponse::BadRequest().body(format!("Failed to execute: {}", e)),
        },
        Err(_e) => {
            return HttpResponse::RequestTimeout()
                .json(HashMap::from([("error", "execute timeout")]))
        }
    };
    HttpResponse::Ok().json(es)
}

// allow GET, POST
#[get("/db/query")]
async fn handle_db_query_get(
    _req: HttpRequest,
    node: web::Data<Arc<RqliteNode>>,
) -> impl Responder {
    let query_str = _req.query_string();
    let qs = QString::from(query_str);
    let stmt = match qs.get("q") {
        Some(s) => s,
        None => return HttpResponse::BadRequest().body("need sql statment"),
    };
    let stmts = vec![command::Statement {
        sql: stmt.to_string(),
        ..Default::default()
    }];
    handle_db_query(qs, stmts, node).await
}

#[post("/db/query")]
async fn handle_db_query_post(
    _req: HttpRequest,
    req_body: String,
    node: web::Data<Arc<RqliteNode>>,
) -> impl Responder {
    let query_str = _req.query_string();
    let qs = QString::from(query_str);

    let stmts = match parse_sql_stmts(&req_body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("invalid query request {}", e);
            let msg = HashMap::from([("error", format!("{}", e))]);
            return HttpResponse::BadRequest().json(msg);
        }
    };
    handle_db_query(qs, stmts, node).await
}

async fn handle_db_query(
    qs: QString,
    stmts: Vec<command::Statement>,
    node: web::Data<Arc<RqliteNode>>,
) -> HttpResponse {
    // extract query arguments
    let is_txn = qs.get("transaction").is_some();
    let timings = qs.get("timings").is_some();
    let redirect = qs.get("redirect").is_some();
    let timeout = match qs_get_i64(&qs, "timeout", 50) {
        Err(_) => return HttpResponse::BadRequest().body("invalid timeout argument"),
        Ok(v) => v,
    };
    let freshness = match qs_get_i64(&qs, "freshness", 50) {
        Err(_) => return HttpResponse::BadRequest().body("invalid freshness argument"),
        Ok(v) => v,
    };
    let level = match qs.get("level") {
        Some(sv) => match sv.to_lowercase().as_str() {
            "none" => command::query_request::Level::QueryRequestLevelNone,
            "srong" => command::query_request::Level::QueryRequestLevelStrong,
            "weak" => command::query_request::Level::QueryRequestLevelWeak,
            _ => return HttpResponse::BadRequest().body("invalid level argument"),
        },
        None => command::query_request::Level::QueryRequestLevelWeak,
    };

    let qr = command::QueryRequest {
        request: Some(command::Request {
            transaction: is_txn,
            statements: stmts,
        }),
        timings: timings,
        freshness: freshness,
        level: level.into(),
    };

    let res = tokio::time::timeout(
        Duration::from_secs(timeout as u64),
        node.query(qr, redirect),
    )
    .await;
    match res {
        Ok(v) => {
            let qs = match v {
                Ok(resp) => resp,
                Err(e) => {
                    let err_msg = HashMap::from([("error", format!("Failed to execute: {}", e))]);
                    return HttpResponse::BadRequest().json(err_msg);
                }
            };
            HttpResponse::Ok().json(qs)
        }
        Err(_) => {
            HttpResponse::RequestTimeout().json(HashMap::from([("error", "execute timeout")]))
        }
    }
}

#[get("/db/backup")]
async fn handle_db_backup(_req: HttpRequest, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let query_str = _req.query_string(); // "name=ferret"
    let qs = QString::from(query_str);
    let no_leader = qs.get("noleader").is_some();
    let fmt = qs.get("fmt").map_or("binary", |v| v);
    let f = match fmt.to_lowercase().as_str() {
        "sql" => BackupFormat::SQL,
        "binary" => BackupFormat::Binary,
        _ => {
            return HttpResponse::BadRequest().json(HashMap::from([(
                "error",
                format!("Invalid `fmt` argument: {}", fmt),
            )]))
        }
    };
    let res = match node.backup(!no_leader, f).await {
        Ok(data) => data,
        Err(e) => return store_err_to_resp(e),
    };
    HttpResponse::Ok().body(res)
}

// handleLoad loads the state contained in a .dump output. This API is different
// from others in that it expects a raw file, not wrapped in any kind of JSON.
#[post("/db/load")]
async fn handle_db_load(
    _req: HttpRequest,
    req_body: String,
    node: web::Data<Arc<RqliteNode>>,
) -> impl Responder {
    let query_str = _req.query_string(); // "name=ferret"
    let qs = QString::from(query_str);
    let timings = qs.get("timings").is_some();
    let er = command::ExecuteRequest {
        request: Some(command::Request {
            statements: vec![command::Statement {
                sql: req_body,
                ..Default::default()
            }],
            ..Default::default()
        }),
        timings: timings,
        ..Default::default()
    };
    HttpResponse::Ok().body("haha")
}

// handleJoin handles cluster-join requests from other nodes.
#[derive(Serialize, Deserialize)]
struct JoinPayload {
    addr: String,
    id: String,
    voter: bool,
}
#[post("/join")]
async fn handle_join(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let res = serde_json::from_str::<JoinRequest>(&req_body);
    let req = match res {
        Ok(r) => r,
        Err(e) => return HttpResponse::BadRequest().body(format!("{}", e)),
    };
    let _ = match node.join(req).await {
        Ok(resp) => resp,
        Err(e) => return store_err_to_resp(e),
    };
    HttpResponse::Ok().body("ok")
}

#[post("/notify")]
async fn handle_notify(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[delete("/remove")]
async fn handle_remove(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let res = serde_json::from_str::<HashMap<String, String>>(&req_body);
    let params = match res {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("{}", e)),
    };
    let id_str = params.get("id");
    if id_str.is_none() {
        return HttpResponse::BadRequest().body(format!("need `id` argument"));
    }
    let id_str = id_str.unwrap();
    let id = match id_str.parse::<u64>() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadGateway().body(format!("invalid `id`: {}", e)),
    };

    let req = RemoveNodeRequest { node_id: id };
    node.remove(req)
        .await
        .map_or_else(|e| store_err_to_resp(e), |_| HttpResponse::Ok().body("ok"))
}

#[get("/status")]
async fn handle_status(
    _req: HttpRequest,
    req_body: String,
    node: web::Data<Arc<RqliteNode>>,
) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[derive(Serialize, Deserialize, Default)]
struct NodeInfo {
    api_addr: String,
    addr: String,
    reachable: bool,
    leader: bool,
    time: f64,
    error: String,
}

// handle_nodes returns status on the other voting nodes in the system.
// This attempts to contact all the nodes in the cluster, so may take
// some time to return.
#[get("/nodes")]
async fn handle_nodes(_req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let res = HashMap::from([("key1", NodeInfo::default())]);
    HttpResponse::Ok().json(res)
}

#[get("/healthz")]
async fn handle_healthz(_req_body: String) -> impl Responder {
    HttpResponse::Ok().body("ok")
}
