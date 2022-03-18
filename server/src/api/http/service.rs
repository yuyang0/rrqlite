use crate::config::Config;
use actix_web::{delete, get, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use core_command::command;
use core_exception::Result;
use core_tracing::tracing;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use qstring::QString;
use tracing_actix_web::TracingLogger;
// use serde::{Deserialize, Serialize};
use super::util::parse_sql_stmts;
use core_store::RqliteNode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn qs_get_i64(qs: &QString, name: &str, dft: i64) -> Result<i64> {
    match qs.get(name) {
        Some(sv) => {
            let iv = sv.parse::<i64>()?;
            Ok(iv)
        }
        None => Ok(dft),
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
    let rediect = qs.get("redirect").is_some();
    let timeout = match qs_get_i64(&qs, "timeout", 50) {
        Err(e) => return HttpResponse::BadRequest().body("invalid timeout argument"),
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
    HttpResponse::Ok().json(er)
}

// allow GET, POST
#[post("/db/query")]
async fn handle_db_query(
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
    let rediect = qs.get("redirect").is_some();
    let timeout = match qs_get_i64(&qs, "timeout", 50) {
        Err(_) => return HttpResponse::BadRequest().body("invalid timeout argument"),
        Ok(v) => v,
    };
    let freshness = match qs_get_i64(&qs, "freshness", 50) {
        Err(_) => return HttpResponse::BadRequest().body("invalid freshness argument"),
        Ok(v) => v,
    };
    let level = match qs.get("level") {
        Some(sv) => match sv.to_lowercase() {
            "none" => command::query_request::Level::QueryRequestLevelNone,
            "srong" => command::query_request::Level::QueryRequestLevelStrong,
            "weak" => command::query_request::Level::QueryRequestLevelWeak,
            _ => return HttpResponse::BadRequest().body("invalid level argument"),
        },
        None => command::query_request::Level::QueryRequestLevelWeak,
    };

    let qr = match stmts_opt {
        Err(e) => {
            tracing::warn!("invalid query request {}", e);
            let msg = HashMap::from([("error", format!("{}", e))]);
            return HttpResponse::BadRequest().json(msg);
        }
        Ok(stmts) => {
            let qr = command::QueryRequest {
                request: Some(command::Request {
                    transaction: is_txn,
                    statements: stmts,
                }),
                timings: timings,
                freshness: freshness,
                level: level,
            };
            qr
        }
    };
    HttpResponse::Ok().json(qr)
}

#[get("/db/backup")]
async fn handle_db_backup(_req: HttpRequest, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let query_str = _req.query_string(); // "name=ferret"
    let qs = QString::from(query_str);
    let is_txn = qs.get("fmt").is_some();
    HttpResponse::Ok().body("haha")
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
    HttpResponse::Ok().body(req_body)
}

#[post("/notify")]
async fn handle_notify(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[delete("/remove")]
async fn handle_remove(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    HttpResponse::Ok().body(req_body)
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
async fn handle_nodes(req_body: String, node: web::Data<Arc<RqliteNode>>) -> impl Responder {
    let res = HashMap::from([("key1", NodeInfo::default())]);
    HttpResponse::Ok().json(res)
}

#[get("/healthz")]
async fn handle_healthz(req_body: String) -> impl Responder {
    HttpResponse::Ok().body("ok")
}
