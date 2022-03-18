use crate::config::Config;
use actix_web::{delete, get, post, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use core_command::command;
use core_exception::Result;
use core_tracing::tracing;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use qstring::QString;
use tracing_actix_web::TracingLogger;
// use serde::{Deserialize, Serialize};
use super::service;
use core_store::RqliteNode;
use core_util_misc::Stoppable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct Server {
    cfg: Config,
    srv: Arc<HttpServer>,
    node: Arc<RqliteNode>,
}

impl Server {
    pub fn new(_conf: &Config, node: Arc<RqliteNode>) -> Result<Server> {
        let mut srv = HttpServer::new(|| Self::build_app(node.clone()));

        Ok(Server {
            cfg: _conf.clone(),
            srv: Arc::new(srv),
            node: node,
        })
    }

    fn build_app(node: Arc<RqliteNode>) -> App {
        App::new()
            .wrap(TracingLogger::default())
            .app_data(web::Data::new(node))
            .service(service::handle_db_execute)
            .service(handle_db_query)
            .service(handle_db_backup)
            .service(handle_db_load)
            .service(handle_join)
            .service(handle_notify)
            .service(handle_remove)
            .service(handle_status)
            .service(handle_nodes)
            .service(handle_healthz)
    }
}

impl Stoppable for Server {
    async fn start(&mut self) -> Result<(), ErrorCode> {
        if self.cfg.http_config.x509_cert.is_empty() {
            srv = self.srv.bind(&self.cfg.http_config.http_addr)?;
        } else {
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
            builder
                .set_private_key_file(&self.cfg.http_config.x509_key, SslFiletype::PEM)
                .unwrap();
            builder
                .set_certificate_chain_file(&self.cfg.http_config.x509_cert)
                .unwrap();
            srv = self
                .srv
                .bind_openssl(&self.cfg.http_config.http_addr, builder)?;
        }
        self.srv.run();
        Ok(())
    }

    /// Blocking stop. It should not return until everything is cleaned up.
    ///
    /// In case a graceful `stop()` had blocked for too long,
    /// the caller submit a FORCE stop by sending a `()` to `force`.
    /// An impl should either close everything at once, or just ignore the `force` signal if it does not support force stop.
    ///
    /// Calling `stop()` twice should get an error.
    async fn stop(&mut self, mut force: Option<broadcast::Receiver<()>>) -> Result<(), ErrorCode> {}
}
