use std::sync::Arc;

use actix_server::{Server as ActixServer, ServerHandle};
use actix_web::{web, App, HttpServer};
use core_exception::Result;
use core_store::RqliteNode;
use core_tracing::tracing;
use core_util_misc::Stoppable;
use futures::future::Either;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing_actix_web::TracingLogger;

use super::service;
use crate::config::Config;

pub struct Server {
    name: String,
    cfg: Config,
    node: Arc<RqliteNode>,
    srv_handle: Option<ServerHandle>,
    join_handle: Option<JoinHandle<std::io::Result<()>>>,
}

impl Server {
    pub fn new(_conf: &Config, app_node: Arc<RqliteNode>) -> Result<Server> {
        Ok(Server {
            name: String::from("http-api"),
            cfg: _conf.clone(),
            node: app_node,
            srv_handle: None,
            join_handle: None,
        })
    }

    fn create_actix_server(&self) -> Result<ActixServer> {
        let node = self.node.clone();
        let mut srv = HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::default())
                .app_data(web::Data::new(node.clone()))
                .service(service::handle_db_execute)
                .service(service::handle_db_query_get)
                .service(service::handle_db_query_post)
                .service(service::handle_db_backup)
                .service(service::handle_db_load)
                .service(service::handle_join)
                .service(service::handle_notify)
                .service(service::handle_remove)
                .service(service::handle_status)
                .service(service::handle_nodes)
                .service(service::handle_readyz)
        });

        let cfg = self.cfg.clone();
        let bind_and_run = || -> Result<ActixServer> {
            if cfg.http.x509_cert.is_empty() {
                srv = srv.bind(&cfg.http.http_addr)?;
            } else {
                let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
                builder
                    .set_private_key_file(&cfg.http.x509_key, SslFiletype::PEM)
                    .unwrap();
                builder
                    .set_certificate_chain_file(&cfg.http.x509_cert)
                    .unwrap();
                srv = srv.bind_openssl(&cfg.http.http_addr, builder)?;
            }
            let srv = srv.run();
            Ok(srv)
        };
        let srv = bind_and_run()?;
        Ok(srv)
    }
}

#[async_trait::async_trait]
impl Stoppable for Server {
    async fn start(&mut self) -> Result<()> {
        let srv = self.create_actix_server()?;
        let handle = srv.handle();
        self.srv_handle = Some(handle);

        let join_handle = tokio::spawn(srv);
        self.join_handle = Some(join_handle);
        Ok(())
    }

    /// Blocking stop. It should not return until everything is cleaned up.
    ///
    /// In case a graceful `stop()` had blocked for too long,
    /// the caller submit a FORCE stop by sending a `()` to `force`.
    /// An impl should either close everything at once, or just ignore the
    /// `force` signal if it does not support force stop.
    ///
    /// Calling `stop()` twice should get an error.
    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<()> {
        let srv_handle = self.srv_handle.take().unwrap();
        let join_handle = srv_handle.stop(true);

        if let Some(mut force) = force {
            let h = Box::pin(join_handle);
            let f = Box::pin(force.recv());

            match futures::future::select(f, h).await {
                Either::Left((_x, _h)) => {
                    tracing::info!("{}: received force shutdown signal", self.name);
                    let task_handle = self.join_handle.as_ref().unwrap();
                    task_handle.abort();
                }
                Either::Right((_, _)) => {
                    tracing::info!("Done: {}: graceful shutdown", self.name);
                }
            }
        } else {
            tracing::info!(
                "{}: force is None, wait for join handle for ever",
                self.name
            );

            let res = join_handle.await;

            tracing::info!(
                "Done: {}: waiting for join handle for ever, res: {:?}",
                self.name,
                res
            );
        }
        Ok(())
    }
}
