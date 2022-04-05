use core_exception::Result;
use core_store::RqliteNode;
use core_tracing::{init_global_tracing, tracing};
use core_util_misc::{StopHandle, Stoppable};
use rqlited::api::http;
use rqlited::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let _conf = Config::load()?;
    let _guards = init_global_tracing("rqlited", &_conf.log_dir, &_conf.log_level);

    let node = RqliteNode::start(&_conf.store).await?;
    let mut stop_handler = StopHandle::create();
    let stop_tx = StopHandle::install_termination_handle();

    // HTTP API service.
    {
        let mut srv = http::Server::new(&_conf, node.clone())?;
        tracing::info!("Starting HTTP API server at {}", _conf.http.http_addr);
        srv.start().await.expect("Failed to start http server");
        stop_handler.push(Box::new(srv));
    }
    // join raft cluster after all service started
    node.join_cluster(&_conf.store.raft).await?;

    stop_handler.wait_to_terminate(stop_tx).await;
    tracing::info!("Rqlited is done shutting down");

    Ok(())
}
