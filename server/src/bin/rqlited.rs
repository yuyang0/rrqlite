use core_exception::Result;
use core_store::RqliteNode;
use core_tracing::{init_global_tracing, tracing};
use core_util_misc::StopHandle;
use rqlited::api::http;
use rqlited::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let _conf = Config::load()?;
    let _guards = init_global_tracing("rqlited", &_conf.log_dir, &_conf.log_level);

    let node = RqliteNode::new();
    let mut stop_handler = StopHandle::create();
    let stop_tx = StopHandle::install_termination_handle();

    let s = Store::new();
    // HTTP API service.
    {
        let srv = http::Server::new(&_conf, Arc::new(node))?;
        tracing::info!(
            "Starting HTTP API server at {}",
            _conf.http_config.http_addr
        );
        srv.start().await.expect("Failed to start http server");
        stop_handler.push(srv);
    }

    stop_handler.wait_to_terminate(stop_tx).await;
    tracing::info!("Databend-meta is done shutting down");

    Ok(())
}
