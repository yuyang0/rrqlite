use core_exception::Result;
use core_store::store::Store;
use core_tracing::{init_global_tracing, tracing};
use rqlited::api::create_server;
use rqlited::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let _conf = Config::load()?;
    let _guards = init_global_tracing("rqlited", &_conf.log_dir, &_conf.log_level);
    let s = Store::new();
    tracing::info!("Starting api server at {}", _conf.http_config.http_addr);
    let web_srv = create_server(&_conf, s).unwrap();
    let web_srv_handle = web_srv.handle();
    let api_handle = tokio::spawn(web_srv);
    tokio::join!(api_handle);

    Ok(())
}
