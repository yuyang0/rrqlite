use clap::Parser;
use core_exception::{ErrorCode, Result};
use core_store::config::StoreConfig;
use serde::{Deserialize, Serialize};

macro_rules! load_field_from_env {
    ($field:expr, $field_type: ty, $env:expr) => {
        if let Some(env_var) = std::env::var_os($env) {
            $field = env_var
                .into_string()
                .expect(format!("cannot convert {} to string", $env).as_str())
                .parse::<$field_type>()
                .expect(format!("cannot convert {} to {}", $env, stringify!($field_type)).as_str());
        }
    };
}

// Config represents the configuration as set by command-line flags.
// All variables will be set, unless explicit noted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser, Default)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', env = "RQLITED_CONFIG_FILE", default_value = "")]
    pub config_file: String,

    // DataPath is path to node data. Always set.
    #[clap(long = "data-path", env = "RQLITED_DATA_PATH", default_value = "")]
    pub data_path: String,

    #[clap(long = "log-dir", env = "RQLITED_LOG_DIR", default_value = "./_logs")]
    pub log_dir: String,

    #[clap(long = "log-level", env = "RQLITED_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    // NodeEncrypt indicates whether node encryption should be enabled.
    #[clap(long, env = "RQLITED_NODE_ENCRYPT")]
    pub node_encrypt: bool,

    // NodeX509CACert is the path the root-CA certficate file for when this
    // node contacts other nodes' Raft servers. May not be set.
    #[clap(
        long = "node-ca-cert",
        env = "RQLITED_NODE_CA_CERT",
        default_value = ""
    )]
    pub node_x509_ca_cert: String,

    // NodeX509Cert is the path to the X509 cert for the Raft server. May not be set.
    #[clap(
        long = "node-cert",
        env = "RQLITED_NODE_CERT",
        default_value = "cert.pem"
    )]
    pub node_x509_cert: String,

    // NodeX509Key is the path to the X509 key for the Raft server. May not be set.
    #[clap(long = "node-key", env = "RQLITED_NODE_KEY", default_value = "key.pem")]
    pub node_x509_key: String,

    // NodeID is the Raft ID for the node.
    #[clap(long = "node-id", env = "RQLITED_NODE_ID", default_value = "")]
    pub node_id: String,

    // BootstrapExpect is the minimum number of nodes required for a bootstrap.
    #[clap(
        long = "bootstrap-expect",
        env = "RQLITED_BOOTSTRAP_EXPECT",
        default_value_t = 0
    )]
    pub bootstrap_expect: i32,

    // BootstrapExpectTimeout is the maximum time a bootstrap operation can take.
    #[clap(
        long = "bootstrap-expect-timeout",
        env = "RQLITED_BOOTSTRAP_EXPECT_TIMEOUT",
        default_value_t = 120
    )]
    pub bootstrap_expect_timeout: i32,

    // NoHTTPVerify disables checking other nodes' HTTP X509 certs for validity.
    #[clap(long = "no-http-verify", env = "RQLITED_NO_HTTP_VERIFY")]
    pub no_http_verify: bool,

    // NoNodeVerify disables checking other nodes' Node X509 certs for validity.
    #[clap(long = "no-node-verify", env = "RQLITED_NO_NODE_VERIFY")]
    pub no_node_verify: bool,

    // DisoMode sets the discovery mode. May not be set.
    #[clap(long = "disco-mode", env = "RQLITED_DISCO_MODE", default_value = "")]
    pub disco_mode: String,

    // DiscoKey sets the discovery prefix key.
    #[clap(
        long = "disco-key",
        env = "RQLITED_DISCO_KEY",
        default_value = "sqlite"
    )]
    pub disco_key: String,

    // DiscoConfig sets the path to any discovery configuration file. May not be set.
    #[clap(
        long = "disco-config",
        env = "RQLITED_DISCO_CONFIG",
        default_value = ""
    )]
    pub disco_config: String,

    // Expvar enables go/expvar information. Defaults to true.
    #[clap(long = "expvar", env = "RQLITED_EXPVAR")]
    pub expvar: bool,

    // PprofEnabled enables Go PProf information. Defaults to true.
    #[clap(long = "pprof-enabled", env = "RQLITED_PPROF_ENABLED")]
    pub pprof_enabled: bool,

    // // FKConstraints enables SQLite foreign key constraints.
    // FKConstraints bool

    // // CompressionSize sets request query size for compression attempt
    // CompressionSize int

    // // CompressionBatch sets request batch threshold for compression attempt.
    // CompressionBatch int

    // // CPUProfile enables CPU profiling.
    // CPUProfile string

    // // MemProfile enables memory profiling.
    // MemProfile string
    #[clap(flatten)]
    pub http: HttpConfig,

    #[clap(flatten)]
    pub store: StoreConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser, Default)]
#[serde(default)]
pub struct HttpConfig {
    // HTTPAddr is the bind network address for the HTTP Server.
    // It never includes a trailing HTTP or HTTPS.
    #[clap(
        long = "http-addr",
        env = "RQLITED_HTTP_ADDR",
        default_value = "localhost:4001"
    )]
    pub http_addr: String,

    // HTTPAdv is the advertised HTTP server network.
    #[clap(
        long = "http-adv-addr",
        env = "RQLITED_HTTP_ADV_ADDR",
        default_value = ""
    )]
    pub http_adv_addr: String,

    // TLS1011 indicates whether the node should support deprecated
    // encryption standards.
    #[clap(long = "tls1011", env = "RQLITED_TLS1011")]
    pub tls1011: bool,

    // AuthFile is the path to the authentication file. May not be set.
    #[clap(long = "auth-file", env = "RQLITED_AUTH_FILE", default_value = "")]
    pub auth_file: String,

    // X509CACert is the path the root-CA certficate file for when this
    // node contacts other nodes' HTTP servers. May not be set.
    #[clap(
        long = "http-ca-cert",
        env = "RQLITED_HTTP_CA_CERT",
        default_value = ""
    )]
    pub x509_ca_cert: String,

    // X509Cert is the path to the X509 cert for the HTTP server. May not be set.
    #[clap(long = "http-cert", env = "RQLITED_HTTP_CERT", default_value = "")]
    pub x509_cert: String,

    // X509Key is the path to the private key for the HTTP server. May not be set.
    #[clap(long = "http-key", env = "RQLITED_HTTP_KEY", default_value = "")]
    pub x509_key: String,
}

impl Config {
    /// First load configs from args.
    /// If config file is not empty, e.g: `-c xx.toml`, reload config from the
    /// file. Prefer to use environment variables in cloud native
    /// deployment. Override configs base on environment variables.
    pub fn load() -> Result<Self> {
        let mut cfg = Config::parse();
        if !cfg.config_file.is_empty() {
            cfg = Self::load_from_toml(cfg.config_file.as_str())?;
        }

        // Self::load_from_env(&mut cfg);
        cfg.check()?;
        Ok(cfg)
    }
    /// Load configs from toml file.
    pub fn load_from_toml(file: &str) -> Result<Self> {
        let txt = std::fs::read_to_string(file)
            .map_err(|e| ErrorCode::ConfigError(format!("File: {}, err: {:?}", file, e)))?;

        let cfg = toml::from_str::<Config>(txt.as_str())
            .map_err(|e| ErrorCode::ConfigError(format!("{:?}", e)))?;

        cfg.check()?;
        Ok(cfg)
    }
    pub fn check(&self) -> Result<()> {
        // self.raft_config.check()?;
        Ok(())
    }

    /// Load configs from environment variables.
    pub fn load_from_env(cfg: &mut Config) {
        load_field_from_env!(cfg.log_level, String, "RQLITED_LOG_LEVEL");
        load_field_from_env!(cfg.log_dir, String, "RQLITED_LOG_DIR");
        //     load_field_from_env!(cfg.metric_api_address, String,
        // METASRV_METRIC_API_ADDRESS);     load_field_from_env!(cfg.
        // admin_api_address, String, ADMIN_API_ADDRESS);
        // load_field_from_env! (cfg.admin_tls_server_cert, String,
        // ADMIN_TLS_SERVER_CERT);     load_field_from_env!(cfg.
        // admin_tls_server_key, String, ADMIN_TLS_SERVER_KEY);
        // load_field_from_env!(cfg.grpc_api_address,
        // String, METASRV_GRPC_API_ADDRESS);     load_field_from_env!(cfg.
        // grpc_tls_server_cert, String, GRPC_TLS_SERVER_CERT);
        //     load_field_from_env!(cfg.grpc_tls_server_key, String,
        // GRPC_TLS_SERVER_KEY);     load_field_from_env!(
        //         cfg.raft_config.raft_listen_host,
        //         String,
        //         raft_config::KVSRV_LISTEN_HOST
        //     );
        //     load_field_from_env!(
        //         cfg.raft_config.raft_advertise_host,
        //         String,
        //         raft_config::KVSRV_ADVERTISE_HOST
        //     );
        //     load_field_from_env!(
        //         cfg.raft_config.raft_api_port,
        //         u32,
        //         raft_config::KVSRV_API_PORT
        //     );
        //     load_field_from_env!(
        //         cfg.raft_config.raft_dir,
        //         String,
        //         raft_config::KVSRV_RAFT_DIR
        //     );
        //     load_field_from_env!(cfg.raft_config.no_sync, bool,
        // raft_config::KVSRV_NO_SYNC);     load_field_from_env!(
        //         cfg.raft_config.snapshot_logs_since_last,
        //         u64,
        //         raft_config::KVSRV_SNAPSHOT_LOGS_SINCE_LAST
        //     );
        //     load_field_from_env!(
        //         cfg.raft_config.heartbeat_interval,
        //         u64,
        //         raft_config::KVSRV_HEARTBEAT_INTERVAL
        //     );
        //     load_field_from_env!(
        //         cfg.raft_config.install_snapshot_timeout,
        //         u64,
        //         raft_config::KVSRV_INSTALL_SNAPSHOT_TIMEOUT
        //     );
        //     load_field_from_env!(cfg.raft_config.single, bool,
        // raft_config::KVSRV_SINGLE);
        // load_field_from_env!(cfg.raft_config.id, u64, raft_config::
        // KVSRV_ID);
    }
}
