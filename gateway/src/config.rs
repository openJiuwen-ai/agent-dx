use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

pub const INGRESS_ETCD_ENDPOINTS_ENV: &str = "ADX_DATA_PLANE_INGRESS_ETCD_ENDPOINTS";
pub const DEFAULT_CONTROL_PLANE_ROUTES: &str = "exact:/,exact:/healthz,prefix:/terminal,prefix:/api/instances,prefix:/api/admin/v1/keys,prefix:/api/jobs,prefix:/api/sandbox,prefix:/functions,prefix:/api-docs,prefix:/admin/v1/functions,prefix:/serverless/v1/functions,prefix:/serverless/v1/stream,prefix:/serverless/v1/componentshealth,prefix:/serverless/v1/posix,prefix:/frontend/v1/instance,prefix:/datasystem/v1,prefix:/serverless/v2,prefix:/app/v1,prefix:/client/v1/lease,prefix:/invocations,prefix:/global-scheduler";

/// Strongly typed process configuration for the Ingress.
///
/// adx's TOML configuration remains the deployment source of truth. The
/// launcher renders the values into the child process environment; this type
/// centralizes parsing and validation so binaries do not scatter `env::var`
/// calls throughout startup code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressConfig {
    pub etcd_endpoints: Vec<String>,
    pub tls_bind: SocketAddr,
    pub plain_bind: SocketAddr,
    pub health_bind: SocketAddr,
    pub tls_cert: String,
    pub tls_key: String,
    pub frontend_address: String,
    pub reverse_proxy: crate::ingress::ReverseProxyConfig,
    pub proxy_routes: Vec<crate::ingress::ProxyRoute>,
    pub control_plane_routes: Vec<crate::ingress::StaticRoute>,
    pub validate_iam: bool,
    pub iam_address: String,
    pub auth_cache_ttl: Duration,
    pub default_direct_port: u16,
    pub default_tunnel_port: u16,
    pub node_security_mode: IngressNodeSecurityMode,
    pub node_tls_ca: String,
    pub node_tls_server_name: String,
    pub node_tls_client_cert: String,
    pub node_tls_client_key: String,
    pub connections_per_node: usize,
    pub max_connections_per_node: usize,
    pub backend_http_max_connections_per_endpoint: usize,
    pub backend_http_max_idle_connections: usize,
    pub backend_http_max_idle_connections_per_endpoint: usize,
    pub backend_http_idle_timeout: Duration,
    pub backend_http_acquire_timeout: Duration,
    pub drain_timeout: Duration,
    pub allowed_client_networks: Vec<ipnet::IpNet>,
    pub allow_any_client: bool,
    pub command_watch_max_subscriptions: usize,
    pub command_watch_queue_capacity: usize,
    pub command_watch_max_frame_bytes: usize,
    pub command_watch_ping_interval: Duration,
    pub etcd_tls_ca: String,
    pub etcd_tls_cert: String,
    pub etcd_tls_key: String,
    pub etcd_tls_domain: String,
    pub etcd_username: String,
    pub etcd_password: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{INGRESS_ETCD_ENDPOINTS_ENV} is not set")]
    MissingEtcdEndpoints,
    #[error("{INGRESS_ETCD_ENDPOINTS_ENV} must contain at least one etcd endpoint")]
    EmptyEtcdEndpoints,
    #[error("{INGRESS_ETCD_ENDPOINTS_ENV} contains non-Unicode data")]
    InvalidEtcdEndpoints,
    #[error("invalid Ingress configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub bind: SocketAddr,
    pub advertise_address: String,
    pub health_bind: SocketAddr,
    pub allowed_target_networks: Vec<ipnet::IpNet>,
    pub allowed_ingress_networks: Vec<ipnet::IpNet>,
    pub allow_any_ingress: bool,
    pub max_streams: usize,
    pub ingress_security_mode: IngressNodeSecurityMode,
    pub tls_cert: String,
    pub tls_key: String,
    pub mtls_client_ca: String,
    pub activity_uds_dir: Option<String>,
    pub activity_interval: Duration,
    pub gateway_epoch: String,
    pub drain_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressNodeSecurityMode {
    Network,
    Mtls,
}

impl std::str::FromStr for IngressNodeSecurityMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "network" => Ok(Self::Network),
            "mtls" => Ok(Self::Mtls),
            _ => Err(format!("expected network or mtls, got {value}")),
        }
    }
}

impl RelayConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind = parse_env("ADX_DATA_PLANE_RELAY_BIND", "0.0.0.0:8443")?;
        let advertise_address =
            env::var("ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS").unwrap_or_default();
        let health_bind = parse_env("ADX_DATA_PLANE_RELAY_HEALTH_BIND", "127.0.0.1:18443")?;
        let allowed_target_networks = parse_cidrs("ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS", true)?;
        let allowed_ingress_networks = parse_cidrs("ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS", false)?;
        let allow_any_ingress = parse_bool_env("ADX_DATA_PLANE_RELAY_ALLOW_ANY_INGRESS", false)?;
        if allowed_ingress_networks.is_empty() && !allow_any_ingress {
            return Err(ConfigError::Invalid(
                "Relay requires ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS unless unrestricted development access is explicitly enabled".into(),
            ));
        }
        let configured_max_streams = parse_env("ADX_DATA_PLANE_RELAY_MAX_STREAMS", "0")?;
        let max_streams = if configured_max_streams == 0 {
            fd_based_stream_budget()?
        } else {
            configured_max_streams
        };
        let ingress_security_mode =
            parse_env("ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE", "network")?;
        let tls_cert = env::var("ADX_DATA_PLANE_RELAY_TLS_CERT").unwrap_or_default();
        let tls_key = env::var("ADX_DATA_PLANE_RELAY_TLS_KEY").unwrap_or_default();
        let mtls_client_ca = env::var("ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA").unwrap_or_default();
        match ingress_security_mode {
            IngressNodeSecurityMode::Mtls
                if tls_cert.is_empty() || tls_key.is_empty() || mtls_client_ca.is_empty() =>
            {
                return Err(ConfigError::Invalid(
                    "Relay mTLS mode requires the server certificate, key, and Ingress client CA"
                        .into(),
                ));
            }
            _ => {}
        }
        let activity_uds_dir = env::var("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let activity_interval = Duration::from_secs(parse_env(
            "ADX_DATA_PLANE_RELAY_ACTIVITY_INTERVAL_SEC",
            "30",
        )?);
        let gateway_epoch =
            env::var("ADX_DATA_PLANE_RELAY_EPOCH").unwrap_or_else(|_| default_gateway_epoch());
        let drain_timeout =
            Duration::from_secs(parse_env("ADX_DATA_PLANE_RELAY_DRAIN_TIMEOUT_SEC", "30")?);
        Ok(Self {
            bind,
            advertise_address,
            health_bind,
            allowed_target_networks,
            allowed_ingress_networks,
            allow_any_ingress,
            max_streams,
            ingress_security_mode,
            tls_cert,
            tls_key,
            mtls_client_ca,
            activity_uds_dir,
            activity_interval,
            gateway_epoch,
            drain_timeout,
        })
    }

    pub fn peer_allowed(&self, peer: std::net::IpAddr) -> bool {
        self.allow_any_ingress
            || self
                .allowed_ingress_networks
                .iter()
                .any(|network| network.contains(&peer))
    }
}

impl IngressConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let etcd_endpoints = Vec::new();
        let tls_bind = parse_env("ADX_DATA_PLANE_INGRESS_TLS_BIND", "0.0.0.0:8443")?;
        let plain_bind = parse_env("ADX_DATA_PLANE_INGRESS_PLAIN_BIND", "0.0.0.0:8080")?;
        let health_bind = parse_env("ADX_DATA_PLANE_INGRESS_HEALTH_BIND", "127.0.0.1:18080")?;
        let tls_cert = env::var("ADX_DATA_PLANE_INGRESS_TLS_CERT").unwrap_or_default();
        let tls_key = env::var("ADX_DATA_PLANE_INGRESS_TLS_KEY").unwrap_or_default();
        if tls_cert.is_empty() || tls_key.is_empty() {
            return Err(ConfigError::Invalid(
                "Ingress TLS listener requires a certificate and key".into(),
            ));
        }
        let frontend_address = env::var("ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8888".into());
        if frontend_address.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "Ingress upstream address is required".into(),
            ));
        }
        let control_plane_routes = crate::ingress::parse_static_routes(
            &env::var("ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES")
                .unwrap_or_else(|_| DEFAULT_CONTROL_PLANE_ROUTES.into()),
        )
        .map_err(|error| {
            ConfigError::Invalid(format!(
                "ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES: {error}"
            ))
        })?;
        let proxy_routes = match env::var("ADX_DATA_PLANE_INGRESS_PROXY_ROUTES_FILE") {
            Ok(path) => {
                let input = std::fs::read_to_string(path).map_err(|error| {
                    ConfigError::Invalid(format!("read proxy routes file: {error}"))
                })?;
                crate::ingress::parse_proxy_routes(&input).map_err(ConfigError::Invalid)?
            }
            Err(env::VarError::NotPresent) => Vec::new(),
            Err(error) => return Err(ConfigError::Invalid(format!("proxy routes file: {error}"))),
        };
        let validate_iam = false;
        let iam_address = String::new();
        let auth_cache_ttl = Duration::from_secs(parse_env(
            "ADX_DATA_PLANE_INGRESS_AUTH_CACHE_TTL_SEC",
            "30",
        )?);
        let default_direct_port = parse_env("ADX_DATA_PLANE_INGRESS_DIRECT_PORT", "50090")?;
        let default_tunnel_port = parse_env("ADX_DATA_PLANE_INGRESS_TUNNEL_PORT", "8765")?;
        let node_security_mode = parse_env("ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE", "network")?;
        let node_tls_ca = env::var("ADX_DATA_PLANE_INGRESS_NODE_TLS_CA").unwrap_or_default();
        let node_tls_server_name =
            env::var("ADX_DATA_PLANE_INGRESS_NODE_TLS_SERVER_NAME").unwrap_or_default();
        let node_tls_client_cert =
            env::var("ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_CERT").unwrap_or_default();
        let node_tls_client_key =
            env::var("ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_KEY").unwrap_or_default();
        match node_security_mode {
            IngressNodeSecurityMode::Mtls
                if node_tls_ca.is_empty()
                    || node_tls_server_name.is_empty()
                    || node_tls_client_cert.is_empty()
                    || node_tls_client_key.is_empty() =>
            {
                return Err(ConfigError::Invalid(
                    "Ingress mTLS mode requires the Relay CA/server name and Ingress client certificate/key"
                        .into(),
                ));
            }
            _ => {}
        }
        let connections_per_node =
            parse_env("ADX_DATA_PLANE_INGRESS_H2_CONNECTIONS_PER_NODE", "2")?;
        let max_connections_per_node =
            parse_env("ADX_DATA_PLANE_INGRESS_H2_MAX_CONNECTIONS_PER_NODE", "4")?;
        if connections_per_node == 0 || max_connections_per_node < connections_per_node {
            return Err(ConfigError::Invalid(
                "H2 connection counts must be non-zero and max must be >= initial count".into(),
            ));
        }
        let backend_http_max_connections_per_endpoint = parse_env(
            "ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_CONNECTIONS_PER_ENDPOINT",
            "64",
        )?;
        let backend_http_max_idle_connections = parse_env(
            "ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS",
            "1024",
        )?;
        let backend_http_max_idle_connections_per_endpoint = parse_env(
            "ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS_PER_ENDPOINT",
            "64",
        )?;
        if backend_http_max_connections_per_endpoint == 0
            || backend_http_max_idle_connections == 0
            || backend_http_max_idle_connections_per_endpoint == 0
            || backend_http_max_idle_connections_per_endpoint
                > backend_http_max_connections_per_endpoint
        {
            return Err(ConfigError::Invalid(
                "backend HTTP pool limits must be non-zero and per-endpoint idle must not exceed per-endpoint connections".into(),
            ));
        }
        let backend_http_idle_timeout = Duration::from_secs(parse_env(
            "ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_IDLE_TIMEOUT_SEC",
            "5",
        )?);
        let backend_http_acquire_timeout = Duration::from_millis(parse_env(
            "ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_ACQUIRE_TIMEOUT_MS",
            "3000",
        )?);
        if backend_http_idle_timeout.is_zero() || backend_http_acquire_timeout.is_zero() {
            return Err(ConfigError::Invalid(
                "backend HTTP pool timeouts must be non-zero".into(),
            ));
        }
        let reverse_proxy = crate::ingress::ReverseProxyConfig {
            max_idle_connections: parse_env(
                "ADX_DATA_PLANE_INGRESS_PROXY_MAX_IDLE_CONNECTIONS",
                "512",
            )?,
            idle_timeout: Duration::from_secs(parse_env(
                "ADX_DATA_PLANE_INGRESS_PROXY_IDLE_TIMEOUT_SEC",
                "30",
            )?),
            connect_timeout: Duration::from_secs(parse_env(
                "ADX_DATA_PLANE_INGRESS_PROXY_CONNECT_TIMEOUT_SEC",
                "5",
            )?),
        };
        if reverse_proxy.idle_timeout.is_zero() || reverse_proxy.connect_timeout.is_zero() {
            return Err(ConfigError::Invalid(
                "reverse proxy HTTP timeouts must be non-zero".into(),
            ));
        }
        let drain_timeout =
            Duration::from_secs(parse_env("ADX_DATA_PLANE_INGRESS_DRAIN_TIMEOUT_SEC", "30")?);
        let allowed_client_networks =
            parse_cidrs("ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS", false)?;
        let allow_any_client = parse_bool_env("ADX_DATA_PLANE_INGRESS_ALLOW_ANY_CLIENT", false)?;
        if allowed_client_networks.is_empty() && !allow_any_client {
            return Err(ConfigError::Invalid(
                "Ingress requires ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS unless unrestricted development access is explicitly enabled".into(),
            ));
        }
        let command_watch_max_subscriptions =
            parse_env("ADX_COMMAND_WATCH_MAX_SUBSCRIPTIONS_PER_CONNECTION", "4096")?;
        let command_watch_queue_capacity = parse_env("ADX_COMMAND_WATCH_QUEUE_CAPACITY", "256")?;
        let command_watch_max_frame_bytes =
            parse_env("ADX_COMMAND_WATCH_MAX_FRAME_BYTES", "1048576")?;
        let command_watch_ping_interval =
            Duration::from_secs(parse_env("ADX_COMMAND_WATCH_PING_INTERVAL_SECS", "20")?);
        if command_watch_max_subscriptions == 0
            || command_watch_queue_capacity == 0
            || command_watch_max_frame_bytes == 0
            || command_watch_ping_interval.is_zero()
        {
            return Err(ConfigError::Invalid(
                "command watch limits and ping interval must be non-zero".into(),
            ));
        }
        let etcd_tls_ca = env::var("ADX_DATA_PLANE_INGRESS_ETCD_TLS_CA").unwrap_or_default();
        let etcd_tls_cert = env::var("ADX_DATA_PLANE_INGRESS_ETCD_TLS_CERT").unwrap_or_default();
        let etcd_tls_key = env::var("ADX_DATA_PLANE_INGRESS_ETCD_TLS_KEY").unwrap_or_default();
        let etcd_tls_domain =
            env::var("ADX_DATA_PLANE_INGRESS_ETCD_TLS_DOMAIN").unwrap_or_default();
        let etcd_username = env::var("ADX_DATA_PLANE_INGRESS_ETCD_USERNAME").unwrap_or_default();
        let etcd_password = env::var("ADX_DATA_PLANE_INGRESS_ETCD_PASSWORD").unwrap_or_default();
        if etcd_tls_cert.is_empty() != etcd_tls_key.is_empty() {
            return Err(ConfigError::Invalid(
                "etcd TLS client certificate and key must be configured together".into(),
            ));
        }
        if etcd_username.is_empty() != etcd_password.is_empty() {
            return Err(ConfigError::Invalid(
                "etcd username and password must be configured together".into(),
            ));
        }
        Ok(Self {
            etcd_endpoints,
            tls_bind,
            plain_bind,
            health_bind,
            tls_cert,
            tls_key,
            frontend_address,
            reverse_proxy,
            proxy_routes,
            control_plane_routes,
            validate_iam,
            iam_address,
            auth_cache_ttl,
            default_direct_port,
            default_tunnel_port,
            node_security_mode,
            node_tls_ca,
            node_tls_server_name,
            node_tls_client_cert,
            node_tls_client_key,
            connections_per_node,
            max_connections_per_node,
            backend_http_max_connections_per_endpoint,
            backend_http_max_idle_connections,
            backend_http_max_idle_connections_per_endpoint,
            backend_http_idle_timeout,
            backend_http_acquire_timeout,
            drain_timeout,
            allowed_client_networks,
            allow_any_client,
            command_watch_max_subscriptions,
            command_watch_queue_capacity,
            command_watch_max_frame_bytes,
            command_watch_ping_interval,
            etcd_tls_ca,
            etcd_tls_cert,
            etcd_tls_key,
            etcd_tls_domain,
            etcd_username,
            etcd_password,
        })
    }

    #[cfg(test)]
    fn parse_etcd_endpoints(value: &str) -> Result<Vec<String>, ConfigError> {
        let etcd_endpoints = value
            .split(',')
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if etcd_endpoints.is_empty() {
            return Err(ConfigError::EmptyEtcdEndpoints);
        }
        Ok(etcd_endpoints)
    }

    pub fn h2_pool_config(&self) -> Result<crate::ingress::H2PoolConfig, ConfigError> {
        let tls_config = if self.node_security_mode == IngressNodeSecurityMode::Mtls {
            let mut roots = rustls::RootCertStore::empty();
            let mut reader = BufReader::new(
                File::open(&self.node_tls_ca)
                    .map_err(|error| ConfigError::Invalid(format!("open node TLS CA: {error}")))?,
            );
            let certificates = rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| ConfigError::Invalid(format!("read node TLS CA: {error}")))?;
            if certificates.is_empty() {
                return Err(ConfigError::Invalid(
                    "node TLS CA contains no certificates".into(),
                ));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|error| ConfigError::Invalid(format!("add node TLS CA: {error}")))?;
            }
            let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
            let mut cert_reader =
                BufReader::new(File::open(&self.node_tls_client_cert).map_err(|error| {
                    ConfigError::Invalid(format!("open Relay TLS client certificate: {error}"))
                })?);
            let certificates = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    ConfigError::Invalid(format!("read Relay TLS client certificate: {error}"))
                })?;
            let mut key_reader =
                BufReader::new(File::open(&self.node_tls_client_key).map_err(|error| {
                    ConfigError::Invalid(format!("open Relay TLS client key: {error}"))
                })?);
            let key = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|error| {
                    ConfigError::Invalid(format!("read Relay TLS client key: {error}"))
                })?
                .ok_or_else(|| ConfigError::Invalid("Relay TLS client key is empty".into()))?;
            let mut config = builder
                .with_client_auth_cert(certificates, key)
                .map_err(|error| {
                    ConfigError::Invalid(format!("configure Relay TLS client identity: {error}"))
                })?;
            config.alpn_protocols = vec![b"h2".to_vec()];
            Some(Arc::new(config))
        } else {
            None
        };
        Ok(crate::ingress::H2PoolConfig {
            connections_per_node: self.connections_per_node,
            max_connections_per_node: self.max_connections_per_node,
            tls_config,
            tls_server_name: (self.node_security_mode == IngressNodeSecurityMode::Mtls)
                .then(|| self.node_tls_server_name.clone()),
            ..Default::default()
        })
    }

    pub fn backend_http_pool_config(&self) -> crate::ingress::http_pool::BackendHttpPoolConfig {
        crate::ingress::http_pool::BackendHttpPoolConfig {
            max_connections_per_endpoint: self.backend_http_max_connections_per_endpoint,
            max_idle_connections: self.backend_http_max_idle_connections,
            max_idle_connections_per_endpoint: self.backend_http_max_idle_connections_per_endpoint,
            idle_timeout: self.backend_http_idle_timeout,
            acquire_timeout: self.backend_http_acquire_timeout,
        }
    }

    #[cfg(feature = "etcd-watch")]
    pub fn etcd_connect_options(&self) -> Result<etcd_client::ConnectOptions, ConfigError> {
        let mut options = etcd_client::ConnectOptions::new()
            .with_connect_timeout(Duration::from_secs(3))
            .with_timeout(Duration::from_secs(3))
            .with_keep_alive(Duration::from_secs(30), Duration::from_secs(10))
            .with_keep_alive_while_idle(true);
        if !self.etcd_username.is_empty() {
            options = options.with_user(&self.etcd_username, &self.etcd_password);
        }
        if !self.etcd_tls_ca.is_empty()
            || !self.etcd_tls_cert.is_empty()
            || self
                .etcd_endpoints
                .iter()
                .any(|endpoint| endpoint.starts_with("https://"))
        {
            let mut tls = etcd_client::TlsOptions::new();
            if !self.etcd_tls_domain.is_empty() {
                tls = tls.domain_name(&self.etcd_tls_domain);
            }
            if !self.etcd_tls_ca.is_empty() {
                let pem = std::fs::read(&self.etcd_tls_ca)
                    .map_err(|error| ConfigError::Invalid(format!("read etcd TLS CA: {error}")))?;
                tls = tls.ca_certificate(etcd_client::Certificate::from_pem(pem));
            }
            if !self.etcd_tls_cert.is_empty() {
                let certificate = std::fs::read(&self.etcd_tls_cert).map_err(|error| {
                    ConfigError::Invalid(format!("read etcd TLS client certificate: {error}"))
                })?;
                let key = std::fs::read(&self.etcd_tls_key).map_err(|error| {
                    ConfigError::Invalid(format!("read etcd TLS client key: {error}"))
                })?;
                tls = tls.identity(etcd_client::Identity::from_pem(certificate, key));
            }
            options = options.with_tls(tls);
        }
        Ok(options)
    }
}

fn parse_env<T>(name: &str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = env::var(name).unwrap_or_else(|_| default.to_owned());
    value
        .parse::<T>()
        .map_err(|error| ConfigError::Invalid(format!("{name}: {error}")))
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool, ConfigError> {
    match env::var(name) {
        Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => Ok(true),
        Ok(value) if value == "0" || value.eq_ignore_ascii_case("false") => Ok(false),
        Ok(value) => Err(ConfigError::Invalid(format!(
            "{name}: invalid boolean {value}"
        ))),
        Err(_) => Ok(default),
    }
}

fn parse_cidrs(name: &str, required: bool) -> Result<Vec<ipnet::IpNet>, ConfigError> {
    let value = env::var(name).unwrap_or_default();
    let networks = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<ipnet::IpNet>()
                .map_err(|error| ConfigError::Invalid(format!("{name}: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if required && networks.is_empty() {
        return Err(ConfigError::Invalid(format!(
            "{name} must contain at least one CIDR"
        )));
    }
    Ok(networks)
}

fn default_gateway_epoch() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

fn fd_based_stream_budget() -> Result<usize, ConfigError> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the plain rlimit structure supplied above and
    // does not retain the pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(ConfigError::Invalid(
            "cannot read RLIMIT_NOFILE for the Node stream budget".into(),
        ));
    }
    let soft_limit = usize::try_from(limit.rlim_cur).unwrap_or(usize::MAX);
    const SERVICE_RESERVED_FDS: usize = 256;
    let budget = soft_limit.saturating_sub(SERVICE_RESERVED_FDS) / 2;
    if budget == 0 {
        return Err(ConfigError::Invalid(format!(
            "RLIMIT_NOFILE {soft_limit} leaves no capacity after reserving {SERVICE_RESERVED_FDS} service FDs"
        )));
    }
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_normalizes_etcd_endpoints() {
        let endpoints =
            IngressConfig::parse_etcd_endpoints(" http://etcd-1:2379, ,https://etcd-2:2379 ")
                .unwrap();
        assert_eq!(endpoints, vec!["http://etcd-1:2379", "https://etcd-2:2379"]);
    }

    #[test]
    fn rejects_an_empty_endpoint_list() {
        assert_eq!(
            IngressConfig::parse_etcd_endpoints(" , ").unwrap_err(),
            ConfigError::EmptyEtcdEndpoints
        );
    }

    #[test]
    fn ingress_node_security_modes_are_network_or_mtls() {
        assert_eq!(
            "network".parse::<IngressNodeSecurityMode>().unwrap(),
            IngressNodeSecurityMode::Network
        );
        assert_eq!(
            "mtls".parse::<IngressNodeSecurityMode>().unwrap(),
            IngressNodeSecurityMode::Mtls
        );
        assert!("token".parse::<IngressNodeSecurityMode>().is_err());
    }
}
