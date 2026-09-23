use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    Redis,
    Coordinator,
    Relay,
    Adxlet,
    #[serde(rename = "apiserver")]
    ApiServer,
    Ingress,
}
impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Coordinator => "coordinator",
            Self::Relay => "relay",
            Self::Adxlet => "adxlet",
            Self::ApiServer => "apiserver",
            Self::Ingress => "ingress",
        }
    }

    pub fn binary(self) -> &'static str {
        match self {
            Self::Redis => "redis-server",
            Self::Coordinator => "adx-coordinator",
            Self::Adxlet => "adxlet",
            Self::Relay => "adx-relay",
            Self::ApiServer => "adx-apiserver",
            Self::Ingress => "adx-ingress",
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub id: String,
    pub role: Role,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    #[serde(default)]
    pub runtime_profile: Option<adx_core::runtime_profile::RuntimeProfile>,
    #[serde(default)]
    pub logging: crate::logging::Policy,
    pub schema_version: u32,
    pub package_dir: PathBuf,
    pub state_dir: PathBuf,
    pub redis_url: String,
    pub namespace: String,
    pub restart_limit: u32,
    pub restart_delay_ms: u64,
    pub stop_timeout_seconds: u64,
    pub services: Vec<Service>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    /// Single host with ADX-managed local Redis.
    #[default]
    Standalone,
    /// Single host connected to an external Redis.
    StandaloneExternalRedis,
    /// Coordinator-only control host.
    Coordinator,
    /// Adxlet and Relay worker host.
    Node,
    /// API Server ingress host with embedded Ingress by default.
    IngressApi,
}

impl Profile {
    pub fn name(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::StandaloneExternalRedis => "standalone-external-redis",
            Self::Coordinator => "coordinator",
            Self::Node => "node",
            Self::IngressApi => "ingress-api",
        }
    }

    pub fn template(self) -> &'static str {
        match self {
            Self::Standalone => include_str!(
                "../../../build/config/examples/deployment-standalone-managed-redis.yaml"
            ),
            Self::StandaloneExternalRedis => {
                include_str!("../../../build/config/examples/deployment.yaml")
            }
            Self::Coordinator => {
                include_str!("../../../build/config/examples/deployment-coordinator.yaml")
            }
            Self::Node => include_str!("../../../build/config/examples/deployment-node.yaml"),
            Self::IngressApi => {
                include_str!("../../../build/config/examples/deployment-ingress-api.yaml")
            }
        }
    }

    pub fn compact_template(self) -> &'static str {
        match self {
            Self::Standalone => "schema_version: 1\nprofile: standalone\n",
            Self::StandaloneExternalRedis => {
                "schema_version: 1\nprofile: standalone-external-redis\n"
            }
            Self::Coordinator => "schema_version: 1\nprofile: coordinator\n",
            Self::Node => concat!(
                "schema_version: 1\n",
                "profile: node\n",
                "service_overrides:\n",
                "  adxlet:\n",
                "    config:\n",
                "      node_id: \"${ADX_NODE_ID}\"\n",
            ),
            Self::IngressApi => "schema_version: 1\nprofile: ingress-api\n",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileDeployment {
    schema_version: u32,
    profile: Profile,
    #[serde(default)]
    internal_security: Option<adx_protocol::auth::SecurityMode>,
    #[serde(default)]
    package_dir: Option<PathBuf>,
    #[serde(default)]
    state_dir: Option<PathBuf>,
    #[serde(default)]
    redis_url: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    restart_limit: Option<u32>,
    #[serde(default)]
    restart_delay_ms: Option<u64>,
    #[serde(default)]
    stop_timeout_seconds: Option<u64>,
    #[serde(default)]
    logging: Option<Value>,
    #[serde(default)]
    runtime_profile: Option<Value>,
    #[serde(default)]
    service_overrides: BTreeMap<Role, ServiceOverride>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceOverride {
    #[serde(default)]
    config: Option<Value>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RedisConfig {
    pub bind: std::net::IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    pub appendfsync: Fsync,
    #[serde(default)]
    pub password_file: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Fsync {
    Always,
    Everysec,
    No,
}
impl RedisConfig {
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let config: Self =
            serde_json::from_value(value.clone()).map_err(|_| "invalid managed Redis config")?;
        if config.port == 0
            || !config.data_dir.is_absolute()
            || config
                .data_dir
                .to_str()
                .is_none_or(|path| path.contains(['\n', '\r', '"', '\\']))
        {
            return Err("invalid Redis port or data directory".into());
        }
        if !config.bind.is_loopback() && config.password_file.is_none() {
            return Err("non-loopback Redis requires password_file".into());
        }
        if config
            .password_file
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err("absolute Redis password_file required".into());
        }
        Ok(config)
    }
    fn text(&self) -> Result<String> {
        let auth = if let Some(path) = &self.password_file {
            let password =
                fs::read_to_string(path).map_err(|_| "cannot read Redis password file")?;
            let password = password.trim_end_matches(['\r', '\n']);
            if password.len() < 32
                || password.len() > 512
                || !password.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(
                    "Redis password must be 32..512 printable non-space ASCII bytes".into(),
                );
            }
            format!(
                "requirepass \"{}\"\n",
                password.replace('\\', "\\\\").replace('"', "\\\"")
            )
        } else {
            String::new()
        };
        let append_fsync = match self.appendfsync {
            Fsync::Always => "always",
            Fsync::Everysec => "everysec",
            Fsync::No => "no",
        };
        Ok(format!(
            "{auth}bind {}\nport {}\ndir \"{}\"\nprotected-mode yes\nappendonly yes\nappendfsync {append_fsync}\nsave \"\"\n",
            self.bind,
            self.port,
            self.data_dir.display(),
        ))
    }
}
#[derive(Clone)]
pub struct Process {
    pub id: String,
    pub role: Role,
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub admin_socket: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IngressProcessMode {
    Embedded,
    Standalone,
}

fn ingress_process_mode(service: &Service, ingress_declared: bool) -> Result<IngressProcessMode> {
    match service.config.get("ingress_mode") {
        None if ingress_declared => Ok(IngressProcessMode::Embedded),
        None => Ok(IngressProcessMode::Standalone),
        Some(Value::String(mode)) if mode == "embedded" => Ok(IngressProcessMode::Embedded),
        Some(Value::String(mode)) if mode == "standalone" => Ok(IngressProcessMode::Standalone),
        _ => Err("ingress_mode must be standalone or embedded".into()),
    }
}

impl Deployment {
    pub fn load(path: &Path) -> Result<Self> {
        if !matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yaml" | "yml")
        ) {
            return Err("deployment configuration must use a .yaml or .yml extension".into());
        }
        let bytes = fs::read(path)?;
        let mut value: Value = serde_saphyr::from_slice(&bytes)
            .map_err(|error| format!("invalid deployment YAML: {error}"))?;
        expand_environment(&mut value)?;
        let deployment = if value.get("profile").is_some() {
            let profile: ProfileDeployment = serde_json::from_value(value)
                .map_err(|error| format!("invalid profile configuration: {error}"))?;
            resolve_profile(profile)?
        } else {
            serde_json::from_value(value)
                .map_err(|error| format!("invalid deployment configuration: {error}"))?
        };
        deployment.validate()?;
        Ok(deployment)
    }

    pub fn effective_yaml(&self) -> Result<String> {
        serde_saphyr::to_string(self)
            .map_err(|error| format!("cannot serialize effective deployment: {error}").into())
    }
    pub fn validate(&self) -> Result<()> {
        self.logging.validate()?;
        if let Some(environment) = &self.runtime_profile {
            environment.validate()?;
        }
        if self.schema_version != 1
            || !self.package_dir.is_absolute()
            || !self.state_dir.is_absolute()
            || self.namespace.is_empty()
            || self.namespace.len() > 128
            || !self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
            || self.state_dir.join("supervisor.sock").as_os_str().len() > 100
            || self.services.is_empty()
            || self.services.len() > 256
            || self.restart_delay_ms == 0
            || self.restart_delay_ms > 60_000
            || self.restart_limit > 1000
            || self.stop_timeout_seconds == 0
            || self.stop_timeout_seconds > 86_400
            || !self.redis_url.starts_with("redis://")
        {
            return Err("invalid deployment version, paths, Redis URL or limits".into());
        }
        let mut ids = BTreeSet::new();
        let mut sockets = BTreeSet::new();
        let mut proxy_owners = BTreeSet::new();
        let mut has_embedded_proxy = false;
        let mut has_standalone_proxy_service = false;
        let ingress_count = self
            .services
            .iter()
            .filter(|service| service.role == Role::Ingress)
            .count();
        let api_count = self
            .services
            .iter()
            .filter(|service| service.role == Role::ApiServer)
            .count();
        if ingress_count > 1 || api_count > 1 {
            return Err("at most one API Server and Ingress per deployment".into());
        }
        let ingress = self
            .services
            .iter()
            .find(|service| service.role == Role::Ingress);
        for service in &self.services {
            if service.id.is_empty()
                || !service
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                || !ids.insert(&service.id)
            {
                return Err("unique safe service IDs required".into());
            }
            if !service.config.is_object() && !service.config.is_null() {
                return Err("service config must be an object".into());
            }
            if service
                .config
                .get("discovery")
                .is_some_and(|discovery| !discovery.is_null() && !discovery.is_object())
            {
                return Err("discovery config must be an object".into());
            }
            if service.role == Role::ApiServer
                && service
                    .config
                    .get("discovery")
                    .and_then(|discovery| discovery.get("poll_seconds"))
                    .is_some_and(|value| {
                        !value
                            .as_u64()
                            .is_some_and(|seconds| seconds > 0 && seconds <= 86_400)
                    })
            {
                return Err(
                    "Sandbox API discovery poll_seconds must be between 1 and 86400".into(),
                );
            }
            if service.role == Role::ApiServer {
                let mode = ingress_process_mode(service, ingress.is_some())?;
                if mode == IngressProcessMode::Embedded {
                    let ingress = ingress
                        .ok_or("embedded Ingress requires an ingress service declaration")?;
                    if service.env.iter().any(|(key, value)| {
                        ingress
                            .env
                            .get(key)
                            .is_some_and(|ingress_value| ingress_value != value)
                    }) {
                        return Err(
                            "embedded Ingress and API Server environment values conflict".into(),
                        );
                    }
                }
            }
            if service.env.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            }) {
                return Err("invalid environment entry".into());
            }
            let proxy_is_embedded = if service.role == Role::Adxlet {
                match service.config.get("proxy_mode") {
                    None => true,
                    Some(Value::String(mode)) if mode == "standalone" => false,
                    Some(Value::String(mode)) if mode == "embedded" => true,
                    _ => return Err("proxy_mode must be standalone or embedded".into()),
                }
            } else {
                false
            };
            has_embedded_proxy |= proxy_is_embedded;
            has_standalone_proxy_service |= service.role == Role::Relay;
            if service.role == Role::Relay || proxy_is_embedded {
                let control_directory = service
                    .env
                    .get("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
                    .filter(|directory| !directory.is_empty())
                    .ok_or("Relay control directory required")?;
                let socket = Path::new(control_directory).join("route.sock");
                if !socket.is_absolute()
                    || socket.as_os_str().len() > 100
                    || !proxy_owners.insert(socket.clone())
                {
                    return Err("unique absolute Relay control socket required".into());
                }
                if proxy_is_embedded
                    && service
                        .config
                        .get("proxy_socket")
                        .and_then(Value::as_str)
                        .map(Path::new)
                        != Some(socket.as_path())
                {
                    return Err("embedded proxy_socket must match its control directory".into());
                }
            }
            if service.role == Role::Redis {
                RedisConfig::parse(&service.config)?;
            }
            if service.role == Role::Adxlet {
                let admin_socket = self.admin_path(service);
                if admin_socket.as_os_str().len() > 100 || !sockets.insert(admin_socket) {
                    return Err("unique short node admin socket paths required".into());
                }
            }
        }
        if self
            .services
            .iter()
            .filter(|service| service.role == Role::Redis)
            .count()
            > 1
        {
            return Err("at most one managed Redis per deployment".into());
        }
        if has_embedded_proxy && has_standalone_proxy_service {
            return Err("embedded Relay cannot be combined with a standalone relay service".into());
        }
        Ok(())
    }
    fn admin_path(&self, service: &Service) -> PathBuf {
        self.state_dir.join(format!("{}-admin.sock", service.id))
    }

    pub fn supervisor_request_timeout(&self) -> std::time::Duration {
        let service_count = u64::try_from(self.services.len()).unwrap_or(u64::MAX);
        let operation_count = service_count.saturating_mul(2).saturating_add(1);
        std::time::Duration::from_secs(self.stop_timeout_seconds.saturating_mul(operation_count))
    }

    pub fn render(&self, output_directory: &Path) -> Result<Vec<Process>> {
        self.validate()?;
        fs::create_dir(output_directory)?;
        fs::set_permissions(output_directory, fs::Permissions::from_mode(0o700))?;
        let mut processes = Vec::new();
        let ingress = self
            .services
            .iter()
            .find(|service| service.role == Role::Ingress);
        let embedded_ingress = self
            .services
            .iter()
            .find(|service| service.role == Role::ApiServer)
            .map(|api| ingress_process_mode(api, ingress.is_some()))
            .transpose()?
            == Some(IngressProcessMode::Embedded);
        for service in &self.services {
            if embedded_ingress && service.role == Role::Ingress {
                continue;
            }
            let mut config = if service.config.is_null() {
                Value::Object(Map::new())
            } else {
                service.config.clone()
            };
            let mut environment = service.env.clone();
            let mut admin_socket = None;
            let config_path = output_directory.join(format!(
                "{}.{}",
                service.id,
                if service.role == Role::Redis {
                    "conf"
                } else {
                    "json"
                }
            ));
            let mut redis_config_text = None;
            let arguments = match service.role {
                Role::Coordinator => {
                    let fields = config_object_mut(&mut config)?;
                    fields.insert(
                        "redis_url".to_owned(),
                        Value::String(self.redis_url.clone()),
                    );
                    fields.insert(
                        "namespace".to_owned(),
                        Value::String(self.namespace.clone()),
                    );
                    vec!["--config".into(), config_path.display().to_string()]
                }
                Role::Adxlet | Role::ApiServer => {
                    let fields = config_object_mut(&mut config)?;
                    if let Some(environment) = &self.runtime_profile {
                        fields.insert(
                            "runtime_profile".to_owned(),
                            serde_json::to_value(environment)?,
                        );
                    }
                    fields.remove("coordinator_address");
                    let discovery = fields
                        .entry("discovery".to_owned())
                        .or_insert_with(|| Value::Object(Map::new()))
                        .as_object_mut()
                        .ok_or("discovery config must be an object")?;
                    discovery.insert(
                        "redis_url".to_owned(),
                        Value::String(self.redis_url.clone()),
                    );
                    discovery.insert(
                        "namespace".to_owned(),
                        Value::String(self.namespace.clone()),
                    );
                    if service.role == Role::ApiServer {
                        discovery
                            .entry("poll_seconds".to_owned())
                            .or_insert(Value::from(5));
                        if embedded_ingress {
                            let ingress =
                                ingress.ok_or("embedded Ingress configuration missing")?;
                            let mut control = if ingress.config.is_null() {
                                Value::Object(Map::new())
                            } else {
                                ingress.config.clone()
                            };
                            let control_fields = config_object_mut(&mut control)?;
                            control_fields.insert(
                                "redis_url".to_owned(),
                                Value::String(self.redis_url.clone()),
                            );
                            control_fields.insert(
                                "namespace".to_owned(),
                                Value::String(self.namespace.clone()),
                            );
                            fields.insert(
                                "ingress_mode".to_owned(),
                                Value::String("embedded".into()),
                            );
                            fields.insert("ingress_control".to_owned(), control);
                            environment.extend(ingress.env.clone());
                        } else {
                            fields.insert(
                                "ingress_mode".to_owned(),
                                Value::String("standalone".into()),
                            );
                            fields.remove("ingress_control");
                        }
                    }
                    if service.role == Role::Adxlet {
                        let socket = self.admin_path(service);
                        fields.insert("admin_socket".to_owned(), serde_json::to_value(&socket)?);
                        admin_socket = Some(socket);
                    }
                    vec!["--config".into(), config_path.display().to_string()]
                }
                Role::Ingress => {
                    let fields = config_object_mut(&mut config)?;
                    fields.insert(
                        "redis_url".to_owned(),
                        Value::String(self.redis_url.clone()),
                    );
                    fields.insert(
                        "namespace".to_owned(),
                        Value::String(self.namespace.clone()),
                    );
                    environment.insert(
                        "ADX_INGRESS_CONTROL_CONFIG".into(),
                        config_path.display().to_string(),
                    );
                    vec![]
                }
                Role::Relay => {
                    if !environment.contains_key("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR") {
                        return Err("Relay requires its control socket directory".into());
                    }
                    vec![]
                }
                Role::Redis => {
                    redis_config_text = Some(RedisConfig::parse(&service.config)?.text()?);
                    vec![config_path.display().to_string()]
                }
            };
            if let Some(text) = redis_config_text {
                private_write(&config_path, text.as_bytes())?;
            } else {
                private_write(&config_path, &serde_json::to_vec_pretty(&config)?)?;
            }
            let binary = self.package_dir.join("bin").join(service.role.binary());
            processes.push(Process {
                id: service.id.clone(),
                role: service.role,
                binary,
                args: arguments,
                env: environment,
                admin_socket,
            });
        }
        processes.sort_by_key(|process| process.role);
        Ok(processes)
    }
}

fn resolve_profile(input: ProfileDeployment) -> Result<Deployment> {
    if input.profile == Profile::Node
        && input
            .service_overrides
            .get(&Role::Adxlet)
            .and_then(|patch| patch.config.as_ref())
            .and_then(|config| config.get("node_id"))
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err("node profile requires config.node_id in the local adxlet override".into());
    }
    let mut deployment: Deployment = serde_saphyr::from_str(input.profile.template())
        .map_err(|error| format!("invalid built-in deployment profile: {error}"))?;
    deployment.schema_version = input.schema_version;
    if let Some(value) = input.package_dir {
        deployment.package_dir = value;
    }
    if let Some(value) = input.state_dir {
        deployment.state_dir = value;
    }
    if let Some(value) = input.redis_url {
        deployment.redis_url = value;
    }
    if let Some(value) = input.namespace {
        deployment.namespace = value;
    }
    if let Some(value) = input.restart_limit {
        deployment.restart_limit = value;
    }
    if let Some(value) = input.restart_delay_ms {
        deployment.restart_delay_ms = value;
    }
    if let Some(value) = input.stop_timeout_seconds {
        deployment.stop_timeout_seconds = value;
    }
    if let Some(patch) = input.logging {
        let mut logging = serde_json::to_value(&deployment.logging)?;
        merge_value(&mut logging, patch);
        deployment.logging = serde_json::from_value(logging)
            .map_err(|error| format!("invalid logging override: {error}"))?;
    }
    if let Some(patch) = input.runtime_profile {
        let mut environment = serde_json::to_value(&deployment.runtime_profile)?;
        merge_value(&mut environment, patch);
        deployment.runtime_profile = serde_json::from_value(environment)
            .map_err(|error| format!("invalid environment override: {error}"))?;
    }
    for (role, patch) in input.service_overrides {
        let service = deployment
            .services
            .iter_mut()
            .find(|service| service.role == role)
            .ok_or_else(|| format!("profile does not contain role: {}", role.name()))?;
        if let Some(config) = patch.config {
            if !config.is_object() {
                return Err(
                    format!("service override config must be an object: {}", role.name()).into(),
                );
            }
            merge_value(&mut service.config, config);
        }
        service.env.extend(patch.env);
    }
    if input.internal_security == Some(adx_protocol::auth::SecurityMode::Network) {
        for service in &mut deployment.services {
            let Some(config) = service.config.as_object_mut() else {
                continue;
            };
            if matches!(
                service.role,
                Role::Coordinator | Role::Adxlet | Role::Ingress
            ) {
                config.insert("tls".into(), serde_json::json!({"mode": "network"}));
            }
            if service.role == Role::ApiServer {
                config.insert("internal_security".into(), serde_json::json!("network"));
                // Certificate/key can still serve a separately configured public API listener.
                if config.get("loopback_http").and_then(Value::as_bool) == Some(true) {
                    for key in ["ca", "certificate", "private_key", "server_name"] {
                        config.remove(key);
                    }
                }
            }
            if service.role == Role::Coordinator {
                if let Some(Value::String(address)) = config.get_mut("advertised_address") {
                    *address = address.replacen("https://", "http://", 1);
                }
            }
            if matches!(service.role, Role::Ingress | Role::Adxlet | Role::Relay) {
                service.env.insert(
                    "ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE".into(),
                    "network".into(),
                );
                service.env.retain(|key, _| {
                    !key.starts_with("ADX_DATA_PLANE_INGRESS_NODE_TLS_")
                        && !key.starts_with("ADX_DATA_PLANE_RELAY_TLS_")
                        && key != "ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA"
                });
            }
        }
    }
    Ok(deployment)
}

fn merge_value(target: &mut Value, patch: Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                if let Some(target) = target.get_mut(&key) {
                    merge_value(target, value);
                } else {
                    target.insert(key, value);
                }
            }
        }
        (target, patch) => *target = patch,
    }
}

fn expand_environment(value: &mut Value) -> Result<()> {
    match value {
        Value::String(text) => *text = expand_environment_string(text)?,
        Value::Array(values) => {
            for value in values {
                expand_environment(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                expand_environment(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn expand_environment_string(input: &str) -> Result<String> {
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    while let Some(relative) = input[cursor..].find('$') {
        let start = cursor + relative;
        output.push_str(&input[cursor..start]);
        match input.as_bytes().get(start + 1) {
            Some(b'$') => {
                output.push('$');
                cursor = start + 2;
            }
            Some(b'{') => {
                let expression_start = start + 2;
                let Some(relative_end) = input[expression_start..].find('}') else {
                    return Err("unterminated environment variable reference".into());
                };
                let end = expression_start + relative_end;
                let expression = &input[expression_start..end];
                let (name, default) = expression
                    .split_once(":-")
                    .map_or((expression, None), |(name, default)| (name, Some(default)));
                if !valid_environment_name(name) {
                    return Err(format!("invalid environment variable reference: {name}").into());
                }
                match std::env::var(name) {
                    Ok(value) if !value.is_empty() => output.push_str(&value),
                    Ok(_) | Err(std::env::VarError::NotPresent) => {
                        output.push_str(
                            default.ok_or_else(|| {
                                format!("environment variable is not set: {name}")
                            })?,
                        );
                    }
                    Err(std::env::VarError::NotUnicode(_)) => {
                        return Err(format!("environment variable is not UTF-8: {name}").into());
                    }
                }
                cursor = end + 1;
            }
            _ => {
                output.push('$');
                cursor = start + 1;
            }
        }
    }
    output.push_str(&input[cursor..]);
    Ok(output)
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

fn config_object_mut(config: &mut Value) -> Result<&mut Map<String, Value>> {
    config
        .as_object_mut()
        .ok_or_else(|| "service config must be an object".into())
}

pub fn private_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}
