use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
    Master,
    NodeProxy,
    NodeManager,
    ApiServer,
    Edge,
}
impl Role {
    pub fn binary(self) -> &'static str {
        match self {
            Self::Redis => "redis-server",
            Self::Master => "adx-master",
            Self::NodeManager => "adx-node-manager",
            Self::NodeProxy => "adx-node-proxy",
            Self::ApiServer => "adx-api-server",
            Self::Edge => "adx-edge-frontend",
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub id: String,
    pub role: Role,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
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
    pub(crate) fn parse(v: &Value) -> Result<Self> {
        let c: Self =
            serde_json::from_value(v.clone()).map_err(|_| "invalid managed Redis config")?;
        if c.port == 0
            || !c.data_dir.is_absolute()
            || c.data_dir
                .to_str()
                .is_none_or(|p| p.contains(['\n', '\r', '"', '\\']))
        {
            return Err("invalid Redis port or data directory".into());
        }
        if !c.bind.is_loopback() && c.password_file.is_none() {
            return Err("non-loopback Redis requires password_file".into());
        }
        if c.password_file.as_ref().is_some_and(|p| !p.is_absolute()) {
            return Err("absolute Redis password_file required".into());
        }
        Ok(c)
    }
    fn text(&self) -> Result<String> {
        let auth = if let Some(path) = &self.password_file {
            let password =
                fs::read_to_string(path).map_err(|_| "cannot read Redis password file")?;
            let password = password.trim_end_matches(['\r', '\n']);
            if password.len() < 32
                || password.len() > 512
                || !password.bytes().all(|b| b.is_ascii_graphic())
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
        Ok(auth + &
        format!("bind {}\nport {}\ndir \"{}\"\nprotected-mode yes\nappendonly yes\nappendfsync {}\nsave \"\"\n",self.bind,self.port,self.data_dir.display(),match self.appendfsync{Fsync::Always=>"always",Fsync::Everysec=>"everysec",Fsync::No=>"no"}))
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
impl Deployment {
    pub fn load(path: &Path) -> Result<Self> {
        let d: Self =
            serde_json::from_slice(&fs::read(path)?).map_err(|_| "invalid deployment JSON")?;
        d.validate()?;
        Ok(d)
    }
    pub fn validate(&self) -> Result<()> {
        self.logging.validate()?;
        if self.schema_version != 1
            || !self.package_dir.is_absolute()
            || !self.state_dir.is_absolute()
            || self.namespace.is_empty()
            || self.namespace.len() > 128
            || !self
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
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
        for s in &self.services {
            if s.id.is_empty()
                || !s
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                || !ids.insert(&s.id)
            {
                return Err("unique safe service IDs required".into());
            }
            if !s.config.is_object() && !s.config.is_null() {
                return Err("service config must be an object".into());
            }
            if s.config
                .get("discovery")
                .is_some_and(|d| !d.is_null() && !d.is_object())
            {
                return Err("discovery config must be an object".into());
            }
            if s.role == Role::ApiServer
                && s.config
                    .get("discovery")
                    .and_then(|d| d.get("poll_seconds"))
                    .is_some_and(|v| !v.as_u64().is_some_and(|n| n > 0 && n <= 86_400))
            {
                return Err(
                    "Sandbox API discovery poll_seconds must be between 1 and 86400".into(),
                );
            }
            if s.env
                .iter()
                .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
            {
                return Err("invalid environment entry".into());
            }
            let embedded = if s.role == Role::NodeManager {
                match s.config.get("proxy_mode") {
                    None => false,
                    Some(Value::String(mode)) if mode == "standalone" => false,
                    Some(Value::String(mode)) if mode == "embedded" => true,
                    _ => return Err("proxy_mode must be standalone or embedded".into()),
                }
            } else {
                false
            };
            if s.role == Role::NodeProxy || embedded {
                let dir = s
                    .env
                    .get("ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR")
                    .filter(|d| !d.is_empty())
                    .ok_or("Node Proxy control directory required")?;
                let socket = Path::new(dir).join("route.sock");
                if !socket.is_absolute()
                    || socket.as_os_str().len() > 100
                    || !proxy_owners.insert(socket.clone())
                {
                    return Err("unique absolute Node Proxy control socket required".into());
                }
                if embedded
                    && s.config
                        .get("proxy_socket")
                        .and_then(Value::as_str)
                        .map(Path::new)
                        != Some(socket.as_path())
                {
                    return Err("embedded proxy_socket must match its control directory".into());
                }
            }
            if s.role == Role::Redis {
                RedisConfig::parse(&s.config)?;
            }
            if s.role == Role::NodeManager {
                let p = self.admin_path(s);
                if p.as_os_str().len() > 100 || !sockets.insert(p) {
                    return Err("unique short node admin socket paths required".into());
                }
            }
        }
        if self
            .services
            .iter()
            .filter(|s| s.role == Role::Redis)
            .count()
            > 1
        {
            return Err("at most one managed Redis per deployment".into());
        }
        Ok(())
    }
    fn admin_path(&self, s: &Service) -> PathBuf {
        self.state_dir.join(format!("{}-admin.sock", s.id))
    }
    pub fn render(&self, dir: &Path) -> Result<Vec<Process>> {
        self.validate()?;
        fs::create_dir(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        let mut result = Vec::new();
        for s in &self.services {
            let mut config = if s.config.is_null() {
                json!({})
            } else {
                s.config.clone()
            };
            let mut env = s.env.clone();
            let mut admin = None;
            let file = dir.join(format!(
                "{}.{}",
                s.id,
                if s.role == Role::Redis {
                    "conf"
                } else {
                    "json"
                }
            ));
            let mut redis_text = None;
            let args = match s.role {
                Role::Master => {
                    config["redis_url"] = json!(self.redis_url);
                    config["namespace"] = json!(self.namespace);
                    vec!["--config".into(), file.display().to_string()]
                }
                Role::NodeManager | Role::ApiServer => {
                    config.as_object_mut().unwrap().remove("master_address");
                    config["discovery"]["redis_url"] = json!(self.redis_url);
                    config["discovery"]["namespace"] = json!(self.namespace);
                    if s.role == Role::ApiServer {
                        config["discovery"]
                            .as_object_mut()
                            .unwrap()
                            .entry("poll_seconds")
                            .or_insert(json!(5));
                    }
                    if s.role == Role::NodeManager {
                        let socket = self.admin_path(s);
                        config["admin_socket"] = json!(socket);
                        admin = Some(socket);
                    }
                    vec!["--config".into(), file.display().to_string()]
                }
                Role::Edge => {
                    config["redis_url"] = json!(self.redis_url);
                    config["namespace"] = json!(self.namespace);
                    env.insert("ADX_EDGE_CONTROL_CONFIG".into(), file.display().to_string());
                    vec![]
                }
                Role::NodeProxy => {
                    if !env.contains_key("ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR") {
                        return Err("Node Proxy requires its control socket directory".into());
                    }
                    vec![]
                }
                Role::Redis => {
                    redis_text = Some(RedisConfig::parse(&s.config)?.text()?);
                    vec![file.display().to_string()]
                }
            };
            if let Some(text) = redis_text {
                private_write(&file, text.as_bytes())?;
            } else {
                private_write(&file, &serde_json::to_vec_pretty(&config)?)?;
            }
            let binary = self.package_dir.join("bin").join(s.role.binary());
            result.push(Process {
                id: s.id.clone(),
                role: s.role,
                binary,
                args,
                env,
                admin_socket: admin,
            });
        }
        result.sort_by_key(|p| p.role);
        Ok(result)
    }
}
pub fn private_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}
