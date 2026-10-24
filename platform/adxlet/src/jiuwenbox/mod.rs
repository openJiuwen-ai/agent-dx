//! Jiuwenbox process and Conch adapters. The external backends are unmodified.
mod client;
pub mod host;
pub mod policy;
mod state;

use self::{
    client::{unavailable, Client},
    host::{HostOperations, LinuxHost},
    state::{Phase, Record, State},
};
use crate::{
    sandboxd::{self, Config as LaunchConfig},
    RuntimeDriver, RuntimeObservation, RuntimeStatus,
};
use adx_core::{
    execution::{NetworkPolicy, StorageSource},
    scheduling::DeviceAllocation,
    EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Result,
};
use async_trait::async_trait;
use hyper::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub image: PathBuf,
    pub sha256: String,
    pub target: String,
    pub command: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub args: Vec<String>,
    pub cwd: String,
    pub user: String,
}

/// An explicit deployment binding; an OCI name is never silently treated as a
/// host root or a Conch template. Versioned template names must be immutable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    #[serde(default)]
    pub template_name: Option<String>,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub process: Option<Process>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Class {
    pub images: BTreeMap<String, Image>,
    /// Trusted operator policy; ADX replaces execution-owned fields below.
    #[serde(default = "empty_object")]
    pub policy: Value,
}
fn empty_object() -> Value {
    json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    pub bootstrap: Bootstrap,
    pub classes: BTreeMap<String, Class>,
    #[serde(default)]
    pub conch_socket: Option<PathBuf>,
    /// Read once at startup.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
}

type ExecutionGates = Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<()>>>>>;

#[derive(Clone)]
pub struct Jiuwenbox {
    config: Arc<Config>,
    launch: LaunchConfig,
    client: Client,
    conch: Option<Client>,
    state: State,
    authority_loaded: Arc<AtomicBool>,
    host: Arc<dyn HostOperations>,
    gates: ExecutionGates,
    bootstrap_root: Arc<tokio::sync::OnceCell<PathBuf>>,
    _lease: Arc<std::fs::File>,
}

fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path.components().all(|p| {
            matches!(
                p,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
}

// Mirrors the unmodified Conch volume manager's reserved guest paths.
fn conch_reserved_target(path: &Path) -> bool {
    path == Path::new("/")
        || ["/proc", "/sys", "/dev", "/run"]
            .iter()
            .any(|root| path.starts_with(root))
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if !absolute(&self.socket)
            || !absolute(&self.state_dir)
            || !absolute(&self.bootstrap.image)
            || self.bootstrap.sha256.len() != 64
            || !self
                .bootstrap
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !absolute(Path::new(&self.bootstrap.target))
            || self.bootstrap.command.is_empty()
            || !Path::new(&self.bootstrap.command[0]).starts_with(&self.bootstrap.target)
            || self.bootstrap.command.iter().any(|v| v.contains('\0'))
            || self.classes.is_empty()
        {
            return Err(Error::Invalid(
                "Jiuwenbox requires absolute paths, pinned EROFS SHA256 and bootstrap command"
                    .into(),
            ));
        }
        if self.classes.contains_key("conch")
            && conch_reserved_target(Path::new(&self.bootstrap.target))
        {
            return Err(Error::Invalid("Conch bootstrap target is reserved".into()));
        }
        for (name, class) in &self.classes {
            if !matches!(name.as_str(), "supervisor" | "conch")
                || !class.policy.is_object()
                || class.images.is_empty()
            {
                return Err(Error::Invalid("Jiuwenbox classes require supervisor/conch, image bindings and an object policy".into()));
            }
            for field in [
                "filesystem_policy",
                "cgroup",
                "network",
                "namespace",
                "process",
                "conch",
            ] {
                if class.policy.get(field).is_some_and(|v| !v.is_object()) {
                    return Err(Error::Invalid(format!(
                        "Jiuwenbox policy.{field} must be an object"
                    )));
                }
            }
            if name == "conch" && self.conch_socket.as_ref().is_none_or(|p| !absolute(p)) {
                return Err(Error::Invalid("Conch recovery socket is required".into()));
            }
            for image in class.images.values() {
                let template = match (&image.template_name, &image.template_id) {
                    (Some(value), None) | (None, Some(value)) => Some(value),
                    _ => None,
                };
                if name == "conch"
                    && template.is_none_or(|value| value.trim().is_empty() || value.contains('\0'))
                {
                    return Err(Error::Invalid(
                        "Conch image binding requires exactly one immutable template_name or template_id"
                            .into(),
                    ));
                }
                if let Some(p) = &image.process {
                    if p.args.is_empty()
                        || p.args[0].trim().is_empty()
                        || p.args.iter().any(|s| s.contains('\0'))
                        || !absolute(Path::new(&p.cwd)) && p.cwd != "/"
                        || p.user.trim().is_empty()
                    {
                        return Err(Error::Invalid(
                            "image process requires argv, absolute cwd and user".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Jiuwenbox {
    pub async fn connect(config: Config, launch: LaunchConfig) -> Result<Self> {
        let driver = Self::with_host(config, launch, Arc::new(LinuxHost)).await?;
        driver.prepare_bootstrap().await?;
        Ok(driver)
    }
    pub async fn with_host(
        config: Config,
        mut launch: LaunchConfig,
        host: Arc<dyn HostOperations>,
    ) -> Result<Self> {
        config.validate()?;
        if launch.rpc_timeout.is_zero() {
            return Err(Error::Invalid("positive Jiuwenbox timeout required".into()));
        }
        if launch.command.is_empty() {
            launch.command = config.bootstrap.command.clone();
        }
        if let Some(profile) = &launch.runtime_profile {
            if profile.bootstrap.r#type != "erofs"
                || Path::new(&profile.bootstrap.root) != config.bootstrap.image
                || profile.bootstrap.target != config.bootstrap.target
                || profile.bootstrap.entrypoint != config.bootstrap.command
            {
                return Err(Error::Invalid(
                    "Jiuwenbox bootstrap must match the deployment RuntimeProfile EROFS".into(),
                ));
            }
        }
        let token = config
            .token_file
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()
            .map_err(unavailable)?
            .map(|s| s.trim().to_owned());
        if token
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.chars().any(char::is_control))
        {
            return Err(Error::Invalid("invalid Jiuwenbox token file".into()));
        }
        let client = Client {
            socket: config.socket.clone(),
            token,
            timeout: launch.rpc_timeout,
        };
        let conch = config.conch_socket.as_ref().map(|socket| Client {
            socket: socket.clone(),
            token: None,
            timeout: launch.rpc_timeout,
        });
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        std::fs::create_dir_all(&config.state_dir).map_err(unavailable)?;
        std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))
            .map_err(unavailable)?;
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(config.state_dir.join("driver.lock"))
            .map_err(unavailable)?;
        lease
            .try_lock()
            .map_err(|_| unavailable("another adxlet owns the Jiuwenbox state directory"))?;
        Ok(Self {
            config: Arc::new(config),
            launch,
            client,
            conch,
            state: State::default(),
            authority_loaded: Arc::default(),
            host,
            gates: Arc::default(),
            bootstrap_root: Arc::default(),
            _lease: Arc::new(lease),
        })
    }
    fn gate(&self, id: &str) -> Arc<AsyncMutex<()>> {
        self.gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(id.into())
            .or_default()
            .clone()
    }
    pub async fn owns(&self, id: &str) -> Result<bool> {
        Ok(self.state.get(id).await?.is_some())
    }
    pub async fn available_runtime_classes(&self) -> Result<Vec<String>> {
        self.client
            .request(Method::GET, "/health", None, false)
            .await?;
        // The default health route may return plain text, so discovery uses the
        // actual sandbox API too and validates its complete collection shape.
        self.list().await?;
        if self.config.classes.contains_key("conch") {
            self.conch()?
                .request(Method::GET, "/health", None, false)
                .await?;
        }
        let timeout = self
            .client
            .request(Method::GET, "/api/v1/timeout", None, false)
            .await?;
        if timeout.get("idle_timeout") != Some(&Value::Null) {
            return Err(unavailable(
                "disable Jiuwenbox idle_timeout for ADX-managed execution",
            ));
        }
        self.backend_inventory().await?;
        Ok(self.config.classes.keys().cloned().collect())
    }
    fn conch(&self) -> Result<&Client> {
        self.conch
            .as_ref()
            .ok_or_else(|| unavailable("Conch recovery client unavailable"))
    }
    async fn list(&self) -> Result<Vec<Value>> {
        self.client
            .request(Method::GET, "/api/v1/sandboxes", None, false)
            .await?
            .as_array()
            .cloned()
            .ok_or_else(|| unavailable("invalid Jiuwenbox inventory"))
    }
    fn root(&self) -> PathBuf {
        self.config
            .state_dir
            .join("bootstrap")
            .join(&self.config.bootstrap.sha256)
    }
    async fn prepare_bootstrap(&self) -> Result<PathBuf> {
        self.bootstrap_root
            .get_or_try_init(|| async {
                self.host
                    .prepare(&self.config.bootstrap, &self.root())
                    .await
            })
            .await
            .cloned()
    }

    /// All ADX-prefixed resources on this node-exclusive backend belong to ADX.
    /// Read every source before performing destructive orphan collection.
    async fn backend_inventory(&self) -> Result<BTreeMap<String, String>> {
        let mut seen = BTreeMap::new();
        for value in self.list().await? {
            let id = value["id"]
                .as_str()
                .ok_or_else(|| unavailable("Jiuwenbox inventory record has no ID"))?;
            if !id.starts_with("adx-") {
                continue;
            }
            // Native list/get responses use `runtime`; some distributions
            // expose the create selector `sandbox_runtime` in responses too.
            let runtime = value["runtime"]
                .as_str()
                .or_else(|| value["sandbox_runtime"].as_str());
            let backend = match runtime {
                Some("conch") => "conch",
                Some("process" | "bwrap" | "supervisor") => "supervisor",
                _ if value["phase"] == "provisioning" => "",
                _ => return Err(unavailable("managed sandbox has unknown runtime")),
            };
            if !managed_id(id) || seen.insert(id.to_owned(), backend.to_owned()).is_some() {
                return Err(Error::Conflict);
            }
        }
        if let Some(native) = &self.conch {
            let listed = native
                .request(Method::GET, "/api/v1/sandboxes?limit=5000", None, false)
                .await?;
            let listed = listed
                .as_array()
                .ok_or_else(|| unavailable("invalid Conch inventory"))?;
            if listed.len() >= 5000 {
                return Err(unavailable("Conch inventory may be truncated"));
            }
            for value in listed {
                let id = value["sandboxID"]
                    .as_str()
                    .or_else(|| value["id"].as_str())
                    .ok_or_else(|| unavailable("Conch inventory record has no ID"))?;
                if !id.starts_with("adx-") {
                    continue;
                }
                if !managed_id(id) || seen.get(id).is_some_and(|b| !b.is_empty() && b != "conch") {
                    return Err(Error::Conflict);
                }
                seen.insert(id.to_owned(), "conch".into());
            }
        }
        for id in self.host.supervisor_inventory().await? {
            if !managed_id(&id)
                || seen
                    .get(&id)
                    .is_some_and(|b| !b.is_empty() && b != "supervisor")
            {
                return Err(Error::Conflict);
            }
            seen.insert(id, "supervisor".into());
        }
        Ok(seen)
    }

    async fn cleanup_backend(&self, id: &str, backend: &str) -> Result<()> {
        if !managed_id(id) {
            return Err(Error::Invalid("invalid managed sandbox ID".into()));
        }
        let path = format!("/api/v1/sandboxes/{id}");
        match self.client.request(Method::GET, &path, None, false).await {
            Ok(value) if value["phase"] == "provisioning" => {
                return Err(unavailable("create remains in flight"))
            }
            Ok(_) => {
                self.client
                    .request(Method::DELETE, &path, None, false)
                    .await?;
                match self.client.request(Method::GET, &path, None, false).await {
                    Err(Error::NotFound) => {}
                    Ok(_) => return Err(unavailable("Jiuwenbox delete not complete")),
                    Err(e) => return Err(e),
                }
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        if backend == "conch" {
            let client = self.conch()?;
            match client.request(Method::GET, &path, None, false).await {
                Ok(_) => {
                    client.request(Method::DELETE, &path, None, false).await?;
                }
                Err(Error::NotFound) => {}
                Err(e) => return Err(e),
            }
            match client.request(Method::GET, &path, None, false).await {
                Err(Error::NotFound) => {}
                Ok(_) => return Err(unavailable("Conch delete not complete")),
                Err(e) => return Err(e),
            }
        } else if backend == "supervisor" {
            self.host.cleanup_supervisor(id).await?;
        }
        match std::fs::remove_dir_all(self.config.state_dir.join("launch").join(id)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(unavailable(e)),
        }
        Ok(())
    }

    fn files(&self, id: &str) -> PathBuf {
        self.config.state_dir.join("launch").join(sandbox_id(id))
    }

    fn plan(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        root: &Path,
    ) -> Result<(Value, Value)> {
        adx_core::execution::validate_process_identity(
            spec.execution.user.as_deref(),
            spec.execution.group.as_deref(),
        )?;
        if spec.execution.user.is_some() && spec.execution.inherit_entrypoint {
            return Err(Error::Invalid(
                "tool identity cannot override an inherited image process".into(),
            ));
        }
        let class = self
            .config
            .classes
            .get(&spec.runtime_class)
            .ok_or_else(|| Error::Invalid("Jiuwenbox runtime class is not configured".into()))?;
        let image = class
            .images
            .get(&spec.image)
            .ok_or_else(|| Error::Invalid("image is not bound on this Jiuwenbox node".into()))?;
        let network = policy::network_policy(
            &spec.runtime_class,
            spec.execution.network.as_ref(),
            &spec.execution.ports,
            spec.execution.tunnel_http_port,
            &self.launch,
        )?;
        if !devices.is_empty()
            || !spec.execution.extra_config.is_empty()
            || spec.execution.limits.disk_bytes > 0
            || spec.resources.disk_bytes > 0
        {
            return Err(Error::Invalid(
                "Jiuwenbox devices, disk limits and extra_config are unsupported".into(),
            ));
        }
        if spec.execution.rootfs.as_ref().is_some_and(|r| {
            r.readonly || !matches!(&r.source,StorageSource::Image(s) if s==&spec.image)
        }) {
            return Err(Error::Invalid(
                "Jiuwenbox requires a writable image binding for the workload filesystem".into(),
            ));
        }
        let mut common = self.launch.clone();
        common.runtime_logs = None;
        let mut request = sandboxd::start_request(spec, id, generation, devices, &common)?;
        request.envs.remove("ADX_CHECKPOINT_HANDOFF_FILE");
        request.envs.remove("ADX_ENV_FILE");
        request.envs.remove("ADX_EXECD_CONTROL_SOCKET_PATH");
        let mut mounts =
            vec![json!({"host_path":root,"sandbox_path":self.config.bootstrap.target,"mode":"ro"})];
        if spec.execution.inherit_entrypoint {
            if image.process.is_none() {
                return Err(Error::Invalid(
                    "image inheritance requires a configured process".into(),
                ));
            }
            mounts.push(
                json!({"host_path":self.files(id),"sandbox_path":"/__adx_launch","mode":"ro"}),
            );
            request.envs.insert(
                "ADX_IMAGE_PROCESS_CONFIG".into(),
                "/__adx_launch/process.json".into(),
            );
        }
        for mount in &spec.execution.mounts {
            let StorageSource::Local(source) = &mount.source else {
                return Err(Error::Invalid(
                    "Jiuwenbox requires Local bind mounts".into(),
                ));
            };
            if spec.runtime_class == "conch" && conch_reserved_target(Path::new(&mount.target)) {
                return Err(Error::Invalid("Conch volume target is reserved".into()));
            }
            if mount.kind != "bind"
                || mount
                    .options
                    .iter()
                    .any(|s| !matches!(s.as_str(), "ro" | "rw" | "rbind"))
                || Path::new(&mount.target).starts_with(&self.config.bootstrap.target)
                || Path::new(&self.config.bootstrap.target).starts_with(&mount.target)
                || Path::new(&mount.target).starts_with("/__adx_launch")
                || Path::new("/__adx_launch").starts_with(&mount.target)
            {
                return Err(Error::Invalid(
                    "unsupported or reserved Jiuwenbox mount".into(),
                ));
            }
            mounts.push(json!({"host_path":source,"sandbox_path":mount.target,"mode":if mount.options.iter().any(|s|s=="ro"){ "ro" }else{"rw"}}));
        }
        let cpu = spec
            .execution
            .limits
            .cpu_millis
            .max(spec.resources.cpu_millis);
        let memory = spec
            .execution
            .limits
            .memory_bytes
            .max(spec.resources.memory_bytes);
        if cpu == 0 || memory == 0 {
            return Err(Error::Invalid(
                "positive Jiuwenbox CPU and memory limits required".into(),
            ));
        }
        let mut backend_policy = class.policy.clone();
        if spec.runtime_class == "conch" {
            if !cpu.is_multiple_of(1000) || !memory.is_multiple_of(1_048_576) {
                return Err(Error::Invalid(
                    "Conch limits require integer vCPUs and MiB".into(),
                ));
            }
            let mut conch = class
                .policy
                .get("conch")
                .cloned()
                .unwrap_or_else(empty_object);
            // The deployment binding owns the selector, including when the
            // operator policy contains a different default. Some Jiuwenbox
            // distributions accept only content IDs and reject template_name.
            let selector = conch
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("Conch policy must be an object".into()))?;
            selector.remove("template_name");
            selector.remove("template_id");
            if let Some(id) = &image.template_id {
                selector.insert("template_id".into(), json!(id));
            } else if let Some(name) = &image.template_name {
                selector.insert("template_name".into(), json!(name));
            }
            let owned = json!({"vcpu_num":cpu/1000,"vcpu_max":cpu/1000,"ram_mb":memory/1_048_576,"filesystem_policy":{"bind_mounts":mounts},"network":network});
            for (key, value) in owned
                .as_object()
                .ok_or_else(|| unavailable("invalid owned Conch policy"))?
            {
                conch[key] = value.clone();
            }
            if let (Some(user), Some(group)) = (&spec.execution.user, &spec.execution.group) {
                conch["run_as_user"] = json!(user);
                conch["run_as_group"] = json!(group);
            }
            backend_policy["conch"] = conch;
        } else {
            if spec.execution.user.is_some() {
                return Err(Error::Invalid(
                    "tool identity override currently requires the Conch runtime".into(),
                ));
            }
            backend_policy["filesystem_policy"]["bind_mounts"] = json!(mounts);
            // A scalar replaces Jiuwenbox's base cpu_max tuple in append mode;
            // sending a list would concatenate/deduplicate the quota and period.
            let quota = cpu
                .checked_mul(100)
                .ok_or_else(|| Error::Invalid("CPU limit overflows".into()))?;
            backend_policy["cgroup"]["cpu_max"] = json!(format!("{quota} 100000"));
            backend_policy["cgroup"]["memory_max"] = json!(memory);
            backend_policy["network"]["mode"] = json!("isolated");
            for (key, value) in network
                .as_object()
                .ok_or_else(|| unavailable("invalid native network policy"))?
            {
                backend_policy["network"][key] = value.clone();
            }
        }
        let body = json!({"sandbox_id":sandbox_id(id),"sandbox_runtime":if spec.runtime_class=="conch"{"conch"}else{"bwrap"},"env":request.envs,"policy_mode":"append","policy":backend_policy});
        let exec = json!({"job_id":"adx-execd","command":self.config.bootstrap.command,"workdir":request.cwd,"env":request.envs});
        Ok((body, exec))
    }

    async fn write_process(&self, spec: &EnvironmentSpec, id: &str) -> Result<()> {
        if !spec.execution.inherit_entrypoint {
            return Ok(());
        }
        let image = &self.config.classes[&spec.runtime_class].images[&spec.image];
        let process = image
            .process
            .as_ref()
            .ok_or_else(|| Error::Invalid("process binding missing".into()))?;
        let dir = self.files(id);
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(&dir).map_err(unavailable)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))
            .map_err(unavailable)?;
        let payload =
            json!({"version":1,"args":process.args,"cwd":process.cwd,"user":process.user});
        let file = dir.join("process.json");
        let bytes = serde_json::to_vec(&payload).map_err(unavailable)?;
        let mut out = std::fs::File::create(&file).map_err(unavailable)?;
        use std::io::Write;
        out.write_all(&bytes).map_err(unavailable)?;
        out.sync_all().map_err(unavailable)?;
        Ok(())
    }

    async fn start_owned(
        &self,
        spec: EnvironmentSpec,
        id: String,
        generation: u64,
        devices: Vec<DeviceAllocation>,
        deadline: Instant,
    ) -> Result<IpAddr> {
        let result = self
            .start_backend(&spec, &id, generation, &devices, deadline)
            .await;
        if result.is_err() {
            self.state.update(&id, |r| r.phase = Phase::Failed).await?;
            if let Err(error) = self.cleanup(&id).await {
                adx_observability::warn!(runtime_id=%id,error=%error,"Jiuwenbox failed start cleanup pending");
            }
        }
        result
    }
    async fn still_starting(&self, id: &str, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(Error::DeadlineExceeded(
                "Jiuwenbox start budget exhausted".into(),
            ));
        }
        if self
            .state
            .get(id)
            .await?
            .is_none_or(|r| r.phase != Phase::Creating)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    async fn start_backend(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        deadline: Instant,
    ) -> Result<IpAddr> {
        self.still_starting(id, deadline).await?;
        let root = self.prepare_bootstrap().await?;
        crate::managed_mounts::prepare(spec).await?;
        self.write_process(spec, id).await?;
        let (body, exec) = self.plan(spec, id, generation, devices, &root)?;
        self.still_starting(id, deadline).await?;
        let response = self
            .client
            .request(Method::POST, "/api/v1/sandboxes", Some(body), true)
            .await?;
        if response["phase"] != "ready" {
            return Err(unavailable("backend create did not reach ready"));
        }
        let ip: IpAddr = response["ip_address"]
            .as_str()
            .ok_or_else(|| unavailable("backend did not return an isolated IP"))?
            .parse()
            .map_err(unavailable)?;
        if !ip.is_ipv4() || ip.is_loopback() || ip.is_unspecified() {
            return Err(unavailable("invalid isolated backend address"));
        }
        self.state.update(id, move |r| r.ip = Some(ip)).await?;
        self.still_starting(id, deadline).await?;
        let response = self
            .client
            .request(
                Method::POST,
                &format!("/api/v1/sandboxes/{}/exec_background", sandbox_id(id)),
                Some(exec),
                true,
            )
            .await?;
        if response["started"] != true {
            return Err(unavailable("Execd background launch failed"));
        }
        self.still_starting(id, deadline).await?;
        self.state
            .update(id, |r| {
                if r.phase == Phase::Creating {
                    r.phase = Phase::Ready;
                }
            })
            .await?;
        Ok(ip)
    }

    async fn cleanup(&self, id: &str) -> Result<()> {
        let Some(record) = self.state.get(id).await? else {
            return Ok(());
        };
        if record.phase != Phase::Deleted {
            self.cleanup_backend(&record.sandbox_id, &record.backend)
                .await?;
            self.state.update(id, |r| r.phase = Phase::Deleted).await?;
        }
        Ok(())
    }

    fn start_cleanup(
        &self,
        id: String,
        lock: tokio::sync::OwnedMutexGuard<()>,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let driver = self.clone();
        tokio::spawn(async move {
            let _lock = lock;
            let result = driver.cleanup(&id).await;
            if let Err(error) = &result {
                adx_observability::warn!(runtime_id=%id, %error, "Jiuwenbox execution cleanup pending");
            }
            result
        })
    }

    async fn running_conch(&self) -> Result<BTreeSet<String>> {
        let running = self
            .conch()?
            .request(
                Method::GET,
                "/api/v1/sandboxes?state=running&limit=5000",
                None,
                false,
            )
            .await?;
        let running = running
            .as_array()
            .ok_or_else(|| unavailable("invalid Conch running inventory"))?;
        if running.len() >= 5000 {
            return Err(unavailable("Conch running inventory may be truncated"));
        }
        running
            .iter()
            .map(|v| {
                v["sandboxID"]
                    .as_str()
                    .or_else(|| v["id"].as_str())
                    .map(str::to_owned)
                    .ok_or_else(|| unavailable("Conch running inventory record has no ID"))
            })
            .collect()
    }

    async fn observation(&self, r: &Record) -> Result<bool> {
        if r.phase == Phase::Deleted {
            return Ok(false);
        }
        if r.epoch != self.client.epoch().await? {
            // This execution's backend binding is no longer usable. Its
            // controller must confirm physical cleanup before releasing it.
            return Ok(false);
        }
        if r.phase != Phase::Ready {
            return Err(unavailable("execution has not settled"));
        }
        let ip = r.ip.ok_or_else(|| unavailable("runtime IP missing"))?;
        let port = self
            .launch
            .env
            .get("EXECD_HTTP_PORT")
            .map(String::as_str)
            .unwrap_or("50090");
        let port = port
            .parse::<u16>()
            .map_err(|_| Error::Invalid("invalid Execd HTTP port".into()))?;
        let status = async {
            let status = client::execd_status(
                std::net::SocketAddr::new(ip, port),
                self.launch.env.get("EXECD_HTTP_TOKEN").map(String::as_str),
                self.launch.rpc_timeout,
            )
            .await?;
            if status.identity.environment_id != r.environment_id
                || status.identity.runtime_id != r.runtime_id
                || status.identity.ownership_generation != r.generation
                || status.revision == 0
            {
                return Err(Error::Conflict);
            }
            Ok(true)
        }
        .await;
        match status {
            Ok(v) => Ok(v),
            Err(Error::Conflict) => Err(Error::Conflict),
            Err(e) => {
                // Backend liveness and Execd readiness are distinct. Keep the controller's
                // readiness failure policy active when the backend is demonstrably alive.
                if r.backend == "supervisor" {
                    return Ok(!self.host.supervisor_absent(&r.sandbox_id).await?);
                }
                if r.backend == "conch" {
                    match self
                        .conch()?
                        .request(
                            Method::GET,
                            &format!("/api/v1/sandboxes/{}", r.sandbox_id),
                            None,
                            false,
                        )
                        .await
                    {
                        Err(Error::NotFound) => return Ok(false),
                        Err(error) => return Err(error),
                        Ok(_) => {
                            if self.running_conch().await?.contains(&r.sandbox_id) {
                                return Ok(true);
                            }
                        }
                    }
                }
                Err(e)
            }
        }
    }
}

fn managed_id(id: &str) -> bool {
    id.starts_with("adx-")
        && id.len() > 4
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn fingerprint(spec: &EnvironmentSpec) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(spec).map_err(unavailable)?)
    ))
}

fn sandbox_id(id: &str) -> String {
    format!(
        "adx-{}",
        &format!("{:x}", Sha256::digest(id.as_bytes()))[..28]
    )
}

#[async_trait]
impl RuntimeDriver for Jiuwenbox {
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<IpAddr> {
        self.start_with_deadline(
            spec,
            id,
            generation,
            devices,
            Instant::now() + self.launch.rpc_timeout,
        )
        .await
    }
    async fn start_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        deadline: Instant,
    ) -> Result<IpAddr> {
        if Instant::now() >= deadline {
            return Err(Error::DeadlineExceeded(
                "Jiuwenbox create expired before admission".into(),
            ));
        }
        if !adx_core::valid_runtime_id(&spec.id, generation, id) {
            return Err(Error::Invalid(
                "runtime ID does not match Environment and generation".into(),
            ));
        }
        // Validate every parameter before reserving an ID or touching the host.
        self.plan(spec, id, generation, devices, &self.root())?;
        let gate = self.gate(id);
        let lock = tokio::time::timeout_at(deadline, gate.lock_owned())
            .await
            .map_err(|_| Error::DeadlineExceeded("waiting for prior Jiuwenbox operation".into()))?;
        let fingerprint = fingerprint(spec)?;
        if let Some(r) = self.state.get(id).await? {
            if r.fingerprint != fingerprint || r.generation != generation {
                return Err(Error::Conflict);
            }
            return if r.phase == Phase::Ready && r.epoch == self.client.epoch().await? {
                r.ip.ok_or_else(|| unavailable("runtime address missing"))
            } else {
                Err(Error::Conflict)
            };
        }
        self.state
            .insert(Record {
                runtime_id: id.into(),
                environment_id: spec.id.clone(),
                tenant_id: spec.tenant_id.clone(),
                generation,
                sandbox_id: sandbox_id(id),
                backend: spec.runtime_class.clone(),
                epoch: self.client.epoch().await?,
                fingerprint,
                phase: Phase::Creating,
                ip: None,
            })
            .await?;
        let driver = self.clone();
        let owned_spec = spec.clone();
        let owned_id = id.to_owned();
        let owned_devices = devices.to_vec();
        let task = tokio::spawn(async move {
            let _lock = lock;
            driver
                .start_owned(owned_spec, owned_id, generation, owned_devices, deadline)
                .await
        });
        match tokio::time::timeout_at(deadline, task).await {
            Ok(result) => result.map_err(unavailable)?,
            Err(_) => {
                self.state
                    .update(id, |r| {
                        if r.phase == Phase::Creating {
                            r.phase = Phase::Failed;
                        }
                    })
                    .await?;
                Err(Error::DeadlineExceeded(
                    "Jiuwenbox create deadline; backend cleanup remains tracked".into(),
                ))
            }
        }
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        match self.state.get(id).await? {
            Some(r) => self.observation(&r).await,
            None => Ok(false),
        }
    }
    async fn observe(&self, id: &str) -> Result<RuntimeStatus> {
        if let Some(record) = self.state.get(id).await? {
            if record.epoch != self.client.epoch().await? {
                return Ok(RuntimeStatus {
                    running: false,
                    failure: Some(adx_core::RuntimeFailure {
                        reason: adx_core::RuntimeFailureReason::Missing,
                        exit_code: None,
                        probe_failure: None,
                        message: "runtime backend restarted".into(),
                    }),
                });
            }
        }
        Ok(RuntimeStatus {
            running: self.is_running(id).await?,
            failure: None,
        })
    }

    async fn reconcile_inventory(
        &self,
        catalog: &[EnvironmentRecord],
    ) -> Result<Vec<RuntimeObservation>> {
        // The catalog is supplied only after the node has validated authority and
        // drained accepted controller operations. Never infer it from local files.
        let seen = self.backend_inventory().await?;
        let epoch = self.client.epoch().await?;
        for r in catalog
            .iter()
            .filter(|r| matches!(r.spec.runtime_class.as_str(), "supervisor" | "conch"))
        {
            if !adx_core::valid_runtime_id(&r.spec.id, r.assignment.generation, &r.runtime.id) {
                return Err(Error::Conflict);
            }
            if self.state.get(&r.runtime.id).await?.is_some() {
                continue;
            }
            let backend_id = sandbox_id(&r.runtime.id);
            let registered = self
                .client
                .request(
                    Method::GET,
                    &format!("/api/v1/sandboxes/{backend_id}"),
                    None,
                    false,
                )
                .await;
            let ready = match registered {
                Ok(value) => {
                    let ready = value["phase"] == "ready"
                        && r.state == EnvironmentState::Running
                        && r.resources_held;
                    if ready
                        && (seen.get(&backend_id) != Some(&r.spec.runtime_class)
                            || value["ip_address"]
                                .as_str()
                                .and_then(|ip| ip.parse::<IpAddr>().ok())
                                != r.runtime.ip)
                    {
                        return Err(Error::Conflict);
                    }
                    ready
                }
                Err(Error::NotFound) => false,
                Err(e) => return Err(e),
            };
            self.state
                .insert(Record {
                    runtime_id: r.runtime.id.clone(),
                    environment_id: r.spec.id.clone(),
                    tenant_id: r.spec.tenant_id.clone(),
                    generation: r.assignment.generation,
                    sandbox_id: backend_id,
                    backend: r.spec.runtime_class.clone(),
                    epoch: epoch.clone(),
                    fingerprint: fingerprint(&r.spec)?,
                    phase: if ready { Phase::Ready } else { Phase::Deleting },
                    ip: r.runtime.ip,
                })
                .await?;
        }
        self.authority_loaded.store(true, Ordering::Release);
        // Startup recovery drains unfinished executions before node admission.
        // Live cleanup belongs to the corresponding controller queue.
        for r in
            self.state.all().await?.into_values().filter(|r| {
                r.phase != Phase::Deleted && (r.phase != Phase::Ready || r.epoch != epoch)
            })
        {
            let lock = self.gate(&r.runtime_id).lock_owned().await;
            self.start_cleanup(r.runtime_id, lock)
                .await
                .map_err(unavailable)??;
        }
        self.reconcile_unowned_executions().await?;
        self.inventory().await
    }

    async fn reconcile_unowned_executions(&self) -> Result<()> {
        let seen = self.backend_inventory().await?;
        let records = self.state.all().await?;
        for (id, backend) in seen {
            let record = records.values().find(|r| r.sandbox_id == id);
            if record.is_some_and(|r| !r.environment_id.is_empty()) {
                continue;
            }
            if !self.authority_loaded.load(Ordering::Acquire) {
                return Err(unavailable(
                    "authoritative catalog required before orphan collection",
                ));
            }
            if let Some(record) = record {
                // A delete-before-create fence has no Environment controller.
                self.remove(&record.runtime_id).await?;
            } else {
                // Unidentified native resources cannot enter a controller queue.
                // Serialize their recovery and retain it if the caller times out.
                let lock = self.gate(&format!("native-orphan:{id}")).lock_owned().await;
                let driver = self.clone();
                tokio::spawn(async move {
                    let _lock = lock;
                    driver.cleanup_backend(&id, &backend).await
                })
                .await
                .map_err(unavailable)??;
            }
        }
        Ok(())
    }

    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        let current = self.client.epoch().await?;
        let seen = self.backend_inventory().await?;
        let records = self.state.all().await?;
        if seen.keys().any(|id| {
            !records
                .values()
                .any(|r| r.sandbox_id == *id && !r.environment_id.is_empty())
        }) {
            return Err(unavailable(
                "unidentified native execution requires authoritative reconciliation",
            ));
        }
        // Inventory describes backend executions. Execd identity/readiness is
        // observed in each controller's queue, never serially across the node.
        let running_conch = if records
            .values()
            .any(|r| r.phase == Phase::Ready && r.epoch == current && r.backend == "conch")
        {
            self.running_conch().await?
        } else {
            BTreeSet::new()
        };
        if self.client.epoch().await? != current {
            return Err(unavailable("backend epoch changed during inventory"));
        }
        let mut observations = vec![];
        for r in records
            .values()
            .filter(|r| r.phase != Phase::Deleted || seen.contains_key(&r.sandbox_id))
        {
            let ready = r.phase == Phase::Ready && r.epoch == current;
            observations.push(RuntimeObservation {
                environment_id: r.environment_id.clone(),
                runtime_id: r.runtime_id.clone(),
                tenant_id: r.tenant_id.clone(),
                generation: r.generation,
                running: if !ready {
                    false
                } else if r.backend == "conch" {
                    running_conch.contains(&r.sandbox_id)
                } else {
                    !self.host.supervisor_absent(&r.sandbox_id).await?
                },
            });
        }
        Ok(observations)
    }
    async fn remove(&self, id: &str) -> Result<()> {
        let gate = self.gate(id);
        let lock = gate.lock_owned().await;
        if let Some(r) = self.state.get(id).await? {
            if r.phase == Phase::Deleted {
                let seen = self.backend_inventory().await?;
                let Some(backend) = seen.get(&r.sandbox_id).cloned() else {
                    return Ok(());
                };
                self.state.update(id, |r| r.backend = backend).await?;
            }
            self.state.update(id, |r| r.phase = Phase::Deleting).await?;
            self.start_cleanup(id.into(), lock)
                .await
                .map_err(unavailable)??;
        } else {
            // Fence accepted operations for this process; recovery uses the authority catalog.
            self.state
                .insert(Record {
                    runtime_id: id.into(),
                    environment_id: String::new(),
                    tenant_id: String::new(),
                    generation: 0,
                    sandbox_id: sandbox_id(id),
                    backend: String::new(),
                    epoch: String::new(),
                    fingerprint: String::new(),
                    phase: Phase::Deleted,
                    ip: None,
                })
                .await?;
        }
        Ok(())
    }
    async fn set_network_policy(
        &self,
        id: &str,
        network: Option<&NetworkPolicy>,
        ports: &[u16],
        tunnel_http_port: Option<u16>,
    ) -> Result<()> {
        let gate = self.gate(id);
        let _lock = gate.lock().await;
        let r = self.state.get(id).await?.ok_or(Error::NotFound)?;
        if r.phase != Phase::Ready {
            return Err(Error::Conflict);
        }
        let mapped =
            policy::network_policy(&r.backend, network, ports, tunnel_http_port, &self.launch)?;
        let fragment = if r.backend == "conch" {
            json!({"conch":{"network":mapped}})
        } else {
            json!({"network":mapped})
        };
        self.client
            .request(
                Method::PUT,
                &format!("/api/v1/policies/{}", r.sandbox_id),
                Some(json!({"policy":fragment,"policy_mode":"override"})),
                false,
            )
            .await?;
        Ok(())
    }
}
