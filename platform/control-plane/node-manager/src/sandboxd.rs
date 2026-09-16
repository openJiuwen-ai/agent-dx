//! sandboxd PR #56 adapter. The RPC task outlives a canceled caller so cleanup
//! cannot race a still-running Start. Ambiguous transport failures retain capacity.
use crate::RuntimeBackend;
use adx_core::{Error, InstanceSpec, Result};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};
use tower::service_fn;

pub mod proto {
    tonic::include_proto!("runtime.v1");
}
use proto::sandbox_service_client::SandboxServiceClient;

#[derive(Clone)]
pub struct Config {
    pub command: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: String,
    pub rpc_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            command: vec!["/usr/local/bin/rrt-runtime".into()],
            env: HashMap::from([
                ("RRT_HTTP_ONLY".into(), "1".into()),
                ("RRT_HTTP_PORT".into(), "50090".into()),
            ]),
            cwd: "/".into(),
            rpc_timeout: Duration::from_secs(30),
        }
    }
}

enum StartState {
    Idle,
    Started(IpAddr),
    Settled,
    Uncertain,
}
type Cell = Arc<tokio::sync::Mutex<StartState>>;

#[derive(Clone)]
pub struct Sandboxd {
    client: SandboxServiceClient<Channel>,
    config: Config,
    starts: Arc<Mutex<BTreeMap<String, Cell>>>,
    backend_ids: Arc<Mutex<BTreeMap<String, String>>>,
}

fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("sandboxd: {error}"))
}

impl Sandboxd {
    pub async fn connect(path: PathBuf, config: Config) -> Result<Self> {
        if config.command.is_empty() || config.rpc_timeout.is_zero() {
            return Err(Error::Invalid(
                "sandboxd command and positive RPC timeout are required".into(),
            ));
        }
        let channel = Endpoint::from_static("http://localhost")
            .connect_timeout(config.rpc_timeout)
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await
            .map_err(unavailable)?;
        Ok(Self {
            client: SandboxServiceClient::new(channel),
            config,
            starts: Arc::default(),
            backend_ids: Arc::default(),
        })
    }

    /// Do not publish node admission before sandboxd's own housekeeping and
    /// resource/image recovery report readiness. A bound UDS alone is insufficient.
    pub async fn wait_ready(&self) -> Result<()> {
        self.wait_ready_with_timeout(self.config.rpc_timeout).await
    }

    pub async fn wait_ready_with_timeout(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                match self
                    .client
                    .clone()
                    .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(e) if e.code() == Code::Unimplemented => return Ok(()),
                    Err(e) if e.code() == Code::Unavailable => {
                        tokio::time::sleep(Duration::from_millis(200)).await
                    }
                    Err(e) => return Err(unavailable(e)),
                }
            }
        })
        .await
        .map_err(|_| unavailable("backend readiness timed out"))?
    }
    async fn capabilities(&self, runtime: &str) -> Result<Option<proto::RuntimeInfo>> {
        match self
            .client
            .clone()
            .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
            .await
        {
            Ok(r) => Ok(r
                .into_inner()
                .runtimes
                .into_iter()
                .find(|r| r.runtime_class == runtime)),
            Err(e) if e.code() == Code::Unimplemented => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }
    async fn execution_request(
        &self,
        spec: &InstanceSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<Request<proto::StartRequest>> {
        let mut config = self.config.clone();
        // Runtime capability metadata owns the guest paths. Never inherit caller
        // values that could redirect the privileged checkpoint handoff.
        config
            .env
            .insert("ADX_CHECKPOINT_HANDOFF_FILE".into(), String::new());
        config.env.insert("ADX_ENV_FILE".into(), String::new());
        if let Some(info) = self.capabilities(&spec.runtime).await? {
            if info.supports_checkpoint_restore {
                config.env.insert(
                    "ADX_CHECKPOINT_HANDOFF_FILE".into(),
                    info.checkpoint_handoff_path,
                );
                config
                    .env
                    .insert("ADX_ENV_FILE".into(), info.restore_env_path);
            }
        }
        Ok(self.request(start_request(
            spec, runtime_id, generation, devices, &config,
        )?))
    }
    async fn execute_start(
        &self,
        runtime_id: &str,
        request: Request<proto::StartRequest>,
    ) -> Result<IpAddr> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        if let StartState::Started(ip) = *state {
            return Ok(ip);
        }
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable("start outcome requires reconciliation"));
        }
        *state = StartState::Uncertain;
        let mut client = self.client.clone();
        let backend_ids = self.backend_ids.clone();
        let logical_id = runtime_id.to_string();
        tokio::spawn(async move {
            let response = match client.start(request).await {
                Ok(response) => response.into_inner(),
                Err(error) => {
                    if matches!(
                        error.code(),
                        Code::InvalidArgument
                            | Code::Unimplemented
                            | Code::PermissionDenied
                            | Code::Unauthenticated
                    ) {
                        *state = StartState::Settled;
                    }
                    return Err(unavailable(error));
                }
            };
            // A completed response settles Start, even when its payload reports failure.
            *state = StartState::Settled;
            if !response.id.is_empty() {
                backend_ids
                    .lock()
                    .unwrap()
                    .insert(logical_id, response.id.clone());
            }
            if response.code != 0 {
                return Err(unavailable(response.message));
            }
            if response.id.is_empty() {
                *state = StartState::Uncertain;
                return Err(unavailable("Start returned no backend identity"));
            }
            let ip: IpAddr = response
                .sandbox_ip
                .parse()
                .map_err(|_| unavailable("Start did not return a valid sandbox IP"))?;
            if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
                return Err(unavailable("Start returned a non-routable sandbox IP"));
            }
            *state = StartState::Started(ip);
            Ok(ip)
        })
        .await
        .map_err(unavailable)?
    }

    fn cell(&self, id: &str) -> Cell {
        self.starts
            .lock()
            .unwrap()
            .entry(id.into())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(StartState::Idle)))
            .clone()
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(self.config.rpc_timeout);
        request
    }

    // Platform execution IDs and backend IDs are independent. Labels rebuild
    // the in-memory mapping after restart; no backend naming convention is assumed.
    async fn list_id(&self, logical_id: &str) -> Result<Vec<proto::SandboxStatus>> {
        let physical = self.backend_ids.lock().unwrap().get(logical_id).cloned();
        let selector = if !logical_id.is_empty() && physical.is_none() {
            HashMap::from([("adx.runtime_id".into(), logical_id.into())])
        } else {
            HashMap::new()
        };
        let response = self
            .client
            .clone()
            .list(self.request(proto::ListSandboxesRequest {
                id: physical.clone().unwrap_or_default(),
                selector,
            }))
            .await;
        let mut sandboxes = match response {
            Ok(response) => response.into_inner().sandboxes,
            Err(error) if physical.is_some() && error.code() == Code::NotFound => {
                return Ok(Vec::new())
            }
            Err(error) => return Err(unavailable(error)),
        };
        let mut discovered = BTreeMap::new();
        for sandbox in &mut sandboxes {
            if !sandbox.labels.contains_key("adx.instance_id") {
                continue;
            }
            let instance = &sandbox.labels["adx.instance_id"];
            let generation = sandbox
                .labels
                .get("adx.generation")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .ok_or_else(|| unavailable("managed runtime has no valid generation"))?;
            let logical = sandbox
                .labels
                .get("adx.runtime_id")
                .filter(|id| adx_core::valid_runtime_id(instance, generation, id))
                .ok_or_else(|| unavailable("managed runtime has invalid execution labels"))?;
            if sandbox.id.is_empty()
                || instance.is_empty()
                || (!logical_id.is_empty() && logical != logical_id)
                || discovered
                    .insert(logical.clone(), sandbox.id.clone())
                    .is_some()
            {
                return Err(unavailable("ambiguous managed runtime identity"));
            }
            sandbox.id = logical.clone();
        }
        let mut ids = self.backend_ids.lock().unwrap();
        for (logical, physical) in &discovered {
            if ids
                .get(logical)
                .is_some_and(|previous| previous != physical)
            {
                return Err(unavailable("backend identity changed during execution"));
            }
        }
        ids.extend(discovered);
        Ok(sandboxes)
    }
    async fn physical_id(&self, logical_id: &str) -> Result<Option<String>> {
        if let Some(id) = self.backend_ids.lock().unwrap().get(logical_id).cloned() {
            return Ok(Some(id));
        }
        self.list_id(logical_id).await?;
        Ok(self.backend_ids.lock().unwrap().get(logical_id).cloned())
    }

    pub async fn has_managed_instances(&self) -> Result<bool> {
        Ok(self
            .list_id("")
            .await?
            .iter()
            .any(|s| s.labels.contains_key("adx.instance_id")))
    }

    pub async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self
            .list_id(id)
            .await?
            .iter()
            .any(|x| x.id == id && x.state == proto::SandboxState::Running as i32))
    }

    pub async fn check_health(&self) -> Result<()> {
        self.client
            .clone()
            .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    pub async fn stats(&self, id: &str) -> Result<proto::StatsResponse> {
        Ok(self
            .client
            .clone()
            .stats(self.request(proto::StatsRequest {
                id: self.physical_id(id).await?.ok_or(Error::NotFound)?,
            }))
            .await
            .map_err(unavailable)?
            .into_inner())
    }
}

pub fn start_request(
    spec: &InstanceSpec,
    runtime_id: &str,
    ownership_generation: u64,
    devices: &[adx_core::scheduling::DeviceAllocation],
    config: &Config,
) -> Result<proto::StartRequest> {
    spec.validate()?;
    adx_core::scheduling::validate_device_assignment(&spec.scheduling.devices, devices)?;
    if runtime_id.is_empty() || ownership_generation == 0 || config.command.is_empty() {
        return Err(Error::Invalid(
            "runtime identity and command are required".into(),
        ));
    }
    // sandboxd's resource map is f64: reject lossy integer conversions.
    if spec.resources.cpu_millis > 1 << 53 || spec.resources.memory_bytes > 1 << 53 {
        return Err(Error::Invalid(
            "resource value exceeds sandboxd numeric precision".into(),
        ));
    }
    let mut envs: HashMap<String, String> = spec.env.clone().into_iter().collect();
    // Deployment configuration owns control ports, tokens, and execution identity.
    envs.extend(config.env.clone());
    envs.remove("ADX_RESTORE_ORIGIN");
    envs.insert("ADX_INSTANCE_ID".into(), spec.id.clone());
    envs.insert("ADX_RUNTIME_ID".into(), runtime_id.into());
    envs.insert(
        "ADX_OWNERSHIP_GENERATION".into(),
        ownership_generation.to_string(),
    );
    let mut xpu = std::collections::BTreeMap::<String, Vec<u32>>::new();
    for d in devices {
        let kind = match d.kind {
            adx_core::scheduling::DeviceKind::Gpu => "gpu",
            adx_core::scheduling::DeviceKind::Npu => "npu",
        };
        xpu.entry(kind.into()).or_default().push(d.id);
    }
    Ok(proto::StartRequest {
        xpu_allocations: xpu
            .into_iter()
            .map(|(kind, mut device_ids)| {
                device_ids.sort();
                proto::XpuAllocation {
                    r#type: kind,
                    device_ids,
                }
            })
            .collect(),
        sandbox_id: String::new(),
        runtime: spec.runtime.clone(),
        rootfs: Some(proto::RootfsConfig {
            readonly: false,
            r#type: proto::RootfsSrcType::Image as i32,
            source: Some(proto::rootfs_config::Source::ImageUrl(spec.image.clone())),
            writable_layer_size_bytes: 0,
        }),
        command: config.command.clone(),
        cwd: config.cwd.clone(),
        envs,
        resources: HashMap::from([
            ("CPU".into(), spec.resources.cpu_millis as f64),
            (
                "Memory".into(),
                spec.resources.memory_bytes as f64 / 1_048_576.0,
            ),
        ]),
        labels: HashMap::from([
            ("adx.instance_id".into(), spec.id.clone()),
            ("adx.tenant_id".into(), spec.tenant_id.clone()),
            ("adx.runtime_id".into(), runtime_id.into()),
            ("adx.generation".into(), ownership_generation.to_string()),
        ]),
        writable_layer_limit_bytes: spec.resources.disk_bytes,
        ..Default::default()
    })
}

#[async_trait]
impl RuntimeBackend for Sandboxd {
    async fn stats(&self, runtime_id: &str) -> Result<crate::metrics::RuntimeUsage> {
        let value = Sandboxd::stats(self, runtime_id).await?;
        Ok(crate::metrics::RuntimeUsage {
            cpu_usage_ns: value.cpu_usage_ns,
            memory_usage_bytes: value.memory_usage_bytes,
            memory_limit_bytes: value.memory_limit_bytes,
        })
    }
    async fn checkpoint_supported(&self, runtime: &str) -> Result<()> {
        match self.capabilities(runtime).await? {
            Some(info)
                if info.supports_checkpoint_restore
                    && !info.checkpoint_handoff_path.is_empty()
                    && !info.restore_env_path.is_empty() =>
            {
                Ok(())
            }
            _ => Err(Error::Invalid(
                "runtime does not provide checkpoint/restore handoff".into(),
            )),
        }
    }
    async fn checkpoint(&self, runtime_id: &str, path: &Path, duration: Duration) -> Result<()> {
        if !path.is_absolute() || duration.is_zero() {
            return Err(Error::Invalid(
                "absolute checkpoint directory and positive timeout required".into(),
            ));
        }
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable("execution outcome requires reconciliation"));
        }
        let id = self.physical_id(runtime_id).await?.ok_or(Error::NotFound)?;
        let mut request = Request::new(proto::CheckpointRequest {
            id,
            checkpoint_dir: path
                .to_str()
                .ok_or_else(|| Error::Invalid("checkpoint path is not UTF-8".into()))?
                .into(),
            timeout_seconds: duration
                .as_secs()
                .try_into()
                .map_err(|_| Error::Invalid("checkpoint timeout overflow".into()))?,
            compress: false,
            leave_running: false,
            snapshot_type: "Full".into(),
        });
        request.set_timeout(duration + self.config.rpc_timeout);
        let mut client = self.client.clone();
        // Retain the operation guard on caller cancellation, as with Start.
        *state = StartState::Uncertain;
        tokio::spawn(async move {
            match client.checkpoint(request).await {
                Ok(_) => {
                    *state = StartState::Settled;
                    Ok(())
                }
                Err(error) => {
                    if matches!(
                        error.code(),
                        Code::InvalidArgument
                            | Code::Unimplemented
                            | Code::PermissionDenied
                            | Code::Unauthenticated
                    ) {
                        *state = StartState::Settled;
                    }
                    Err(unavailable(error))
                }
            }
        })
        .await
        .map_err(unavailable)?
    }
    async fn restore(
        &self,
        spec: &InstanceSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
    ) -> Result<IpAddr> {
        self.restore_from(spec, runtime_id, generation, devices, path, None)
            .await
    }
    async fn restore_from(
        &self,
        spec: &InstanceSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<IpAddr> {
        self.checkpoint_supported(&spec.runtime).await?;
        if !path.is_absolute() {
            return Err(Error::Invalid("absolute checkpoint path required".into()));
        }
        let mut request = self
            .execution_request(spec, runtime_id, generation, devices)
            .await?;
        if let Some(origin) = origin {
            let context = adx_core::runtime::RuntimeRestore {
                target: adx_core::runtime::RuntimeIdentity {
                    instance_id: spec.id.clone(),
                    runtime_id: runtime_id.into(),
                    ownership_generation: generation,
                },
                origin: Some(origin.clone()),
            };
            context.validate(origin)?;
            request.get_mut().envs.insert(
                "ADX_RESTORE_ORIGIN".into(),
                serde_json::to_string(origin).map_err(unavailable)?,
            );
        }
        request.get_mut().checkpoint_info = Some(proto::CheckpointInfo {
            checkpoint_dir: path
                .to_str()
                .ok_or_else(|| Error::Invalid("checkpoint path is not UTF-8".into()))?
                .into(),
        });
        self.execute_start(runtime_id, request).await
    }
    async fn inventory(&self) -> Result<Vec<crate::RuntimeObservation>> {
        self.list_id("")
            .await?
            .into_iter()
            .filter(|s| s.labels.contains_key("adx.instance_id"))
            .map(|s| {
                let instance_id = s.labels["adx.instance_id"].clone();
                let tenant_id = s
                    .labels
                    .get("adx.tenant_id")
                    .cloned()
                    .ok_or_else(|| unavailable("managed runtime has no tenant"))?;
                let generation = s
                    .labels
                    .get("adx.generation")
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .ok_or_else(|| unavailable("managed runtime has invalid identity"))?;
                Ok(crate::RuntimeObservation {
                    instance_id,
                    tenant_id,
                    generation,
                    runtime_id: s.id,
                    running: s.state == proto::SandboxState::Running as i32,
                })
            })
            .collect()
    }

    async fn is_running(&self, runtime_id: &str) -> Result<bool> {
        Sandboxd::is_running(self, runtime_id).await
    }
    async fn start(
        &self,
        spec: &InstanceSpec,
        runtime_id: &str,
        ownership_generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        let request = self
            .execution_request(spec, runtime_id, ownership_generation, devices)
            .await?;
        self.execute_start(runtime_id, request).await
    }

    async fn remove(&self, runtime_id: &str) -> Result<()> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable(
                "cannot confirm cleanup while Start outcome is unknown",
            ));
        }
        if let Some(physical) = self.physical_id(runtime_id).await? {
            match self
                .client
                .clone()
                .delete(self.request(proto::DeleteRequest {
                    id: physical,
                    timeout: 0,
                }))
                .await
            {
                Ok(_) => (),
                Err(error) if error.code() == Code::NotFound => (),
                Err(error) => return Err(unavailable(error)),
            }
        }
        if self
            .list_id(runtime_id)
            .await?
            .iter()
            .any(|x| x.id == runtime_id)
        {
            return Err(unavailable("runtime remains after Delete"));
        }
        self.backend_ids.lock().unwrap().remove(runtime_id);
        *state = StartState::Idle;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn field_four_is_the_sandbox_ip() {
        let decoded = proto::StartResponse::decode(&b"\x22\x0810.0.0.2"[..]).unwrap();
        assert_eq!(decoded.sandbox_ip, "10.0.0.2");
        assert_eq!(
            proto::StartResponse::decode(&b""[..]).unwrap().sandbox_ip,
            ""
        );
    }

    #[test]
    fn request_preserves_execution_identity_image_and_resource_units() {
        let spec = InstanceSpec {
            snapshot_id: None,
            lifecycle: Default::default(),
            env: [
                ("USER_VALUE".into(), "preserved".into()),
                ("ADX_INSTANCE_ID".into(), "spoofed-by-spec".into()),
                ("ADX_RESTORE_ORIGIN".into(), "spoofed-origin".into()),
            ]
            .into(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image:tag".into(),
            runtime: "runsc".into(),
            resources: adx_core::Resources {
                cpu_millis: 1500,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: 5 * 1024 * 1024 * 1024,
            },
            priority: 0,
        };
        let config = Config {
            env: HashMap::from([
                ("ADX_INSTANCE_ID".into(), "spoofed".into()),
                ("USER_ENV".into(), "kept".into()),
            ]),
            ..Config::default()
        };
        let request = start_request(&spec, "i-7", 7, &[], &config).unwrap();
        assert!(!request.envs.contains_key("ADX_RESTORE_ORIGIN"));
        assert_eq!(request.envs["ADX_INSTANCE_ID"], "i");
        assert_eq!(request.envs["ADX_RUNTIME_ID"], "i-7");
        assert_eq!(request.envs["ADX_OWNERSHIP_GENERATION"], "7");
        assert_eq!(request.envs["USER_ENV"], "kept");
        assert_eq!(request.envs["USER_VALUE"], "preserved");
        assert!(start_request(&spec, "i-7", 0, &[], &config).is_err());
        assert!(request.sandbox_id.is_empty());
        assert_eq!(request.labels["adx.runtime_id"], "i-7");
        assert_eq!(request.labels["adx.generation"], "7");
        assert_eq!(request.runtime, "runsc");
        assert_eq!(request.resources["CPU"], 1500.0);
        assert_eq!(request.resources["Memory"], 2048.0);
        assert_eq!(request.writable_layer_limit_bytes, 5 * 1024 * 1024 * 1024);
        assert_eq!(
            request.rootfs.unwrap().source,
            Some(proto::rootfs_config::Source::ImageUrl("image:tag".into()))
        );
    }
}
