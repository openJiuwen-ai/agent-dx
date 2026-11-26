use crate::rollout::actor::{CommitStage, Launch, Operation, Owner, Reply};
use crate::{
    config::{Deployment, Process, Role, ShutdownMode},
    Result,
};
use adx_protocol::relay as pb;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::{
            fs::{FileTypeExt, OpenOptionsExt, PermissionsExt},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixListener, UnixStream},
};
use tower::service_fn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    Status,
    Stop { mode: Option<ShutdownMode> },
}

impl Request {
    fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Stop { mode: None } => "stop",
            Self::Stop {
                mode: Some(ShutdownMode::Preserve),
            } => "stop preserve",
            Self::Stop {
                mode: Some(ShutdownMode::Drain),
            } => "stop drain",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "status" => Some(Self::Status),
            "stop" => Some(Self::Stop { mode: None }),
            "stop preserve" => Some(Self::Stop {
                mode: Some(ShutdownMode::Preserve),
            }),
            "stop drain" => Some(Self::Stop {
                mode: Some(ShutdownMode::Drain),
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownPhase {
    Drain,
    Terminate,
}

/// A failed shutdown stage; remaining dependencies stay supervised for retry.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ShutdownFailure {
    pub mode: ShutdownMode,
    pub phase: ShutdownPhase,
    pub service_id: String,
    pub message: String,
}

impl std::fmt::Display for ShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let phase = match self.phase {
            ShutdownPhase::Drain => "drain",
            ShutdownPhase::Terminate => "terminate",
        };
        write!(
            formatter,
            "supervisor operation failed: mode={}, phase={phase}, service={}: {}",
            self.mode, self.service_id, self.message
        )
    }
}

impl std::error::Error for ShutdownFailure {}

enum SignalTarget {
    Process,
    ProcessGroup,
}

struct ManagedService {
    config: Process,
    child: Option<Child>,
    restarts: u64,
    next_start: Instant,
    failed: bool,
    capture: Option<crate::logging::Capture>,
    finished_log: Option<crate::logging::Status>,
    signal_target: SignalTarget,
    exit_status: Option<std::process::ExitStatus>,
    route_inode: Option<(u64, u64)>,
    generation_pid: Option<u32>,
    rollout: Option<RolloutState>,
}
struct RolloutState {
    task: String,
    owner: Owner,
    old_pid: u32,
    retiring: Option<Box<ManagedService>>,
    spawn: Option<SpawnReceipt>,
    stop: Option<(u32, Instant, Instant)>,
    selected: bool,
}
impl RolloutState {
    fn reserved(task: String, owner: Owner, old_pid: u32) -> Self {
        Self {
            task,
            owner,
            old_pid,
            retiring: None,
            spawn: None,
            stop: None,
            selected: false,
        }
    }
    fn committed(&self, attempt: &str, pid: Option<u32>) -> bool {
        self.selected
            && self.spawn.as_ref().is_some_and(|spawn| {
                spawn.attempt == attempt
                    && spawn.owned_pid.is_some()
                    && pid.is_none_or(|pid| spawn.owned_pid == Some(pid))
            })
    }
}
struct SpawnReceipt {
    attempt: String,
    config: Process,
    owned_pid: Option<u32>,
    closed: Vec<String>,
}
impl SpawnReceipt {
    fn replay(&self, attempt: &str, config: &Process, generation: Option<u32>) -> Result<u32> {
        if self.attempt != attempt
            || serde_json::to_value(&self.config)? != serde_json::to_value(config)?
            || self.owned_pid.is_none()
            || self.owned_pid != generation
        {
            return Err("spawn receipt conflict or unresolved spawn".into());
        }
        self.owned_pid.ok_or_else(|| "spawn not confirmed".into())
    }
}
impl ManagedService {
    fn fenced(&self) -> bool {
        self.rollout.as_ref().is_some_and(|state| !state.selected)
    }

    /// Unstarted, unfenced generation with no rollout ownership.
    fn new(config: Process) -> Self {
        Self {
            config,
            child: None,
            restarts: 0,
            next_start: Instant::now(),
            failed: false,
            capture: None,
            finished_log: None,
            signal_target: SignalTarget::ProcessGroup,
            exit_status: None,
            route_inode: None,
            generation_pid: None,
            rollout: None,
        }
    }
    fn failed_signal(&self) -> i32 {
        if self.config.role == Role::Adxlet {
            libc::SIGUSR1
        } else {
            libc::SIGTERM
        }
    }
    fn stop_failed(&mut self, pid: u32, deadline: Instant) -> Result<()> {
        if self.generation_pid != Some(pid) || !self.fenced() {
            return Err("failed Child generation changed".into());
        }
        if self
            .rollout
            .as_ref()
            .is_some_and(|state| state.stop.is_some())
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("rollback stop deadline exceeded".into());
        }
        if self.child.is_none() && self.exit_status.is_some() {
            return Ok(());
        }
        if self.child.as_ref().map(Child::id) != Some(pid) {
            return Err("failed Child absent".into());
        }
        // Signal only the frozen owned Child, not other generations or descendants.
        self.signal_target = SignalTarget::Process;
        self.rollout
            .as_mut()
            .ok_or("rollout ownership absent")?
            .stop = Some((
            pid,
            deadline.min(Instant::now() + Duration::from_secs(5)),
            deadline,
        ));
        self.signal(self.failed_signal())
    }
    fn is_afs(&self) -> bool {
        matches!(self.config.role, Role::AfsMeta | Role::AfsNode)
    }

    fn start(&mut self, logs: &Path, policy: &crate::logging::Policy) -> Result<()> {
        self.start_from(logs, policy, None)
    }
    fn start_from(
        &mut self,
        logs: &Path,
        policy: &crate::logging::Policy,
        executable: Option<&File>,
    ) -> Result<()> {
        use std::os::fd::AsRawFd;
        let pinned =
            executable.map(|file| PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd())));
        let mut command = Command::new(pinned.as_deref().unwrap_or(&self.config.binary));
        command
            .args(&self.config.args)
            .envs(&self.config.env)
            .stdin(Stdio::null())
            .process_group(0);
        let capture = if policy.enabled {
            Some(crate::logging::Capture::attach(
                &mut command,
                logs,
                &self.config.id,
                policy.clone(),
            )?)
        } else {
            let log = OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(logs.join(format!("{}.log", self.config.id)))?;
            command.stdout(log.try_clone()?).stderr(log);
            None
        };
        self.child = Some(command.spawn()?);
        self.generation_pid = self.child.as_ref().map(Child::id);
        if let Some(state) = self.rollout.as_mut() {
            state.stop = None;
        }
        self.exit_status = None;
        self.route_inode = None;
        self.signal_target = SignalTarget::ProcessGroup;
        self.capture = capture;
        self.failed = false;
        Ok(())
    }
    async fn finish_logs(&mut self) -> Result<()> {
        if let Some(mut capture) = self.capture.take() {
            let state = tokio::task::spawn_blocking(move || {
                capture.finish();
                capture.status()
            })
            .await?;
            if let Some(error) = &state.error {
                eprintln!(
                    "service {} log error: {} (failed batch bytes {})",
                    self.config.id, error, state.failed_bytes
                );
            }
            self.finished_log = Some(state);
        }
        Ok(())
    }
    fn signal(&mut self, signal: i32) -> Result<()> {
        if let Some(child) = &mut self.child {
            if child.try_wait()?.is_none() {
                let id = i32::try_from(child.id())?;
                let target = match self.signal_target {
                    SignalTarget::Process => id,
                    SignalTarget::ProcessGroup => -id,
                };
                // SAFETY: the child has not been reaped, so its PID (also its process-group ID)
                // cannot have been reused. kill does not retain the scalar identifier.
                if unsafe { libc::kill(target, signal) } != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error.into());
                    }
                }
            }
        }
        Ok(())
    }
}
impl Drop for ManagedService {
    fn drop(&mut self) {
        let _ = self.signal(libc::SIGKILL);
        if let Some(child) = &mut self.child {
            let _ = child.wait();
        }
    }
}
struct SupervisorGuard {
    file: File,
    socket: PathBuf,
}
impl Drop for SupervisorGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket);
        // SAFETY: file owns this valid descriptor for the duration of the call; flock does not
        // retain it. Unlock failure cannot be recovered while dropping the guard.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
fn acquire_lock(root: &Path) -> Result<SupervisorGuard> {
    fs::create_dir_all(root)?;
    let meta = fs::symlink_metadata(root)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err("state directory must be a real directory".into());
    }
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join("supervisor.lock"))?;
    // SAFETY: file owns this valid descriptor for the duration of the call; flock does not retain
    // it. The descriptor remains owned by Guard after the lock succeeds.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("deployment already supervised".into());
    }
    let socket = root.join("supervisor.sock");
    if let Ok(m) = fs::symlink_metadata(&socket) {
        if !m.file_type().is_socket() {
            return Err("supervisor socket path occupied".into());
        }
        fs::remove_file(&socket)?;
    }
    Ok(SupervisorGuard { file, socket })
}
pub async fn request(root: &Path, request: Request, timeout: Duration) -> Result<Value> {
    exchange(root, request.as_str().as_bytes(), timeout).await
}

async fn exchange(root: &Path, request: &[u8], timeout: Duration) -> Result<Value> {
    tokio::time::timeout(timeout, async {
        let mut stream = UnixStream::connect(root.join("supervisor.sock")).await?;
        stream.write_all(request).await?;
        stream.write_all(b"\n").await?;
        let mut data = Vec::new();
        stream.take(1024 * 1024).read_to_end(&mut data).await?;
        let response: Value = serde_json::from_slice(&data)?;
        if response.get("ok") != Some(&Value::Bool(true)) {
            if let Some(error) = response.get("error") {
                if let Ok(failure) = serde_json::from_value::<ShutdownFailure>(error.clone()) {
                    return Err(Box::new(failure) as Box<dyn std::error::Error + Send + Sync>);
                }
                let message = error
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| error.to_string());
                return Err(std::io::Error::other(format!(
                    "supervisor operation failed: {message}"
                ))
                .into());
            }
            return Err(std::io::Error::other(
                "supervisor operation failed: invalid action or response",
            )
            .into());
        }
        Ok(response)
    })
    .await?
}

pub async fn rollout_request(root: &Path, request: crate::rollout::Request) -> Result<Value> {
    let bytes = serde_json::to_vec(&request)?;
    if bytes.len() > 4095 {
        return Err("rollout request too large".into());
    }
    exchange(root, &bytes, Duration::from_secs(3)).await
}

async fn drain(path: PathBuf, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async move {
        let channel = tonic::transport::Endpoint::from_static("http://node-admin")
            .connect_with_connector(service_fn(move |_| {
                let admin_socket = path.clone();
                async move { UnixStream::connect(admin_socket).await.map(TokioIo::new) }
            }))
            .await?;
        pb::node_admin_service_client::NodeAdminServiceClient::new(channel)
            .drain(pb::DrainRequest {})
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
async fn drain_service(service: &mut ManagedService, timeout: Duration) -> Result<()> {
    let Some(path) = &service.config.admin_socket else {
        return Ok(());
    };
    if service
        .child
        .as_mut()
        .map(|child| child.try_wait())
        .transpose()?
        .flatten()
        .is_some()
        || service.child.is_none()
    {
        return Err("Adxlet unavailable for cleanup".into());
    }
    drain(path.clone(), timeout).await
}

async fn terminate_service(
    service: &mut ManagedService,
    timeout: Duration,
    mode: ShutdownMode,
) -> Result<()> {
    if service.is_afs() && service.child.is_none() && service.failed {
        return Err("AFS service failed before stop".into());
    }
    let signal = if service.config.role == Role::Adxlet && mode == ShutdownMode::Preserve {
        // Descendants may own retained execution. Keep SIGUSR1 and any subsequent
        // timeout/Drop SIGKILL scoped to adxlet, even if they share its process group.
        service.signal_target = SignalTarget::Process;
        libc::SIGUSR1
    } else {
        service.signal_target = SignalTarget::ProcessGroup;
        libc::SIGTERM
    };
    service.signal(signal)?;
    let deadline = Instant::now() + timeout;
    while let Some(child) = &mut service.child {
        if let Some(status) = child.try_wait()? {
            if service.is_afs() && !status.success() {
                service.child = None;
                service.failed = true;
                service.finish_logs().await?;
                return Err("AFS service exited unsuccessfully during stop".into());
            }
            break;
        }
        if Instant::now() >= deadline {
            service.signal(libc::SIGKILL)?;
            if let Some(child) = &mut service.child {
                child.wait()?;
            }
            service.child = None;
            service.failed = true;
            service.finish_logs().await?;
            return Err("service termination deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    service.finish_logs().await?;
    Ok(())
}

async fn stop(
    children: &mut [ManagedService],
    timeout: Duration,
    mode: ShutdownMode,
) -> std::result::Result<(), ShutdownFailure> {
    eprintln!("stopping supervised services: mode={mode}");
    if mode == ShutdownMode::Drain {
        // All node cleanup must succeed while Coordinator, Redis and local proxies remain alive.
        for service in children.iter_mut() {
            drain_service(service, timeout)
                .await
                .map_err(|error| ShutdownFailure {
                    mode,
                    phase: ShutdownPhase::Drain,
                    service_id: service.config.id.clone(),
                    message: error.to_string(),
                })?;
        }
    }
    for service in children.iter_mut().rev() {
        if let Some(retiring) = service
            .rollout
            .as_mut()
            .and_then(|state| state.retiring.as_mut())
        {
            terminate_service(retiring, timeout, mode)
                .await
                .map_err(|error| ShutdownFailure {
                    mode,
                    phase: ShutdownPhase::Terminate,
                    service_id: retiring.config.id.clone(),
                    message: error.to_string(),
                })?;
        }
        terminate_service(service, timeout, mode)
            .await
            .map_err(|error| ShutdownFailure {
                mode,
                phase: ShutdownPhase::Terminate,
                service_id: service.config.id.clone(),
                message: error.to_string(),
            })?;
    }
    Ok(())
}
async fn health_status(url: &str) -> Value {
    let Some(target) = url.strip_prefix("http://") else {
        return json!({"ready": false, "error": "unsupported health URL"});
    };
    let Some((authority, path)) = target.split_once('/') else {
        return json!({"ready": false, "error": "invalid health URL"});
    };
    let path = format!("/{path}");
    let Ok(Ok(mut stream)) =
        tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(authority)).await
    else {
        return json!({"ready": false, "error": "health endpoint unavailable"});
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).await.is_err() {
        return json!({"ready": false, "error": "health request failed"});
    }
    let mut data = Vec::new();
    match tokio::time::timeout(Duration::from_millis(500), stream.read_to_end(&mut data)).await {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return json!({"ready": false, "error": "health response read failed"}),
        Err(_) => return json!({"ready": false, "error": "health response timed out"}),
    }
    let status_line =
        String::from_utf8_lossy(data.split(|b| *b == b'\n').next().unwrap_or_default())
            .trim_end_matches('\r')
            .to_owned();
    if !data.starts_with(b"HTTP/1.1 200 ") && !data.starts_with(b"HTTP/1.0 200 ") {
        return json!({"ready": false, "status_line": status_line});
    }
    let body = data
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| &data[index + 4..])
        .unwrap_or_default();
    let Ok(body) = serde_json::from_slice::<Value>(body) else {
        return json!({"ready": false, "status_line": status_line, "error": "health response is not JSON"});
    };
    let ready = body.get("status").and_then(Value::as_str) == Some("ready");
    json!({"ready": ready, "status_line": status_line, "body": body})
}

async fn status(children: &[ManagedService], shutdown_mode: ShutdownMode) -> Value {
    let mut services = Vec::new();
    for service in children {
        let health = if service.child.as_ref().is_some_and(|child| child.id() > 0) {
            match &service.config.health_url {
                Some(url) => Some(health_status(url).await),
                None => None,
            }
        } else {
            None
        };
        services.push(json!({
            "id": service.config.id,
            "role": service.config.role,
            "pid": service.child.as_ref().map(Child::id),
            "restarts": service.restarts,
            "failed": service.failed,
            "rollout_task": service.rollout.as_ref().filter(|state| !state.selected).map(|state| &state.task),
            "retiring_pid": service.rollout.as_ref().and_then(|state| state.retiring.as_ref()).and_then(|old| old.child.as_ref().map(Child::id)),
            "health": health,
            "logging": service
                .capture
                .as_ref()
                .map(crate::logging::Capture::status)
                .or_else(|| service.finished_log.clone()),
        }));
    }
    json!({"ok": true, "shutdown_mode": shutdown_mode, "services": services})
}

fn stopped_status(children: &[ManagedService]) -> Value {
    let services = children
        .iter()
        .map(|service| {
            json!({
                "id": service.config.id,
                "role": service.config.role,
                "pid": service.child.as_ref().map(Child::id),
                "restarts": service.restarts,
                "failed": service.failed,
                "logging": service
                    .capture
                    .as_ref()
                    .map(crate::logging::Capture::status)
                    .or_else(|| service.finished_log.clone()),
            })
        })
        .collect::<Vec<_>>();
    json!({"ok": true, "services": services})
}

fn rollout_command(
    children: &mut [ManagedService],
    service_id: String,
    task_id: String,
    operation: Operation,
    logs: &Path,
    policy: &crate::logging::Policy,
    deadline: Instant,
) -> Result<Reply> {
    if Instant::now() >= deadline {
        return Err("actor operation deadline exceeded".into());
    }
    if matches!(&operation, Operation::RuntimeInspect) {
        let index = children
            .iter()
            .position(|s| s.config.id == service_id)
            .ok_or("managed component missing")?;
        let service = &mut children[index];
        let task_observation = !task_id.is_empty()
            && service
                .rollout
                .as_ref()
                .is_some_and(|state| state.task == task_id);
        if !task_observation && service.fenced() {
            return Err("runtime generation unresolved".into());
        }
        let child = service.child.as_mut().ok_or("owned Child missing")?;
        if child.try_wait()?.is_some() {
            return Err("owned Child exited".into());
        }
        let pid = child.id();
        let executable = File::open(format!("/proc/{pid}/exe"))?;
        if child.try_wait()?.is_some() {
            return Err("Child exited while pinning executable".into());
        }
        let relay = if service.config.role == Role::Relay {
            Some(
                service
                    .config
                    .env
                    .get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET")
                    .map(PathBuf::from)
                    .or_else(|| {
                        service
                            .config
                            .env
                            .get("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
                            .map(|d| Path::new(d).join("route.sock"))
                    })
                    .ok_or("Relay socket missing")?,
            )
        } else {
            None
        };
        let owner = if relay.is_some() {
            service
                .config
                .env
                .get("ADX_SUPERVISOR_OWNER_SERVICE")
                .cloned()
                .ok_or("Relay owner missing")?
        } else {
            service_id.clone()
        };
        if relay.is_some() {
            let mut pairs = children.iter().filter(|service| {
                service.config.role == Role::Relay
                    && service.config.env.get("ADX_SUPERVISOR_OWNER_SERVICE") == Some(&owner)
            });
            if pairs.next().is_none() || pairs.next().is_some() {
                return Err("unique managed Relay pair required".into());
            }
        }
        let runtime_config = children
            .iter()
            .find(|service| service.config.id == service_id)
            .ok_or("component missing")?
            .config
            .clone();
        let owner = children
            .iter_mut()
            .find(|s| s.config.id == owner && s.config.role == Role::Adxlet)
            .ok_or("Node owner missing")?;
        if !task_observation && owner.fenced() {
            return Err("Node owner generation unresolved".into());
        }
        let child = owner.child.as_mut().ok_or("Node Child missing")?;
        if child.try_wait()?.is_some() {
            return Err("Node Child exited".into());
        }
        return Ok(Reply::Runtime {
            config: Box::new(runtime_config),
            pid,
            executable,
            admin_pid: child.id(),
            admin_socket: owner
                .config
                .admin_socket
                .clone()
                .ok_or("Node admin missing")?,
            relay,
        });
    }

    let index = children
        .iter()
        .position(|service| {
            service.config.id == service_id
                && matches!(service.config.role, Role::Relay | Role::Adxlet)
        })
        .ok_or("managed process not found")?;
    let service = &mut children[index];
    if matches!(operation, Operation::Release) {
        if service.config.role != Role::Relay {
            return Err("release requires managed Relay".into());
        }
        // A later task can cancel before Fence; preserve the selected task's
        // receipt while releasing only an actual unresolved reservation.
        if let Some(state) = service.rollout.as_ref().filter(|state| !state.selected) {
            if state.task != task_id || state.retiring.is_some() || state.spawn.is_some() {
                return Err("another or irreversible generation retained".into());
            }
            service.rollout = None;
        }
        return Ok(Reply::State {
            exited: false,
            success: false,
        });
    }
    if service
        .rollout
        .as_ref()
        .is_some_and(|state| !state.selected && state.task != task_id)
    {
        return Err("another task owns process generation".into());
    }
    // Receipt is an actor-owned fact; querying it never depends on the paired
    // process or transport being alive and never closes a pending attempt.
    if let Operation::Receipt {
        owner,
        old_pid,
        attempt,
        pid,
    } = &operation
    {
        let state = service.rollout.as_ref().ok_or("attempt outcome unknown")?;
        if state.task != task_id || state.owner != *owner || state.old_pid != *old_pid {
            return Err("attempt observation identity conflict".into());
        }
        if state.committed(attempt, *pid) {
            return Ok(Reply::Committed);
        }
        let spawn = state.spawn.as_ref().ok_or("attempt outcome unknown")?;
        if spawn.attempt != *attempt || pid.is_some_and(|pid| spawn.owned_pid != Some(pid)) {
            return Err("attempt observation conflict".into());
        }
        return match spawn.owned_pid {
            None => Ok(Reply::NeverCreated),
            Some(pid) if service.generation_pid == Some(pid) => Ok(Reply::Candidate {
                pid,
                exited: service.child.is_none() && service.exit_status.is_some(),
            }),
            Some(_) => Err("candidate generation changed".into()),
        };
    }
    // Commit updates the owner's configuration for future spawns. Its current
    // Child may already have exited; local generation facts never depend on it.
    let owner_index = if matches!(operation, Operation::Commit { .. })
        && children[index].config.role == Role::Relay
    {
        let original = children[index]
            .rollout
            .as_ref()
            .and_then(|state| state.retiring.as_deref())
            .unwrap_or(&children[index]);
        let owner_id = original
            .config
            .env
            .get("ADX_SUPERVISOR_OWNER_SERVICE")
            .ok_or("Relay owner missing")?;
        let owner_index = children
            .iter()
            .position(|service| {
                service.config.role == Role::Adxlet && service.config.id == *owner_id
            })
            .ok_or("paired managed adxlet unavailable")?;
        Some(owner_index)
    } else {
        None
    };
    let service = &mut children[index];
    match operation {
        Operation::Fence { pid, owner } => {
            if !matches!(
                (&owner, service.config.role),
                (Owner::Relay, Role::Relay) | (Owner::Adxlet(_), Role::Adxlet)
            ) {
                return Err("rollout owner role conflict".into());
            }
            if let Some(state) = service.rollout.as_ref().filter(|state| !state.selected) {
                if state.owner != owner || state.old_pid != pid || state.retiring.is_some() {
                    return Err("old process generation changed".into());
                }
            } else {
                let child = service
                    .child
                    .as_mut()
                    .ok_or("old Child absent before fence")?;
                if service.generation_pid != Some(pid)
                    || child.id() != pid
                    || child.try_wait()?.is_some()
                {
                    return Err("old Child not live before fence".into());
                }
                service.rollout = Some(RolloutState::reserved(task_id, owner, pid));
            }
            Ok(Reply::State {
                exited: false,
                success: false,
            })
        }
        Operation::Observe { pid, owner } => {
            let state = service
                .rollout
                .as_ref()
                .ok_or("unfenced process observation")?;
            if state.task != task_id
                || state.owner != owner
                || state.old_pid != pid
                || state.selected
            {
                return Err("old observation identity conflict".into());
            }
            let old = state.retiring.as_deref().unwrap_or(service);
            if old.generation_pid != Some(pid) {
                return Err("old Child identity mismatch".into());
            }
            Ok(Reply::State {
                exited: old.child.is_none() && old.exit_status.is_some(),
                success: old.exit_status.is_some_and(|status| status.success()),
            })
        }
        Operation::Start {
            pid,
            config,
            binary,
            launch,
            attempt,
            owner,
        } => {
            if !binary.is_absolute() || attempt.is_empty() {
                return Err("absolute candidate path and attempt required".into());
            }
            let mut config = *config;
            if config.id != service.config.id || config.role != service.config.role {
                return Err("candidate service ownership conflict".into());
            }
            config.binary = binary;
            let executable = match (launch, service.config.role) {
                (Launch::Relay { directory }, Role::Relay) if directory.is_absolute() => {
                    config.env.insert(
                        "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET".into(),
                        directory.join("route.sock").to_string_lossy().into_owned(),
                    );
                    None
                }
                (
                    Launch::Adxlet {
                        bootstrap,
                        executable,
                    },
                    Role::Adxlet,
                ) if bootstrap.is_absolute() => {
                    config.env.insert(
                        "ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET".into(),
                        bootstrap.to_string_lossy().into_owned(),
                    );
                    Some(executable)
                }
                _ => return Err("candidate launch role or path conflict".into()),
            };
            let state = service.rollout.as_ref().ok_or("generation not reserved")?;
            if state.task != task_id
                || state.owner != owner
                || state.old_pid != pid
                || state.selected
            {
                return Err("unreserved process identity".into());
            }
            if let Some(spawn) = state.spawn.as_ref() {
                if spawn.closed.contains(&attempt) {
                    return Err("closed attempt cannot be restarted".into());
                }
                if spawn.attempt == attempt {
                    if serde_json::to_value(&spawn.config)? != serde_json::to_value(&config)? {
                        return Err("spawn receipt configuration conflict".into());
                    }
                    return match spawn.owned_pid {
                        None => Ok(Reply::NeverCreated),
                        Some(_) => Ok(Reply::Candidate {
                            pid: spawn.replay(&attempt, &config, service.generation_pid)?,
                            exited: service.child.is_none() && service.exit_status.is_some(),
                        }),
                    };
                }
                if !spawn.closed.contains(&spawn.attempt)
                    || (spawn.owned_pid.is_some()
                        && (service.child.is_some() || service.exit_status.is_none()))
                {
                    return Err("previous attempt not closed and reaped".into());
                }
            }
            let old = state.retiring.as_deref().unwrap_or(service);
            if old.generation_pid != Some(pid) {
                return Err("old process generation changed".into());
            }
            if service.config.role == Role::Adxlet
                && (old.child.is_some() || old.exit_status.is_none())
            {
                return Err("old adxlet not actually reaped".into());
            }
            if state.retiring.is_some()
                && (config.id != old.config.id
                    || config.role != old.config.role
                    || config.configuration_sha256 != old.config.configuration_sha256
                    || config.sources != old.config.sources
                    || config.admin_socket != old.config.admin_socket
                    || config.health_url != old.config.health_url)
            {
                return Err("original process snapshot ownership conflict".into());
            }
            if state.retiring.is_some() {
                let mut original_env = old.config.env.clone();
                let mut candidate_env = config.env.clone();
                let endpoint = if config.role == Role::Adxlet {
                    "ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET"
                } else {
                    "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET"
                };
                original_env.remove(endpoint);
                candidate_env.remove(endpoint);
                if original_env != candidate_env {
                    return Err("original process environment ownership conflict".into());
                }
            }
            let closed = state
                .spawn
                .as_ref()
                .map_or_else(Vec::new, |spawn| spawn.closed.clone());
            let receipt = SpawnReceipt {
                attempt,
                config: config.clone(),
                owned_pid: None,
                closed,
            };
            if state.retiring.is_none() {
                // Move ownership once: the retained original has no nested rollout
                // state, while the candidate slot owns this task's receipt.
                let mut state = service.rollout.take().ok_or("rollout ownership absent")?;
                state.spawn = Some(receipt);
                let mut candidate = ManagedService::new(config);
                candidate.restarts = service.restarts;
                candidate.rollout = Some(state);
                let old = std::mem::replace(service, candidate);
                service
                    .rollout
                    .as_mut()
                    .ok_or("rollout ownership absent")?
                    .retiring = Some(Box::new(old));
            } else {
                service.config = config;
                service
                    .rollout
                    .as_mut()
                    .ok_or("rollout ownership absent")?
                    .spawn = Some(receipt);
            }
            let created = service.start_from(logs, policy, executable.as_ref());
            let state = service.rollout.as_mut().ok_or("rollout ownership absent")?;
            let spawn = state.spawn.as_mut().ok_or("spawn receipt absent")?;
            spawn.owned_pid = service.child.as_ref().map(Child::id);
            if let Some(pid) = spawn.owned_pid {
                // Never report NeverCreated after a Child has been recorded,
                // including if a future post-spawn step gains a failure path.
                created?;
                Ok(Reply::Candidate { pid, exited: false })
            } else if created.is_err() {
                Ok(Reply::NeverCreated)
            } else {
                Err("spawn outcome unknown".into())
            }
        }
        Operation::StopFailed {
            owner,
            old_pid,
            pid,
            attempt,
        } => {
            let state = service
                .rollout
                .as_ref()
                .ok_or("candidate task stop conflict")?;
            if state.task != task_id || state.owner != owner || state.old_pid != old_pid {
                return Err("candidate generation stop conflict".into());
            }
            if state.committed(&attempt, pid) {
                return Ok(Reply::Committed);
            }
            if state.selected {
                return Err("selected candidate cannot be stopped".into());
            }
            let old = state.retiring.as_deref().unwrap_or(service);
            if service.config.role == Role::Adxlet
                && (old.generation_pid != Some(old_pid)
                    || old.child.is_some()
                    || old.exit_status.is_none())
            {
                return Err("old generation not reaped".into());
            }
            if let Some(spawn) = state.spawn.as_ref() {
                if spawn.attempt != attempt || spawn.owned_pid != pid {
                    return Err("candidate receipt stop conflict".into());
                }
            } else if pid.is_some() || state.retiring.is_some() {
                return Err("candidate creation outcome unknown".into());
            }
            let config = service.config.clone();
            let state = service.rollout.as_mut().ok_or("rollout ownership absent")?;
            let spawn = state.spawn.get_or_insert_with(|| SpawnReceipt {
                attempt: attempt.clone(),
                config,
                owned_pid: None,
                closed: Vec::new(),
            });
            if !spawn.closed.contains(&attempt) {
                spawn.closed.push(attempt);
            }
            let Some(pid) = pid else {
                return Ok(Reply::NeverCreated);
            };
            service.stop_failed(pid, deadline)?;
            Ok(Reply::Candidate {
                pid,
                exited: service.child.is_none() && service.exit_status.is_some(),
            })
        }
        Operation::Commit {
            owner,
            old_pid,
            pid,
            attempt,
            stage,
        } => {
            let state = service
                .rollout
                .as_ref()
                .ok_or("candidate ownership absent")?;
            if state.task != task_id || state.owner != owner || state.old_pid != old_pid {
                return Err("candidate commit identity conflict".into());
            }
            if !state.committed(&attempt, Some(pid))
                && (state.selected
                    || state.spawn.as_ref().is_none_or(|spawn| {
                        spawn.attempt != attempt
                            || spawn.owned_pid != Some(pid)
                            || spawn.closed.contains(&attempt)
                    })
                    || state.retiring.as_ref().is_none_or(|old| {
                        old.generation_pid != Some(old_pid)
                            || old.child.is_some()
                            || old.exit_status.is_none()
                    }))
            {
                return Err("candidate ownership changed or old Child not reaped".into());
            }
            let child = service.child.as_mut().ok_or("candidate Child absent")?;
            if service.generation_pid != Some(pid)
                || child.id() != pid
                || child.try_wait()?.is_some()
            {
                return Err("candidate no longer live".into());
            }
            let endpoint = if owner_index.is_some() {
                Some(
                    service
                        .config
                        .env
                        .get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET")
                        .cloned()
                        .ok_or("candidate endpoint missing")?,
                )
            } else {
                None
            };
            if stage == CommitStage::Select {
                service
                    .config
                    .env
                    .remove("ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET");
                let state = service.rollout.as_mut().ok_or("rollout ownership absent")?;
                state.retiring = None;
                state.stop = None;
                state.selected = true;
            } else if owner_index.is_none() {
                return Err("confirmation requires managed Relay".into());
            }
            if let (Some(index), Some(endpoint)) = (owner_index, endpoint) {
                children[index]
                    .config
                    .env
                    .insert("ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into(), endpoint);
            }
            Ok(Reply::Candidate { pid, exited: false })
        }
        Operation::RuntimeInspect | Operation::Receipt { .. } | Operation::Release => {
            Err("actor operation dispatch error".into())
        }
    }
}

fn preflight_executable(process: &mut Process) -> Result<()> {
    if !process.binary.is_absolute() {
        return Err("absolute executable path required".into());
    }
    process.binary = fs::canonicalize(&process.binary)?;
    let metadata = fs::metadata(&process.binary)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err("package executable missing or not executable".into());
    }
    Ok(())
}

fn install_external(specs: &mut [Process], group: &mut [Process]) -> Result<()> {
    for replacement in group.iter_mut() {
        let original = specs
            .iter_mut()
            .find(|process| process.id == replacement.id && process.role == replacement.role)
            .ok_or("external group differs from rendered service set")?;
        for (key, value) in &original.env {
            if key.starts_with("ADX_SUPERVISOR_") {
                replacement.env.insert(key.clone(), value.clone());
            }
        }
        *original = replacement.clone();
    }
    if specs
        .iter()
        .filter(|process| matches!(process.role, Role::Adxlet | Role::Relay))
        .count()
        != group.len()
    {
        return Err("external group is not complete".into());
    }
    Ok(())
}

pub async fn run(mut deployment: Deployment) -> Result<()> {
    deployment.validate()?;
    let guard = acquire_lock(&deployment.state_dir)?;
    // Resolve current once. Restarts of every unchanged component must continue
    // using this installation even if the operator later flips current.
    deployment.package_dir = fs::canonicalize(&deployment.package_dir)?;
    let mut rollout = crate::rollout::Tasks::new(&deployment)?;
    let (rollout_sender, mut rollout_commands) = tokio::sync::mpsc::channel(8);
    rollout.set_executor(rollout_sender);
    let rollout = std::sync::Arc::new(rollout);
    let rollout_io = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let config_directory = deployment
        .state_dir
        .join(format!("config-{}", rollout.boot_nonce()));
    let mut process_specs = deployment.render(&config_directory)?;
    // Preserve generated parent control wiring when installing external processes.
    for process in &mut process_specs {
        if process.role == Role::Relay {
            let socket = process
                .env
                .get("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
                .map(|dir| Path::new(dir).join("route.sock"));
            let mut owners = deployment.services.iter().filter(|owner| {
                owner.role == Role::Adxlet
                    && deployment.rollout_mode(owner) == crate::rollout::Mode::Standalone
                    && owner
                        .config
                        .get("node_id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| !id.is_empty())
                    && owner
                        .config
                        .get("proxy_socket")
                        .and_then(Value::as_str)
                        .map(PathBuf::from)
                        == socket
            });
            if let Some(owner) = owners.next() {
                if owners.next().is_none() {
                    process
                        .env
                        .insert("ADX_SUPERVISOR_OWNER_SERVICE".into(), owner.id.clone());
                }
            }
        }
        if process.role == Role::Adxlet {
            if let Some(socket) = deployment
                .services
                .iter()
                .find(|service| service.id == process.id)
                .and_then(|service| service.config.get("proxy_socket"))
                .and_then(Value::as_str)
            {
                process
                    .env
                    .insert("ADX_SUPERVISOR_RELAY_SOCKET".into(), socket.to_owned());
                process
                    .env
                    .entry("ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into())
                    .or_insert_with(|| socket.to_owned());
            }
        }
    }
    if let Some(path) = deployment.external_declaration.clone() {
        let snapshot = deployment.clone();
        let directory = deployment
            .state_dir
            .join(format!("external-{}", rollout.boot_nonce()));
        let stopped = rollout.stop_flag();
        let runtime = tokio::runtime::Handle::current();
        let mut prepared = tokio::task::spawn_blocking(move || {
            crate::external::prepare(&path, &snapshot, &directory, &stopped, &runtime)
        })
        .await
        .map_err(|_| "external preparation worker failed")??;
        install_external(&mut process_specs, &mut prepared)?;
    }
    for process in &mut process_specs {
        preflight_executable(process)?;
    }
    for service in &deployment.services {
        if service.role == crate::config::Role::Redis {
            let config = crate::config::RedisConfig::parse(&service.config)?;
            fs::create_dir_all(config.data_dir)?;
        }
    }
    let logs = deployment.state_dir.join("logs");
    fs::create_dir_all(&logs)?;
    let listener = UnixListener::bind(&guard.socket)?;
    fs::set_permissions(&guard.socket, fs::Permissions::from_mode(0o600))?;
    let mut children = Vec::new();
    for config in process_specs {
        let mut service = ManagedService::new(config);
        if let Err(error) = service.start(&logs, &deployment.logging) {
            service.failed = true;
            eprintln!("service {} spawn failed: {error}", service.config.id);
            service.next_start =
                Instant::now() + Duration::from_millis(deployment.restart_delay_ms);
        }
        children.push(service);
    }
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
            Some(command) = rollout_commands.recv() => {
                if command.reply.is_closed() || Instant::now() >= command.expires || rollout.is_stopped() {
                    let _ = command.reply.send(Err("stale or cancelled actor command".into()));
                    continue;
                }
                if command.expected_pid.is_some_and(|pid| !children.iter().any(|service|
                    service.config.id==command.service_id && service.child.as_ref().is_some_and(|child| child.id()==pid))) {
                    let _=command.reply.send(Err("selected owned Child changed".into()));
                    continue;
                }
                let result = rollout_command(&mut children, command.service_id, command.task_id,
                    command.operation, &logs, &deployment.logging, command.expires);
                let _ = command.reply.send(result);
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let mut reader = BufReader::new(stream.take(crate::rollout::MAX_REQUEST_BYTES + 1));
                let mut request_line = String::new();
                let read_result = tokio::time::timeout(
                    Duration::from_secs(2),
                    reader.read_line(&mut request_line),
                )
                .await;
                match read_result {
                    Ok(Ok(0)) | Err(_) => continue,
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) => continue,
                }

                let bounded = request_line.len() <= crate::rollout::MAX_REQUEST_BYTES as usize;
                let request = bounded.then(|| Request::parse(request_line.trim())).flatten();
                let response = match request {
                    Some(Request::Status) => status(&children, deployment.shutdown_mode).await,
                    Some(Request::Stop { mode }) => {
                        rollout.cancel();
                        let mode = mode.unwrap_or(deployment.shutdown_mode);
                        match stop(
                            &mut children,
                            Duration::from_secs(deployment.stop_timeout_seconds),
                            mode,
                        )
                        .await
                        {
                            Ok(()) => {
                                let service_status = stopped_status(&children)
                                    .get("services")
                                    .cloned()
                                    .unwrap_or_else(|| json!([]));
                                json!({
                                    "ok": true,
                                    "stopped": true,
                                    "shutdown_mode": mode,
                                    "services": service_status,
                                })
                            }
                            Err(error) => {
                                eprintln!("stop incomplete: {error}");
                                json!({"ok": false, "shutdown_mode": mode, "error": error})
                            }
                        }
                    }
                    None if bounded => {
                        let mut stream = reader.into_inner().into_inner();
                        match rollout_io.clone().try_acquire_owned() {
                            Ok(permit) => {
                                let tasks = rollout.clone();
                                // Dedicated bounded background IO never occupies the Child actor
                                // or Tokio's shutdown-waited blocking pool.
                                let _ = std::thread::Builder::new().name("rollout-state".into()).spawn(move || {
                                    let _permit = permit;
                                    let response = tasks.response(request_line.trim());
                                    if let Ok(mut stream) = stream.into_std() {
                                        use std::io::Write;
                                        let _ = stream.set_nonblocking(false);
                                        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                                        if let Ok(bytes) = serde_json::to_vec(&response) { let _ = stream.write_all(&bytes); }
                                    }
                                });
                            }
                            Err(_) => { let _ = stream.write_all(b"{\"ok\":false,\"error\":{\"code\":\"busy\"}}").await; }
                        }
                        continue;
                    },
                    None => json!({"ok": false, "error": {"code": "invalid_request"}}),
                };
                let should_exit = matches!(request, Some(Request::Stop { .. }))
                    && response.get("ok") == Some(&Value::Bool(true));

                let mut stream = reader.into_inner().into_inner();
                let bytes = serde_json::to_vec(&response)?;
                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                    stream.write_all(&bytes).await?;
                    stream.shutdown().await
                }).await;
                if should_exit {
                    return Ok(());
                }
            }
            _ = term.recv() => {
                rollout.cancel();
                match stop(
                    &mut children,
                    Duration::from_secs(deployment.stop_timeout_seconds),
                    deployment.shutdown_mode,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) => eprintln!("stop incomplete; dependencies remain running: {error}"),
                }
            }
            _ = interrupt.recv() => {
                rollout.cancel();
                match stop(
                    &mut children,
                    Duration::from_secs(deployment.stop_timeout_seconds),
                    deployment.shutdown_mode,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(error) => eprintln!("stop incomplete; dependencies remain running: {error}"),
                }
            }
            _ = tick.tick() => {
                for service in &mut children {
                    if let Some((pid, terminate_at, deadline)) = service.rollout.as_ref().and_then(|state| state.stop) {
                        if service.generation_pid == Some(pid) && service.child.is_some()
                            && Instant::now() >= terminate_at && Instant::now() < deadline {
                            service.signal_target = SignalTarget::Process;
                            if let Err(error) = service.signal(libc::SIGKILL) {
                                eprintln!("failed candidate {} escalation failed: {error}", service.config.id);
                            }
                            // No refreshed grace and no repeated escalation.
                            if let Some(state) = service.rollout.as_mut() { state.stop = Some((pid, deadline, deadline)); }
                        }
                    }
                    if let Some(old) = service.rollout.as_mut().and_then(|state| state.retiring.as_mut()) {
                        if let Some(child) = old.child.as_mut() {
                            if let Some(status) = child.try_wait()? {
                                old.exit_status = Some(status);
                                old.child = None;
                                old.finish_logs().await?;
                            }
                        }
                    }
                    if let Some(child) = &mut service.child {
                        if service.route_inode.is_none() {
                            if let Some(path) = service.config.env.get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET") {
                                if let Ok(Ok(socket)) = tokio::time::timeout(Duration::from_millis(50), UnixStream::connect(path)).await {
                                    if socket.peer_cred().ok().and_then(|cred| cred.pid()).and_then(|pid| u32::try_from(pid).ok()) == Some(child.id()) {
                                        use std::os::unix::fs::MetadataExt;
                                        if let Ok(meta) = fs::symlink_metadata(path) { service.route_inode = Some((meta.dev(), meta.ino())); }
                                    }
                                }
                            }
                        }
                        if let Some(status) = child.try_wait()? {
                            service.exit_status = Some(status);
                            if let (Some(path), Some(inode)) = (service.config.env.get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET"), service.route_inode.take()) {
                                use std::os::unix::fs::MetadataExt;
                                if fs::symlink_metadata(path).is_ok_and(|meta| (meta.dev(), meta.ino()) == inode && meta.file_type().is_socket()) {
                                    let _ = fs::remove_file(path);
                                }
                            }
                            let failed_afs_exit = service.is_afs() && !status.success();
                            service.child = None;
                            service.finish_logs().await?;
                            if failed_afs_exit {
                                service.failed = true;
                            } else {
                                service.failed = !service.is_afs();
                                service.next_start = Instant::now()
                                    + Duration::from_millis(deployment.restart_delay_ms);
                            }
                        }
                    }
                    // AFS failures retain their state for drain/recovery; ordinary ADX
                    // services keep the shared supervisor's continuous retry policy.
                    if service.child.is_none()
                        && !service.fenced()
                        && (!service.is_afs() || !service.failed)
                        && Instant::now() >= service.next_start
                    {
                        service.restarts = service.restarts.saturating_add(1);
                        if let Err(error) = service.start(&logs, &deployment.logging) {
                            service.failed = true;
                            eprintln!("service {} spawn failed: {error}", service.config.id);
                            service.next_start = Instant::now()
                                + Duration::from_millis(deployment.restart_delay_ms);
                        }
                    }
                }

            }
        }
    }
}

#[cfg(test)]
mod rollout_tests {
    fn rollout(service: &mut ManagedService) -> &mut RolloutState {
        let old_pid = service.generation_pid.unwrap_or(42);
        service
            .rollout
            .get_or_insert_with(|| RolloutState::reserved("task".into(), Owner::Relay, old_pid))
    }

    use super::*;

    fn failed_status(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        std::process::ExitStatus::from_raw(code << 8)
    }

    fn process(
        id: &str,
        role: Role,
        binary: &str,
        args: Vec<String>,
        admin_socket: Option<PathBuf>,
    ) -> Process {
        Process {
            configuration_sha256: "e".repeat(64),
            sources: Default::default(),
            id: id.into(),
            role,
            binary: binary.into(),
            args,
            env: Default::default(),
            admin_socket,
            health_url: None,
        }
    }

    struct ActorHarness<'a> {
        root: &'a Path,
        service_id: &'a str,
        policy: crate::logging::Policy,
        deadline: Instant,
    }

    impl<'a> ActorHarness<'a> {
        fn new(root: &'a Path, service_id: &'a str, seconds: u64) -> Self {
            Self {
                root,
                service_id,
                policy: crate::logging::Policy::default(),
                deadline: Instant::now() + Duration::from_secs(seconds),
            }
        }

        fn command(
            &self,
            services: &mut [ManagedService],
            task: &str,
            operation: Operation,
        ) -> Result<Reply> {
            rollout_command(
                services,
                self.service_id.into(),
                task.into(),
                operation,
                self.root,
                &self.policy,
                self.deadline,
            )
        }
    }

    fn adxlet_generation(
        root: &Path,
        pid: u32,
        args: Vec<String>,
        identity: &crate::rollout::ProcessIdentity,
    ) -> ManagedService {
        let mut service = ManagedService::new(process(
            "node",
            Role::Adxlet,
            "/bin/sleep",
            args,
            Some(root.join("admin.sock")),
        ));
        service.signal_target = SignalTarget::Process;
        service.exit_status = Some(failed_status(1));
        service.generation_pid = Some(pid);
        service.rollout = Some(RolloutState::reserved(
            "task".into(),
            Owner::Adxlet(identity.clone()),
            42,
        ));
        service
    }

    #[test]
    fn spawn_replay_requires_exact_attempt_and_owned_generation() {
        let config = process(
            "node",
            Role::Adxlet,
            "/original",
            vec!["--config".into(), "/rendered/original.json".into()],
            None,
        );
        let mut spawn = SpawnReceipt {
            attempt: "attempt".into(),
            config: config.clone(),
            owned_pid: None,
            closed: Vec::new(),
        };
        assert!(spawn.replay("attempt", &config, Some(77)).is_err());
        spawn.owned_pid = Some(77);
        assert_eq!(spawn.replay("attempt", &config, Some(77)).unwrap(), 77);
        let mut changed = config.clone();
        changed.args.push("changed-rendered-config".into());
        assert!(spawn.replay("attempt", &changed, Some(77)).is_err());
        assert!(spawn.replay("attempt", &config, Some(88)).is_err());
        assert!(spawn.replay("other", &config, Some(77)).is_err());
        assert_eq!(spawn.config.args[1], "/rendered/original.json");
    }

    #[test]
    fn candidate_actor_reuses_original_rendered_config_and_replays_same_child() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "node", 10);
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                old_relay_session: "relay".into(),
                original_sha256: "a".repeat(64),
                ..Default::default()
            },
        };
        let make = |pid, args| adxlet_generation(root.path(), pid, args, &identity);
        let mut original = make(42, vec!["60".into()]);
        original.config.sources.insert(
            Role::Adxlet,
            crate::config::ProcessSource {
                version: Some("a".repeat(40)),
                archive_sha256: Some("b".repeat(64)),
                raw_config_sha256: Some("c".repeat(64)),
            },
        );
        original.config.env.insert(
            "ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into(),
            "/original/route.sock".into(),
        );
        original
            .config
            .env
            .insert("RENDERED_CONFIG".into(), "/original/config.json".into());
        let mut failed = make(77, vec!["invalid-target-args".into()]);
        failed
            .config
            .env
            .insert("RENDERED_CONFIG".into(), "/target/config.json".into());
        rollout(&mut failed).retiring = Some(Box::new(original));
        let binary = root.path().join("rollback");
        let bootstrap = root.path().join("first-listener.sock");
        let mut target_config = failed
            .rollout
            .as_ref()
            .unwrap()
            .retiring
            .as_ref()
            .unwrap()
            .config
            .clone();
        target_config.args = vec!["45".into()];
        let operation = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: binary.clone(),
            attempt: "attempt".into(),
            owner: Owner::Adxlet(identity.clone()),
            launch: Launch::Adxlet {
                executable: File::open("/bin/sleep").unwrap(),
                bootstrap: bootstrap.clone(),
            },
        };
        for changed in ["id", "role", "digest"] {
            let mut wrong = target_config.clone();
            match changed {
                "id" => wrong.id = "another-service".into(),
                "role" => wrong.role = Role::Relay,
                _ => wrong.configuration_sha256 = "wrong-digest".into(),
            }
            assert!(
                actor
                    .command(
                        std::slice::from_mut(&mut failed),
                        "task",
                        Operation::Start {
                            config: Box::new(wrong),
                            pid: 42,
                            binary: binary.clone(),
                            attempt: "wrong".into(),
                            owner: Owner::Adxlet(identity.clone()),
                            launch: Launch::Adxlet {
                                executable: File::open("/bin/sleep").unwrap(),
                                bootstrap: bootstrap.clone()
                            }
                        }
                    )
                    .is_err(),
                "original snapshot ownership: {changed}"
            );
        }
        let first = actor
            .command(std::slice::from_mut(&mut failed), "task", operation())
            .unwrap();
        let Reply::Candidate { pid, exited: false } = first else {
            panic!("fresh owned rollback expected");
        };
        assert_eq!(failed.config.args, vec!["45"]);
        assert_eq!(failed.config.sources, target_config.sources);
        assert_eq!(
            failed.config.env["RENDERED_CONFIG"],
            "/original/config.json"
        );
        assert_eq!(
            failed.config.env["ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET"],
            "/original/route.sock"
        );
        assert_eq!(
            failed.config.env["ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET"],
            bootstrap.to_string_lossy()
        );
        assert!(
            matches!(actor.command(std::slice::from_mut(&mut failed), "task", operation()).unwrap(), Reply::Candidate { pid:replay, exited:false } if replay == pid)
        );
        assert_eq!(
            failed
                .rollout
                .as_ref()
                .unwrap()
                .spawn
                .as_ref()
                .unwrap()
                .attempt,
            "attempt"
        );
        failed.child.as_mut().unwrap().kill().unwrap();
        let status = failed.child.as_mut().unwrap().wait().unwrap();
        failed.exit_status = Some(status);
        failed.child = None;
        assert!(
            matches!(actor.command(std::slice::from_mut(&mut failed), "task", operation()).unwrap(), Reply::Candidate { pid:replay, exited:true } if replay == pid)
        );
        actor
            .command(
                std::slice::from_mut(&mut failed),
                "task",
                Operation::StopFailed {
                    owner: Owner::Adxlet(identity.clone()),
                    old_pid: 42,
                    attempt: "attempt".into(),
                    pid: Some(pid),
                },
            )
            .unwrap();
        // An actor-recorded creation failure is immutable even if the same
        // executable FD becomes usable before the caller retries.
        let invalid = root.path().join("not-executable");
        std::fs::write(&invalid, b"invalid executable").unwrap();
        let never_created = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: invalid.clone(),
            attempt: "failed-attempt".into(),
            owner: Owner::Adxlet(identity.clone()),
            launch: Launch::Adxlet {
                executable: File::open(&invalid).unwrap(),
                bootstrap: bootstrap.clone(),
            },
        };
        assert!(matches!(
            actor
                .command(std::slice::from_mut(&mut failed), "task", never_created())
                .unwrap(),
            Reply::NeverCreated
        ));
        assert!(matches!(
            actor
                .command(std::slice::from_mut(&mut failed), "task", never_created())
                .unwrap(),
            Reply::NeverCreated
        ));
        assert!(actor
            .command(std::slice::from_mut(&mut failed), "task", operation())
            .is_err());
        assert!(matches!(
            actor
                .command(
                    std::slice::from_mut(&mut failed),
                    "task",
                    Operation::Receipt {
                        owner: Owner::Adxlet(identity.clone()),
                        old_pid: 42,
                        attempt: "failed-attempt".into(),
                        pid: None
                    }
                )
                .unwrap(),
            Reply::NeverCreated
        ));
    }

    #[test]
    fn stop_failed_never_created_seals_late_start_without_affecting_next_attempt() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "node", 10);
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection::default(),
        };
        let config = process("node", Role::Adxlet, "/bin/sleep", vec!["60".into()], None);
        let mut service = ManagedService::new(config.clone());
        rollout(&mut service).selected = false;
        rollout(&mut service).task = "task".into();
        rollout(&mut service).owner = Owner::Adxlet(identity.clone());
        service.generation_pid = Some(42);
        service.exit_status = Some(failed_status(1));
        assert!(matches!(
            actor
                .command(
                    std::slice::from_mut(&mut service),
                    "task",
                    Operation::StopFailed {
                        owner: Owner::Adxlet(identity.clone()),
                        old_pid: 42,
                        pid: None,
                        attempt: "A".into()
                    }
                )
                .unwrap(),
            Reply::NeverCreated
        ));
        assert!(service
            .rollout
            .as_ref()
            .unwrap()
            .spawn
            .as_ref()
            .unwrap()
            .closed
            .contains(&"A".into()));
        let start = |attempt: &str| Operation::Start {
            config: Box::new(config.clone()),
            pid: 42,
            binary: root.path().join("original"),
            attempt: attempt.into(),
            owner: Owner::Adxlet(identity.clone()),
            launch: Launch::Adxlet {
                executable: File::open("/bin/sleep").unwrap(),
                bootstrap: root.path().join("bootstrap.sock"),
            },
        };
        assert!(actor
            .command(std::slice::from_mut(&mut service), "task", start("A"))
            .is_err());
        let Reply::Candidate { pid, exited: false } = actor
            .command(std::slice::from_mut(&mut service), "task", start("B"))
            .unwrap()
        else {
            panic!("B should own one fresh Child");
        };
        assert!(actor
            .command(
                std::slice::from_mut(&mut service),
                "task",
                Operation::StopFailed {
                    owner: Owner::Adxlet(identity),
                    old_pid: 42,
                    pid: None,
                    attempt: "A".into()
                }
            )
            .is_err());
        assert_eq!(service.child.as_ref().unwrap().id(), pid);
        assert!(service
            .child
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none());
    }

    #[test]
    fn stop_failed_closes_attempt_so_late_start_of_same_attempt_is_rejected() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "node", 10);
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                old_relay_session: "relay".into(),
                original_sha256: "a".repeat(64),
                ..Default::default()
            },
        };
        let make = |pid, args| adxlet_generation(root.path(), pid, args, &identity);
        let original = make(42, vec!["60".into()]);
        let mut failed = make(77, vec!["invalid-target-args".into()]);
        rollout(&mut failed).retiring = Some(Box::new(original));
        let binary = root.path().join("rollback");
        let bootstrap = root.path().join("first-listener.sock");
        let target_config = failed
            .rollout
            .as_ref()
            .unwrap()
            .retiring
            .as_ref()
            .unwrap()
            .config
            .clone();
        let start = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: binary.clone(),
            attempt: "attempt".into(),
            owner: Owner::Adxlet(identity.clone()),
            launch: Launch::Adxlet {
                executable: File::open("/bin/sleep").unwrap(),
                bootstrap: bootstrap.clone(),
            },
        };
        let Reply::Candidate { pid, exited: false } = actor
            .command(std::slice::from_mut(&mut failed), "task", start())
            .unwrap()
        else {
            panic!("fresh owned attempt expected");
        };
        assert!(matches!(
            actor.command(std::slice::from_mut(&mut failed), "task", Operation::StopFailed { owner: Owner::Adxlet( identity.clone()), old_pid: 42, pid: Some(pid), attempt: "attempt".into() })
            .unwrap(),
            Reply::Candidate { pid: stopped, exited: false } if stopped == pid
        ));
        assert!(
            actor
                .command(
                    std::slice::from_mut(&mut failed),
                    "task",
                    Operation::Commit {
                        pid,
                        old_pid: 42,
                        attempt: "attempt".into(),
                        owner: Owner::Adxlet(identity.clone()),
                        stage: CommitStage::Select
                    }
                )
                .is_err(),
            "a closed A cannot Complete after StopFailed"
        );
        // Reap the stopped owned Child exactly as the supervisor loop would.
        let child = failed.child.as_mut().unwrap();
        let _ = child.kill();
        let status = child.wait().unwrap();
        failed.exit_status = Some(status);
        failed.child = None;
        // No next attempt has begun yet. A late Start for the stopped attempt
        // must be rejected instead of replaying as a successful start.
        let late = actor.command(std::slice::from_mut(&mut failed), "task", start());
        assert!(
            late.is_err(),
            "late Start of an attempt closed by StopFailed must be rejected"
        );
    }

    #[test]
    fn adxlet_observe_requires_exact_fenced_reaped_generation() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "node", 10);
        let mut service = ManagedService::new(process(
            "node",
            Role::Adxlet,
            "/unused",
            vec![],
            Some(root.path().join("admin.sock")),
        ));
        service.signal_target = SignalTarget::Process;
        rollout(&mut service).task = "task".into();
        rollout(&mut service).selected = false;
        service.generation_pid = Some(42);
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                ..Default::default()
            },
        };
        rollout(&mut service).owner = Owner::Adxlet(identity.clone());
        assert_eq!(service.failed_signal(), libc::SIGUSR1);
        service.config.role = Role::Relay;
        assert_eq!(service.failed_signal(), libc::SIGTERM);
        service.config.role = Role::Adxlet;
        let observe = |service: &mut ManagedService, pid| {
            actor.command(
                std::slice::from_mut(service),
                "task",
                Operation::Observe {
                    pid,
                    owner: Owner::Adxlet(identity.clone()),
                },
            )
        };
        assert!(matches!(
            observe(&mut service, 42).unwrap(),
            Reply::State { exited: false, .. }
        ));
        service.exit_status = Some(failed_status(1));
        assert!(matches!(
            observe(&mut service, 42).unwrap(),
            Reply::State {
                exited: true,
                success: false,
                ..
            }
        ));
        assert!(observe(&mut service, 43).is_err());
        rollout(&mut service).selected = true;
        assert!(observe(&mut service, 42).is_err());

        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                ..Default::default()
            },
        };
        rollout(&mut service).selected = false;
        rollout(&mut service).owner = Owner::Adxlet(identity.clone());
        // Missing receipt is unknown, never proof that no candidate was created.
        assert!(actor
            .command(
                std::slice::from_mut(&mut service),
                "task",
                Operation::Receipt {
                    owner: Owner::Adxlet(identity.clone()),
                    old_pid: 42,
                    attempt: "attempt".into(),
                    pid: None
                }
            )
            .is_err());
        let mut wrong = identity.clone();
        wrong.identity.epoch += 1;
        assert!(actor
            .command(
                std::slice::from_mut(&mut service),
                "task",
                Operation::Receipt {
                    owner: Owner::Adxlet(wrong),
                    old_pid: 42,
                    attempt: "attempt".into(),
                    pid: None
                }
            )
            .is_err());
        // A completed task remains observable and replayable after its fence is cleared.
        let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        let pid = child.id();
        service.generation_pid = Some(pid);
        service.child = Some(child);
        rollout(&mut service).selected = true;

        let mut identity = identity;
        identity.identity.old_relay_session = "relay".into();
        rollout(&mut service).owner = Owner::Adxlet(identity.clone());
        rollout(&mut service).spawn = Some(SpawnReceipt {
            attempt: "attempt".into(),
            config: service.config.clone(),
            owned_pid: Some(pid),
            closed: Vec::new(),
        });
        assert!(matches!(
            actor
                .command(
                    std::slice::from_mut(&mut service),
                    "task",
                    Operation::Receipt {
                        owner: Owner::Adxlet(identity.clone()),
                        old_pid: 42,
                        attempt: "attempt".into(),
                        pid: Some(pid)
                    }
                )
                .unwrap(),
            Reply::Committed
        ));
        assert!(matches!(
            actor
                .command(
                    std::slice::from_mut(&mut service),
                    "task",
                    Operation::StopFailed {
                        owner: Owner::Adxlet(identity.clone()),
                        old_pid: 42,
                        pid: Some(pid),
                        attempt: "attempt".into()
                    }
                )
                .unwrap(),
            Reply::Committed
        ));
        assert!(service
            .child
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none());
        assert!(
            matches!(actor.command(std::slice::from_mut(&mut service), "task", Operation::Commit { pid, old_pid:42, attempt:"attempt".into(), owner: Owner::Adxlet(identity.clone()), stage: CommitStage::Select }).unwrap(), Reply::Candidate { pid:actual, exited:false } if actual == pid)
        );
        assert!(actor
            .command(
                std::slice::from_mut(&mut service),
                "task",
                Operation::Commit {
                    pid,
                    old_pid: 43,
                    attempt: "attempt".into(),
                    owner: Owner::Adxlet(identity),
                    stage: CommitStage::Select
                }
            )
            .is_err());
        let child = service.child.as_mut().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    fn relay_pair(root: &Path, policy: &crate::logging::Policy) -> Vec<ManagedService> {
        let mut owner = ManagedService::new(process(
            "node",
            Role::Adxlet,
            "/bin/sleep",
            vec!["60".into()],
            Some(root.join("admin.sock")),
        ));
        owner.start(root, policy).unwrap();
        let mut relay = ManagedService::new(process(
            "relay",
            Role::Relay,
            "/bin/sleep",
            vec!["60".into()],
            None,
        ));
        relay
            .config
            .env
            .insert("ADX_SUPERVISOR_OWNER_SERVICE".into(), "node".into());
        relay.start(root, policy).unwrap();
        relay.rollout = Some(RolloutState::reserved(
            "task".into(),
            Owner::Relay,
            relay.generation_pid.unwrap(),
        ));
        vec![owner, relay]
    }

    #[test]
    fn relay_failed_spawn_is_not_retried_when_executable_appears() {
        use crate::rollout::actor::Operation;
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "relay", 5);
        let mut children = relay_pair(root.path(), &actor.policy);
        let old_pid = children[1].generation_pid.unwrap();
        let config = children[1].config.clone();
        let binary = root.path().join("candidate");
        let apply =
            |children: &mut [ManagedService], operation| actor.command(children, "task", operation);
        let start = || Operation::Start {
            config: Box::new(config.clone()),
            attempt: "A".into(),
            owner: Owner::Relay,
            pid: old_pid,
            binary: binary.clone(),
            launch: Launch::Relay {
                directory: root.path().into(),
            },
        };
        assert!(matches!(
            apply(&mut children, start()).unwrap(),
            Reply::NeverCreated
        ));
        std::fs::copy("/bin/sleep", &binary).unwrap();
        let replay = apply(&mut children, start());
        assert!(
            matches!(replay.unwrap(), Reply::NeverCreated),
            "a failed attempt must not create a Child after replay"
        );
        assert!(children[1].child.is_none());
        assert_eq!(
            children[1]
                .rollout
                .as_ref()
                .unwrap()
                .retiring
                .as_ref()
                .unwrap()
                .generation_pid,
            Some(old_pid)
        );
    }

    #[test]
    fn relay_closed_candidate_rejects_late_start() {
        use crate::rollout::actor::Operation;
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "relay", 5);
        let mut children = relay_pair(root.path(), &actor.policy);
        let old_pid = children[1].generation_pid.unwrap();
        let config = children[1].config.clone();
        let apply =
            |children: &mut [ManagedService], operation| actor.command(children, "task", operation);
        let start = || Operation::Start {
            config: Box::new(config.clone()),
            attempt: "A".into(),
            owner: Owner::Relay,
            pid: old_pid,
            binary: "/bin/sleep".into(),
            launch: Launch::Relay {
                directory: root.path().into(),
            },
        };
        apply(&mut children, start()).unwrap();
        let pid = children[1].generation_pid.unwrap();
        apply(
            &mut children,
            Operation::StopFailed {
                pid: Some(pid),
                old_pid,
                attempt: "A".into(),
                owner: Owner::Relay,
            },
        )
        .unwrap();
        assert!(
            apply(&mut children, start()).is_err(),
            "closing an attempt must reject a delayed Start"
        );
    }

    #[test]
    fn relay_receipt_replays_created_and_reaped_child_after_reply_loss() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "relay", 600);
        let make = |id: &str, role| {
            let mut service =
                ManagedService::new(process(id, role, "/bin/sleep", vec!["60".into()], None));
            service.signal_target = SignalTarget::Process;
            service
        };
        let mut owner = make("node", Role::Adxlet);
        owner.config.admin_socket = Some(root.path().join("admin.sock"));
        owner.config.sources.insert(
            Role::Adxlet,
            crate::config::ProcessSource {
                version: Some("a".repeat(40)),
                archive_sha256: None,
                raw_config_sha256: None,
            },
        );
        owner.child = Some(Command::new("/bin/sleep").arg("60").spawn().unwrap());
        // An embedded Adxlet has no separately managed Relay pair. Observation
        // still pins the actual owned Child, but never permits replacement.
        let observed = ActorHarness::new(root.path(), "node", 5)
            .command(
                std::slice::from_mut(&mut owner),
                "",
                Operation::RuntimeInspect,
            )
            .unwrap();
        assert!(
            matches!(observed, Reply::Runtime { pid, admin_pid, relay: None, .. }
            if pid == owner.child.as_ref().unwrap().id() && admin_pid == pid)
        );
        rollout(&mut owner).selected = false;
        assert!(ActorHarness::new(root.path(), "node", 5)
            .command(
                std::slice::from_mut(&mut owner),
                "",
                Operation::RuntimeInspect
            )
            .is_err());
        rollout(&mut owner).selected = true;
        let mut relay = make("relay", Role::Relay);
        relay
            .config
            .env
            .insert("ADX_SUPERVISOR_OWNER_SERVICE".into(), "node".into());
        let original_config = relay.config.clone();
        relay.config.binary = "/failed-candidate".into();
        relay.generation_pid = Some(42);
        relay.exit_status = Some(failed_status(1));
        let mut original = make("relay", Role::Relay);
        original.config = original_config.clone();
        original.generation_pid = Some(99);
        original.exit_status = Some(failed_status(0));
        relay.rollout = Some(RolloutState {
            task: "task".into(),
            owner: Owner::Relay,
            old_pid: 99,
            retiring: Some(Box::new(original)),
            spawn: Some(SpawnReceipt {
                attempt: "A".into(),
                config: relay.config.clone(),
                owned_pid: Some(42),
                closed: vec!["A".into()],
            }),
            stop: None,
            selected: false,
        });
        let mut children = vec![owner, relay];
        let operation = |directory| Operation::Start {
            pid: 99,
            config: Box::new(original_config.clone()),
            binary: "/bin/sleep".into(),
            launch: Launch::Relay { directory },
            attempt: "B".into(),
            owner: Owner::Relay,
        };
        drop(
            actor
                .command(&mut children, "task", operation(root.path().into()))
                .unwrap(),
        );
        let started_pid = children[1].child.as_ref().unwrap().id();
        assert_ne!(started_pid, 42);
        let receipt = || Operation::Receipt {
            owner: Owner::Relay,
            old_pid: 99,
            attempt: "B".into(),
            pid: None,
        };
        assert!(
            matches!(actor.command(&mut children, "task", receipt()).unwrap(), Reply::Candidate { pid, exited: false } if pid == started_pid)
        );
        assert!(
            matches!(actor.command(&mut children, "task", operation(root.path().into())).unwrap(), Reply::Candidate { pid, exited: false } if pid == started_pid)
        );
        assert!(actor
            .command(&mut children, "task", operation(root.path().join("other")))
            .is_err());
        // Paired-node failure cannot hide a supervisor-owned reap or prevent
        // closing this attempt. Both are local facts, independent of Node RPC.
        let mut owner = children[0].child.take().unwrap();
        owner.kill().unwrap();
        children[0].exit_status = Some(owner.wait().unwrap());
        assert!(matches!(
            actor
                .command(
                    &mut children,
                    "task",
                    Operation::Observe {
                        pid: 99,
                        owner: Owner::Relay
                    }
                )
                .unwrap(),
            Reply::State { exited: true, .. }
        ));
        assert!(actor
            .command(&mut children, "task", Operation::RuntimeInspect)
            .is_err());
        assert!(
            matches!(actor.command(&mut children, "task", Operation::Commit {
            owner: Owner::Relay, old_pid: 99, attempt: "B".into(), pid: started_pid, stage: CommitStage::Confirm,
        }).unwrap(), Reply::Candidate { pid, exited: false } if pid == started_pid)
        );
        assert_eq!(
            children[0].config.env["ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET"],
            root.path().join("route.sock").to_string_lossy()
        );
        assert!(
            children[1].fenced(),
            "Confirm must retain the selection fence"
        );
        // The same attempt remains the same reaped Child, never a respawn.
        let mut child = children[1].child.take().unwrap();
        child.kill().unwrap();
        children[1].exit_status = Some(child.wait().unwrap());
        assert!(
            matches!(actor.command(&mut children, "task", operation(root.path().into())).unwrap(), Reply::Candidate { pid, exited: true } if pid == started_pid)
        );
        assert!(children[1].child.is_none());
        assert!(matches!(
            actor.command(&mut children, "task", Operation::StopFailed { owner: Owner::Relay, old_pid: 99, attempt: "B".into(), pid: Some(started_pid) }).unwrap(),
            Reply::Candidate { pid, exited: true } if pid == started_pid
        ));
        assert!(matches!(
            actor.command(&mut children, "task", receipt()).unwrap(),
            Reply::Candidate { pid, exited: true } if pid == started_pid
        ));
    }

    #[test]
    fn begin_fence_blocks_old_restart_without_transferring_child_to_worker() {
        let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        let pid = child.id();
        let mut service = ManagedService::new(process(
            "relay",
            Role::Relay,
            "/bin/sleep",
            vec!["60".into()],
            None,
        ));
        service.child = Some(child);
        rollout(&mut service).task = "task".into();
        rollout(&mut service).selected = false;
        service.generation_pid = Some(pid);
        // This test did not create a process group; signal only its owned PID.
        service.signal_target = SignalTarget::Process;
        assert_eq!(service.child.as_ref().map(Child::id), Some(pid));
        assert!(service.fenced());
        assert!(service
            .child
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap()
            .is_none());
        // Missing generation endpoints cannot change the actual exit observation.
        assert!(service.exit_status.is_none());
        let deadline = Instant::now() + Duration::from_secs(600);
        assert!(service.stop_failed(pid.wrapping_add(1), deadline).is_err());
        assert!(service.rollout.as_ref().unwrap().stop.is_none());
        service.stop_failed(pid, deadline).unwrap();
        let frozen = service.rollout.as_ref().unwrap().stop;
        service
            .stop_failed(pid, deadline + Duration::from_secs(60))
            .unwrap();
        assert_eq!(service.rollout.as_ref().unwrap().stop, frozen);
    }

    #[test]
    fn release_after_selected_task_preserves_receipt_but_rejects_unresolved_owner() {
        let root = tempfile::tempdir().unwrap();
        let actor = ActorHarness::new(root.path(), "relay", 5);
        let mut children = relay_pair(root.path(), &actor.policy);
        let old_pid = children[1].generation_pid.unwrap();
        let config = children[1].config.clone();
        let apply = |children: &mut [ManagedService], task: &str, operation| {
            actor.command(children, task, operation)
        };
        let Reply::Candidate { pid, .. } = apply(
            &mut children,
            "task",
            Operation::Start {
                config: Box::new(config),
                pid: old_pid,
                binary: "/bin/sleep".into(),
                launch: Launch::Relay {
                    directory: root.path().into(),
                },
                attempt: "A".into(),
                owner: Owner::Relay,
            },
        )
        .unwrap() else {
            panic!("owned candidate expected");
        };
        let old = children[1]
            .rollout
            .as_mut()
            .unwrap()
            .retiring
            .as_mut()
            .unwrap();
        let mut child = old.child.take().unwrap();
        child.kill().unwrap();
        old.exit_status = Some(child.wait().unwrap());
        apply(
            &mut children,
            "task",
            Operation::Commit {
                owner: Owner::Relay,
                old_pid,
                attempt: "A".into(),
                pid,
                stage: CommitStage::Select,
            },
        )
        .unwrap();
        // The next reservation can be cancelled before Fence reaches this actor.
        apply(&mut children, "next", Operation::Release)
            .expect("cancelling an unfenced next task must succeed");
        assert!(matches!(
            apply(
                &mut children,
                "task",
                Operation::Receipt {
                    owner: Owner::Relay,
                    old_pid,
                    attempt: "A".into(),
                    pid: Some(pid),
                }
            )
            .unwrap(),
            Reply::Committed
        ));
        assert_eq!(children[1].child.as_ref().unwrap().id(), pid);
        assert!(!children[1].fenced());
        // A different task's genuine unresolved fence still cannot be released.
        apply(
            &mut children,
            "pending",
            Operation::Fence {
                pid,
                owner: Owner::Relay,
            },
        )
        .unwrap();
        assert!(apply(&mut children, "next", Operation::Release).is_err());
        assert!(children[1].fenced());
    }

    #[test]
    fn release_precedes_paired_adxlet_liveness_and_keeps_ownership_guards() {
        use crate::rollout::actor::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let release = |relay: &mut ManagedService| {
            ActorHarness::new(root.path(), "relay", 5).command(
                std::slice::from_mut(relay),
                "task",
                Operation::Release,
            )
        };
        let fenced = || {
            let mut relay = ManagedService::new(process(
                "relay",
                Role::Relay,
                "/bin/sleep",
                vec!["60".into()],
                None,
            ));
            rollout(&mut relay).task = "task".into();
            rollout(&mut relay).selected = false;
            relay
        };
        // No paired adxlet exists: Release must not depend on its liveness.
        let mut relay = fenced();
        assert!(matches!(
            release(&mut relay).unwrap(),
            Reply::State {
                exited: false,
                success: false,
            }
        ));
        assert!(!relay.fenced());
        assert!(relay.rollout.is_none());
        let mut other = fenced();
        rollout(&mut other).task = "other".into();
        assert!(release(&mut other).is_err());
        assert!(other.fenced());
        let mut retained = fenced();
        rollout(&mut retained).retiring = Some(Box::new(fenced()));
        assert!(release(&mut retained).is_err());
        assert!(retained.fenced());
        assert_eq!(
            retained.rollout.as_ref().map(|state| state.task.as_str()),
            Some("task")
        );
    }

    #[test]
    fn invalid_rollout_request_does_not_poison_the_next_request() {
        let mut deployment: Deployment =
            serde_saphyr::from_str(crate::config::Profile::Node.template()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        deployment.state_dir = directory.path().into();
        let tasks = crate::rollout::Tasks::new(&deployment).unwrap();
        for line in [
            "{",
            "[]",
            r#"{"action":"rollout_submit"}"#,
            r#"{"action":"rollout_node_info","unexpected":true}"#,
            r#"{"action":"rollout_node_info","declaration_path":"/x"}"#,
        ] {
            assert_eq!(tasks.response(line)["ok"], false);
        }
        assert_eq!(
            tasks.response(r#"{"action":"rollout_node_info"}"#)["ok"],
            true
        );
        assert!(matches!(Request::parse("status"), Some(Request::Status)));
        assert!(matches!(
            Request::parse("stop drain"),
            Some(Request::Stop {
                mode: Some(ShutdownMode::Drain)
            })
        ));
        assert!(matches!(
            Request::parse("stop preserve"),
            Some(Request::Stop {
                mode: Some(ShutdownMode::Preserve)
            })
        ));
    }
}
