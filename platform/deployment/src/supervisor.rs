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
    rollout_task: Option<String>,
    retiring: Option<Box<ManagedService>>,
    exit_status: Option<std::process::ExitStatus>,
    rollout_fenced: bool,
    route_inode: Option<(u64, u64)>,
    generation_pid: Option<u32>,
    rollback_stop: Option<(u32, Instant, Instant)>,
    process_identity: Option<crate::rollout::ProcessIdentity>,
    process_completed: Option<(String, u32, u32, String)>,
    rollback_spawn: Option<SpawnReceipt>,
}
struct SpawnReceipt {
    old_pid: u32,
    attempt: String,
    config: Process,
    owned_pid: Option<u32>,
    closed: Vec<String>,
}
impl SpawnReceipt {
    fn replay(
        &self,
        old_pid: u32,
        attempt: &str,
        binary: &Path,
        bootstrap: &Path,
        generation: Option<u32>,
    ) -> Result<u32> {
        if self.old_pid != old_pid
            || self.attempt != attempt
            || self.config.binary != binary
            || self
                .config
                .env
                .get("ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET")
                .map(PathBuf::from)
                .as_deref()
                != Some(bootstrap)
            || self.owned_pid.is_none()
            || self.owned_pid != generation
        {
            return Err("spawn receipt conflict or unresolved spawn".into());
        }
        self.owned_pid.ok_or_else(|| "spawn not confirmed".into())
    }
}
impl ManagedService {
    fn process_committed(
        &self,
        task_id: &str,
        old_pid: u32,
        attempt: &str,
        pid: Option<u32>,
    ) -> bool {
        self.process_completed
            .as_ref()
            .is_some_and(|(task, old, selected, saved)| {
                task == task_id
                    && *old == old_pid
                    && saved == attempt
                    && pid.is_none_or(|pid| *selected == pid)
            })
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
            rollout_task: None,
            retiring: None,
            exit_status: None,
            rollout_fenced: false,
            route_inode: None,
            generation_pid: None,
            rollback_stop: None,
            process_identity: None,
            process_completed: None,
            rollback_spawn: None,
        }
    }
    /// Unstarted rollout candidate fenced to `task_id`, inheriting the restart count.
    fn fenced_candidate(config: Process, restarts: u64, task_id: String) -> Self {
        let mut service = Self::new(config);
        service.restarts = restarts;
        service.rollout_task = Some(task_id);
        service.rollout_fenced = true;
        service
    }
    fn failed_signal(&self) -> i32 {
        if self.config.role == Role::Adxlet {
            libc::SIGUSR1
        } else {
            libc::SIGTERM
        }
    }
    fn stop_failed(&mut self, pid: u32, deadline: Instant) -> Result<()> {
        if self.generation_pid != Some(pid) || !self.rollout_fenced {
            return Err("failed Child generation changed".into());
        }
        if self.rollback_stop.is_some() {
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
        self.rollback_stop = Some((
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
        self.rollback_stop = None;
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
        if let Some(retiring) = service.retiring.as_mut() {
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
            "rollout_task": service.rollout_task,
            "retiring_pid": service.retiring.as_ref().and_then(|old| old.child.as_ref().map(Child::id)),
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

fn adxlet_rollout_command(
    children: &mut [ManagedService],
    service_id: String,
    task_id: String,
    operation: crate::rollout::adxlet::Operation,
    logs: &Path,
    policy: &crate::logging::Policy,
    deadline: Instant,
) -> Result<crate::rollout::relay::Reply> {
    use crate::rollout::{adxlet::Operation, relay::Reply};
    if Instant::now() >= deadline {
        return Err("adxlet actor deadline exceeded".into());
    }
    let service = children
        .iter_mut()
        .find(|service| service.config.id == service_id && service.config.role == Role::Adxlet)
        .ok_or("managed adxlet not found")?;
    if service
        .rollout_task
        .as_ref()
        .is_some_and(|id| id != &task_id)
    {
        return Err("another task owns adxlet".into());
    }
    match operation {
        Operation::Receipt {
            identity,
            old_pid,
            attempt,
            pid,
        } => {
            if service.process_identity.as_ref() != Some(&identity) {
                return Err("attempt observation identity conflict".into());
            }
            if service.process_committed(&task_id, old_pid, &attempt, pid) {
                return Ok(Reply::Committed);
            }
            let spawn = service
                .rollback_spawn
                .as_ref()
                .ok_or("attempt outcome unknown")?;
            if service.rollout_task.as_deref() != Some(task_id.as_str()) {
                return Err("attempt task conflict".into());
            }
            if spawn.old_pid != old_pid
                || spawn.attempt != attempt
                || pid.is_some_and(|pid| spawn.owned_pid != Some(pid))
            {
                return Err("attempt observation conflict".into());
            }
            match spawn.owned_pid {
                None if service.child.is_none() => Ok(Reply::NeverCreated),
                None => Err("candidate creation outcome unknown".into()),
                Some(pid) => Ok(Reply::Candidate {
                    pid,
                    exited: service.child.is_none() && service.exit_status.is_some(),
                }),
            }
        }
        Operation::StopFailed {
            identity,
            old_pid,
            pid,
            attempt,
        } => {
            if service.process_identity.as_ref() != Some(&identity) {
                return Err("candidate generation stop conflict".into());
            }
            if service.process_committed(&task_id, old_pid, &attempt, pid) {
                return Ok(Reply::Committed);
            }
            if service.rollout_task.as_deref() != Some(task_id.as_str()) || !service.rollout_fenced
            {
                return Err("candidate task stop conflict".into());
            }
            let old = service.retiring.as_deref().unwrap_or(&*service);
            if old.generation_pid != Some(old_pid)
                || old.child.is_some()
                || old.exit_status.is_none()
            {
                return Err("old generation not reaped".into());
            }
            if let Some(spawn) = &service.rollback_spawn {
                if spawn.old_pid != old_pid || spawn.attempt != attempt || spawn.owned_pid != pid {
                    return Err("candidate receipt stop conflict".into());
                }
            } else {
                // No Start has reached this actor. Atomically seal A using
                // the existing receipt before proving NeverCreated, so a
                // delayed Start(A) cannot race an approved original Start(B).
                if pid.is_some() || service.retiring.is_some() || service.child.is_some() {
                    return Err("candidate creation outcome unknown".into());
                }
                service.rollback_spawn = Some(SpawnReceipt {
                    old_pid,
                    attempt: attempt.clone(),
                    config: service.config.clone(),
                    owned_pid: None,
                    closed: Vec::new(),
                });
            }
            let spawn = service
                .rollback_spawn
                .as_mut()
                .ok_or("candidate receipt absent")?;
            if !spawn.closed.contains(&attempt) {
                spawn.closed.push(attempt);
            }
            let Some(pid) = pid else {
                if service.child.is_some() {
                    return Err("candidate creation outcome unknown".into());
                }
                return Ok(Reply::NeverCreated);
            };
            service.stop_failed(pid, deadline)?;
            Ok(Reply::Candidate {
                pid,
                exited: service.child.is_none() && service.exit_status.is_some(),
            })
        }
        Operation::Fence { pid, identity } => {
            if service.retiring.is_some()
                || (service.rollout_fenced && service.process_identity.as_ref() != Some(&identity))
                || service.generation_pid != Some(pid)
            {
                return Err("old adxlet generation changed".into());
            }
            if !service.rollout_fenced {
                let child = service
                    .child
                    .as_mut()
                    .ok_or("old adxlet absent before fence")?;
                if child.id() != pid || child.try_wait()?.is_some() {
                    return Err("old adxlet not live before fence".into());
                }
                service.rollout_fenced = true;
                service.rollout_task = Some(task_id);
                service.process_identity = Some(identity);
                service.process_completed = None;
                service.rollback_spawn = None;
            }
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: None,
            })
        }
        Operation::Observe { pid, identity } => {
            if service.process_identity.as_ref() != Some(&identity)
                || service.rollout_task.as_deref() != Some(task_id.as_str())
            {
                return Err("old observation identity conflict".into());
            }
            let old = service.retiring.as_deref().unwrap_or(&*service);
            if !old.rollout_fenced || old.generation_pid != Some(pid) {
                return Err("unfenced old adxlet observation".into());
            }
            Ok(Reply::State {
                exited: old.child.is_none() && old.exit_status.is_some(),
                success: old.exit_status.is_some_and(|status| status.success()),
                candidate_pid: service
                    .retiring
                    .as_ref()
                    .and_then(|_| service.child.as_ref().map(Child::id)),
            })
        }
        Operation::Start {
            pid,
            config: target_config,
            binary,
            executable,
            bootstrap,
            attempt,
            identity,
        } => {
            if service.process_identity.as_ref() != Some(&identity) {
                return Err("unreserved process identity".into());
            }
            if service.rollout_task.as_deref() != Some(task_id.as_str()) {
                return Err("process task mismatch".into());
            }
            if !binary.is_absolute() || !bootstrap.is_absolute() {
                return Err("absolute candidate paths required".into());
            }
            if let Some(spawn) = &service.rollback_spawn {
                if spawn.closed.contains(&attempt) {
                    return Err("closed attempt cannot be restarted".into());
                }
                if spawn.attempt == attempt {
                    if spawn.config.configuration_sha256 != target_config.configuration_sha256 {
                        return Err("spawn configuration identity conflict".into());
                    }
                    if spawn.old_pid == pid
                        && spawn.config.binary == binary
                        && spawn.owned_pid.is_none()
                        && spawn
                            .config
                            .env
                            .get("ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET")
                            .map(PathBuf::from)
                            .as_ref()
                            == Some(&bootstrap)
                    {
                        return Ok(Reply::NeverCreated);
                    }
                    let owned_pid =
                        spawn.replay(pid, &attempt, &binary, &bootstrap, service.generation_pid)?;
                    return Ok(Reply::Candidate {
                        pid: owned_pid,
                        exited: service.child.is_none() && service.exit_status.is_some(),
                    });
                }
            }
            let mut closed = service
                .rollback_spawn
                .as_ref()
                .map_or_else(Vec::new, |spawn| spawn.closed.clone());
            if let Some(spawn) = &service.rollback_spawn {
                if !closed.contains(&spawn.attempt) {
                    closed.push(spawn.attempt.clone());
                }
            }
            if let Some(old) = service.retiring.as_ref() {
                if old.generation_pid != Some(pid)
                    || service.child.is_some()
                    || (service.exit_status.is_none()
                        && service
                            .rollback_spawn
                            .as_ref()
                            .is_none_or(|spawn| spawn.owned_pid.is_some()))
                    || !service.rollout_fenced
                {
                    return Err("candidate not actually reaped".into());
                }
                if target_config.id != old.config.id
                    || target_config.role != old.config.role
                    || target_config.configuration_sha256 != old.config.configuration_sha256
                    || target_config.sources != old.config.sources
                    || target_config.env != old.config.env
                    || target_config.admin_socket != old.config.admin_socket
                    || target_config.health_url != old.config.health_url
                {
                    return Err("original process snapshot ownership conflict".into());
                }
                // The worker has verified this pinned Process and executable.
                // The old live --config path is not a restoration snapshot.
                service.config = *target_config;
                service.config.binary = binary;
                service.config.env.insert(
                    "ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET".into(),
                    bootstrap.to_string_lossy().into_owned(),
                );
                service.rollback_spawn = Some(SpawnReceipt {
                    old_pid: pid,
                    attempt,
                    config: service.config.clone(),
                    owned_pid: None,
                    closed,
                });
                if service.start_from(logs, policy, Some(&executable)).is_err() {
                    return Ok(Reply::NeverCreated);
                }
                if let Some(spawn) = &mut service.rollback_spawn {
                    spawn.owned_pid = service.generation_pid;
                }
                return Ok(Reply::Candidate {
                    pid: service.generation_pid.ok_or("candidate PID absent")?,
                    exited: false,
                });
            }
            if !service.rollout_fenced
                || service.rollout_task.as_deref() != Some(task_id.as_str())
                || service.generation_pid != Some(pid)
                || service.child.is_some()
                || service.exit_status.is_none()
            {
                return Err("old adxlet not fenced and reaped".into());
            }
            let mut config = *target_config;
            config.binary = binary;
            config.env.insert(
                "ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET".into(),
                bootstrap.to_string_lossy().into_owned(),
            );
            // Preserve rendered args, original listeners and the current active Relay.
            let mut candidate = ManagedService::fenced_candidate(config, service.restarts, task_id);
            candidate.process_identity = Some(identity);
            candidate.rollback_spawn = Some(SpawnReceipt {
                old_pid: pid,
                attempt,
                config: candidate.config.clone(),
                owned_pid: None,
                closed,
            });
            let created = candidate
                .start_from(logs, policy, Some(&executable))
                .is_ok();
            if let Some(spawn) = &mut candidate.rollback_spawn {
                spawn.owned_pid = candidate.generation_pid;
            }
            let old = std::mem::replace(service, candidate);
            service.retiring = Some(Box::new(old));
            if !created {
                return Ok(Reply::NeverCreated);
            }
            Ok(Reply::Candidate {
                pid: service.generation_pid.ok_or("candidate PID missing")?,
                exited: false,
            })
        }
        Operation::Complete {
            pid,
            old_pid,
            attempt,
            identity,
        } => {
            if service.process_identity.as_ref() != Some(&identity) {
                return Err("unreserved ready identity".into());
            }
            if service.process_committed(&task_id, old_pid, &attempt, Some(pid))
                && service.generation_pid == Some(pid)
                && !service.rollout_fenced
                && service.retiring.is_none()
            {
                let child = service.child.as_mut().ok_or("selected Child absent")?;
                if child.id() != pid || child.try_wait()?.is_some() {
                    return Err("selected Child no longer live".into());
                }
                return Ok(Reply::Candidate { pid, exited: false });
            }
            if service.rollout_task.as_deref() != Some(task_id.as_str())
                || service.rollback_spawn.as_ref().is_none_or(|spawn| {
                    spawn.old_pid != old_pid
                        || spawn.attempt != attempt
                        || spawn.owned_pid != Some(pid)
                        || spawn.closed.contains(&attempt)
                })
                || service.generation_pid != Some(pid)
                || !service.rollout_fenced
                || service.retiring.as_ref().is_none_or(|old| {
                    old.generation_pid != Some(old_pid)
                        || old.child.is_some()
                        || old.exit_status.is_none()
                })
            {
                return Err("candidate ownership changed".into());
            }
            let child = service.child.as_mut().ok_or("candidate Child absent")?;
            if child.id() != pid || child.try_wait()?.is_some() {
                return Err("candidate no longer live".into());
            }
            service
                .config
                .env
                .remove("ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET");
            service.retiring = None;
            service.rollout_fenced = false;
            service.rollout_task = None;
            service.process_completed = Some((task_id, old_pid, pid, attempt));
            Ok(Reply::Candidate { pid, exited: false })
        }
    }
}

fn rollout_command(
    children: &mut [ManagedService],
    service_id: String,
    task_id: String,
    operation: crate::rollout::relay::Operation,
    logs: &Path,
    policy: &crate::logging::Policy,
    deadline: Instant,
) -> Result<crate::rollout::relay::Reply> {
    if Instant::now() >= deadline {
        return Err("actor stage deadline exceeded".into());
    }
    use crate::rollout::relay::{Operation, Reply};
    if matches!(&operation, Operation::RuntimeInspect) {
        let index = children
            .iter()
            .position(|s| s.config.id == service_id)
            .ok_or("managed component missing")?;
        let service = &mut children[index];
        let task_observation =
            !task_id.is_empty() && service.rollout_task.as_deref() == Some(task_id.as_str());
        if !task_observation
            && (service.retiring.is_some()
                || service.rollout_task.is_some()
                || service.rollout_fenced)
        {
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
        if !task_observation
            && (owner.retiring.is_some() || owner.rollout_fenced || owner.rollout_task.is_some())
        {
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
    let operation = match operation {
        Operation::Adxlet(operation) => {
            return adxlet_rollout_command(
                children, service_id, task_id, operation, logs, policy, deadline,
            )
        }
        operation => operation,
    };
    let index = children
        .iter()
        .position(|service| service.config.id == service_id && service.config.role == Role::Relay)
        .ok_or("managed Relay not found")?;
    // Only executor's certified never-sent-Begin cleanup calls Release; it must
    // not depend on paired adxlet liveness, so it is handled before that guard.
    if matches!(operation, Operation::Release) {
        let service = &mut children[index];
        if service
            .rollout_task
            .as_ref()
            .is_some_and(|id| id != &task_id)
            || service.retiring.is_some()
        {
            return Err("another or irreversible generation retained".into());
        }
        service.rollout_fenced = false;
        service.rollout_task = None;
        return Ok(Reply::State {
            exited: false,
            success: false,
            candidate_pid: None,
        });
    }
    let original = children[index]
        .retiring
        .as_deref()
        .unwrap_or(&children[index]);
    let owner_index = children
        .iter()
        .position(|service| {
            service.config.role == Role::Adxlet
                && service.config.admin_socket.is_some()
                && original
                    .config
                    .env
                    .get("ADX_SUPERVISOR_OWNER_SERVICE")
                    .is_some_and(|id| id == &service.config.id)
        })
        .ok_or("paired managed adxlet unavailable")?;
    let owner = &mut children[owner_index];
    if owner
        .child
        .as_mut()
        .ok_or("adxlet Child absent")?
        .try_wait()?
        .is_some()
    {
        return Err("owned adxlet exited".into());
    }
    let service = &mut children[index];
    if service
        .rollout_task
        .as_ref()
        .is_some_and(|id| id != &task_id)
    {
        return Err("another task owns Relay".into());
    }
    match operation {
        Operation::RuntimeInspect => Err("runtime inspect dispatch error".into()),
        Operation::Adxlet(_) => Err("adxlet operation on Relay actor".into()),
        Operation::Fence { pid } => {
            let child = service.child.as_mut().ok_or("old Child absent")?;
            if child.id() != pid || child.try_wait()?.is_some() {
                return Err("old Child changed".into());
            }
            service.rollout_fenced = true;
            service.rollout_task = Some(task_id);
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: None,
            })
        }
        Operation::Start {
            pid,
            config: target_config,
            binary,
            directory,
        } => {
            if service.retiring.is_some() {
                if service.config.configuration_sha256 != target_config.configuration_sha256 {
                    return Err("candidate configuration identity conflict".into());
                }
                return Ok(Reply::State {
                    exited: false,
                    success: false,
                    candidate_pid: service.child.as_ref().map(Child::id),
                });
            }
            if !service.rollout_fenced {
                return Err("generation not reserved".into());
            }
            if service.child.as_ref().map(Child::id) != Some(pid) && service.exit_status.is_none() {
                return Err("old Child changed".into());
            }
            let mut config = target_config;
            config.binary = binary;
            config.env.insert(
                "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET".into(),
                directory.join("route.sock").to_string_lossy().into_owned(),
            );
            // Directory creation and durability are done by the worker before
            // this command; spawning is the actor's only generation mutation.
            let mut candidate = ManagedService::fenced_candidate(config, service.restarts, task_id);
            if Instant::now() >= deadline {
                return Err("candidate start deadline exceeded".into());
            }
            candidate.start(logs, policy)?;
            let old = std::mem::replace(service, candidate);
            service.retiring = Some(Box::new(old));
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: service.child.as_ref().map(Child::id),
            })
        }
        Operation::Observe { pid } => {
            let is_candidate = service.retiring.is_some();
            let old = if is_candidate {
                service
                    .retiring
                    .as_deref_mut()
                    .ok_or("retiring generation missing")?
            } else {
                &mut *service
            };
            if old.child.as_ref().is_some_and(|child| child.id() != pid) {
                return Err("old Child identity mismatch".into());
            }
            // Tick is the sole reaper, so log capture is finished with the Child.
            let exited = old.child.is_none();
            let success = old.exit_status.is_some_and(|status| status.success());
            let candidate_pid = if is_candidate {
                service.child.as_ref().map(Child::id)
            } else {
                None
            };
            Ok(Reply::State {
                exited,
                success,
                candidate_pid,
            })
        }
        Operation::Candidate => {
            if service.retiring.is_none() {
                return Err("candidate not started".into());
            }
            Ok(Reply::Candidate {
                pid: service.generation_pid.ok_or("candidate identity missing")?,
                exited: service.child.is_none() && service.exit_status.is_some(),
            })
        }
        Operation::StopFailed { pid } => {
            if service.retiring.is_none() {
                return Err("failed candidate not owned".into());
            }
            service.stop_failed(pid, deadline)?;
            Ok(Reply::Candidate {
                pid,
                exited: service.child.is_none() && service.exit_status.is_some(),
            })
        }
        Operation::RestartFailed {
            pid,
            binary,
            directory,
        } => {
            if !service.rollout_fenced
                || service.retiring.is_none()
                || service.rollout_task.as_deref() != Some(task_id.as_str())
            {
                return Err("replacement not fenced by this task".into());
            }
            if service.config.binary == binary {
                if service
                    .config
                    .env
                    .get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET")
                    .map(PathBuf::from)
                    != Some(directory.join("route.sock"))
                {
                    return Err("rollback replay endpoint conflict".into());
                }
                if service.child.is_none() && service.generation_pid == Some(pid) {
                    if Instant::now() >= deadline {
                        return Err("rollback start deadline exceeded".into());
                    }
                    service.start(logs, policy)?;
                }
                return Ok(Reply::State {
                    exited: false,
                    success: false,
                    candidate_pid: service.child.as_ref().map(Child::id),
                });
            }
            if service.generation_pid != Some(pid)
                || service.child.is_some()
                || service.exit_status.is_none()
            {
                return Err("failed candidate has not been reaped".into());
            }
            // The original retiring Child remains owned, including revocation
            // lifetime; only the proven-dead candidate slot is replaced.
            service.config = service
                .retiring
                .as_ref()
                .ok_or("original process missing")?
                .config
                .clone();
            service.config.binary = binary;
            service.config.env.insert(
                "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET".into(),
                directory.join("route.sock").to_string_lossy().into_owned(),
            );
            if Instant::now() >= deadline {
                return Err("rollback start deadline exceeded".into());
            }
            service.start(logs, policy)?;
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: service.child.as_ref().map(Child::id),
            })
        }
        Operation::Confirm { pid } => {
            if service.child.as_ref().map(Child::id) != Some(pid)
                || service
                    .child
                    .as_mut()
                    .ok_or("candidate Child missing")?
                    .try_wait()?
                    .is_some()
            {
                return Err("candidate Child changed".into());
            }
            if Instant::now() >= deadline {
                return Err("confirmation stage expired".into());
            }
            let endpoint = service
                .config
                .env
                .get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET")
                .cloned()
                .ok_or("candidate endpoint missing")?;
            children[owner_index]
                .config
                .env
                .insert("ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into(), endpoint);
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: Some(pid),
            })
        }
        Operation::Select { pid } => {
            if service.rollout_task.is_none()
                && service.retiring.is_none()
                && service.child.as_ref().map(Child::id) == Some(pid)
                && !service.rollout_fenced
            {
                return Ok(Reply::State {
                    exited: false,
                    success: false,
                    candidate_pid: Some(pid),
                });
            }
            if service.retiring.is_none() || service.child.is_none() {
                return Err("candidate Child unavailable".into());
            }
            if service.child.as_ref().map(Child::id) != Some(pid)
                || service
                    .child
                    .as_mut()
                    .ok_or("candidate Child missing")?
                    .try_wait()?
                    .is_some()
            {
                return Err("candidate Child exited before selection".into());
            }
            // The selected executable and generation path already live in config.
            let endpoint = service
                .config
                .env
                .get("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET")
                .cloned()
                .ok_or("selected endpoint missing")?;
            if service
                .retiring
                .as_ref()
                .is_none_or(|old| old.child.is_some())
            {
                return Err("old Child not reaped".into());
            }
            if Instant::now() >= deadline {
                return Err("selection stage expired".into());
            }
            service.rollout_fenced = false;
            service.retiring = None;
            service.rollout_task = None;
            children[owner_index]
                .config
                .env
                .insert("ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into(), endpoint);
            Ok(Reply::State {
                exited: false,
                success: false,
                candidate_pid: Some(pid),
            })
        }
        // Handled before the paired adxlet liveness guard; never reaches here.
        Operation::Release => Err("release dispatch error".into()),
    }
}

struct ExternalStartup {
    selected: crate::external::PreparedGroup,
    ready: Option<crate::external::PreparedGroup>,
    fallback: bool,
    failed: bool,
    deadline: Instant,
    stopping: Option<Instant>,
    escalated: bool,
    probe: tokio::task::JoinHandle<()>,
}

fn adx_group(role: Role) -> bool {
    matches!(role, Role::Adxlet | Role::Relay)
}

fn configuration_exit(status: std::process::ExitStatus) -> bool {
    status.code() == Some(i32::from(adx_process::CONFIGURATION_EXIT_CODE))
}

fn startup_blocks_restart(pending: bool, role: Role) -> bool {
    pending && adx_group(role)
}

fn startup_receipt(current_fallback: bool, receipt_fallback: bool, success: bool) -> Option<bool> {
    (current_fallback == receipt_fallback).then_some(success)
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

fn install_external(
    specs: &mut [Process],
    group: &mut crate::external::PreparedGroup,
) -> Result<()> {
    for replacement in &mut group.processes {
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
        .filter(|process| adx_group(process.role))
        .count()
        != group.processes.len()
    {
        return Err("external group is not complete".into());
    }
    Ok(())
}

fn startup_probe(
    tasks: std::sync::Arc<crate::rollout::Tasks>,
    deployment: &Deployment,
    group: &crate::external::PreparedGroup,
    deadline: Instant,
    sender: tokio::sync::mpsc::Sender<(bool, bool)>,
    fallback: bool,
) -> Result<tokio::task::JoinHandle<()>> {
    let mut expected = Vec::new();
    for process in &group.processes {
        let identity = group
            .components
            .iter()
            .find(|component| component.role == process.role)
            .ok_or("missing startup binary identity")?;
        let owner = deployment
            .services
            .iter()
            .find(|service| service.role == Role::Adxlet)
            .ok_or("missing startup owner")?;
        let node = owner
            .config
            .get("node_id")
            .and_then(Value::as_str)
            .filter(|node| !node.is_empty())
            .ok_or("missing startup node identity")?;
        expected.push((
            process.id.clone(),
            node.to_owned(),
            identity.binary_sha256.clone(),
            identity.effective_config_sha256.clone(),
        ));
    }
    Ok(tokio::spawn(async move {
        let proof = async {
            loop {
                if tasks.is_stopped() {
                    return false;
                }
                let mut group_sessions = None;
                let mut complete = true;
                for (service, node, digest, configuration) in &expected {
                    match tasks.observe_startup(service, node).await {
                        Ok(proof)
                            if proof.executable_sha256 == *digest
                                && proof.configuration_sha256 == *configuration =>
                        {
                            let sessions = (proof.node_session, proof.relay_session);
                            if group_sessions
                                .as_ref()
                                .is_some_and(|previous| previous != &sessions)
                            {
                                complete = false;
                            }
                            group_sessions = Some(sessions);
                        }
                        _ => {
                            complete = false;
                            break;
                        }
                    }
                }
                if complete {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let success = tokio::time::timeout_at(deadline.into(), proof)
            .await
            .unwrap_or(false);
        let _ = sender.send((fallback, success)).await;
    }))
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
    // Validate the complete executable set before starting anything.
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
        if deployment.external_declaration.is_none() || !adx_group(process.role) {
            preflight_executable(process)?;
        }
    }
    let mut external = if let Some(path) = deployment.external_declaration.clone() {
        let snapshot = deployment.clone();
        let directory = deployment
            .state_dir
            .join(format!("external-{}", rollout.boot_nonce()));
        let stopped = rollout.stop_flag();
        let runtime = tokio::runtime::Handle::current();
        Some(
            tokio::task::spawn_blocking(move || {
                crate::external::prepare(&path, &snapshot, &directory, &stopped, &runtime)
            })
            .await
            .map_err(|_| "external preparation worker failed")??,
        )
    } else {
        None
    };
    if let Some(prepared) = external.as_mut() {
        for process in prepared
            .target
            .processes
            .iter_mut()
            .chain(&mut prepared.ready.processes)
        {
            preflight_executable(process)?;
        }
        install_external(&mut process_specs, &mut prepared.target)?;
        // Preserve the same generated parent control wiring for the frozen fallback.
        let mut fallback_specs = process_specs.clone();
        install_external(&mut fallback_specs, &mut prepared.ready)?;
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
    let mut startup_deadline = None;
    for config in process_specs {
        let mut service = ManagedService::new(config);
        if external.is_some() && adx_group(service.config.role) {
            startup_deadline.get_or_insert_with(|| Instant::now() + Duration::from_secs(300));
        }
        if let Err(error) = service.start(&logs, &deployment.logging) {
            service.failed = true;
            eprintln!("service {} spawn failed: {error}", service.config.id);
            service.next_start =
                Instant::now() + Duration::from_millis(deployment.restart_delay_ms);
        }
        if external.is_some() && adx_group(service.config.role) {
            service.signal_target = SignalTarget::Process;
        }
        children.push(service);
    }
    let (probe_sender, mut probe_results) = tokio::sync::mpsc::channel(1);
    let mut startup = if let Some(prepared) = external {
        let startup_deadline = startup_deadline.ok_or("external group had no ADX spawn attempt")?;
        let probe = startup_probe(
            rollout.clone(),
            &deployment,
            &prepared.target,
            startup_deadline,
            probe_sender.clone(),
            false,
        )?;
        Some(ExternalStartup {
            selected: prepared.target,
            ready: Some(prepared.ready),
            fallback: false,
            failed: false,
            deadline: startup_deadline,
            stopping: None,
            escalated: false,
            probe,
        })
    } else {
        None
    };
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
            Some((fallback, success)) = probe_results.recv(), if startup.as_ref().is_some_and(|state| state.stopping.is_none()) => {
                if let Some(state) = startup.as_mut() {
                    let Some(success) = startup_receipt(state.fallback, fallback, success) else { continue; };
                    let live = children.iter_mut().filter(|service| adx_group(service.config.role))
                        .all(|service| service.child.as_mut().is_some_and(|child| child.try_wait().is_ok_and(|status| status.is_none())));
                    if success && live && Instant::now() < state.deadline {
                        rollout.complete_startup(state.fallback, &state.selected)?;
                        eprintln!("external startup selected {}", if state.fallback { "ready fallback" } else { "target" });
                        startup = None;
                    } else {
                        state.failed = true;
                    }
                }
            }
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
                    if let Some((pid, terminate_at, deadline)) = service.rollback_stop {
                        if service.generation_pid == Some(pid) && service.child.is_some()
                            && Instant::now() >= terminate_at && Instant::now() < deadline {
                            service.signal_target = SignalTarget::Process;
                            if let Err(error) = service.signal(libc::SIGKILL) {
                                eprintln!("failed candidate {} escalation failed: {error}", service.config.id);
                            }
                            // No refreshed grace and no repeated escalation.
                            service.rollback_stop = Some((pid, deadline, deadline));
                        }
                    }
                    if let Some(old) = service.retiring.as_mut() {
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
                        && !startup_blocks_restart(startup.is_some(), service.config.role)
                        && !service.rollout_fenced
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
                if let Some(state) = startup.as_mut() {
                    let failed = children.iter().filter(|service| adx_group(service.config.role))
                        .any(|service| service.child.is_none() || service.failed);
                    let configuration_failed = children.iter().filter(|service| adx_group(service.config.role))
                        .any(|service| service.exit_status.is_some_and(configuration_exit));
                    if configuration_failed { state.ready.take(); }
                    if state.stopping.is_none() && (state.failed || failed || Instant::now() >= state.deadline) {
                        state.probe.abort();
                        let deadline = Instant::now() + Duration::from_secs(deployment.stop_timeout_seconds);
                        state.stopping = Some(deadline);
                        for service in children.iter_mut().filter(|service| adx_group(service.config.role)) {
                            service.signal_target = SignalTarget::Process;
                            if let Err(error) = service.signal(service.failed_signal()) {
                                eprintln!("external startup stop {} failed: {error}", service.config.id);
                            }
                        }
                    }
                    if let Some(stop_deadline) = state.stopping {
                        let reaped = children.iter().filter(|service| adx_group(service.config.role)).all(|service| service.child.is_none());
                        if reaped {
                            if let Some(mut ready) = state.ready.take() {
                                let mut specs: Vec<_> = children.iter().map(|service| service.config.clone()).collect();
                                install_external(&mut specs, &mut ready)?;
                                state.fallback = true;
                                state.failed = false;
                                state.selected = ready;
                                state.deadline = Instant::now() + Duration::from_secs(300);
                                state.stopping = None;
                                state.escalated = false;
                                for service in children.iter_mut().filter(|service| adx_group(service.config.role)) {
                                    service.config = state.selected.processes.iter().find(|process| process.id == service.config.id)
                                        .ok_or("missing ready process")?.clone();
                                    service.failed = false;
                                    if let Err(error) = service.start(&logs, &deployment.logging) {
                                        service.failed = true;
                                        eprintln!("external ready spawn {} failed: {error}", service.config.id);
                                    }
                                    service.signal_target = SignalTarget::Process;
                                }
                                state.probe = startup_probe(rollout.clone(), &deployment, &state.selected, state.deadline, probe_sender.clone(), true)?;
                            } else {
                                rollout.cancel();
                                for service in &mut children { service.signal_target = SignalTarget::Process; }
                                return Err(if configuration_failed {
                                    if state.fallback {
                                        "external ready configuration failed; owned ADX group reaped"
                                    } else {
                                        "external target configuration failed; owned ADX group reaped; ready not attempted"
                                    }
                                } else {
                                    "external target and ready startup failed; owned ADX group reaped"
                                }.into());
                            }
                        } else if Instant::now() >= stop_deadline && !state.escalated {
                            for service in children.iter_mut().filter(|service| adx_group(service.config.role)) {
                                service.signal_target = SignalTarget::Process;
                                if let Err(error) = service.signal(libc::SIGKILL) { eprintln!("external startup escalation failed: {error}"); }
                            }
                            state.escalated = true;
                            eprintln!("external startup escalated owned Child stop; next group remains blocked until actual reap");
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod rollout_tests {
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

    #[test]
    fn configuration_exit_matches_only_actual_configuration_status_code() {
        assert!(configuration_exit(failed_status(78)));
        assert!(!configuration_exit(failed_status(1)));
        assert!(!configuration_exit(failed_status(0)));
        assert!(failed_status(0).success());
        assert!(!failed_status(1).success());
    }

    #[tokio::test]
    async fn queued_target_receipts_cannot_complete_or_fail_ready_leg() {
        let (sender, mut results) = tokio::sync::mpsc::channel(3);
        sender.send((false, true)).await.unwrap();
        sender.send((false, false)).await.unwrap();
        sender.send((true, true)).await.unwrap();
        let fallback = true;
        let mut accepted = Vec::new();
        while let Ok((receipt_leg, success)) = results.try_recv() {
            if let Some(success) = startup_receipt(fallback, receipt_leg, success) {
                accepted.push(success);
            }
        }
        assert_eq!(accepted, vec![true]);
    }

    #[test]
    fn external_pending_blocks_only_adx_automatic_restart() {
        assert!(startup_blocks_restart(true, Role::Adxlet));
        assert!(startup_blocks_restart(true, Role::Relay));
        assert!(!startup_blocks_restart(true, Role::Coordinator));
        assert!(!startup_blocks_restart(false, Role::Adxlet));
    }

    #[test]
    fn spawn_replay_requires_exact_attempt_and_owned_generation() {
        let mut config = process(
            "node",
            Role::Adxlet,
            "/original",
            vec!["--config".into(), "/rendered/original.json".into()],
            None,
        );
        config.env.insert(
            "ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET".into(),
            "/relay/route.sock".into(),
        );
        config.env.insert(
            "ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET".into(),
            "/rollback.sock".into(),
        );
        let mut spawn = SpawnReceipt {
            old_pid: 42,
            attempt: "attempt".into(),
            config,
            owned_pid: None,
            closed: Vec::new(),
        };
        assert!(spawn
            .replay(
                42,
                "attempt",
                Path::new("/original"),
                Path::new("/rollback.sock"),
                Some(88)
            )
            .is_err());
        // Even PID reuse is an exact actor-recorded new generation, not evidence
        // inferred from inequality with the failed PID.
        spawn.owned_pid = Some(77);
        assert_eq!(
            spawn
                .replay(
                    42,
                    "attempt",
                    Path::new("/original"),
                    Path::new("/rollback.sock"),
                    Some(77)
                )
                .unwrap(),
            77
        );
        assert!(spawn
            .replay(
                42,
                "attempt",
                Path::new("/original"),
                Path::new("/other.sock"),
                Some(77)
            )
            .is_err());
        assert!(spawn
            .replay(
                42,
                "attempt",
                Path::new("/original"),
                Path::new("/rollback.sock"),
                Some(88)
            )
            .is_err());
        assert_eq!(spawn.config.args[1], "/rendered/original.json");
        assert_eq!(
            spawn.config.env["ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET"],
            "/relay/route.sock"
        );
        assert!(spawn
            .replay(
                42,
                "other",
                Path::new("/original"),
                Path::new("/rollback.sock"),
                Some(77)
            )
            .is_err());
    }

    #[test]
    fn candidate_actor_reuses_original_rendered_config_and_replays_same_child() {
        use crate::rollout::{adxlet::Operation, relay::Reply};
        let root = tempfile::tempdir().unwrap();
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                old_relay_session: "relay".into(),
                original_sha256: "a".repeat(64),
                ..Default::default()
            },
        };
        let make = |pid, args| {
            let mut service = ManagedService::new(process(
                "node",
                Role::Adxlet,
                "/bin/sleep",
                args,
                Some(root.path().join("admin.sock")),
            ));
            service.signal_target = SignalTarget::Process;
            service.rollout_task = Some("task".into());
            service.exit_status = Some(failed_status(1));
            service.rollout_fenced = true;
            service.generation_pid = Some(pid);
            service.process_identity = Some(identity.clone());
            service
        };
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
        failed.retiring = Some(Box::new(original));
        let binary = root.path().join("rollback");
        let bootstrap = root.path().join("first-listener.sock");
        let mut target_config = failed.retiring.as_ref().unwrap().config.clone();
        target_config.args = vec!["45".into()];
        let operation = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: binary.clone(),
            executable: File::open("/bin/sleep").unwrap(),
            bootstrap: bootstrap.clone(),
            attempt: "attempt".into(),
            identity: identity.clone(),
        };
        let policy = crate::logging::Policy::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        for changed in ["id", "role", "digest"] {
            let mut wrong = target_config.clone();
            match changed {
                "id" => wrong.id = "another-service".into(),
                "role" => wrong.role = Role::Relay,
                _ => wrong.configuration_sha256 = "wrong-digest".into(),
            }
            assert!(
                adxlet_rollout_command(
                    std::slice::from_mut(&mut failed),
                    "node".into(),
                    "task".into(),
                    Operation::Start {
                        config: Box::new(wrong),
                        pid: 42,
                        binary: binary.clone(),
                        executable: File::open("/bin/sleep").unwrap(),
                        bootstrap: bootstrap.clone(),
                        attempt: "wrong".into(),
                        identity: identity.clone(),
                    },
                    root.path(),
                    &policy,
                    deadline,
                )
                .is_err(),
                "original snapshot ownership: {changed}"
            );
        }
        let first = adxlet_rollout_command(
            std::slice::from_mut(&mut failed),
            "node".into(),
            "task".into(),
            operation(),
            root.path(),
            &policy,
            deadline,
        )
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
            matches!(adxlet_rollout_command(std::slice::from_mut(&mut failed), "node".into(), "task".into(),
            operation(), root.path(), &policy, deadline).unwrap(), Reply::Candidate { pid:replay, exited:false } if replay == pid)
        );
        assert_eq!(failed.rollback_spawn.as_ref().unwrap().attempt, "attempt");
        failed.child.as_mut().unwrap().kill().unwrap();
        let status = failed.child.as_mut().unwrap().wait().unwrap();
        failed.exit_status = Some(status);
        failed.child = None;
        assert!(
            matches!(adxlet_rollout_command(std::slice::from_mut(&mut failed), "node".into(), "task".into(),
            operation(), root.path(), &policy, deadline).unwrap(), Reply::Candidate { pid:replay, exited:true } if replay == pid)
        );
        // An actor-recorded creation failure is immutable even if the same
        // executable FD becomes usable before the caller retries.
        let invalid = root.path().join("not-executable");
        std::fs::write(&invalid, b"invalid executable").unwrap();
        let never_created = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: invalid.clone(),
            executable: File::open(&invalid).unwrap(),
            bootstrap: bootstrap.clone(),
            attempt: "failed-attempt".into(),
            identity: identity.clone(),
        };
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut failed),
                "node".into(),
                "task".into(),
                never_created(),
                root.path(),
                &policy,
                deadline
            )
            .unwrap(),
            Reply::NeverCreated
        ));
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut failed),
                "node".into(),
                "task".into(),
                never_created(),
                root.path(),
                &policy,
                deadline
            )
            .unwrap(),
            Reply::NeverCreated
        ));
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut failed),
            "node".into(),
            "task".into(),
            operation(),
            root.path(),
            &policy,
            deadline
        )
        .is_err());
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut failed),
                "node".into(),
                "task".into(),
                Operation::Receipt {
                    identity: identity.clone(),
                    old_pid: 42,
                    attempt: "failed-attempt".into(),
                    pid: None
                },
                root.path(),
                &policy,
                deadline
            )
            .unwrap(),
            Reply::NeverCreated
        ));
    }

    #[test]
    fn stop_failed_never_created_seals_late_start_without_affecting_next_attempt() {
        use crate::rollout::{adxlet::Operation, relay::Reply};
        let root = tempfile::tempdir().unwrap();
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection::default(),
        };
        let config = process("node", Role::Adxlet, "/bin/sleep", vec!["60".into()], None);
        let mut service = ManagedService::new(config.clone());
        service.rollout_fenced = true;
        service.rollout_task = Some("task".into());
        service.process_identity = Some(identity.clone());
        service.generation_pid = Some(42);
        service.exit_status = Some(failed_status(1));
        let policy = crate::logging::Policy::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut service),
                "node".into(),
                "task".into(),
                Operation::StopFailed {
                    identity: identity.clone(),
                    old_pid: 42,
                    pid: None,
                    attempt: "A".into(),
                },
                root.path(),
                &policy,
                deadline,
            )
            .unwrap(),
            Reply::NeverCreated
        ));
        assert!(service
            .rollback_spawn
            .as_ref()
            .unwrap()
            .closed
            .contains(&"A".into()));
        let start = |attempt: &str| Operation::Start {
            config: Box::new(config.clone()),
            pid: 42,
            binary: root.path().join("original"),
            executable: File::open("/bin/sleep").unwrap(),
            bootstrap: root.path().join("bootstrap.sock"),
            attempt: attempt.into(),
            identity: identity.clone(),
        };
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            start("A"),
            root.path(),
            &policy,
            deadline,
        )
        .is_err());
        let Reply::Candidate { pid, exited: false } = adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            start("B"),
            root.path(),
            &policy,
            deadline,
        )
        .unwrap() else {
            panic!("B should own one fresh Child");
        };
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            Operation::StopFailed {
                identity,
                old_pid: 42,
                pid: None,
                attempt: "A".into(),
            },
            root.path(),
            &policy,
            deadline,
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
        use crate::rollout::{adxlet::Operation, relay::Reply};
        let root = tempfile::tempdir().unwrap();
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                old_relay_session: "relay".into(),
                original_sha256: "a".repeat(64),
                ..Default::default()
            },
        };
        let make = |pid, args| {
            let mut service = ManagedService::new(process(
                "node",
                Role::Adxlet,
                "/bin/sleep",
                args,
                Some(root.path().join("admin.sock")),
            ));
            service.signal_target = SignalTarget::Process;
            service.rollout_task = Some("task".into());
            service.exit_status = Some(failed_status(1));
            service.rollout_fenced = true;
            service.generation_pid = Some(pid);
            service.process_identity = Some(identity.clone());
            service
        };
        let original = make(42, vec!["60".into()]);
        let mut failed = make(77, vec!["invalid-target-args".into()]);
        failed.retiring = Some(Box::new(original));
        let binary = root.path().join("rollback");
        let bootstrap = root.path().join("first-listener.sock");
        let target_config = failed.retiring.as_ref().unwrap().config.clone();
        let start = || Operation::Start {
            config: Box::new(target_config.clone()),
            pid: 42,
            binary: binary.clone(),
            executable: File::open("/bin/sleep").unwrap(),
            bootstrap: bootstrap.clone(),
            attempt: "attempt".into(),
            identity: identity.clone(),
        };
        let policy = crate::logging::Policy::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let Reply::Candidate { pid, exited: false } = adxlet_rollout_command(
            std::slice::from_mut(&mut failed),
            "node".into(),
            "task".into(),
            start(),
            root.path(),
            &policy,
            deadline,
        )
        .unwrap() else {
            panic!("fresh owned attempt expected");
        };
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut failed),
                "node".into(),
                "task".into(),
                Operation::StopFailed {
                    identity: identity.clone(),
                    old_pid: 42,
                    pid: Some(pid),
                    attempt: "attempt".into(),
                },
                root.path(),
                &policy,
                deadline,
            )
            .unwrap(),
            Reply::Candidate { pid: stopped, exited: false } if stopped == pid
        ));
        assert!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut failed),
                "node".into(),
                "task".into(),
                Operation::Complete {
                    pid,
                    old_pid: 42,
                    attempt: "attempt".into(),
                    identity: identity.clone(),
                },
                root.path(),
                &policy,
                deadline,
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
        let late = adxlet_rollout_command(
            std::slice::from_mut(&mut failed),
            "node".into(),
            "task".into(),
            start(),
            root.path(),
            &policy,
            deadline,
        );
        assert!(
            late.is_err(),
            "late Start of an attempt closed by StopFailed must be rejected"
        );
    }

    #[test]
    fn adxlet_observe_requires_exact_fenced_reaped_generation() {
        use crate::rollout::{adxlet::Operation, relay::Reply};
        let root = tempfile::tempdir().unwrap();
        let mut service = ManagedService::new(process(
            "node",
            Role::Adxlet,
            "/unused",
            vec![],
            Some(root.path().join("admin.sock")),
        ));
        service.signal_target = SignalTarget::Process;
        service.rollout_task = Some("task".into());
        service.rollout_fenced = true;
        service.generation_pid = Some(42);
        let policy = crate::logging::Policy::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                ..Default::default()
            },
        };
        service.process_identity = Some(identity.clone());
        assert_eq!(service.failed_signal(), libc::SIGUSR1);
        service.config.role = Role::Relay;
        assert_eq!(service.failed_signal(), libc::SIGTERM);
        service.config.role = Role::Adxlet;
        let observe = |service: &mut ManagedService, pid| {
            adxlet_rollout_command(
                std::slice::from_mut(service),
                "node".into(),
                "task".into(),
                Operation::Observe {
                    pid,
                    identity: identity.clone(),
                },
                root.path(),
                &policy,
                deadline,
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
        service.rollout_fenced = false;
        assert!(observe(&mut service, 42).is_err());

        let identity = crate::rollout::ProcessIdentity {
            node_id: "node".into(),
            identity: crate::rollout::ProcessSelection {
                ..Default::default()
            },
        };
        service.rollout_fenced = true;
        service.process_identity = Some(identity.clone());
        // Missing receipt is unknown, never proof that no candidate was created.
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            Operation::Receipt {
                identity: identity.clone(),
                old_pid: 42,
                attempt: "attempt".into(),
                pid: None
            },
            root.path(),
            &policy,
            deadline
        )
        .is_err());
        let mut wrong = identity.clone();
        wrong.identity.epoch += 1;
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            Operation::Receipt {
                identity: wrong,
                old_pid: 42,
                attempt: "attempt".into(),
                pid: None
            },
            root.path(),
            &policy,
            deadline
        )
        .is_err());
        // A completed task remains observable and replayable after its fence is cleared.
        let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        let pid = child.id();
        service.generation_pid = Some(pid);
        service.child = Some(child);
        service.rollout_fenced = false;
        service.rollout_task = None;
        let mut identity = identity;
        identity.identity.old_relay_session = "relay".into();
        service.process_identity = Some(identity.clone());
        service.process_completed = Some(("task".into(), 42, pid, "attempt".into()));
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut service),
                "node".into(),
                "task".into(),
                Operation::Receipt {
                    identity: identity.clone(),
                    old_pid: 42,
                    attempt: "attempt".into(),
                    pid: Some(pid)
                },
                root.path(),
                &policy,
                deadline
            )
            .unwrap(),
            Reply::Committed
        ));
        assert!(matches!(
            adxlet_rollout_command(
                std::slice::from_mut(&mut service),
                "node".into(),
                "task".into(),
                Operation::StopFailed {
                    identity: identity.clone(),
                    old_pid: 42,
                    pid: Some(pid),
                    attempt: "attempt".into(),
                },
                root.path(),
                &policy,
                deadline,
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
            matches!(adxlet_rollout_command(std::slice::from_mut(&mut service), "node".into(), "task".into(),
            Operation::Complete { pid, old_pid:42, attempt:"attempt".into(), identity:identity.clone() },
            root.path(), &policy, deadline).unwrap(), Reply::Candidate { pid:actual, exited:false } if actual == pid)
        );
        assert!(adxlet_rollout_command(
            std::slice::from_mut(&mut service),
            "node".into(),
            "task".into(),
            Operation::Complete {
                pid,
                old_pid: 43,
                attempt: "attempt".into(),
                identity
            },
            root.path(),
            &policy,
            deadline
        )
        .is_err());
        let child = service.child.as_mut().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn restart_failed_replays_after_reply_loss_without_requiring_failed_current_pid() {
        use crate::rollout::relay::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
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
        let observed = rollout_command(
            std::slice::from_mut(&mut owner),
            "node".into(),
            String::new(),
            Operation::RuntimeInspect,
            root.path(),
            &crate::logging::Policy::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            matches!(observed, Reply::Runtime { pid, admin_pid, relay: None, .. }
            if pid == owner.child.as_ref().unwrap().id() && admin_pid == pid)
        );
        owner.rollout_fenced = true;
        assert!(rollout_command(
            std::slice::from_mut(&mut owner),
            "node".into(),
            String::new(),
            Operation::RuntimeInspect,
            root.path(),
            &crate::logging::Policy::default(),
            Instant::now() + Duration::from_secs(5),
        )
        .is_err());
        owner.rollout_fenced = false;
        let mut relay = make("relay", Role::Relay);
        relay.config.binary = "/failed-candidate".into();
        relay
            .config
            .env
            .insert("ADX_SUPERVISOR_OWNER_SERVICE".into(), "node".into());
        relay.config.env.insert(
            "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET".into(),
            "/failed/route.sock".into(),
        );
        relay.rollout_task = Some("task".into());
        relay.rollout_fenced = true;
        relay.generation_pid = Some(42);
        relay.exit_status = Some(failed_status(1));
        // Generations share the stable service identity; the retiring original
        // keeps its real rendered owner pairing, which the actor resolves.
        let mut original = make("relay", Role::Relay);
        original
            .config
            .env
            .insert("ADX_SUPERVISOR_OWNER_SERVICE".into(), "node".into());
        relay.retiring = Some(Box::new(original));
        let mut children = vec![owner, relay];
        let policy = crate::logging::Policy::default();
        let deadline = Instant::now() + Duration::from_secs(600);
        let operation = || Operation::RestartFailed {
            pid: 42,
            binary: "/bin/sleep".into(),
            directory: root.path().into(),
        };
        // The actor applied Restart, but the caller never received its reply.
        drop(
            rollout_command(
                &mut children,
                "relay".into(),
                "task".into(),
                operation(),
                root.path(),
                &policy,
                deadline,
            )
            .unwrap(),
        );
        let started_pid = children[1].child.as_ref().unwrap().id();
        assert_ne!(started_pid, 42);
        let reply = rollout_command(
            &mut children,
            "relay".into(),
            "task".into(),
            operation(),
            root.path(),
            &policy,
            deadline,
        )
        .unwrap();
        assert!(
            matches!(reply, Reply::State { candidate_pid:Some(pid), .. } if pid == started_pid)
        );
        let wrong_path = Operation::RestartFailed {
            pid: 42,
            binary: "/bin/sleep".into(),
            directory: root.path().join("other"),
        };
        assert!(rollout_command(
            &mut children,
            "relay".into(),
            "task".into(),
            wrong_path,
            root.path(),
            &policy,
            deadline
        )
        .is_err());
        // Explicitly reap the owner and the restarted candidate Child.
        for service in &mut children {
            if let Some(mut child) = service.child.take() {
                let _ = child.kill();
                child.wait().unwrap();
            }
        }
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
        service.rollout_task = Some("task".into());
        service.rollout_fenced = true;
        service.generation_pid = Some(pid);
        // This test did not create a process group; signal only its owned PID.
        service.signal_target = SignalTarget::Process;
        assert_eq!(service.child.as_ref().map(Child::id), Some(pid));
        assert!(service.rollout_fenced);
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
        assert!(service.rollback_stop.is_none());
        service.stop_failed(pid, deadline).unwrap();
        let frozen = service.rollback_stop;
        service
            .stop_failed(pid, deadline + Duration::from_secs(60))
            .unwrap();
        assert_eq!(service.rollback_stop, frozen);
    }

    #[test]
    fn release_precedes_paired_adxlet_liveness_and_keeps_ownership_guards() {
        use crate::rollout::relay::{Operation, Reply};
        let root = tempfile::tempdir().unwrap();
        let policy = crate::logging::Policy::default();
        let release = |relay: &mut ManagedService| {
            rollout_command(
                std::slice::from_mut(relay),
                "relay".into(),
                "task".into(),
                Operation::Release,
                root.path(),
                &policy,
                Instant::now() + Duration::from_secs(5),
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
            relay.rollout_task = Some("task".into());
            relay.rollout_fenced = true;
            relay
        };
        // No paired adxlet exists: Release must not depend on its liveness.
        let mut relay = fenced();
        assert!(matches!(
            release(&mut relay).unwrap(),
            Reply::State {
                exited: false,
                success: false,
                candidate_pid: None
            }
        ));
        assert!(!relay.rollout_fenced);
        assert!(relay.rollout_task.is_none());
        let mut other = fenced();
        other.rollout_task = Some("other".into());
        assert!(release(&mut other).is_err());
        assert!(other.rollout_fenced);
        let mut retained = fenced();
        retained.retiring = Some(Box::new(fenced()));
        assert!(release(&mut retained).is_err());
        assert!(retained.rollout_fenced);
        assert_eq!(retained.rollout_task.as_deref(), Some("task"));
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
