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
}
impl ManagedService {
    fn is_afs(&self) -> bool {
        matches!(self.config.role, Role::AfsMeta | Role::AfsNode)
    }

    fn start(&mut self, logs: &Path, policy: &crate::logging::Policy) -> Result<()> {
        let mut command = Command::new(&self.config.binary);
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
    let socket = root.join("supervisor.sock");
    tokio::time::timeout(timeout, async {
        let mut stream = UnixStream::connect(&socket).await.map_err(|error| {
            format!(
                "cannot connect to the supervisor socket {}: {error}; the supervisor is not running, or the deployment state_dir differs from the running instance",
                socket.display()
            )
        })?;
        stream
            .write_all(format!("{}\n", request.as_str()).as_bytes())
            .await?;
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
    .await
    .map_err(|_| {
        format!(
            "supervisor did not answer within {}s at {}; it may be stopped, or the deployment state_dir differs from the running instance",
            timeout.as_secs(),
            socket.display()
        )
    })?
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
pub async fn run(deployment: Deployment) -> Result<()> {
    deployment.validate()?;
    let guard = acquire_lock(&deployment.state_dir)?;
    let config_directory = deployment
        .state_dir
        .join(format!("config-{}", std::process::id()));
    let process_specs = deployment.render(&config_directory)?;
    // Validate the complete executable set before starting anything.
    for process in &process_specs {
        let metadata = fs::metadata(&process.binary)?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
            return Err("package executable missing or not executable".into());
        }
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
        let mut service = ManagedService {
            config,
            child: None,
            restarts: 0,
            next_start: Instant::now(),
            failed: false,
            capture: None,
            finished_log: None,
            signal_target: SignalTarget::ProcessGroup,
        };
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
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let mut reader = BufReader::new(stream.take(128));
                let mut request_line = String::new();
                let read_result = tokio::time::timeout(
                    Duration::from_secs(2),
                    reader.read_line(&mut request_line),
                )
                .await;
                match read_result {
                    Ok(Ok(0)) | Err(_) => continue,
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => return Err(error.into()),
                }

                let request = Request::parse(request_line.trim());
                let response = match request {
                    Some(Request::Status) => status(&children, deployment.shutdown_mode).await,
                    Some(Request::Stop { mode }) => {
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
                    None => json!({"ok": false, "error": "unknown action"}),
                };
                let should_exit = matches!(request, Some(Request::Stop { .. }))
                    && response.get("ok") == Some(&Value::Bool(true));

                let mut stream = reader.into_inner().into_inner();
                let _ = stream.write_all(&serde_json::to_vec(&response)?).await;
                let _ = stream.shutdown().await;
                if should_exit {
                    return Ok(());
                }
            }
            _ = term.recv() => {
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
                    if let Some(child) = &mut service.child {
                        if let Some(status) = child.try_wait()? {
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
