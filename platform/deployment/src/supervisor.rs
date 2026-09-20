use crate::{
    config::{Deployment, Process},
    Result,
};
use adx_protocol::node_proxy as pb;
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
    net::{UnixListener, UnixStream},
};
use tower::service_fn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    Status,
    Stop,
}

impl Request {
    fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Stop => "stop",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "status" => Some(Self::Status),
            "stop" => Some(Self::Stop),
            _ => None,
        }
    }
}

struct ManagedService {
    config: Process,
    child: Option<Child>,
    restarts: u32,
    next_start: Instant,
    failed: bool,
    capture: Option<crate::logging::Capture>,
    finished_log: Option<crate::logging::Status>,
}
impl ManagedService {
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
        self.capture = capture;
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
                let id = child.id() as i32;
                // SAFETY: the child has not been reaped, so its process group cannot belong to a
                // reused PID. kill does not retain the scalar process-group identifier.
                if unsafe { libc::kill(-id, signal) } != 0 {
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
    tokio::time::timeout(timeout, async {
        let mut stream = UnixStream::connect(root.join("supervisor.sock")).await?;
        stream
            .write_all(format!("{}\n", request.as_str()).as_bytes())
            .await?;
        let mut data = Vec::new();
        stream.take(1024 * 1024).read_to_end(&mut data).await?;
        let response: Value = serde_json::from_slice(&data)?;
        if response.get("ok") != Some(&Value::Bool(true)) {
            return Err(
                "supervisor operation failed; dependencies retained, inspect component logs".into(),
            );
        }
        Ok(response)
    })
    .await?
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
async fn stop(children: &mut [ManagedService], timeout: Duration) -> Result<()> {
    // All node cleanup must succeed while Master, Redis and local proxies remain alive.
    for service in children.iter_mut() {
        if let Some(path) = &service.config.admin_socket {
            if service
                .child
                .as_mut()
                .map(|child| child.try_wait())
                .transpose()?
                .flatten()
                .is_some()
                || service.child.is_none()
            {
                return Err("Node Manager unavailable for cleanup".into());
            }
            drain(path.clone(), timeout).await?;
        }
    }
    for service in children.iter_mut().rev() {
        service.signal(libc::SIGTERM)?;
        let deadline = Instant::now() + timeout;
        while let Some(child) = &mut service.child {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                service.signal(libc::SIGKILL)?;
                if let Some(child) = &mut service.child {
                    child.wait()?;
                }
                return Err("service termination deadline exceeded".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        service.finish_logs().await?;
    }
    Ok(())
}
fn status(children: &[ManagedService]) -> Value {
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
        };
        if let Err(error) = service.start(&logs, &deployment.logging) {
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
                    Some(Request::Status) => status(&children),
                    Some(Request::Stop) => {
                        match stop(
                            &mut children,
                            Duration::from_secs(deployment.stop_timeout_seconds),
                        )
                        .await
                        {
                            Ok(()) => {
                                let service_status = status(&children)
                                    .get("services")
                                    .cloned()
                                    .unwrap_or_else(|| json!([]));
                                json!({
                                    "ok": true,
                                    "stopped": true,
                                    "services": service_status,
                                })
                            }
                            Err(_) => {
                                json!({"ok": false, "error": "cleanup or termination failed"})
                            }
                        }
                    }
                    None => json!({"ok": false, "error": "unknown action"}),
                };
                let should_exit = request == Some(Request::Stop)
                    && response.get("ok") == Some(&Value::Bool(true));

                let mut stream = reader.into_inner().into_inner();
                let _ = stream.write_all(&serde_json::to_vec(&response)?).await;
                let _ = stream.shutdown().await;
                if should_exit {
                    return Ok(());
                }
            }
            _ = term.recv() => {
                if stop(
                    &mut children,
                    Duration::from_secs(deployment.stop_timeout_seconds),
                )
                .await
                .is_ok()
                {
                    return Ok(());
                }
                eprintln!("stop cleanup incomplete; dependencies remain running");
            }
            _ = interrupt.recv() => {
                if stop(
                    &mut children,
                    Duration::from_secs(deployment.stop_timeout_seconds),
                )
                .await
                .is_ok()
                {
                    return Ok(());
                }
                eprintln!("stop cleanup incomplete; dependencies remain running");
            }
            _ = tick.tick() => {
                for service in &mut children {
                    if let Some(child) = &mut service.child {
                        if child.try_wait()?.is_some() {
                            service.child = None;
                            service.finish_logs().await?;
                            service.next_start = Instant::now()
                                + Duration::from_millis(deployment.restart_delay_ms);
                        }
                    }
                    if service.child.is_none()
                        && !service.failed
                        && Instant::now() >= service.next_start
                    {
                        if service.restarts >= deployment.restart_limit {
                            service.failed = true;
                            continue;
                        }
                        service.restarts += 1;
                        if service.start(&logs, &deployment.logging).is_err() {
                            service.next_start = Instant::now()
                                + Duration::from_millis(deployment.restart_delay_ms);
                        }
                    }
                }
            }
        }
    }
}
