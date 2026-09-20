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
struct Managed {
    config: Process,
    child: Option<Child>,
    restarts: u32,
    next_start: Instant,
    failed: bool,
    capture: Option<crate::logging::Capture>,
    finished_log: Option<crate::logging::Status>,
}
impl Managed {
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
                    let e = std::io::Error::last_os_error();
                    if e.raw_os_error() != Some(libc::ESRCH) {
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }
}
impl Drop for Managed {
    fn drop(&mut self) {
        let _ = self.signal(libc::SIGKILL);
        if let Some(c) = &mut self.child {
            let _ = c.wait();
        }
    }
}
struct Guard {
    file: File,
    socket: PathBuf,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket);
        // SAFETY: file owns this valid descriptor for the duration of the call; flock does not
        // retain it. Unlock failure cannot be recovered while dropping the guard.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
fn lock(root: &Path) -> Result<Guard> {
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
    Ok(Guard { file, socket })
}
pub async fn request(root: &Path, action: &str, timeout: Duration) -> Result<Value> {
    tokio::time::timeout(timeout, async {
        let mut s = UnixStream::connect(root.join("supervisor.sock")).await?;
        s.write_all(format!("{action}\n").as_bytes()).await?;
        let mut data = Vec::new();
        s.take(1024 * 1024).read_to_end(&mut data).await?;
        let v: Value = serde_json::from_slice(&data)?;
        if v["ok"] != true {
            return Err(
                "supervisor operation failed; dependencies retained, inspect component logs".into(),
            );
        }
        Ok(v)
    })
    .await?
}
async fn drain(path: PathBuf, timeout: Duration) -> Result<()> {
    tokio::time::timeout(timeout, async move {
        let channel = tonic::transport::Endpoint::from_static("http://node-admin")
            .connect_with_connector(service_fn(move |_| {
                let p = path.clone();
                async move { UnixStream::connect(p).await.map(TokioIo::new) }
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
async fn stop(children: &mut [Managed], timeout: Duration) -> Result<()> {
    // All node cleanup must succeed while Master, Redis and local proxies remain alive.
    for m in children.iter_mut() {
        if let Some(path) = &m.config.admin_socket {
            if m.child
                .as_mut()
                .map(|c| c.try_wait())
                .transpose()?
                .flatten()
                .is_some()
                || m.child.is_none()
            {
                return Err("Node Manager unavailable for cleanup".into());
            }
            drain(path.clone(), timeout).await?;
        }
    }
    for m in children.iter_mut().rev() {
        m.signal(libc::SIGTERM)?;
        let end = Instant::now() + timeout;
        while let Some(child) = &mut m.child {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= end {
                m.signal(libc::SIGKILL)?;
                if let Some(c) = &mut m.child {
                    c.wait()?;
                }
                return Err("service termination deadline exceeded".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        m.finish_logs().await?;
    }
    Ok(())
}
fn status(children: &[Managed]) -> Value {
    json!({"ok":true,"services":children.iter().map(|m|json!({"id":m.config.id,"role":m.config.role,"pid":m.child.as_ref().map(Child::id),"restarts":m.restarts,"failed":m.failed,"logging":m.capture.as_ref().map(|capture|capture.status()).or_else(||m.finished_log.clone())})).collect::<Vec<_>>()})
}
pub async fn run(d: Deployment) -> Result<()> {
    d.validate()?;
    let guard = lock(&d.state_dir)?;
    let configdir = d.state_dir.join(format!("config-{}", std::process::id()));
    let specs = d.render(&configdir)?;
    // Validate the complete executable set before starting anything.
    for s in &specs {
        let m = fs::metadata(&s.binary)?;
        if !m.is_file() || m.permissions().mode() & 0o111 == 0 {
            return Err("package executable missing or not executable".into());
        }
    }
    for service in &d.services {
        if service.role == crate::config::Role::Redis {
            let config = crate::config::RedisConfig::parse(&service.config)?;
            fs::create_dir_all(config.data_dir)?;
        }
    }
    let logs = d.state_dir.join("logs");
    fs::create_dir_all(&logs)?;
    let listener = UnixListener::bind(&guard.socket)?;
    fs::set_permissions(&guard.socket, fs::Permissions::from_mode(0o600))?;
    let mut children: Vec<Managed> = Vec::new();
    for config in specs {
        let mut m = Managed {
            config,
            child: None,
            restarts: 0,
            next_start: Instant::now(),
            failed: false,
            capture: None,
            finished_log: None,
        };
        if let Err(error) = m.start(&logs, &d.logging) {
            eprintln!("service {} spawn failed: {error}", m.config.id);
            m.next_start = Instant::now() + Duration::from_millis(d.restart_delay_ms);
        }
        children.push(m);
    }
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
         accepted=listener.accept()=>{
          let (stream,_)=accepted?;let mut stream=BufReader::new(stream.take(128));let mut line=String::new();
          if tokio::time::timeout(Duration::from_secs(2),stream.read_line(&mut line)).await.is_err(){continue;}
          let quitting=line.trim()=="stop";
          let response=match line.trim(){"status"=>status(&children),"stop"=>match stop(&mut children,Duration::from_secs(d.stop_timeout_seconds)).await{Ok(())=>json!({"ok":true,"stopped":true,"services":status(&children)["services"]}),Err(_)=>json!({"ok":false,"error":"cleanup or termination failed"})},_=>json!({"ok":false,"error":"unknown action"})};
          let done=quitting&&response["ok"]==true;
          let mut stream=stream.into_inner().into_inner();let _=stream.write_all(&serde_json::to_vec(&response)?).await;let _=stream.shutdown().await;
          if done{return Ok(());}
         }
         _=term.recv()=>{if stop(&mut children,Duration::from_secs(d.stop_timeout_seconds)).await.is_ok(){return Ok(());}eprintln!("stop cleanup incomplete; dependencies remain running");}
         _=interrupt.recv()=>{if stop(&mut children,Duration::from_secs(d.stop_timeout_seconds)).await.is_ok(){return Ok(());}eprintln!("stop cleanup incomplete; dependencies remain running");}
         _=tick.tick()=>{
          for m in &mut children {
           if let Some(c)=&mut m.child {if c.try_wait()?.is_some(){m.child=None;m.finish_logs().await?;m.next_start=Instant::now()+Duration::from_millis(d.restart_delay_ms);}}
           if m.child.is_none()&&!m.failed&&Instant::now()>=m.next_start {
            if m.restarts>=d.restart_limit{m.failed=true;continue;}
            m.restarts+=1;
            if m.start(&logs, &d.logging).is_err(){m.next_start=Instant::now()+Duration::from_millis(d.restart_delay_ms);}
           }
          }
         }
        }
    }
}
