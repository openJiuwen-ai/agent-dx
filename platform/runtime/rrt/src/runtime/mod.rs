//! Capsule runtime boot, HTTP operations and checkpoint listener recovery.
use adx_core::runtime::RuntimeIdentity;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

macro_rules! rrt_info {
    ($($arg:tt)*) => {
        $crate::runtime::log_info(format_args!($($arg)*))
    };
}

macro_rules! rrt_warn {
    ($($arg:tt)*) => {
        $crate::runtime::log_warn(format_args!($($arg)*))
    };
}

macro_rules! rrt_error {
    ($($arg:tt)*) => {
        $crate::runtime::log_error(format_args!($($arg)*))
    };
}

mod activity;
mod bash;
mod checkpoint_socket;
pub(crate) mod child_env;
mod cmd;
mod codec;
pub mod control;
mod dispatch;
mod entrypoint;
mod fs;
mod httpserver;
mod tunnel;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeReadyState {
    Starting,
    Ready,
    Failed(String),
}

pub async fn serve_http_only(
    port: u16,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    boot(port, token, None).await
}
pub async fn serve_tunnel_only(ws_port: u16, http_port: u16) {
    tunnel::run_standalone(ws_port, http_port).await;
}
fn port(name: &str, default: u16) -> Result<u16, Box<dyn std::error::Error>> {
    let value = std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u16>()?;
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let tunnel = if std::env::var_os("RRT_TUNNEL_WS_PORT").is_some() {
        let ws = port("RRT_TUNNEL_WS_PORT", 8765)?;
        Some((
            ws,
            port("RRT_TUNNEL_HTTP_PORT", ws.checked_add(1).unwrap_or(0))?,
        ))
    } else {
        None
    };
    boot(
        port("RRT_HTTP_PORT", 50090)?,
        std::env::var("RRT_HTTP_TOKEN").ok(),
        tunnel,
    )
    .await
}
fn identity_from(
    environment: &std::collections::HashMap<String, String>,
) -> std::io::Result<Option<RuntimeIdentity>> {
    let keys = [
        "ADX_CAPSULE_ID",
        "ADX_RUNTIME_ID",
        "ADX_OWNERSHIP_GENERATION",
    ];
    if keys.iter().all(|key| !environment.contains_key(*key)) {
        return Ok(None);
    }
    let required = |key: &str| {
        environment
            .get(key)
            .cloned()
            .ok_or_else(|| std::io::Error::other(format!("missing {key}")))
    };
    let identity = RuntimeIdentity {
        capsule_id: required(keys[0])?,
        runtime_id: required(keys[1])?,
        ownership_generation: required(keys[2])?.parse().map_err(std::io::Error::other)?,
    };
    identity.validate().map_err(std::io::Error::other)?;
    Ok(Some(identity))
}
struct RuntimeHooks {
    checkpoint: Option<Arc<checkpoint_socket::CheckpointSocket>>,
    http: httpserver::HttpServerControl,
    tunnel: Option<tunnel::TunnelServerControl>,
    port: u16,
}
impl control::CheckpointHooks for RuntimeHooks {
    fn open(&self) -> std::io::Result<control::Handoff> {
        let opened = crate::startup::open_checkpoint_handoff()?
            .ok_or_else(|| std::io::Error::other("backend checkpoint handoff unavailable"))?;
        Ok(Box::pin(async {
            Ok(
                match crate::startup::wait_for_checkpoint_handoff(opened).await? {
                    crate::startup::CheckpointOutcome::Resume => control::HandoffOutcome::Resume,
                    crate::startup::CheckpointOutcome::Restore => control::HandoffOutcome::Restore,
                    crate::startup::CheckpointOutcome::Error => control::HandoffOutcome::Error,
                },
            )
        }))
    }
    fn restore(&self, previous: &RuntimeIdentity) -> std::io::Result<RuntimeIdentity> {
        Ok(self.restore_context(previous)?.target)
    }
    fn restore_context(
        &self,
        previous: &RuntimeIdentity,
    ) -> std::io::Result<adx_core::runtime::RuntimeRestore> {
        let path = crate::startup::restore_environment_file_path()
            .ok_or_else(|| std::io::Error::other("restore environment is required"))?;
        let environment = crate::startup::read_environment_file(&path)?;
        let target = identity_from(&environment)?
            .ok_or_else(|| std::io::Error::other("restored identity is missing"))?;
        let restored = adx_core::runtime::RuntimeRestore {
            target,
            origin: environment
                .get("ADX_RESTORE_ORIGIN")
                .filter(|value| !value.is_empty())
                .map(|value| serde_json::from_str(value).map_err(std::io::Error::other))
                .transpose()?,
        };
        restored.validate(previous).map_err(std::io::Error::other)?;
        if let Some(value) = environment.get("RRT_HTTP_PORT") {
            if value.parse::<u16>().ok() != Some(self.port) {
                return Err(std::io::Error::other(
                    "restore cannot change the inherited HTTP port",
                ));
            }
        }
        if let Some(tunnel) = &self.tunnel {
            tunnel.validate_restored_ports(&environment)?;
        }
        child_env::refresh_from_map(&environment);
        for (key, value) in &environment {
            std::env::set_var(key, value);
        }
        if let Some(token) = environment.get("RRT_HTTP_TOKEN") {
            self.http.update_token(token.clone())?;
        }
        self.http.rearm()?;
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.rearm()?;
        }
        if let Some(tunnel) = &self.tunnel {
            tunnel.rearm().map_err(std::io::Error::other)?;
        }
        Ok(restored)
    }
}
async fn boot(
    port: u16,
    token: Option<String>,
    tunnel_ports: Option<(u16, u16)>,
) -> Result<(), Box<dyn std::error::Error>> {
    child_env::initialize();
    entrypoint::initialize();
    let identity = identity_from(&std::env::vars().collect())?;
    let (http_ready, http) = start_http_server_with_control(port, token).await?;
    let mut readiness = vec![http_ready];
    let tunnel = if let Some((ws, http)) = tunnel_ports {
        let (ready, control) = start_tunnel_runtime_server_with_control(ws, http).await?;
        readiness.push(ready);
        Some(control)
    } else {
        None
    };
    let checkpoint = match std::env::var_os("ADX_RRT_CONTROL_SOCKET_PATH").filter(|v| !v.is_empty())
    {
        Some(directory) => {
            if identity.is_none() {
                return Err("checkpoint socket requires runtime identity".into());
            }
            let (socket, ready) =
                checkpoint_socket::CheckpointSocket::bind(std::path::Path::new(&directory)).await?;
            readiness.push(ready);
            Some(socket)
        }
        None => None,
    };
    for ready in &readiness {
        wait_for_runtime_ready(ready.clone()).await?;
    }
    entrypoint::complete_create().map_err(|failure| std::io::Error::other(failure.message))?;
    let hooks = Arc::new(RuntimeHooks {
        http,
        tunnel,
        port,
        checkpoint,
    });
    if let Some(identity) = identity {
        control::install(control::Controller::new(identity, hooks.clone())?)?;
    }
    let _keep_listeners_alive = hooks;
    let mut watchers = tokio::task::JoinSet::new();
    for mut ready in readiness {
        watchers.spawn(async move {
            loop {
                ready
                    .changed()
                    .await
                    .map_err(|_| "runtime listener closed".to_string())?;
                if let RuntimeReadyState::Failed(message) = ready.borrow().clone() {
                    return Err::<(), String>(message);
                }
            }
        });
    }
    match watchers.join_next().await {
        Some(result) => result??,
        None => return Err("runtime has no listeners".into()),
    }
    Ok(())
}

async fn start_http_server_with_control(
    port: u16,
    token: Option<String>,
) -> Result<
    (
        watch::Receiver<RuntimeReadyState>,
        httpserver::HttpServerControl,
    ),
    std::io::Error,
> {
    let listener = httpserver::bind(port).await.map_err(|err| {
        let message = format!("failed to bind RRT HTTP port {port}: {err}");
        rrt_error!("[rrt-http] readiness failed: {message}");
        std::io::Error::new(err.kind(), message)
    })?;
    let address = listener
        .local_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| format!("0.0.0.0:{port}"));
    let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
    let control = httpserver::HttpServerControl::start(listener, token, ready_tx)?;
    rrt_info!("[rrt-http] readiness ready address={address}");
    Ok((ready_rx, control))
}

async fn start_tunnel_runtime_server_with_control(
    ws_port: u16,
    http_port: u16,
) -> Result<
    (
        watch::Receiver<RuntimeReadyState>,
        tunnel::TunnelServerControl,
    ),
    String,
> {
    let bound = tunnel::BoundTunnelServers::bind(ws_port, http_port)
        .await
        .map_err(|message| {
            rrt_error!("[rrt-tunnel] readiness failed: {message}");
            message
        })?;
    let (ready_tx, ready_rx) = watch::channel(RuntimeReadyState::Starting);
    let control = tunnel::TunnelServerControl::start(bound, ready_tx).map_err(|message| {
        rrt_error!("[rrt-tunnel] readiness failed: {message}");
        message
    })?;
    rrt_info!("[rrt-tunnel] readiness ready ws=0.0.0.0:{ws_port} http=127.0.0.1:{http_port}");
    Ok((ready_rx, control))
}

async fn wait_for_runtime_ready(
    mut ready: watch::Receiver<RuntimeReadyState>,
) -> Result<(), String> {
    loop {
        match ready.borrow_and_update().clone() {
            RuntimeReadyState::Ready => return Ok(()),
            RuntimeReadyState::Failed(message) => return Err(message),
            RuntimeReadyState::Starting => {}
        }
        if ready.changed().await.is_err() {
            return Err(
                "RRT service readiness channel closed before startup completed".to_string(),
            );
        }
    }
}

pub(crate) fn log_info(args: std::fmt::Arguments<'_>) {
    log_stdout("INFO", args);
}

pub(crate) fn log_warn(args: std::fmt::Arguments<'_>) {
    log_stderr("WARN", args);
}

pub(crate) fn log_error(args: std::fmt::Arguments<'_>) {
    log_stderr("ERROR", args);
}

fn log_stdout(level: &str, args: std::fmt::Arguments<'_>) {
    let ts = format_local_timestamp();
    println!("[{ts} {level}] {args}");
}

fn log_stderr(level: &str, args: std::fmt::Arguments<'_>) {
    let ts = format_local_timestamp();
    eprintln!("[{ts} {level}] {args}");
}

fn format_local_timestamp() -> String {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(now) => now,
        Err(_) => Duration::from_secs(0),
    };
    let secs = now.as_secs() as i64;
    let millis = now.subsec_millis();
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}")
}

// Howard Hinnant civil_from_days algorithm. Input is Unix days since
// 1970-01-01 UTC; output is Gregorian UTC date. It avoids adding a time crate
// just for log formatting.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}
