//! Workload-local checkpoint trigger. Adxlet consumes the pending request
//! through runtime status; only its durable completion ACK releases the caller.
use super::{control, RuntimeReadyState};
use adx_core::Error;
use serde::Deserialize;
use std::{
    io,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::UnixListener as StdListener,
    },
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::watch,
};

const MAX_CHECKPOINT_BODY_BYTES: usize = 1024;
const DEFAULT_CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CheckpointRequest {
    timeout_seconds: Option<u64>,
}

pub(crate) struct CheckpointSocket {
    listener: StdListener,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    ready: watch::Sender<RuntimeReadyState>,
}
impl CheckpointSocket {
    pub(crate) async fn bind(
        directory: &Path,
    ) -> io::Result<(Arc<Self>, watch::Receiver<RuntimeReadyState>)> {
        if !directory.is_absolute() {
            return Err(io::Error::other(
                "checkpoint socket directory must be absolute",
            ));
        }
        tokio::fs::create_dir_all(directory).await?;
        let path = directory.join("execd.sock");
        match tokio::fs::symlink_metadata(&path).await {
            Ok(meta) if meta.file_type().is_socket() => match UnixStream::connect(&path).await {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "checkpoint socket is in use",
                    ))
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    tokio::fs::remove_file(&path).await?
                }
                Err(error) => return Err(error),
            },
            Ok(_) => return Err(io::Error::other("checkpoint socket path is not a socket")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(path.clone())?.into_std()?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
        let (ready, receiver) = watch::channel(RuntimeReadyState::Starting);
        let socket = Arc::new(Self {
            listener,
            task: Mutex::new(None),
            ready,
        });
        socket.rearm()?;
        Ok((socket, receiver))
    }
    pub(crate) fn rearm(&self) -> io::Result<()> {
        let mut previous = self
            .task
            .lock()
            .map_err(|_| io::Error::other("checkpoint listener lock poisoned"))?;
        let listener = self.listener.try_clone()?;
        listener.set_nonblocking(true)?;
        let listener = UnixListener::from_std(listener)?;
        self.ready.send_replace(RuntimeReadyState::Ready);
        let ready = self.ready.clone();
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        tokio::spawn(async move {
                            let _ = handle(stream).await;
                        });
                    }
                    Err(error) => {
                        ready.send_replace(RuntimeReadyState::Failed(format!(
                            "checkpoint listener: {error}"
                        )));
                        break;
                    }
                }
            }
        });
        if let Some(old) = previous.replace(task) {
            old.abort();
        }
        Ok(())
    }
}
async fn handle(mut stream: UnixStream) -> io::Result<()> {
    let head = tokio::time::timeout(Duration::from_secs(5), async {
        let mut head = Vec::new();
        loop {
            let byte = stream.read_u8().await?;
            head.push(byte);
            if head.ends_with(b"\r\n\r\n") {
                return Ok::<_, io::Error>(head);
            }
            if head.len() >= 8192 {
                return Err(io::Error::other("headers too large"));
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "checkpoint header timeout"))??;
    let head = String::from_utf8_lossy(&head);
    let mut words = head.lines().next().unwrap_or_default().split_whitespace();
    let method = words.next().unwrap_or_default();
    let path = words.next().unwrap_or_default();
    let timeout = if path == "/checkpoint" && method == "POST" {
        read_timeout(&head, &mut stream).await
    } else {
        Ok(None)
    };
    let (status, body) = if path != "/checkpoint" {
        (404, serde_json::json!({"error":"not found"}))
    } else if method != "POST" {
        (405, serde_json::json!({"error":"method not allowed"}))
    } else if let Err(error) = &timeout {
        (400, serde_json::json!({"error":error.to_string()}))
    } else if let Some(controller) = control::current() {
        static SEQUENCE: AtomicU64 = AtomicU64::new(1);
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let id = format!("execd-{time}-{}", SEQUENCE.fetch_add(1, Ordering::Relaxed));
        let _activity = super::activity::enter(super::activity::ActivitySource::Checkpoint);
        match controller.request_checkpoint(id, timeout.unwrap()).await {
            Ok(()) => (200, serde_json::json!({"status":"completed"})),
            Err(Error::Invalid(error)) => (400, serde_json::json!({"error":error})),
            Err(Error::Conflict) => (
                409,
                serde_json::json!({"error":"checkpoint already in progress"}),
            ),
            Err(Error::Unavailable(error)) if error == "checkpoint request timed out" => {
                (504, serde_json::json!({"error":error}))
            }
            Err(error) => (503, serde_json::json!({"error":error.to_string()})),
        }
    } else {
        (
            503,
            serde_json::json!({"error":"runtime identity unavailable"}),
        )
    };
    let body = body.to_string();
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        504 => "Gateway Timeout",
        400 => "Bad Request",
        _ => "Service Unavailable",
    };
    stream.write_all(format!("HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    stream.shutdown().await
}

async fn read_timeout(head: &str, stream: &mut UnixStream) -> io::Result<Option<Duration>> {
    let mut content_length = None;
    for line in head.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding") && !value.trim().is_empty() {
            return Err(io::Error::other("transfer encoding is not supported"));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(io::Error::other("duplicate content-length"));
            }
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| io::Error::other("invalid content-length"))?,
            );
        }
    }
    let length = content_length.unwrap_or(0);
    if length > MAX_CHECKPOINT_BODY_BYTES {
        return Err(io::Error::other("checkpoint request body too large"));
    }
    if length == 0 {
        return Ok(Some(DEFAULT_CHECKPOINT_TIMEOUT));
    }
    let mut body = vec![0; length];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "checkpoint body timeout"))??;
    let request: CheckpointRequest =
        serde_json::from_slice(&body).map_err(|error| io::Error::other(error.to_string()))?;
    let Some(seconds) = request.timeout_seconds else {
        return Ok(Some(DEFAULT_CHECKPOINT_TIMEOUT));
    };
    if !(1..=3600).contains(&seconds) {
        return Err(io::Error::other(
            "checkpoint timeout must be between 1 and 3600 seconds",
        ));
    }
    Ok(Some(Duration::from_secs(seconds)))
}
