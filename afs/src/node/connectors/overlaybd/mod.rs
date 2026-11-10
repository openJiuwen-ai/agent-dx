//! Node-local OverlayBD block export. AFS serves the backing blob ranges.
mod device;

use super::{ConnectorContext, ConnectorModule};
use crate::node::vfs::lazy_fill::LazyFillRegistry;
use afs_error::{Error, Result};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    sync::{Arc, OnceLock},
};
use tokio::{
    net::UnixListener,
    sync::{Mutex, Semaphore, oneshot},
    task::JoinHandle,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OverlaybdConfig {
    pub enabled: bool,
    /// Keep the kernel device and resume reads after this process exits.
    pub recover_on_restart: bool,
    /// Trusted local directory containing image.json and global.json.
    pub directory: PathBuf,
    /// Public image reference routed to this configured layer stack.
    pub image: String,
}

pub struct OverlaybdConnector {
    config: OverlaybdConfig,
    server: OnceLock<Mutex<Option<Server>>>,
}

const CONTROL_CONCURRENCY: usize = 32;

#[derive(Clone)]
struct ControlState {
    devices: Arc<Mutex<device::Devices>>,
    admission: Arc<Semaphore>,
}

struct Server {
    admission: Arc<Semaphore>,
    close: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
    recovery: Option<JoinHandle<()>>,
    socket: PathBuf,
    _lock: File,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    runtime_id: String,
    image: Option<String>,
    acquire_deadline_ms: Option<u64>,
}

type HttpError = (StatusCode, String);

impl Request {
    fn validate(&self) -> std::result::Result<(), HttpError> {
        if self.runtime_id.is_empty() || self.runtime_id.len() > 1024 {
            return Err((StatusCode::BAD_REQUEST, "invalid runtime ID".into()));
        }
        Ok(())
    }
}

impl OverlaybdConnector {
    pub fn new(config: OverlaybdConfig) -> Result<Self> {
        if !config.directory.is_absolute() || config.image.trim().is_empty() {
            return Err(Error::coded(
                afs_error::CONFIG_INVALID,
                "OverlayBD requires an absolute directory and a configured image",
            ));
        }
        Ok(Self {
            config,
            server: OnceLock::new(),
        })
    }

    fn serve_control(&self) -> std::io::Result<Server> {
        let dir = &self.config.directory;
        let mut create = fs::DirBuilder::new();
        create.mode(0o700);
        match create.create(dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let metadata = fs::symlink_metadata(dir)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(std::io::Error::other("OverlayBD directory must be private"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("connector.lock"))?;
        lock.try_lock().map_err(std::io::Error::other)?;
        let socket = dir.join("connector.sock");
        match fs::remove_file(&socket) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let service = device::Devices::shared(self.config.clone())?;
        let admission = Arc::new(Semaphore::new(CONTROL_CONCURRENCY));
        let listener = UnixListener::bind(&socket)?;
        let recovery = self.config.recover_on_restart.then(|| {
            let service = service.clone();
            tokio::spawn(async move {
                if let Err(error) = service.lock().await.recover().await {
                    afs_logging::error!("overlaybd.recovery_failed"; "error" => error.to_string());
                }
            })
        });
        let router = Router::new()
            .route("/acquire", post(acquire))
            .route("/release", post(release))
            .with_state(ControlState {
                devices: service,
                admission: admission.clone(),
            });
        let (close, closed) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = closed.await;
                })
                .await
        });
        Ok(Server {
            admission,
            close,
            task,
            recovery,
            socket,
            _lock: lock,
        })
    }
}

impl ConnectorModule for OverlaybdConnector {
    fn id(&self) -> &'static str {
        "overlaybd"
    }

    fn register_lazy_handlers(&self, _: &mut LazyFillRegistry) -> Result<()> {
        Ok(())
    }

    fn http_routes(&self, _: ConnectorContext) -> Result<Router> {
        Ok(Router::new())
    }

    fn start(&self) -> Result<()> {
        if self.server.get().is_some() {
            return Err(config_error("OverlayBD connector already assembled"));
        }
        let server = self.serve_control().map_err(config_error)?;
        self.server
            .set(Mutex::new(Some(server)))
            .map_err(|_| config_error("OverlayBD connector already assembled"))?;
        Ok(())
    }

    fn stop(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(server) = self.server.get()
                && let Some(server) = server.lock().await.take()
            {
                server.admission.close();
                let _ = server.close.send(());
                let result = server
                    .task
                    .await
                    .map_err(config_error)
                    .and_then(|result| result.map_err(config_error));
                let recovery = if let Some(recovery) = server.recovery {
                    recovery.await.map_err(config_error)
                } else {
                    Ok(())
                };
                // A disconnected client may leave a journal operation in flight.
                while server.admission.available_permits() != CONTROL_CONCURRENCY {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                let removed = fs::remove_file(server.socket).map_err(config_error);
                result.and(recovery).and(removed)?;
            }
            Ok(())
        })
    }
}

async fn acquire(
    State(control): State<ControlState>,
    Json(request): Json<Request>,
) -> std::result::Result<Json<Option<PathBuf>>, HttpError> {
    request.validate()?;
    let image = request
        .image
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing image".into()))?;
    let deadline = request
        .acquire_deadline_ms
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "missing acquire deadline".into()))?;
    let permit = control.admission.try_acquire_owned().map_err(http_error)?;
    // A caller timeout does not interrupt mount creation or its journal write.
    tokio::spawn(async move {
        let _permit = permit;
        control
            .devices
            .lock()
            .await
            .acquire(request.runtime_id, &image, deadline)
            .await
    })
    .await
    .map_err(http_error)?
    .map(Json)
    .map_err(http_error)
}

async fn release(
    State(control): State<ControlState>,
    Json(request): Json<Request>,
) -> std::result::Result<StatusCode, HttpError> {
    request.validate()?;
    let permit = control.admission.try_acquire_owned().map_err(http_error)?;
    tokio::spawn(async move {
        let _permit = permit;
        control
            .devices
            .lock()
            .await
            .release(&request.runtime_id)
            .await
    })
    .await
    .map_err(http_error)?
    .map_err(http_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn http_error(error: impl std::fmt::Display) -> HttpError {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::coded(afs_error::CONFIG_INVALID, error.to_string())
}
