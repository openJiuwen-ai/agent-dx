//! Optional process composition. Both modes keep the same protected binding RPC.
use adx_core::{Error, Result};
use data_plane_gateway::{config::RelayConfig, node::RelayService};
use std::path::Path;
use tokio::{sync::oneshot, task::JoinHandle};
#[derive(Default, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    Standalone,
    #[default]
    Embedded,
}

/// Process exit intent: SIGUSR1 preserves ownership; SIGTERM/SIGINT depart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownMode {
    Preserve,
    Drain,
}
pub struct EmbeddedProxy {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}
impl EmbeddedProxy {
    pub async fn start(config: RelayConfig, route_socket: &Path) -> Result<Self> {
        let dir = config
            .activity_uds_dir
            .as_ref()
            .ok_or_else(|| Error::Invalid("embedded proxy control directory required".into()))?;
        if Path::new(dir).join("route.sock") != route_socket {
            return Err(Error::Invalid(
                "embedded proxy route socket must match Adxlet".into(),
            ));
        }
        let service = RelayService::bind(config)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            service
                .serve(async {
                    let _ = stopped.await;
                })
                .await
                .map_err(|e| Error::Unavailable(e.to_string()))
        });
        Ok(Self {
            stop: Some(stop),
            task,
        })
    }
    pub async fn failed(&mut self) -> Result<()> {
        let result = (&mut self.task)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        match result {
            Ok(()) => Err(Error::Unavailable("embedded Relay exited".into())),
            Err(error) => Err(error),
        }
    }
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.task)
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?
    }
}
impl Drop for EmbeddedProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Close admission on shutdown, keeping node services alive through Relay drain.
/// Returns admission, service or Relay failures to the process owner.
pub async fn serve_until_shutdown<E>(
    manager: &crate::Adxlet,
    mut proxy: Option<EmbeddedProxy>,
    services: impl std::future::Future<Output = std::result::Result<(), E>>,
    signal: impl std::future::Future<Output = ShutdownMode>,
) -> std::result::Result<ShutdownMode, E>
where
    E: From<Error>,
{
    tokio::pin!(services);
    // A background service exit alone does not authorize planned departure.
    let mode = tokio::select! {
        result = async {
            match &mut proxy {
                Some(proxy) => proxy.failed().await,
                None => std::future::pending::<Result<()>>().await,
            }
        } => return result.map(|()| ShutdownMode::Preserve).map_err(E::from),
        result = &mut services => return result.map(|()| ShutdownMode::Preserve),
        mode = signal => mode,
    };
    manager.begin_shutdown().map_err(E::from)?;
    let drain = async {
        if let Some(proxy) = proxy {
            proxy.shutdown().await.map_err(E::from)?;
        }
        Ok(())
    };
    // Prefer a completed drain when both branches are ready. A service exit
    // during drain returns immediately: dropping EmbeddedProxy aborts Relay
    // rather than waiting without heartbeat/runtime control services.
    tokio::select! {
        biased;
        result = drain => result.map(|()| mode),
        result = &mut services => result.map(|()| mode),
    }
}

#[cfg(test)]
#[path = "proxy_shutdown_tests.rs"]
mod shutdown_tests;

#[cfg(test)]
mod tests {
    use super::ProxyMode;

    #[test]
    fn proxy_is_embedded_by_default() {
        assert!(ProxyMode::default() == ProxyMode::Embedded);
    }

    #[test]
    fn standalone_mode_remains_explicitly_selectable() {
        let mode: ProxyMode = serde_json::from_str("\"standalone\"").unwrap();
        assert!(mode == ProxyMode::Standalone);
    }
}
