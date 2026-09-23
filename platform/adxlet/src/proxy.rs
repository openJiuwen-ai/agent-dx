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
