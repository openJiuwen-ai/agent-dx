//! Optional in-process Edge using the same service as the standalone binary.
use data_plane_gateway::{
    common::resource::raise_nofile_soft_limit_from_env,
    config::EdgeFrontendConfig,
    edge::{master_routes::ControlConfig, EdgeFrontendService},
};
use tokio::{sync::oneshot, task::JoinHandle};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct EmbeddedEdge {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl EmbeddedEdge {
    pub async fn start(control: ControlConfig) -> Result<Self> {
        let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
        adx_observability::info!(nofile_soft_limit, "embedded Edge FD limit configured");
        let service = EdgeFrontendService::bind(EdgeFrontendConfig::from_env()?, control).await?;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            service
                .serve(async {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(Self {
            stop: Some(stop),
            task,
        })
    }

    pub async fn failed(&mut self) -> Result<()> {
        let result = (&mut self.task).await?;
        match result {
            Ok(()) => Err("embedded Edge exited".into()),
            Err(error) => Err(error),
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        (&mut self.task).await?
    }
}

impl Drop for EmbeddedEdge {
    fn drop(&mut self) {
        self.task.abort();
    }
}
