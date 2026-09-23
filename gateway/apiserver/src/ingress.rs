//! Optional in-process Ingress using the same service as the standalone binary.
use adx_process::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::{
    config::IngressConfig,
    ingress::{coordinator_routes::ControlConfig, IngressService},
};
use tokio::{sync::oneshot, task::JoinHandle};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct EmbeddedIngress {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl EmbeddedIngress {
    pub async fn start(control: ControlConfig) -> Result<Self> {
        let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
        adx_observability::info!(nofile_soft_limit, "embedded Ingress FD limit configured");
        let service = IngressService::bind(IngressConfig::from_env()?, control).await?;
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
            Ok(()) => Err("embedded Ingress exited".into()),
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

impl Drop for EmbeddedIngress {
    fn drop(&mut self) {
        self.task.abort();
    }
}
