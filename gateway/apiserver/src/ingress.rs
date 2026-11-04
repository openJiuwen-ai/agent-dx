//! Optional in-process Ingress using the same service as the standalone binary.
use adx_agent_core::environment::Environment;
use adx_process::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::{
    config::IngressConfig,
    ingress::{coordinator_routes::ControlConfig, IngressReadiness, IngressService},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::oneshot, task::JoinHandle};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct EmbeddedIngress {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    readiness: IngressReadiness,
}

impl EmbeddedIngress {
    /// Start the embedded Ingress. `drain_budget` overrides the Ingress's own
    /// drain timeout so the host's shutdown paths (management HTTP,
    /// background creates, embedded Activator operations, Ingress drain)
    /// share ONE budget starting at the SAME instant (SIGTERM) — the
    /// joint-exit contract of the co-process rolling upgrade.
    pub async fn start(
        control: ControlConfig,
        environment_service: Option<Arc<dyn Environment>>,
        directory: Arc<dyn data_plane_gateway::ingress::environment_files::EnvironmentDirectory>,
        drain_budget: Duration,
    ) -> Result<Self> {
        let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
        adx_observability::info!(nofile_soft_limit, "embedded Ingress FD limit configured");
        let mut config = IngressConfig::from_env()?;
        config.drain_timeout = drain_budget;
        let service = match environment_service {
            Some(service) => {
                IngressService::bind_with_environment_service(config, control, service, directory)
                    .await?
            }
            None => IngressService::bind(config, control).await?,
        };
        let readiness = service.readiness();
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
            readiness,
        })
    }

    pub async fn failed(&mut self) -> Result<()> {
        let result = (&mut self.task).await?;
        match result {
            Ok(()) => Err("embedded Ingress exited".into()),
            Err(error) => Err(error),
        }
    }

    /// The embedded Ingress's admission readiness (route sync, enabled entry
    /// paths, not draining). The host folds this into its own `/readyz`.
    pub fn readiness(&self) -> &IngressReadiness {
        &self.readiness
    }

    /// Trigger the embedded Ingress drain without waiting for it to finish,
    /// so it can drain in parallel with management HTTP connection shutdown
    /// instead of serially after it. Idempotent.
    pub fn begin_drain(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    /// Wait for the embedded Ingress task to finish after `begin_drain`.
    pub async fn await_completion(&mut self) -> Result<()> {
        (&mut self.task).await?
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.begin_drain();
        self.await_completion().await
    }
}

impl Drop for EmbeddedIngress {
    fn drop(&mut self) {
        self.task.abort();
    }
}
