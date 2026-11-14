//! Bounded worker lifecycle owned by the existing Gateway process.
use super::super::composio::WebhookVerifier;
use super::{DeliveryPort, Error, Inbox, WebhookHttp, Worker};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};

/// Opaque product task: the shared service only spawns and supplies shutdown.
pub(crate) struct BackgroundService(IngestionRuntime);

impl BackgroundService {
    pub(crate) fn new(runtime: IngestionRuntime) -> Self {
        Self(runtime)
    }

    pub(crate) async fn serve(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), crate::ingress::service::ServiceError> {
        self.0
            .serve(shutdown)
            .await
            .map_err(|error| Box::new(error) as crate::ingress::service::ServiceError)
    }
}

pub struct IngestionRuntime {
    pub webhook: Arc<WebhookHttp>,
    shutdown: watch::Sender<bool>,
    tasks: JoinSet<Result<(), Error>>,
}

impl IngestionRuntime {
    /// All dependencies are real, initialized owners. No empty delivery,
    /// anonymous authorization or volatile queue fallback is installed.
    pub fn start(
        inbox: Inbox,
        verifier: Arc<WebhookVerifier>,
        delivery: Arc<dyn DeliveryPort>,
        workers: usize,
        lease: Duration,
    ) -> Result<Self, Error> {
        if !(1..=16).contains(&workers) || tokio::runtime::Handle::try_current().is_err() {
            return Err(Error::InvalidLimit);
        }
        // Validate before spawning any task, so partial startup cannot leak.
        let worker = Arc::new(Worker::new(inbox.clone(), delivery, lease)?);
        let (shutdown, receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        for _ in 0..workers {
            let worker = worker.clone();
            let receiver = receiver.clone();
            tasks.spawn(async move { worker.run(receiver).await });
        }
        let maintenance = inbox.clone();
        let mut stop = receiver.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _=interval.tick()=>if maintenance.purge_terminal(1000).await.is_err() {
                        tracing::warn!("ingestion terminal retention temporarily unavailable");
                    },
                    changed=stop.changed()=>if changed.is_err() || *stop.borrow(){return Ok(());},
                }
            }
        });
        Ok(Self {
            webhook: Arc::new(WebhookHttp::new(verifier, inbox)),
            shutdown,
            tasks,
        })
    }

    /// Completion is observed directly, without shared-layer health polling.
    pub async fn serve(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), Error> {
        if *shutdown.borrow() {
            return self.stop().await;
        }
        if wait_for_runtime(&mut self.tasks, &mut shutdown).await {
            self.stop().await
        } else {
            Err(Error::Unavailable)
        }
    }

    pub async fn stop(mut self) -> Result<(), Error> {
        let _ = self.shutdown.send(true);
        let mut result = Ok(());
        while let Some(task) = self.tasks.join_next().await {
            if !matches!(task, Ok(Ok(()))) {
                result = Err(Error::Unavailable);
            }
        }
        result
    }
}

impl Drop for IngestionRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.tasks.abort_all();
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

async fn wait_for_runtime(
    tasks: &mut JoinSet<Result<(), Error>>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = wait_for_shutdown(shutdown) => true,
        _ = tasks.join_next() => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn worker_failure_is_observed_without_a_poll_interval() {
        let (_stop, mut receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Err(Error::Unavailable) });
        assert!(!tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_runtime(&mut tasks, &mut receiver)
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn shutdown_stops_waiting_while_an_unfinished_worker_is_running() {
        let (stop, mut receiver) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending());
        stop.send(true).unwrap();
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_runtime(&mut tasks, &mut receiver)
        )
        .await
        .unwrap());
    }
}
