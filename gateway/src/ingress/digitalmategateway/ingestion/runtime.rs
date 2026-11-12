//! Bounded worker lifecycle owned by the existing Gateway process.
use super::super::composio::WebhookVerifier;
use super::{DeliveryPort, Error, Inbox, WebhookHttp, Worker};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

pub struct IngestionRuntime {
    pub webhook: Arc<WebhookHttp>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), Error>>>,
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
        let mut tasks = Vec::with_capacity(workers);
        for _ in 0..workers {
            let worker = worker.clone();
            let receiver = receiver.clone();
            tasks.push(tokio::spawn(async move { worker.run(receiver).await }));
        }
        let maintenance = inbox.clone();
        let mut stop = receiver.clone();
        tasks.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _=interval.tick()=>if maintenance.purge_terminal(1000).await.is_err() {
                        tracing::warn!("ingestion terminal retention temporarily unavailable");
                    },
                    changed=stop.changed()=>if changed.is_err() || *stop.borrow(){return Ok(());},
                }
            }
        }));
        Ok(Self {
            webhook: Arc::new(WebhookHttp::new(verifier, inbox)),
            shutdown,
            tasks,
        })
    }

    pub fn healthy(&self) -> bool {
        !*self.shutdown.borrow() && self.tasks.iter().all(|task| !task.is_finished())
    }

    pub async fn stop(mut self) -> Result<(), Error> {
        let _ = self.shutdown.send(true);
        let mut result = Ok(());
        for task in self.tasks.drain(..) {
            if !matches!(task.await, Ok(Ok(()))) {
                result = Err(Error::Unavailable);
            }
        }
        result
    }
}

impl Drop for IngestionRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}
