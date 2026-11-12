use super::{DeliveryBatch, DurableAck, Error, Inbox};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub enum DeliveryFailure {
    Unknown,
    RetryAfter(Duration),
    Rejected,
}

/// Production port must ensure the exact existing Binding generation through
/// the published ADX access owner, then authenticated Relay delivery. A request
/// timeout is unknown: the same persisted batch ID/digest must be retried.
#[async_trait]
pub trait DeliveryPort: Send + Sync {
    async fn deliver(&self, batch: &DeliveryBatch) -> Result<DurableAck, DeliveryFailure>;
}

pub enum WorkerOutcome {
    Idle,
    Accepted,
    Deferred,
}

pub struct Worker {
    inbox: Inbox,
    delivery: Arc<dyn DeliveryPort>,
    lease: Duration,
}

impl Worker {
    pub fn new(
        inbox: Inbox,
        delivery: Arc<dyn DeliveryPort>,
        lease: Duration,
    ) -> Result<Self, Error> {
        if lease < Duration::from_secs(3) || lease > Duration::from_secs(300) {
            return Err(Error::InvalidLimit);
        }
        Ok(Self {
            inbox,
            delivery,
            lease,
        })
    }

    pub async fn run_once(&self) -> Result<WorkerOutcome, Error> {
        let Some(lease) = self.inbox.claim(self.lease).await? else {
            return Ok(WorkerOutcome::Idle);
        };
        let dispatch = self.delivery.deliver(&lease.batch);
        tokio::pin!(dispatch);
        let mut heartbeat = tokio::time::interval(self.lease / 3);
        heartbeat.tick().await;
        let result = loop {
            tokio::select! {
                value=&mut dispatch=>break value,
                _=heartbeat.tick()=>self.inbox.renew(&lease,self.lease).await?,
            }
        };
        match result {
            Ok(ack) => {
                self.inbox.accept(&lease, &ack).await?;
                Ok(WorkerOutcome::Accepted)
            }
            Err(failure) => {
                let (delay, reason) = match failure {
                    DeliveryFailure::Unknown => (Duration::from_secs(5), "delivery_unknown"),
                    DeliveryFailure::RetryAfter(delay) => (
                        delay.clamp(Duration::from_millis(100), Duration::from_secs(3600)),
                        "rate_limited",
                    ),
                    DeliveryFailure::Rejected => (Duration::from_secs(60), "receiver_rejected"),
                };
                self.inbox.retry(&lease, delay, reason).await?;
                Ok(WorkerOutcome::Deferred)
            }
        }
    }

    /// Spawn a bounded number of workers at the Gateway assembly. Shutdown
    /// drops in-flight work; leases and persisted batches recover it safely.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<(), Error> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                result=self.run_once()=>{
                    match result {
                        Ok(_) => {},
                        // A transient store outage or another replica taking an
                        // expired lease must not permanently stop background work.
                        Err(Error::Unavailable | Error::LeaseLost) => {
                            tracing::warn!("ingestion worker retrying after unavailable store or lost lease");
                        },
                        Err(error) => return Err(error),
                    }
                    tokio::select! {
                        _=tokio::time::sleep(Duration::from_millis(100))=>{},
                        result=shutdown.changed()=>if result.is_err(){return Ok(());},
                    }
                },
                result=shutdown.changed()=>if result.is_err(){return Ok(());},
            }
        }
    }
}
