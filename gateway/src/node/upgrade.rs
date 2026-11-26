//! Standalone Relay drain and preserving controller takeover.
use super::route_control::{proto, BindingService};
use std::{sync::Arc, time::Duration};
use tokio::sync::{watch, Mutex};
use tonic::{Request, Response, Status};

pub(crate) const DRAIN_LIMIT: Duration = Duration::from_secs(120);
pub(crate) struct UpgradeService {
    bindings: Arc<BindingService>,
    state: Mutex<UpgradeState>,
    accepted: watch::Sender<bool>,
    released: watch::Sender<Option<Result<proto::RelayUpgradeState, Status>>>,
}
#[derive(Default)]
struct UpgradeState {
    closed: bool,
    task: Option<proto::RelayUpgradeRequest>,
}
impl UpgradeService {
    pub(crate) fn new(bindings: Arc<BindingService>) -> Arc<Self> {
        Arc::new(Self {
            bindings,
            state: Mutex::new(UpgradeState::default()),
            accepted: watch::channel(false).0,
            released: watch::channel(None).0,
        })
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.accepted.subscribe()
    }
    pub(crate) async fn close_admission(&self) -> bool {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.task.is_some()
    }
    async fn begin_drain(
        &self,
        request: proto::RelayUpgradeRequest,
    ) -> Result<proto::RelayUpgradeState, Status> {
        if request.task_id.trim().is_empty()
            || request.proxy_session_id != self.bindings.inspect().await.proxy_session_id
        {
            return Err(Status::failed_precondition(
                "task and current Relay session required",
            ));
        }
        {
            let mut state = self.state.lock().await;
            if let Some(current) = &state.task {
                if current != &request {
                    return Err(Status::failed_precondition(
                        "another Relay upgrade owns this session",
                    ));
                }
            } else {
                if state.closed {
                    return Err(Status::failed_precondition("Relay is stopping"));
                }
                state.task = Some(request);
                self.bindings.start_process_drain();
                // The listener owner consumes this independently of the RPC future.
                self.accepted.send_replace(true);
            }
        }
        let mut released = self.released.subscribe();
        loop {
            if let Some(result) = released.borrow_and_update().clone() {
                return result;
            }
            released
                .changed()
                .await
                .map_err(|_| Status::unavailable("Relay listener owner stopped"))?;
        }
    }
    pub(crate) async fn released(&self, deadline: u64) {
        if let Some(task) = &self.state.lock().await.task {
            self.released.send_if_modified(|receipt| {
                if receipt.is_some() {
                    return false;
                }
                *receipt = Some(Ok(proto::RelayUpgradeState {
                    task_id: task.task_id.clone(),
                    proxy_session_id: task.proxy_session_id.clone(),
                    listeners_released: true,
                    drain_deadline_unix_ms: deadline,
                }));
                true
            });
        }
    }
    pub(crate) async fn finish(&self, phase: &str) {
        self.released.send_if_modified(|receipt| {
            if receipt.is_some() {
                return false;
            }
            *receipt = Some(Err(Status::unavailable(format!(
                "Relay stopped before listener release: {phase}"
            ))));
            true
        });
    }
}
#[tonic::async_trait]
impl proto::relay_upgrade_service_server::RelayUpgradeService for Arc<UpgradeService> {
    async fn takeover_bindings(
        &self,
        r: Request<proto::BindingTakeoverRequest>,
    ) -> Result<Response<proto::BindingState>, Status> {
        let state = self.state.lock().await;
        if state.closed || state.task.is_some() {
            return Err(Status::failed_precondition("Relay is draining"));
        }
        Ok(Response::new(self.bindings.takeover(r.into_inner()).await?))
    }
    async fn begin_drain(
        &self,
        r: Request<proto::RelayUpgradeRequest>,
    ) -> Result<Response<proto::RelayUpgradeState>, Status> {
        Ok(Response::new(
            UpgradeService::begin_drain(self, r.into_inner()).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::protocol::GatewayPolicy, node::Relay};

    #[tokio::test]
    async fn cancelled_begin_survives_and_replays_one_release_receipt() {
        let bindings = Arc::new(BindingService::new(Arc::new(Relay::new(
            GatewayPolicy::new(Vec::new()),
        ))));
        let request = proto::RelayUpgradeRequest {
            task_id: "release".into(),
            proxy_session_id: bindings.inspect().await.proxy_session_id,
        };
        let upgrade = UpgradeService::new(bindings);
        let mut accepted = upgrade.subscribe();
        let pending = {
            let upgrade = upgrade.clone();
            let request = request.clone();
            tokio::spawn(async move { upgrade.begin_drain(request).await })
        };
        accepted.changed().await.unwrap();
        assert!(
            !pending.is_finished(),
            "acceptance alone must not acknowledge release"
        );
        pending.abort();
        assert!(upgrade.close_admission().await);
        upgrade.released(123_000).await;
        let receipt = upgrade.begin_drain(request.clone()).await.unwrap();
        assert!(receipt.listeners_released);
        upgrade.released(999_000).await;
        upgrade.finish("completed").await;
        assert_eq!(upgrade.begin_drain(request.clone()).await.unwrap(), receipt);
        let mut wrong = request;
        wrong.task_id = "other".into();
        assert_eq!(
            upgrade.begin_drain(wrong).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
}
