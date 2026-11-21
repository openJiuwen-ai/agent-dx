//! Standalone-only lifecycle and candidate binding control.
use super::route_control::{proto, BindingService};
use std::{sync::Arc, time::Duration};
use tokio::sync::{watch, Mutex};
use tonic::{Request, Response, Status};

pub(crate) const DRAIN_LIMIT: Duration = Duration::from_secs(120);
pub(crate) struct UpgradeService {
    bindings: Arc<BindingService>,
    state: Mutex<UpgradeState>,
    accepted: watch::Sender<bool>,
}
#[derive(Default)]
struct UpgradeState {
    closed: bool,
    task: Option<proto::RelayUpgradeState>,
}
impl UpgradeService {
    pub(crate) fn new(bindings: Arc<BindingService>) -> Arc<Self> {
        let (accepted, _) = watch::channel(false);
        Arc::new(Self {
            bindings,
            state: Mutex::new(UpgradeState::default()),
            accepted,
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
    async fn request(
        &self,
        request: proto::RelayUpgradeRequest,
        begin: bool,
    ) -> Result<proto::RelayUpgradeState, Status> {
        if request.task_id.trim().is_empty()
            || request.proxy_session_id != self.bindings.inspect().await.proxy_session_id
        {
            return Err(Status::failed_precondition(
                "task and current Relay session required",
            ));
        }
        let mut state = self.state.lock().await;
        if let Some(current) = state.task.as_ref() {
            if current.task_id != request.task_id {
                return Err(Status::failed_precondition(
                    "another Relay upgrade owns this session",
                ));
            }
            return Ok(current.clone());
        }
        if !begin {
            return Err(Status::not_found("Relay upgrade not accepted"));
        }
        if state.closed {
            return Err(Status::failed_precondition("Relay is stopping"));
        }
        let current = proto::RelayUpgradeState {
            task_id: request.task_id,
            proxy_session_id: request.proxy_session_id,
            phase: "accepted".into(),
            listeners_released: false,
            drain_deadline_unix_ms: 0,
        };
        state.task = Some(current.clone());
        self.bindings.start_process_drain();
        self.accepted.send_replace(true);
        Ok(current)
    }
    pub(crate) async fn released(&self, deadline: u64) {
        if let Some(state) = self.state.lock().await.task.as_mut() {
            state.listeners_released = true;
            state.drain_deadline_unix_ms = deadline;
            state.phase = "draining".into();
        }
    }
    pub(crate) async fn finish(&self, phase: &str) {
        if let Some(state) = self.state.lock().await.task.as_mut() {
            state.phase = phase.into();
        }
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
    async fn install_candidate_bindings(
        &self,
        r: Request<proto::BindingSnapshot>,
    ) -> Result<Response<proto::BindingState>, Status> {
        Ok(Response::new(
            self.bindings.install_candidate(r.into_inner()).await?,
        ))
    }
    async fn open_candidate_bindings(
        &self,
        r: Request<proto::BindingState>,
    ) -> Result<Response<proto::BindingState>, Status> {
        Ok(Response::new(
            self.bindings.open_candidate(r.into_inner()).await?,
        ))
    }
    async fn begin(
        &self,
        r: Request<proto::RelayUpgradeRequest>,
    ) -> Result<Response<proto::RelayUpgradeState>, Status> {
        Ok(Response::new(self.request(r.into_inner(), true).await?))
    }
    async fn query(
        &self,
        r: Request<proto::RelayUpgradeRequest>,
    ) -> Result<Response<proto::RelayUpgradeState>, Status> {
        Ok(Response::new(self.request(r.into_inner(), false).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common::protocol::GatewayPolicy, node::Relay};

    #[tokio::test]
    async fn closing_admission_rejects_late_begin_but_keeps_accepted_replays() {
        for begin_first in [false, true] {
            let bindings = Arc::new(BindingService::new(Arc::new(Relay::new(
                GatewayPolicy::new(Vec::new()),
            ))));
            let request = proto::RelayUpgradeRequest {
                task_id: "release".into(),
                proxy_session_id: bindings.inspect().await.proxy_session_id,
            };
            let upgrade = UpgradeService::new(bindings);
            if begin_first {
                upgrade.request(request.clone(), true).await.unwrap();
            }
            assert_eq!(upgrade.close_admission().await, begin_first);
            assert_eq!(upgrade.close_admission().await, begin_first);
            if begin_first {
                upgrade.finish("interrupted").await;
                assert_eq!(
                    upgrade.request(request.clone(), true).await.unwrap().phase,
                    "interrupted"
                );
                assert_eq!(
                    upgrade.request(request, false).await.unwrap().phase,
                    "interrupted"
                );
            } else {
                assert_eq!(
                    upgrade
                        .request(request.clone(), true)
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::FailedPrecondition
                );
                assert_eq!(
                    upgrade.request(request, false).await.unwrap_err().code(),
                    tonic::Code::NotFound
                );
            }
        }
    }

    #[tokio::test]
    async fn accepted_is_not_release_and_replays_preserve_lifecycle_evidence() {
        let bindings = Arc::new(BindingService::new(Arc::new(Relay::new(
            GatewayPolicy::new(Vec::new()),
        ))));
        let request = proto::RelayUpgradeRequest {
            task_id: "release".into(),
            proxy_session_id: bindings.inspect().await.proxy_session_id,
        };
        let upgrade = UpgradeService::new(bindings);
        assert_eq!(
            upgrade
                .request(request.clone(), false)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        let accepted = upgrade.request(request.clone(), true).await.unwrap();
        assert_eq!(accepted.phase, "accepted");
        assert!(!accepted.listeners_released);
        assert_eq!(accepted.drain_deadline_unix_ms, 0);
        assert_eq!(
            upgrade.request(request.clone(), true).await.unwrap(),
            accepted
        );
        upgrade.released(123_000).await;
        let draining = upgrade.request(request.clone(), false).await.unwrap();
        assert!(draining.listeners_released);
        assert_eq!(draining.phase, "draining");
        assert_eq!(
            upgrade.request(request.clone(), true).await.unwrap(),
            draining
        );
        for phase in ["completed", "forced", "interrupted", "failed"] {
            upgrade.finish(phase).await;
            let replay = upgrade.request(request.clone(), true).await.unwrap();
            assert_eq!(replay.phase, phase);
            assert_eq!(replay.drain_deadline_unix_ms, 123_000);
        }
        let mut wrong = request.clone();
        wrong.task_id = "other".into();
        assert_eq!(
            upgrade.request(wrong, true).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        let mut wrong = request;
        wrong.proxy_session_id = "other".into();
        assert_eq!(
            upgrade.request(wrong, false).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
}
