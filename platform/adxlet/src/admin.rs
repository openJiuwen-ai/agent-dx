//! Explicit stop deletes locally owned Environments before their dependencies stop.
use crate::{Adxlet, Durability};
use adx_core::{EnvironmentState, Error, Result};
use adx_protocol::relay as pb;
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::Path,
    sync::{atomic::Ordering, Arc},
};

impl Adxlet {
    async fn ordinary_runtime_ready(&self) -> bool {
        self.registration_acknowledged.load(Ordering::Acquire)
            && self.authority_initialized.load(Ordering::Acquire)
            && !self.authority_reconciling.load(Ordering::Acquire)
            && !self.is_draining()
            && !self.stopping.load(Ordering::Acquire)
            && !self.process_sealing()
            && *self.lifecycle_ready.read().await
    }
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
    pub async fn drain(&self) -> Result<usize> {
        let _exclusive = self.process_transition.lock().await;
        if self.process_sealing() {
            return Err(Error::Conflict);
        }
        let _recovery = self.recovery.lock().await;
        let mut ready = self.lifecycle_ready.write().await;
        if !*ready && !self.is_draining() {
            return Err(Error::Unavailable(
                "authoritative node reconciliation required before cleanup".into(),
            ));
        }
        if !self
            .local_holds
            .lock()
            .expect("shared state lock poisoned")
            .is_empty()
        {
            return Err(Error::Unavailable(
                "unconfirmed local claims require reconciliation before drain".into(),
            ));
        }
        self.draining.store(true, Ordering::Release);
        self.set_maintenance(true);
        *ready = false;
        let handles: Vec<_> = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .values()
            .map(|(_, _, handle)| handle.clone())
            .collect();
        let count = handles.len();
        for handle in handles {
            let r = handle.delete().await?;
            if r.record.state != EnvironmentState::Deleted
                || r.record.resources_held
                || r.durability != Durability::Published
            {
                return Err(Error::Unavailable(
                    "cleanup is not committed to cluster storage".into(),
                ));
            }
            self.release_published_controller(&r);
        }
        Ok(count)
    }
}
pub struct NodeAdmin {
    manager: Arc<Adxlet>,
    routes: Arc<crate::routes::UdsRoutes>,
    parent_pid: libc::pid_t,
}
#[tonic::async_trait]
impl pb::node_admin_service_server::NodeAdminService for NodeAdmin {
    async fn inspect(
        &self,
        request: tonic::Request<pb::AdxletRuntimeInspectRequest>,
    ) -> std::result::Result<tonic::Response<pb::AdxletRuntimeInspection>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let relay = self
                .routes
                .inspect_process_relay()
                .await
                .map_err(adx_protocol::status)?;
            if relay.proxy_session_id.is_empty() || relay.controller_session_id.is_empty() {
                return Err(tonic::Status::unavailable("active Relay identity unknown"));
            }
            let catalog_ready = relay.ready
                && self
                    .routes
                    .process_preflight(&relay.proxy_session_id)
                    .await
                    .is_ok();
            Ok(tonic::Response::new(pb::AdxletRuntimeInspection {
                node_id: self.manager.node_id.clone(),
                node_session: self.manager.services.upgrade.session().into(),
                relay_session: relay.proxy_session_id,
                route_owner: relay.controller_session_id,
                ready: catalog_ready && self.manager.ordinary_runtime_ready().await,
            }))
        })
        .await
        .map_err(|_| tonic::Status::unavailable("runtime inspection timed out"))?
    }
    async fn drain(
        &self,
        _: tonic::Request<pb::DrainRequest>,
    ) -> std::result::Result<tonic::Response<pb::DrainResponse>, tonic::Status> {
        let count = self.manager.drain().await.map_err(adx_protocol::status)?;
        Ok(tonic::Response::new(pb::DrainResponse {
            deleted_environments: count as u64,
        }))
    }
}
/// Both modes inspect the actual route catalog; only standalone exposes handoff.
pub async fn serve_with_handoff(
    manager: Arc<Adxlet>,
    path: &Path,
    routes: Arc<crate::routes::UdsRoutes>,
    mode: crate::proxy::ProxyMode,
    parent_pid: libc::pid_t,
    coordinator: tokio::sync::watch::Receiver<adx_transport::rpc::RpcChannel>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() || tokio::net::UnixStream::connect(path).await.is_ok() {
            return Err("node admin path occupied".into());
        }
        std::fs::remove_file(path)?;
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    tonic::transport::Server::builder()
        .add_service(pb::node_admin_service_server::NodeAdminServiceServer::new(
            NodeAdmin {
                manager: manager.clone(),
                routes: routes.clone(),
                parent_pid,
            },
        ))
        .add_optional_service((mode == crate::proxy::ProxyMode::Standalone).then(|| {
            pb::adxlet_process_service_server::AdxletProcessServiceServer::new(ProcessAdmin {
                manager,
                routes: routes.clone(),
                parent_pid,
                coordinator,
                acceptance: tokio::sync::Mutex::new(()),
            })
        }))
        .add_optional_service((mode == crate::proxy::ProxyMode::Standalone).then(|| {
            pb::relay_handoff_service_server::RelayHandoffServiceServer::new(RelayHandoff {
                routes,
                parent_pid,
            })
        }))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
        .await?;
    Ok(())
}

struct RelayHandoff {
    routes: Arc<crate::routes::UdsRoutes>,
    parent_pid: libc::pid_t,
}
struct ProcessAdmin {
    manager: Arc<Adxlet>,
    routes: Arc<crate::routes::UdsRoutes>,
    parent_pid: libc::pid_t,
    coordinator: tokio::sync::watch::Receiver<adx_transport::rpc::RpcChannel>,
    acceptance: tokio::sync::Mutex<()>,
}

#[tonic::async_trait]
impl pb::adxlet_process_service_server::AdxletProcessService for ProcessAdmin {
    async fn begin(
        &self,
        request: tonic::Request<pb::AdxletProcessRequest>,
    ) -> std::result::Result<tonic::Response<pb::AdxletProcessState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        let identity = request.into_inner();
        let _acceptance = self.acceptance.lock().await;
        if identity.old_node_session != self.manager.services.upgrade.session() {
            return Err(tonic::Status::failed_precondition(
                "old node session changed",
            ));
        }
        if self
            .manager
            .services
            .upgrade
            .process_identity()
            .map_err(adx_protocol::status)?
            .is_some()
        {
            let state = self
                .manager
                .services
                .upgrade
                .process_state()
                .map_err(adx_protocol::status)?;
            if state.identity.as_ref() != Some(&identity) {
                return Err(tonic::Status::failed_precondition("process task conflict"));
            }
            return Ok(tonic::Response::new(state));
        }
        self.routes
            .process_preflight(&identity.old_relay_session)
            .await
            .map_err(adx_protocol::status)?;
        let remaining = identity
            .deadline_unix_millis
            .checked_sub(adx_core::observation::now_millis().map_err(adx_protocol::status)?)
            .filter(|remaining| *remaining > 0)
            .ok_or_else(|| tonic::Status::failed_precondition("local deadline elapsed"))?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(remaining);
        self.manager
            .services
            .upgrade
            .fence_process(identity.clone(), deadline.into_std())
            .map_err(adx_protocol::status)?;
        let manager = self.manager.clone();
        let coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout_at(deadline, async {
                manager.seal_process(identity.clone()).await?;
                let request = adx_protocol::control::YieldNodeSessionRequest {
                    owner: Some(adx_protocol::control::MaintenanceOwner { node_id: manager.node_id.clone(),
                        rollout_id: identity.rollout_id.clone(), local_task_id: identity.local_task_id.clone(), epoch: identity.epoch }),
                    expected_session: identity.old_node_session.clone() };
                loop {
                    let channel = coordinator.borrow().clone();
                    let mut client = adx_protocol::control::node_handoff_service_client::NodeHandoffServiceClient::new(channel);
                    match client.r#yield(request.clone()).await {
                        Ok(response) => {
                            let receipt = response.into_inner();
                            if receipt.owner != request.owner || receipt.current_session != identity.old_node_session
                                || !receipt.yielded { return Err(Error::Conflict); }
                            // SAFETY: signal this process only, through the existing Preserve handler.
                            if unsafe { libc::kill(libc::getpid(), libc::SIGUSR1) } != 0 {
                                return Err(Error::Unavailable("Preserve signal failed".into()));
                            }
                            return Ok::<(), Error>(());
                        }
                        Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
                    }
                }
            }).await;
            if !matches!(result, Ok(Ok(()))) {
                adx_observability::warn!(
                    "process seal/Yield unresolved; retaining old process fence"
                );
            }
        });
        self.manager
            .services
            .upgrade
            .process_state()
            .map(tonic::Response::new)
            .map_err(adx_protocol::status)
    }
    async fn query(
        &self,
        request: tonic::Request<pb::AdxletProcessRequest>,
    ) -> std::result::Result<tonic::Response<pb::AdxletProcessState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        if self
            .manager
            .services
            .upgrade
            .completed_process(request.get_ref())
            .map_err(adx_protocol::status)?
        {
            return Ok(tonic::Response::new(pb::AdxletProcessState {
                identity: Some(request.into_inner()),
                phase: "ready".into(),
                node_session: self.manager.services.upgrade.session().into(),
            }));
        }
        if self
            .manager
            .services
            .upgrade
            .process_identity()
            .map_err(adx_protocol::status)?
            .is_none()
        {
            return Err(tonic::Status::failed_precondition(
                "process task has not begun",
            ));
        }
        let state = self
            .manager
            .services
            .upgrade
            .process_state()
            .map_err(adx_protocol::status)?;
        if state.identity.as_ref() != Some(request.get_ref()) {
            return Err(tonic::Status::failed_precondition("process task conflict"));
        }
        Ok(tonic::Response::new(state))
    }
}
fn authorize_parent<T>(
    parent_pid: libc::pid_t,
    request: &tonic::Request<T>,
) -> std::result::Result<(), &'static str> {
    let peer = request
        .extensions()
        .get::<tonic::transport::server::UdsConnectInfo>()
        .and_then(|info| info.peer_cred.as_ref())
        .and_then(|cred| cred.pid());
    // SAFETY: getppid has no pointer arguments or side effects.
    let current_parent = unsafe { libc::getppid() };
    if !adx_process::launch_parent_matches(parent_pid, current_parent, peer) {
        return Err("launch-time parent supervisor required");
    }
    Ok(())
}
#[tonic::async_trait]
impl pb::relay_handoff_service_server::RelayHandoffService for RelayHandoff {
    async fn cancel(
        &self,
        request: tonic::Request<pb::RelayHandoffRequest>,
    ) -> std::result::Result<tonic::Response<pb::RelayHandoffState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        self.routes
            .cancel_handoff(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(adx_protocol::status)
    }
    async fn reserve(
        &self,
        request: tonic::Request<pb::RelayHandoffRequest>,
    ) -> std::result::Result<tonic::Response<pb::RelayHandoffState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        self.routes
            .reserve_handoff(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(adx_protocol::status)
    }
    async fn finish(
        &self,
        request: tonic::Request<pb::RelayHandoffProof>,
    ) -> std::result::Result<tonic::Response<pb::RelayHandoffState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        self.routes
            .finish_handoff(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(adx_protocol::status)
    }
    async fn switch(
        &self,
        request: tonic::Request<pb::RelaySwitchRequest>,
    ) -> std::result::Result<tonic::Response<pb::RelayHandoffState>, tonic::Status> {
        authorize_parent(self.parent_pid, &request).map_err(tonic::Status::permission_denied)?;
        self.routes
            .switch_handoff(request.into_inner())
            .await
            .map(tonic::Response::new)
            .map_err(adx_protocol::status)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use adx_core::{EnvironmentRecord, EnvironmentSpec};
    use pb::node_admin_service_server::NodeAdminService;

    struct Hooks;
    #[tonic::async_trait]
    impl crate::RuntimeDriver for Hooks {
        async fn start(
            &self,
            _: &EnvironmentSpec,
            _: &str,
            _: u64,
            _: &[adx_core::scheduling::DeviceAllocation],
        ) -> Result<std::net::IpAddr> {
            Err(Error::Unavailable("not a runtime fixture".into()))
        }
        async fn is_running(&self, _: &str) -> Result<bool> {
            Ok(false)
        }
        async fn remove(&self, _: &str) -> Result<()> {
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl crate::Readiness for Hooks {
        async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl crate::Routes for Hooks {
        async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
            Ok(())
        }
        async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
            Ok(())
        }
        async fn retire_orphan(&self, _: &crate::RuntimeObservation) -> Result<()> {
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl crate::StateSink for Hooks {
        async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
            Ok(Durability::Published)
        }
    }
    pub(crate) fn manager() -> Adxlet {
        let hooks = Arc::new(Hooks);
        Adxlet::new(
            "node".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks,
        )
    }

    #[tokio::test]
    async fn ordinary_readiness_requires_ack_and_all_lifecycle_flags_not_admission() {
        let node = manager();
        node.authority_initialized.store(true, Ordering::Release);
        assert!(!node.ordinary_runtime_ready().await);
        node.registration_acknowledged
            .store(true, Ordering::Release);
        assert!(node.ordinary_runtime_ready().await);
        node.set_maintenance(true);
        assert!(!node.accepting_allocations());
        assert!(node.ordinary_runtime_ready().await);
        for flag in [&node.authority_reconciling, &node.draining, &node.stopping] {
            flag.store(true, Ordering::Release);
            assert!(!node.ordinary_runtime_ready().await);
            flag.store(false, Ordering::Release);
        }
        *node.lifecycle_ready.write().await = false;
        assert!(!node.ordinary_runtime_ready().await);
    }

    #[tokio::test]
    async fn node_admin_inspect_rejects_missing_direct_parent_credentials() {
        let admin = NodeAdmin {
            manager: Arc::new(manager()),
            routes: Arc::new(
                crate::routes::UdsRoutes::new(
                    "/unconnected/route.sock".into(),
                    std::time::Duration::from_millis(10),
                )
                .unwrap(),
            ),
            // SAFETY: getppid takes no arguments and only observes this process.
            parent_pid: unsafe { libc::getppid() },
        };
        let error = admin
            .inspect(tonic::Request::new(pb::AdxletRuntimeInspectRequest {}))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
}
