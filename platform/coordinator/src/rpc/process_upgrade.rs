//! Maintenance protects execution; ordinary Node registration proves readiness.
use super::*;
use crate::storage::{MaintenanceOwner, MaintenanceRelease, NodeMaintenance};

fn owner(value: Option<pb::MaintenanceOwner>) -> Result<MaintenanceOwner> {
    let value = value.ok_or(Error::Conflict)?;
    let owner = MaintenanceOwner {
        node_id: value.node_id,
        rollout_id: value.rollout_id,
        local_task_id: value.local_task_id,
        epoch: value.epoch,
    };
    owner.validate()?;
    Ok(owner)
}
fn wire(marker: NodeMaintenance) -> pb::NodeHandoffState {
    pb::NodeHandoffState {
        owner: Some(pb::MaintenanceOwner {
            node_id: marker.owner.node_id,
            rollout_id: marker.owner.rollout_id,
            local_task_id: marker.owner.local_task_id,
            epoch: marker.owner.epoch,
        }),
        origin_session: marker.origin_session,
        current_session: marker.current_session,
        yielded: marker.yielded,
    }
}
fn release_live(live: Option<&LiveNode>, node: &str, session: &str, timeout: Duration) -> bool {
    live.is_some_and(|live| {
        live.fresh(session, timeout)
            && live.inspected
            && live.sequence > 0
            && live.report.node_id == node
            && live.report.session_id == session
            && !live.report.reconciling
    })
}

#[tonic::async_trait]
impl pb::node_handoff_service_server::NodeHandoffService for CoordinatorRpc {
    async fn r#yield(
        &self,
        request: Request<pb::YieldNodeSessionRequest>,
    ) -> std::result::Result<Response<pb::NodeHandoffState>, Status> {
        let identity = owner(request.get_ref().owner.clone()).map_err(status)?;
        if self.0.peers.authenticate(&request)? != Principal::Node(identity.node_id.clone()) {
            return Err(Status::permission_denied("owning Node required for Yield"));
        }
        let expected = request.into_inner().expected_session;
        let mut state = self.0.state.lock().await;
        state.needs_recovery = true;
        let marker = state
            .session
            .yield_node_session(identity, expected)
            .await
            .map_err(status)?;
        state.recover_authoritative_state().await.map_err(status)?;
        self.0.changed.notify_waiters();
        Ok(Response::new(wire(marker)))
    }
    async fn query_release(
        &self,
        request: Request<pb::MaintenanceReleaseRequest>,
    ) -> std::result::Result<Response<pb::MaintenanceReleaseReceipt>, Status> {
        let identity = owner(request.get_ref().owner.clone()).map_err(status)?;
        if self.0.peers.authenticate(&request)? != Principal::Node(identity.node_id.clone()) {
            return Err(Status::permission_denied(
                "owning Node required for QueryRelease",
            ));
        }
        let session = request.get_ref().node_session.clone();
        let state = self.0.state.lock().await;
        let receipt = state
            .session
            .maintenance_release(&identity)
            .await
            .map_err(status)?
            .ok_or_else(|| Status::not_found("release receipt absent"))?;
        if receipt.node_session != session {
            return Err(Status::failed_precondition("release session differs"));
        }
        Ok(Response::new(pb::MaintenanceReleaseReceipt {
            owner: request.into_inner().owner,
            node_session: receipt.node_session,
        }))
    }
}

impl CoordinatorRpc {
    /// Capture the durable predecessor before probing. Heartbeat expiry and an
    /// empty live cache after restart do not weaken a maintained node's proof.
    pub(super) async fn process_registration_predecessor(
        &self,
        request: &pb::RegisterNodeRequest,
    ) -> Result<Option<(NodeMaintenance, String)>> {
        let session = self.0.state.lock().await.session.clone();
        let Some(marker) = session.process_upgrade(&request.node_id).await? else {
            return Ok(None);
        };
        let saved = session.get_node(&request.node_id).await?;
        if saved.address != request.node_address || saved.proxy_address != request.proxy_address {
            return Err(Error::Conflict);
        }
        let actual = saved.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        if actual == request.session_id {
            return Ok(None);
        }
        if !marker.yielded
            || !request.reconciling
            || request.accepting_allocations
            || request.session_id == marker.origin_session
            || !self.proves_node_session(request).await
        {
            return Err(Error::Conflict);
        }
        Ok(Some((marker, actual)))
    }

    pub(super) async fn prepare_process_maintenance(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Prepare {
            owner: MaintenanceOwner,
            node_session: String,
        }
        let request: Prepare = serde_json::from_value(body)
            .map_err(|_| Error::Invalid("owner and current session required".into()))?;
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        if let Some(marker) = state
            .session
            .process_upgrade(&request.owner.node_id)
            .await?
        {
            return if marker.owner == request.owner && marker.origin_session == request.node_session
            {
                serde_json::to_value(marker).map_err(|_| Error::Conflict)
            } else {
                Err(Error::Conflict)
            };
        }
        if state
            .session
            .maintenance_release(&request.owner)
            .await?
            .is_some()
        {
            return Err(Error::Conflict);
        }
        if state.live.get(&request.owner.node_id).is_none_or(|live| {
            !live.inspected || !live.fresh(&request.node_session, self.0.heartbeat_timeout)
        }) {
            return Err(Error::Conflict);
        }
        state.needs_recovery = true;
        let marker = state
            .session
            .prepare_process_maintenance(request.owner, request.node_session)
            .await?;
        state.recover_authoritative_state().await?;
        self.0.changed.notify_waiters();
        serde_json::to_value(marker).map_err(|_| Error::Conflict)
    }
    pub(super) async fn release_process_maintenance(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let request: MaintenanceRelease = serde_json::from_value(body)
            .map_err(|_| Error::Invalid("exact owner/current session required".into()))?;
        let mut state = self.0.state.lock().await;
        if let Some(receipt) = state.session.maintenance_release(&request.owner).await? {
            return if receipt == request {
                serde_json::to_value(receipt).map_err(|_| Error::Conflict)
            } else {
                Err(Error::Conflict)
            };
        }
        state.recover_authoritative_state().await?;
        if !release_live(
            state.live.get(&request.owner.node_id),
            &request.owner.node_id,
            &request.node_session,
            self.0.heartbeat_timeout,
        ) {
            return Err(Error::Unavailable(
                "release waits for fresh inspected normal report".into(),
            ));
        }
        state.needs_recovery = true;
        let receipt = state.session.release_process_maintenance(request).await?;
        state.recover_authoritative_state().await?;
        self.0.changed.notify_waiters();
        serde_json::to_value(receipt).map_err(|_| Error::Conflict)
    }
    pub(super) async fn query_process_maintenance(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Query {
            owner: MaintenanceOwner,
        }
        let request: Query = serde_json::from_value(body)
            .map_err(|_| Error::Invalid("exact maintenance owner required".into()))?;
        let state = self.0.state.lock().await;
        serde_json::to_value(
            state
                .session
                .process_maintenance_status(request.owner)
                .await?,
        )
        .map_err(|_| Error::Conflict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn http_maintenance_queries_exact_owner_and_keeps_release_receipt() {
        let rig = crate::rpc::cancellation_test_redis::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        session
            .register_session(
                Node {
                    id: "n".into(),
                    runtime_classes: vec!["runsc".into()],
                    capacity: Default::default(),
                    available: true,
                    labels: Default::default(),
                    devices: vec![],
                },
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                Some(NodeSession {
                    id: "a".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let own = serde_json::json!({"node_id":"n","rollout_id":"r","local_task_id":"t","epoch":1});
        let base = "/v1/adxlet-process-upgrades";
        let query = format!("{base}/query");
        let status = rpc
            .upgrade_http(
                &hyper::Method::POST,
                &query,
                serde_json::json!({"owner":own}),
            )
            .await
            .unwrap();
        assert_eq!(status, serde_json::json!({"status":"absent","owner":own}));
        for path in [
            "/v1/node-upgrades/process-maintenance",
            "/v1/node-upgrades/process-maintenance/release",
            "/v1/node-upgrades/process-maintenance/query",
        ] {
            assert!(matches!(
                rpc.upgrade_http(&hyper::Method::POST, path, serde_json::json!({}))
                    .await,
                Err(Error::NotFound)
            ));
        }
        assert!(matches!(
            rpc.upgrade_http(
                &hyper::Method::POST,
                base,
                serde_json::json!({"action":"replace-session","owner":own,"node_session":"a"})
            )
            .await,
            Err(Error::Invalid(_))
        ));
        {
            let mut state = rpc.0.state.lock().await;
            state.live.insert(
                "n".into(),
                LiveNode {
                    expired: false,
                    inspected: true,
                    session: "a".into(),
                    sequence: 1,
                    last_seen: tokio::time::Instant::now(),
                    report: pb::RegisterNodeRequest {
                        node_id: "n".into(),
                        session_id: "a".into(),
                        heartbeat_sequence: 1,
                        ..Default::default()
                    },
                },
            );
        }
        rpc.upgrade_http(
            &hyper::Method::POST,
            base,
            serde_json::json!({"action":"prepare","owner":own,"node_session":"a"}),
        )
        .await
        .unwrap();
        let active = rpc
            .upgrade_http(
                &hyper::Method::POST,
                &query,
                serde_json::json!({"owner":own}),
            )
            .await
            .unwrap();
        assert_eq!(
            active,
            serde_json::json!({"status":"active","owner":own,"origin_session":"a","current_session":"a","yielded":false})
        );
        let mut other = own.clone();
        other["epoch"] = 2.into();
        assert!(matches!(
            rpc.upgrade_http(
                &hyper::Method::POST,
                &query,
                serde_json::json!({"owner":other})
            )
            .await,
            Err(Error::Conflict)
        ));
        // A real normal report is still required, even for cancelling before Yield.
        session
            .register_session(
                Node {
                    id: "n".into(),
                    runtime_classes: vec!["runsc".into()],
                    capacity: Default::default(),
                    available: false,
                    labels: Default::default(),
                    devices: vec![],
                },
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                Some(NodeSession {
                    id: "a".into(),
                    sequence: 2,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        session.set_node_scheduling("n", true, false).await.unwrap();
        let release = serde_json::json!({"action":"release","owner":own,"node_session":"a"});
        rpc.upgrade_http(&hyper::Method::POST, base, release.clone())
            .await
            .unwrap();
        assert!(session.get_node("n").await.unwrap().scheduling_paused);
        session
            .prepare_process_maintenance(serde_json::from_value(other).unwrap(), "a".into())
            .await
            .unwrap();
        let released = rpc
            .upgrade_http(
                &hyper::Method::POST,
                &query,
                serde_json::json!({"owner":own}),
            )
            .await
            .unwrap();
        assert_eq!(
            released,
            serde_json::json!({"status":"released","owner":own,"node_session":"a"})
        );
        // A lost response can be replayed while a later task owns the node.
        rpc.upgrade_http(&hyper::Method::POST, base, release)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn release_requires_the_normal_fresh_inspected_report() {
        let mut live = LiveNode {
            expired: false,
            inspected: true,
            session: "s".into(),
            sequence: 1,
            last_seen: tokio::time::Instant::now(),
            report: pb::RegisterNodeRequest {
                node_id: "n".into(),
                session_id: "s".into(),
                reconciling: false,
                accepting_allocations: false,
                ..Default::default()
            },
        };
        let timeout = Duration::from_secs(30);
        assert!(release_live(Some(&live), "n", "s", timeout));
        assert!(!release_live(Some(&live), "n", "other", timeout));
        live.expired = true;
        assert!(!release_live(Some(&live), "n", "s", timeout));
        live.expired = false;
        let last_seen = live.last_seen;
        live.last_seen = tokio::time::Instant::now() - timeout;
        assert!(!release_live(Some(&live), "n", "s", timeout));
        live.last_seen = last_seen;
        live.report.accepting_allocations = true;
        assert!(release_live(Some(&live), "n", "s", timeout));
        live.report.reconciling = true;
        assert!(!release_live(Some(&live), "n", "s", timeout));
        live.report.reconciling = false;
        live.inspected = false;
        assert!(!release_live(Some(&live), "n", "s", timeout));
        assert!(!release_live(None, "n", "s", timeout));
    }
}
