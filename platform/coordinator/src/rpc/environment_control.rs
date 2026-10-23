//! Tenant control RPCs and durable expiry claims. Node tasks still own lifecycle.
use super::*;
use futures_util::{stream, StreamExt};

fn expiry_pause_request(
    stored: &StoredEnvironment,
    control: &adx_core::environment_control::EnvironmentControlRecord,
    record: &EnvironmentRecord,
    caller: Option<pb::CallerContext>,
    node_session_id: String,
) -> Result<pb::PauseEnvironmentRequest> {
    let pause = stored.spec.lifecycle.pause.clone().unwrap_or_default();
    Ok(pb::PauseEnvironmentRequest {
        assignment: Some(stored.assignment.clone().try_into()?),
        caller,
        operation_id: format!("expiry-{}-{}", control.expires_at_unix_ms, control.revision),
        expected_revision: record.revision,
        retention: Some(pause.retention.into()),
        timeout_seconds: pause.timeout_seconds,
        expiry_revision: Some(record.revision),
        node_session_id,
    })
}

impl CoordinatorRpc {
    /// Protocol entrypoint scope belongs here, outside the shared metadata query.
    pub(super) async fn list_e2b_environment_metadata_rpc(
        &self,
        request: Request<pb::ListEnvironmentMetadataRequest>,
    ) -> std::result::Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        self.query_environment_metadata(request, "e2b", |stored| {
            stored.spec.compatibility.is_some()
        })
        .await
    }

    pub(super) async fn get_environment_control_rpc(
        &self,
        request: Request<pb::GetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let state = self.0.state.lock().await;
        let stored = state.session.get(&r.id).await.map_err(status)?;
        tenant(r.caller.as_ref(), &stored.spec.tenant_id)?;
        Ok(Response::new(
            stored
                .control
                .ok_or_else(|| Status::not_found("environment control not found"))?
                .into(),
        ))
    }
    pub(super) async fn update_environment_control_rpc(
        &self,
        request: Request<pb::UpdateEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let mut state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        let stored = state.session.get(&r.id).await.map_err(status)?;
        tenant(r.caller.as_ref(), &stored.spec.tenant_id)?;
        let changed = state
            .session
            .set_environment_timeout(&r.id, r.timeout_seconds, r.extend_only)
            .await
            .map_err(status)?;
        let control = changed
            .control
            .clone()
            .ok_or_else(|| Status::not_found("environment control not found"))?;
        state.remember_environment(changed);
        Ok(Response::new(control.into()))
    }
    pub(super) async fn list_environment_controls_rpc(
        &self,
        request: Request<pb::ListEnvironmentControlsRequest>,
    ) -> std::result::Result<Response<pb::ListEnvironmentControlsResponse>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let caller = r
            .caller
            .ok_or_else(|| Status::unauthenticated("caller context required"))?;
        if caller.tenant_id.is_empty() && !caller.administrator {
            return Err(Status::unauthenticated("tenant required"));
        }
        let state = self.0.state.lock().await;
        let saved = state.session.snapshot().await.map_err(status)?;
        let records = saved
            .environments
            .into_values()
            .filter(|e| caller.administrator || e.spec.tenant_id == caller.tenant_id)
            .filter_map(|e| e.control)
            .map(Into::into)
            .collect();
        Ok(Response::new(pb::ListEnvironmentControlsResponse {
            records,
        }))
    }
    pub async fn expire_environments(&self) -> Result<usize> {
        let now = crate::storage::environment_control::now_ms()?;
        let (session, ids) = {
            let mut state = self.0.state.lock().await;
            state.healthy()?;
            let saved = state.session.snapshot().await?;
            let cursor = state.expiry_cursor.clone().unwrap_or_default();
            let candidates = saved
                .environments
                .into_iter()
                .filter(|(_, e)| {
                    e.control
                        .as_ref()
                        .is_some_and(|c| c.expires_at_unix_ms != 0 && now >= c.expires_at_unix_ms)
                })
                .collect::<BTreeMap<_, _>>();
            let ids = candidates
                .range((
                    std::ops::Bound::Excluded(cursor.clone()),
                    std::ops::Bound::Unbounded,
                ))
                .chain(candidates.range(..=cursor))
                .take(16)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            state.expiry_cursor = ids.last().cloned();
            (state.session.clone(), ids)
        };
        let mut outcomes = stream::iter(ids)
            .map(|id| {
                let session = session.clone();
                // Keep the lifecycle RPC future off the scanner's poll stack.
                async move { Box::pin(self.expire_environment(&session, &id, now)).await }
            })
            .buffer_unordered(4);
        let mut completed = 0;
        let mut first_error = None;
        while let Some(outcome) = outcomes.next().await {
            match outcome {
                Ok(done) => completed += usize::from(done),
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(completed)
    }
    async fn expire_environment(&self, session: &Session, id: &str, now: u64) -> Result<bool> {
        let stored = match session.claim_environment_expiry(id, now).await {
            Ok(s) => s,
            Err(Error::Conflict | Error::NotFound) => return Ok(false),
            Err(e) => return Err(e),
        };
        let record = stored.result.as_ref().ok_or(Error::Conflict)?;
        let control = stored.control.as_ref().ok_or(Error::Conflict)?;
        let (node, session_id) = {
            let mut state = self.0.state.lock().await;
            state.remember_environment(stored.clone());
            let node = state
                .nodes
                .get(&stored.assignment.node_id)
                .ok_or(Error::Conflict)?
                .clone();
            let session_id = node.session.as_ref().ok_or(Error::Conflict)?.id.clone();
            (node, session_id)
        };
        let channel = self
            .0
            .node_tls
            .endpoint(&node.address)
            .map_err(|_| Error::Invalid("invalid node address".into()))?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout)
            .connect()
            .await
            .map_err(|_| Error::Unavailable("expiry node unavailable".into()))?;
        let mut client =
            pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel));
        let caller = Some(pb::CallerContext {
            tenant_id: stored.spec.tenant_id.clone(),
            administrator: false,
        });
        let response = if stored.spec.run_policy.as_ref().is_some_and(|policy| {
            policy.action == adx_core::environment_control::RunExpiryAction::Pause
        }) && record.state == EnvironmentState::Running
        {
            client
                .pause_environment(expiry_pause_request(
                    &stored,
                    control,
                    record,
                    caller,
                    session_id.clone(),
                )?)
                .await
        } else {
            client
                .delete_environment(pb::DeleteEnvironmentRequest {
                    assignment: Some(stored.assignment.clone().try_into()?),
                    caller,
                    failed_revision: None,
                    node_session_id: session_id.clone(),
                    expiry_revision: Some(record.revision),
                })
                .await
        }
        .map_err(adx_protocol::dependency_status)?
        .into_inner();
        if response.durability != pb::Durability::Published as i32 {
            return Err(Error::Unavailable("expiry publication pending".into()));
        }
        let result: EnvironmentRecord = response.record.ok_or(Error::Conflict)?.try_into()?;
        self.commit(result, session_id).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::expiry_pause_request;
    use crate::storage::StoredEnvironment;
    use adx_core::{
        environment_control::{
            EnvironmentCompatibility, EnvironmentControlRecord, RunExpiryAction, RunPolicy,
        },
        lifecycle::{CheckpointStorage, LifecyclePolicy, PausePolicy, RetentionPolicy},
        Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Runtime,
    };
    use adx_protocol::control as pb;

    fn stored(pause: Option<PausePolicy>) -> StoredEnvironment {
        let spec = EnvironmentSpec {
            run_policy: Some(RunPolicy {
                timeout_seconds: 1,
                action: RunExpiryAction::Pause,
            }),
            template_identity: None,
            compatibility: Some(EnvironmentCompatibility {
                template_id: "template".into(),
                envd_version: "v1".into(),
                envd_access_token: "a".repeat(32),
                traffic_access_token: "b".repeat(32),
                allow_public_traffic: false,
            }),
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: LifecyclePolicy {
                pause,
                ..Default::default()
            },
            env: Default::default(),
            id: "environment".into(),
            tenant_id: "tenant".into(),
            image: "image".into(),
            runtime_class: "runc".into(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 0,
            },
            priority: 0,
            scheduling: Default::default(),
            execution: Default::default(),
        };
        let assignment = Assignment {
            environment_id: spec.id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        StoredEnvironment {
            user_metadata: Default::default(),
            resume: None,
            control: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec,
            assignment,
            result: None,
        }
    }

    fn record(stored: &StoredEnvironment) -> EnvironmentRecord {
        EnvironmentRecord {
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
            runtime: Runtime {
                id: "runtime".into(),
                ip: None,
            },
            spec: stored.spec.clone(),
            assignment: stored.assignment.clone(),
            state: EnvironmentState::Running,
            revision: 7,
            resources_held: true,
            checkpoint: None,
            last_operation: None,
        }
    }

    fn request(stored: &StoredEnvironment) -> pb::PauseEnvironmentRequest {
        let record = record(stored);
        expiry_pause_request(
            stored,
            &EnvironmentControlRecord {
                environment_id: stored.spec.id.clone(),
                expires_at_unix_ms: 10,
                started_at_unix_ms: 1,
                revision: 2,
                generation: 1,
                expiry_claimed: true,
                timeout_override_seconds: None,
            },
            &record,
            None,
            "session".into(),
        )
        .expect("fixture")
    }

    #[test]
    fn expiry_pause_request_preserves_native_pause_policy() {
        let forever = request(&stored(Some(PausePolicy {
            retention: RetentionPolicy::Forever,
            timeout_seconds: 123,
            storage: CheckpointStorage::Shared,
        })));
        assert_eq!(forever.timeout_seconds, 123);
        assert_eq!(
            forever.retention.and_then(|r| r.policy),
            Some(pb::retention_policy::Policy::Forever(true))
        );

        let ttl = request(&stored(Some(PausePolicy {
            retention: RetentionPolicy::Ttl { ttl_seconds: 456 },
            timeout_seconds: 78,
            storage: CheckpointStorage::Shared,
        })));
        assert_eq!(ttl.timeout_seconds, 78);
        assert_eq!(
            ttl.retention.and_then(|r| r.policy),
            Some(pb::retention_policy::Policy::TtlSeconds(456))
        );

        let default = request(&stored(None));
        assert_eq!(
            default.timeout_seconds,
            PausePolicy::default().timeout_seconds
        );
        assert_eq!(
            default.retention.and_then(|r| r.policy),
            Some(pb::retention_policy::Policy::TtlSeconds(90_000))
        );
    }
}
