//! Running timer estimates come from the live node owner. Public startedAt comes
//! from the native creation fact and remains stable across Pause/Resume. Paused
//! queries use the native restore-point retention without requiring a live owner;
//! updates still require owner acceptance and never persist dynamic run deadlines.
use super::*;

fn first_running_at(stored: &StoredEnvironment) -> Result<u64> {
    let creation = stored
        .creation
        .as_ref()
        .ok_or_else(|| Error::Unavailable("environment creation time unavailable".into()))?;
    let started = if creation.result == crate::storage::CreationResult::Success {
        creation
            .settled_at_unix_millis
            .filter(|time| *time != 0)
            .unwrap_or(creation.started_at_unix_millis)
    } else {
        creation.started_at_unix_millis
    };
    if started == 0 {
        return Err(Error::Unavailable(
            "environment creation time unavailable".into(),
        ));
    }
    Ok(started)
}

fn paused_control(stored: &StoredEnvironment) -> Result<Option<pb::EnvironmentControlRecord>> {
    let Some(record) = stored
        .result
        .as_ref()
        .filter(|record| record.state == EnvironmentState::Paused)
    else {
        return Ok(None);
    };
    stored.spec.run_policy.as_ref().ok_or(Error::NotFound)?;
    let checkpoint = record
        .checkpoint
        .as_ref()
        .or(record.snapshot_source.as_ref())
        .ok_or(Error::Conflict)?;
    let expires_at_unix_ms = checkpoint
        .expires_at_unix_seconds
        .map(|seconds| {
            seconds
                .checked_mul(1000)
                .ok_or_else(|| Error::Unavailable("checkpoint retention time out of range".into()))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Some(pb::EnvironmentControlRecord {
        environment_id: stored.spec.id.clone(),
        started_at_unix_ms: first_running_at(stored)?,
        expires_at_unix_ms,
        generation: stored.assignment.generation,
        revision: record.revision,
    }))
}

impl CoordinatorRpc {
    async fn control_environment(
        &self,
        id: &str,
        caller: Option<&pb::CallerContext>,
    ) -> std::result::Result<StoredEnvironment, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        let stored = state.session.get(id).await.map_err(status)?;
        tenant(caller, &stored.spec.tenant_id)?;
        Ok(stored)
    }

    async fn control_owner(
        &self,
        stored: &StoredEnvironment,
    ) -> std::result::Result<(StoredNode, String, u64), Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        if stored.invalidated {
            return Err(Status::failed_precondition("environment owner invalidated"));
        }
        let node = state
            .nodes
            .get(&stored.assignment.node_id)
            .ok_or_else(|| Status::unavailable("environment owner unavailable"))?
            .clone();
        let session = node
            .session
            .as_ref()
            .filter(|session| session.routable)
            .ok_or_else(|| Status::unavailable("environment owner unavailable"))?
            .id
            .clone();
        Ok((node, session, state.session.epoch()))
    }

    async fn control_client(
        &self,
        address: &str,
    ) -> std::result::Result<
        pb::node_service_client::NodeServiceClient<adx_transport::rpc::RpcChannel>,
        Status,
    > {
        let channel = self
            .0
            .node_tls
            .endpoint(address)
            .map_err(|_| Status::unavailable("invalid node address"))?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout)
            .connect()
            .await
            .map_err(|_| Status::unavailable("environment owner unavailable"))?;
        Ok(pb::node_service_client::NodeServiceClient::new(
            self.0.node_tls.wrap(channel),
        ))
    }

    pub(super) async fn get_environment_control_rpc(
        &self,
        request: Request<pb::GetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let request = request.into_inner();
        let stored = self
            .control_environment(&request.id, request.caller.as_ref())
            .await?;
        if let Some(control) = paused_control(&stored).map_err(status)? {
            return Ok(Response::new(control));
        }
        let (node, session, coordinator_epoch) = self.control_owner(&stored).await?;
        let started_at = first_running_at(&stored).map_err(status)?;
        let mut control = self
            .control_client(&node.address)
            .await?
            .get_environment_control(pb::NodeGetEnvironmentControlRequest {
                assignment: Some(stored.assignment.try_into().map_err(status)?),
                caller: request.caller,
                node_session_id: session,
                coordinator_epoch,
            })
            .await?
            .into_inner();
        control.started_at_unix_ms = started_at;
        Ok(Response::new(control))
    }

    pub(super) async fn batch_get_environment_control_rpc(
        &self,
        request: Request<pb::BatchGetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::BatchGetEnvironmentControlResponse>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let request = request.into_inner();
        if request.ids.len() > 1000 {
            return Err(Status::invalid_argument(
                "at most 1000 controls per request",
            ));
        }
        let mut owners = BTreeMap::<String, Vec<pb::NodeGetEnvironmentControlRequest>>::new();
        let mut first_running = BTreeMap::new();
        let mut records = Vec::new();
        let mut requested = BTreeSet::new();
        {
            let state = self.0.state.lock().await;
            state.healthy().map_err(status)?;
            for id in request.ids {
                if !requested.insert(id.clone()) {
                    continue;
                }
                let Some(stored) = state.environments.get(&id) else {
                    // Deletion can race the caller's directory page.
                    continue;
                };
                tenant(request.caller.as_ref(), &stored.spec.tenant_id)?;
                if stored
                    .result
                    .as_ref()
                    .is_some_and(|record| record.state == EnvironmentState::Deleted)
                {
                    continue;
                }
                if let Some(control) = paused_control(stored).map_err(status)? {
                    records.push(control);
                    continue;
                }
                if stored.invalidated {
                    return Err(Status::failed_precondition("environment owner invalidated"));
                }
                let node = state
                    .nodes
                    .get(&stored.assignment.node_id)
                    .ok_or_else(|| Status::unavailable("environment owner unavailable"))?;
                let session = node
                    .session
                    .as_ref()
                    .filter(|session| session.routable)
                    .ok_or_else(|| Status::unavailable("environment owner unavailable"))?;
                first_running.insert(id, first_running_at(stored).map_err(status)?);
                owners.entry(node.address.clone()).or_default().push(
                    pb::NodeGetEnvironmentControlRequest {
                        assignment: Some(stored.assignment.clone().try_into().map_err(status)?),
                        caller: request.caller.clone(),
                        node_session_id: session.id.clone(),
                        coordinator_epoch: state.session.epoch(),
                    },
                );
            }
        }
        for (address, requests) in owners {
            let controls = self
                .control_client(&address)
                .await?
                .batch_get_environment_control(pb::NodeBatchGetEnvironmentControlRequest {
                    requests,
                })
                .await?
                .into_inner()
                .records;
            for mut control in controls {
                control.started_at_unix_ms =
                    *first_running.get(&control.environment_id).ok_or_else(|| {
                        Status::unavailable("owner returned an unrequested environment control")
                    })?;
                records.push(control);
            }
        }
        Ok(Response::new(pb::BatchGetEnvironmentControlResponse {
            records,
        }))
    }

    pub(super) async fn update_environment_control_rpc(
        &self,
        request: Request<pb::UpdateEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let request = request.into_inner();
        let stored = self
            .control_environment(&request.id, request.caller.as_ref())
            .await?;
        let (node, session, coordinator_epoch) = self.control_owner(&stored).await?;
        let started_at = first_running_at(&stored).map_err(status)?;
        let mut control = self
            .control_client(&node.address)
            .await?
            .update_environment_control(pb::NodeUpdateEnvironmentControlRequest {
                assignment: Some(stored.assignment.clone().try_into().map_err(status)?),
                caller: request.caller,
                node_session_id: session,
                timeout_seconds: request.timeout_seconds,
                extend_only: request.extend_only,
                coordinator_epoch,
            })
            .await?
            .into_inner();
        if control.expires_at_unix_ms == 0 {
            if let Some(paused) = paused_control(&stored).map_err(status)? {
                return Ok(Response::new(paused));
            }
        }
        control.started_at_unix_ms = started_at;
        Ok(Response::new(control))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_started_at_uses_first_success_and_never_fabricates_epoch() {
        let mut stored: StoredEnvironment = serde_json::from_value(serde_json::json!({
            "creation_generation": 1,
            "creation": {
                "request_id": "create", "result": "success", "reason": null,
                "settled_at_unix_millis": 2000, "started_at_unix_millis": 1000
            },
            "spec": {
                "id": "environment", "tenant_id": "tenant", "image": "image",
                "runtime_class": "runc", "priority": 0,
                "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
            },
            "assignment": {"environment_id": "environment", "node_id": "node",
                "shard_id": 0, "generation": 1, "devices": []},
            "result": null
        }))
        .unwrap();
        assert_eq!(first_running_at(&stored).unwrap(), 2000);
        stored.creation.as_mut().unwrap().settled_at_unix_millis = None;
        assert_eq!(first_running_at(&stored).unwrap(), 1000);
        let creation = stored.creation.as_mut().unwrap();
        creation.result = crate::storage::CreationResult::Failure;
        creation.settled_at_unix_millis = Some(3000);
        assert_eq!(first_running_at(&stored).unwrap(), 1000);
        stored.creation.as_mut().unwrap().started_at_unix_millis = 0;
        assert!(first_running_at(&stored).is_err());
        stored.creation = None;
        assert!(first_running_at(&stored).is_err());
    }
}
