//! Catalog RPCs. Nodes publish completed artifacts; Frontend carries validated
//! tenant identity. A delete only closes new references until physical GC settles.
use super::*;
use adx_core::snapshots::{Snapshot, SnapshotState};

impl MasterRpc {
    /// Independent maintenance loop: never hold the scheduler lock while waiting
    /// for storage deletion on a node. Deleting records cannot acquire new refs.
    pub async fn collect_snapshots(&self) -> Result<usize> {
        let (session, candidates) = {
            let state = self.0.state.lock().await;
            state.healthy()?;
            let snapshots = state.session.retained_snapshots().await?;
            for snapshot in &snapshots {
                for reference in &snapshot.references {
                    if let adx_core::snapshots::Reference::Restore { capsule_id } = reference {
                        if let Some(capsule) = state.capsules.get(capsule_id) {
                            if capsule.spec.snapshot_id.as_deref() == Some(snapshot.id.as_str())
                                && capsule.result.as_ref().is_some_and(|record| {
                                    record
                                        .checkpoint
                                        .as_ref()
                                        .is_some_and(|cp| cp.artifact != snapshot.artifact)
                                        || (!record.resources_held
                                            && matches!(
                                                record.state,
                                                CapsuleState::Failed | CapsuleState::Deleted
                                            ))
                                })
                            {
                                state
                                    .session
                                    .release_snapshot(&snapshot.id, reference.clone())
                                    .await?;
                            }
                        }
                    }
                }
            }
            let snapshots = state.session.retained_snapshots().await?;
            let mut candidates = Vec::new();
            for snapshot in &snapshots {
                if !snapshot.collectable() {
                    continue;
                }
                // Refuse corrupted/aliased ownership instead of deleting another
                // snapshot or Capsule recovery point's bytes.
                let same = |a: &adx_core::CheckpointArtifact| {
                    a.storage == snapshot.artifact.storage
                        && a.location == snapshot.artifact.location
                };
                if snapshots
                    .iter()
                    .any(|s| s.id != snapshot.id && same(&s.artifact))
                    || state.capsules.values().any(|i| {
                        i.result
                            .as_ref()
                            .and_then(|r| r.checkpoint.as_ref())
                            .is_some_and(|c| same(&c.artifact))
                    })
                {
                    continue;
                }
                let Some(live) = state.live.get(&snapshot.source_node_id) else {
                    continue;
                };
                if !live.inspected
                    || live.report.reconciling
                    || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                {
                    continue;
                }
                let Some(node) = state.nodes.get(&snapshot.source_node_id) else {
                    continue;
                };
                candidates.push((snapshot.clone(), node.address.clone(), live.session.clone()));
            }
            (state.session.clone(), candidates)
        };
        let mut completed = 0;
        let mut error = None;
        for (snapshot, address, node_session_id) in candidates {
            let result = async {
                // Recheck persistent authority before issuing each destructive RPC.
                if session.get_snapshot(&snapshot.id).await? != snapshot {
                    return Err(Error::Conflict);
                }
                let channel = Endpoint::from_shared(format!("https://{address}"))
                    .map_err(|_| Error::Invalid("invalid node address".into()))?
                    .tls_config(self.0.node_tls.clone())
                    .map_err(|_| Error::Invalid("invalid node TLS configuration".into()))?
                    .connect_timeout(self.0.timeout)
                    .timeout(self.0.timeout)
                    .connect()
                    .await
                    .map_err(|_| {
                        Error::Unavailable("snapshot collector node unavailable".into())
                    })?;
                let ack = pb::node_service_client::NodeServiceClient::new(channel)
                    .collect_snapshot(pb::CollectSnapshotRequest {
                        snapshot: Some(snapshot.clone().try_into()?),
                        node_session_id,
                    })
                    .await
                    .map_err(|_| {
                        Error::Unavailable("snapshot artifact deletion incomplete".into())
                    })?
                    .into_inner();
                if ack.snapshot_id != snapshot.id || ack.revision != snapshot.revision {
                    return Err(Error::Conflict);
                }
                session
                    .finish_snapshot_deletion(&snapshot.id, snapshot.revision)
                    .await?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => completed += 1,
                Err(e) => error = Some(e),
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok(completed)
    }
}

#[tonic::async_trait]
impl pb::snapshot_service_server::SnapshotService for MasterRpc {
    async fn publish_snapshot(
        &self,
        request: Request<pb::PublishSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("master.snapshot.publish_snapshot", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let Principal::Node(node_id) = principal else {
                    return Err(Status::permission_denied(
                        "only source nodes publish snapshots",
                    ));
                };
                let r = request.into_inner();
                let snapshot: Snapshot = r
                    .snapshot
                    .ok_or_else(|| Status::invalid_argument("snapshot required"))?
                    .try_into()
                    .map_err(status)?;
                if snapshot.source_node_id != node_id {
                    return Err(Status::permission_denied("snapshot source node mismatch"));
                }
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let live = state
                    .live
                    .get(&node_id)
                    .ok_or_else(|| Status::failed_precondition("source node must register"))?;
                if live.session != r.node_session_id
                    || !live.inspected
                    || live.report.reconciling
                    || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                {
                    return Err(Status::failed_precondition(
                        "source node session is not ready",
                    ));
                }
                let source = state.capsules.get(&snapshot.template.id).ok_or_else(|| {
                    Status::failed_precondition("snapshot source is not assigned")
                })?;
                if source.assignment.node_id != node_id || source.spec != snapshot.template {
                    return Err(Status::failed_precondition(
                        "snapshot source ownership changed",
                    ));
                }
                // Retry an already accepted immutable publication without requiring that
                // its source Capsule still retain the original recovery point.
                match state.session.get_snapshot(&snapshot.id).await {
                    Ok(existing)
                        if existing.same_content(&snapshot)
                            && existing.state == SnapshotState::Ready =>
                    {
                        return Ok(Response::new(existing.try_into().map_err(status)?));
                    }
                    Ok(_) => return Err(Status::already_exists("snapshot ID already used")),
                    Err(Error::NotFound) => (),
                    Err(error) => return Err(status(error)),
                }
                let checkpoint = source
                    .result
                    .as_ref()
                    .and_then(|r| r.checkpoint.as_ref())
                    .ok_or_else(|| {
                        Status::failed_precondition("source checkpoint must be committed first")
                    })?;
                if checkpoint.source_runtime_id != snapshot.source_runtime_id
                    || checkpoint.artifact.size_bytes != snapshot.artifact.size_bytes
                    || checkpoint.artifact == snapshot.artifact
                {
                    return Err(Status::failed_precondition(
                        "snapshot requires an independent copy of the source checkpoint",
                    ));
                }
                if state
                    .session
                    .retained_snapshots()
                    .await
                    .map_err(status)?
                    .iter()
                    .any(|s| {
                        s.artifact.storage == snapshot.artifact.storage
                            && s.artifact.location == snapshot.artifact.location
                    })
                {
                    return Err(Status::failed_precondition(
                        "snapshot artifact already owned",
                    ));
                }
                let saved = state
                    .session
                    .publish_snapshot(snapshot)
                    .await
                    .map_err(status)?;
                Ok(Response::new(saved.try_into().map_err(status)?))
            })
            .await
    }

    async fn get_snapshot(
        &self,
        request: Request<pb::GetSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        let trace = adx_observability::trace::Trace::rpc("master.snapshot.get_snapshot", &request);
        trace
            .run_result(async {
                let principal = self.0.peers.authenticate(&request)?;
                let r = request.into_inner();
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                match &principal {
                    Principal::ApiServer => (),
                    Principal::Node(id) => {
                        let live = state
                            .live
                            .get(id)
                            .ok_or_else(|| Status::failed_precondition("node must register"))?;
                        if live.session != r.node_session_id
                            || !live.inspected
                            || live.report.reconciling
                            || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                        {
                            return Err(Status::failed_precondition(
                                "source node session is not ready",
                            ));
                        }
                    }
                    _ => {
                        return Err(Status::permission_denied(
                            "Frontend or source node identity required",
                        ))
                    }
                }
                let snapshot = state.session.get_snapshot(&r.id).await.map_err(status)?;
                match principal {
                    Principal::ApiServer => {
                        tenant(r.caller.as_ref(), &snapshot.template.tenant_id)?
                    }
                    Principal::Node(id) if id == snapshot.source_node_id => (),
                    _ => return Err(Status::permission_denied("snapshot source node mismatch")),
                }
                if snapshot.state == SnapshotState::Deleted {
                    return Err(Status::not_found("snapshot deleted"));
                }
                Ok(Response::new(snapshot.try_into().map_err(status)?))
            })
            .await
    }

    async fn list_snapshots(
        &self,
        request: Request<pb::ListSnapshotsRequest>,
    ) -> std::result::Result<Response<pb::ListSnapshotsResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("master.snapshot.list_snapshots", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("Frontend identity required"));
                }
                let r = request.into_inner();
                let caller = r
                    .caller
                    .as_ref()
                    .ok_or_else(|| Status::unauthenticated("caller context required"))?;
                tenant(Some(caller), &caller.tenant_id)?;
                if r.page_size > 1000
                    || r.page_token.len() > 256
                    || r.page_token.chars().any(char::is_control)
                {
                    return Err(Status::invalid_argument("invalid snapshot page"));
                }
                let size = if r.page_size == 0 { 100 } else { r.page_size } as usize;
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let snapshots = state
                    .session
                    .list_snapshots(&caller.tenant_id)
                    .await
                    .map_err(status)?;
                let mut page: Vec<_> = snapshots
                    .into_iter()
                    .filter(|s| {
                        s.id > r.page_token && (r.name.is_empty() || s.names.contains(&r.name))
                    })
                    .take(size + 1)
                    .collect();
                let next_page_token = if page.len() > size {
                    page.pop();
                    page.last()
                        .map(|snapshot| snapshot.id.clone())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                Ok(Response::new(pb::ListSnapshotsResponse {
                    snapshots: page
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>>>()
                        .map_err(status)?,
                    next_page_token,
                }))
            })
            .await
    }

    async fn delete_snapshot(
        &self,
        request: Request<pb::DeleteSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("master.snapshot.delete_snapshot", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("Frontend identity required"));
                }
                let r = request.into_inner();
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let snapshot = state.session.get_snapshot(&r.id).await.map_err(status)?;
                tenant(r.caller.as_ref(), &snapshot.template.tenant_id)?;
                let deleted = state
                    .session
                    .delete_snapshot(&r.id, &snapshot.template.tenant_id)
                    .await
                    .map_err(status)?;
                Ok(Response::new(deleted.try_into().map_err(status)?))
            })
            .await
    }
}
