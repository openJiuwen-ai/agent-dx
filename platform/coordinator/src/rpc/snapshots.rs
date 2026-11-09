//! Catalog RPCs. Nodes publish completed artifacts; Frontend carries validated
//! tenant identity. A delete only closes new references until physical GC settles.
use super::*;
use adx_core::snapshots::{Snapshot, SnapshotState};

impl CoordinatorRpc {
    /// Independent maintenance loop: never hold the scheduler lock while waiting
    /// for storage deletion on a node. Deleting records cannot acquire new refs.
    pub async fn collect_snapshots(&self) -> Result<usize> {
        let (session, environments, nodes) = {
            let state = self.0.state.lock().await;
            state.healthy()?;
            let nodes: BTreeMap<_, _> = state
                .nodes
                .iter()
                .filter_map(|(id, node)| {
                    let live = state.live.get(id)?;
                    let ready = node
                        .session
                        .as_ref()
                        .is_some_and(|s| s.routable && s.id == live.session)
                        && live.inspected
                        && !live.report.reconciling
                        && live.last_seen.elapsed() < self.0.heartbeat_timeout;
                    ready.then(|| (id.clone(), (node.address.clone(), live.session.clone())))
                })
                .collect();
            (state.session.clone(), state.environments.clone(), nodes)
        };
        let candidates = {
            let snapshots = session.retained_snapshots().await?;
            for snapshot in &snapshots {
                for reference in &snapshot.references {
                    if let adx_core::snapshots::Reference::Restore { environment_id } = reference {
                        if let Some(environment) = environments.get(environment_id) {
                            if environment.spec.snapshot_id.as_deref() == Some(snapshot.id.as_str())
                                && environment.result.as_ref().is_some_and(|record| {
                                    !record.execution_artifacts.contains(&snapshot.artifact)
                                        && record
                                            .snapshot_source
                                            .as_ref()
                                            .is_none_or(|p| p.artifact != snapshot.artifact)
                                        && (record
                                            .checkpoint
                                            .as_ref()
                                            .is_some_and(|cp| cp.artifact != snapshot.artifact)
                                            || record.state == EnvironmentState::Deleted)
                                })
                            {
                                session
                                    .release_consumed_snapshot(&snapshot.id, environment)
                                    .await?;
                            }
                        }
                    }
                }
            }
            let snapshots = session.retained_snapshots().await?;
            let holds = session.execution_holds().await?;
            let mut candidates = Vec::new();
            for snapshot in &snapshots {
                if !snapshot.collectable() {
                    continue;
                }
                // Refuse corrupted/aliased ownership instead of deleting another
                // snapshot or Environment recovery point's bytes.
                let same = |a: &adx_core::CheckpointArtifact| {
                    a.storage == snapshot.artifact.storage
                        && (a.scope.is_empty()
                            || snapshot.artifact.scope.is_empty()
                            || a.scope == snapshot.artifact.scope)
                        && a.location == snapshot.artifact.location
                };
                if snapshots
                    .iter()
                    .any(|s| s.id != snapshot.id && same(&s.artifact))
                    || environments.values().any(|i| {
                        i.result.as_ref().is_some_and(|r| {
                            r.checkpoint
                                .iter()
                                .chain(r.snapshot_source.iter())
                                .any(|p| same(&p.artifact))
                                || r.execution_artifacts.iter().any(same)
                        })
                    })
                    || holds.iter().any(|h| h.artifacts.iter().any(same))
                {
                    continue;
                }
                if snapshot.artifact.storage != "local" {
                    candidates.push((snapshot.clone(), String::new(), String::new(), false));
                    continue;
                }
                if let Some((address, session_id)) = nodes.get(&snapshot.source_node_id) {
                    candidates.push((snapshot.clone(), address.clone(), session_id.clone(), false));
                    continue;
                }
                // Local artifacts are owned by their source node.  A shared
                // backend is reclaimed directly above; never reinterpret a
                // node cache release as physical deletion by delegating it to
                // another node after the source has stopped.
            }
            candidates
        };
        let mut completed = 0;
        let mut error = None;
        for (snapshot, address, node_session_id, delegated) in candidates {
            let result = async {
                // Recheck persistent authority before issuing each destructive RPC.
                if session.get_snapshot(&snapshot.id).await? != snapshot {
                    return Err(Error::Conflict);
                }
                // Local node RPCs are destructive too. Keep exclusion through
                // their ACK and final catalog update, just like shared GC.
                let _deletion = session
                    .claim_artifact_deletion(&snapshot.artifact, Some(&snapshot.id))
                    .await?;
                if snapshot.artifact.storage != "local" {
                    let reclaimers = self
                        .0
                        .reclaimers
                        .read()
                        .expect("reclaimer registry lock poisoned")
                        .clone();
                    reclaimers.remove(&snapshot.artifact).await?;
                    session.forget_artifact(&snapshot.artifact).await?;
                    session
                        .finish_snapshot_deletion(&snapshot.id, snapshot.revision)
                        .await?;
                    return Ok(());
                }
                let channel = self
                    .0
                    .node_tls
                    .endpoint(&address)
                    .map_err(|_| Error::Invalid("invalid node address".into()))?
                    .connect_timeout(self.0.timeout)
                    .timeout(self.0.timeout)
                    .connect()
                    .await
                    .map_err(|_| {
                        Error::Unavailable("snapshot collector node unavailable".into())
                    })?;
                let ack =
                    pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
                        .collect_snapshot(pb::CollectSnapshotRequest {
                            snapshot: Some(snapshot.clone().try_into()?),
                            node_session_id,
                            delegated,
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
impl pb::snapshot_service_server::SnapshotService for CoordinatorRpc {
    async fn publish_snapshot(
        &self,
        request: Request<pb::PublishSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.snapshot.publish_snapshot", &request);
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
                // Serialize duplicate ownership of one artifact without blocking
                // heartbeats or unrelated environments behind metadata IO.
                let _publication = self
                    .create_guard(format!(
                        "snapshot-artifact:{}:{}{}",
                        snapshot.artifact.storage.len(),
                        snapshot.artifact.storage,
                        snapshot.artifact.location
                    ))
                    .await;
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
                let source = state
                    .environments
                    .get(&snapshot.template.id)
                    .ok_or_else(|| {
                        Status::failed_precondition("snapshot source is not assigned")
                    })?;
                if source.assignment.node_id != node_id || !snapshot.matches_template(&source.spec)
                {
                    return Err(Status::failed_precondition(
                        "snapshot source ownership changed",
                    ));
                }
                let source = source.clone();
                let session = state.session.clone();
                drop(state);
                // Retry an already accepted immutable publication without requiring that
                // its source Environment still retain the original recovery point.
                match session.get_snapshot(&snapshot.id).await {
                    Ok(existing)
                        if existing.same_content(&snapshot)
                            && existing.state == SnapshotState::Ready =>
                    {
                        let saved = session
                            .publish_snapshot_from_source(
                                snapshot,
                                &source.assignment,
                                &r.node_session_id,
                            )
                            .await
                            .map_err(status)?;
                        return Ok(Response::new(saved.try_into().map_err(status)?));
                    }
                    Ok(_) => return Err(Status::already_exists("snapshot ID already used")),
                    Err(Error::NotFound) => (),
                    Err(error) => return Err(status(error)),
                }
                if !source.result.as_ref().is_some_and(|record| {
                    record.runtime.id == snapshot.source_runtime_id
                        && record.execution_artifacts.contains(&snapshot.artifact)
                }) {
                    return Err(Status::failed_precondition(
                        "snapshot source input must be committed first",
                    ));
                }
                if session
                    .snapshot_artifact_owned(&snapshot.artifact)
                    .await
                    .map_err(status)?
                {
                    return Err(Status::failed_precondition(
                        "snapshot artifact already owned",
                    ));
                }
                let saved = session
                    .publish_snapshot_from_source(snapshot, &source.assignment, &r.node_session_id)
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
        let trace =
            adx_observability::trace::Trace::rpc("coordinator.snapshot.get_snapshot", &request);
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
                let session = state.session.clone();
                drop(state);
                let snapshot = session.get_snapshot(&r.id).await.map_err(status)?;
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                match principal {
                    Principal::ApiServer => {
                        tenant(r.caller.as_ref(), &snapshot.template.tenant_id)?
                    }
                    Principal::Node(id)
                        if state.live.get(&id).is_some_and(|live| {
                            live.session == r.node_session_id
                                && live.inspected
                                && !live.report.reconciling
                                && live.last_seen.elapsed() < self.0.heartbeat_timeout
                        }) && (id == snapshot.source_node_id
                            || state.environments.values().any(|e| {
                                e.assignment.node_id == id
                                    && e.spec.snapshot_id.as_deref() == Some(snapshot.id.as_str())
                            })) => {}
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
            adx_observability::trace::Trace::rpc("coordinator.snapshot.list_snapshots", &request);
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
                let kind: Option<adx_core::snapshots::SnapshotKind> = r
                    .kind
                    .map(pb::SnapshotKind::try_from)
                    .transpose()
                    .map_err(|_| Status::invalid_argument("invalid snapshot kind"))?
                    .map(Into::into);
                let size = if r.page_size == 0 { 100 } else { r.page_size } as usize;
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let session = state.session.clone();
                drop(state);
                let snapshots = session
                    .list_snapshots(&caller.tenant_id)
                    .await
                    .map_err(status)?;
                let mut page: Vec<_> = snapshots
                    .into_iter()
                    .filter(|s| {
                        kind.is_none_or(|kind| s.kind == kind)
                            && s.id > r.page_token
                            && (r.name.is_empty() || s.names.contains(&r.name))
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
            adx_observability::trace::Trace::rpc("coordinator.snapshot.delete_snapshot", &request);
        trace
            .run_result(async {
                if self.0.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied("Frontend identity required"));
                }
                let r = request.into_inner();
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                let session = state.session.clone();
                drop(state);
                let snapshot = session.get_snapshot(&r.id).await.map_err(status)?;
                tenant(r.caller.as_ref(), &snapshot.template.tenant_id)?;
                let mut users = std::collections::BTreeSet::new();
                let mut templates = std::collections::BTreeSet::new();
                for reference in &snapshot.references {
                    match reference {
                        adx_core::snapshots::Reference::Restore { environment_id } => {
                            users.insert(environment_id.clone());
                        }
                        adx_core::snapshots::Reference::Template {
                            node_id,
                            template_id,
                        } => {
                            templates.insert(format!("{node_id}/{template_id}"));
                        }
                    }
                }
                let state = self.0.state.lock().await;
                state.healthy().map_err(status)?;
                for environment in state.environments.values() {
                    if environment.result.as_ref().is_some_and(|record| {
                        record.execution_artifacts.contains(&snapshot.artifact)
                            || record
                                .snapshot_source
                                .as_ref()
                                .is_some_and(|p| p.artifact == snapshot.artifact)
                    }) {
                        users.insert(environment.spec.id.clone());
                    }
                }
                drop(state);
                for hold in session.execution_holds().await.map_err(status)? {
                    if hold.artifacts.contains(&snapshot.artifact) {
                        users.insert(hold.assignment.environment_id);
                    }
                }
                if !users.is_empty() || !templates.is_empty() {
                    return Err(status(Error::SnapshotInUse {
                        sandbox_ids: users.into_iter().collect(),
                        template_ids: templates.into_iter().collect(),
                    }));
                }
                let deleted = session
                    .delete_snapshot(&r.id, &snapshot.template.tenant_id)
                    .await
                    .map_err(status)?;
                Ok(Response::new(deleted.try_into().map_err(status)?))
            })
            .await
    }
}
