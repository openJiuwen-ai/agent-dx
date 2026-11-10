//! Operator-only upgrade orchestration; execution remains owned by adxlet.
use super::*;
use crate::storage::{UpgradeNode, UpgradeOperation, UpgradeTask};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct Begin {
    task_id: String,
    epoch: u64,
    namespace: String,
    daemonset_uid: String,
    target_image: String,
    nodes: Vec<UpgradeNode>,
}
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
struct Action {
    task_id: String,
    epoch: u64,
    operation_id: String,
    node_id: String,
    pod_uid: String,
    node_session_id: String,
    environment_id: String,
    expected_source_generation: Option<u64>,
    expected_source_revision: Option<u64>,
    expected_target_control_revision: Option<u64>,
    target_node_id: String,
    permit_id: String,
    delete_intent: bool,
    rollback: bool,
    register_source: bool,
    pod_resource_version: String,
    expected_delete_resource_version: String,
    stage: String,
    template_restored: bool,
    target_pod_uid: String,
    environment_namespace: String,
    checkpoint_storage_alias: String,
}

fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| Error::Invalid("invalid upgrade request".into()))
}

fn replacement_binding(
    task: &UpgradeTask,
    request: &Action,
    current: &StoredNode,
    observation: &pb::NodeUpgradeInspection,
) -> Result<bool> {
    let session = &current.session.as_ref().ok_or(Error::Conflict)?.id;
    let previous = task.nodes.get(&request.node_id);
    if previous
        .is_some_and(|node| node.pod_uid == request.pod_uid && node.node_session_id == *session)
    {
        return Ok(false);
    }
    if request.delete_intent
        || request.pod_uid.is_empty()
        || request.node_session_id != *session
        || observation.protocol_version != 3
        || observation.node_session_id != *session
        || observation.task_id != task.task_id
        || observation.epoch != task.epoch
        || observation.phase != "isolated"
        || observation.unknown
        || !observation.strict_empty
        || !current.scheduling_paused
        || current
            .upgrade_pause
            .as_ref()
            .is_none_or(|owner| owner.task_id != task.task_id || owner.epoch != task.epoch)
        || task.operations.values().any(|operation| {
            !operation.settled()
                && (operation.source_node_id == request.node_id
                    || operation.target_node_id == request.node_id)
        })
        || previous.is_some_and(|node| {
            node.phase != "quiescing"
                || node.permit_id.is_none()
                || node.delete_resource_version.is_none()
                || node.pod_uid == request.pod_uid
                || node.node_session_id == *session
                || observation.revision < node.control_revision
        })
    {
        return Err(Error::Conflict);
    }
    Ok(true)
}

fn evidence(i: &pb::NodeUpgradeInspection, node: Option<&UpgradeNode>, id: &str) -> Value {
    json!({"task_id":i.task_id,"epoch":i.epoch,"node_id":id,
        "node_session_id":i.node_session_id,"pod_uid":node.map(|n| &n.pod_uid),"revision":i.revision,
        "phase":if i.phase=="quiescing" {"Quiescing"} else {i.phase.as_str()},
        "controllers":i.controllers,"holds":i.holds,"backends":i.backends,
        "authority_tasks":i.authority_tasks,"unknown":i.unknown,"strict_empty":node.is_some() && i.strict_empty,
        "may_have_executed":i.may_have_executed,"permit_id":node.and_then(|n| n.permit_id.as_ref()),
        "runtime_rootfs_sha256":i.runtime_rootfs_sha256,"runtime_rootfs_path":i.runtime_rootfs_path,
        "protocol_version":i.protocol_version})
}

async fn wake_reconciliation(
    client: &mut pb::node_service_client::NodeServiceClient<adx_transport::rpc::RpcChannel>,
    session: &str,
    ready: bool,
    deadline: tokio::time::Instant,
) {
    if ready {
        return;
    }
    let mut request = Request::new(pb::WakeNodeReconciliationRequest {
        node_session_id: session.into(),
    });
    request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
    // The periodic comparison remains authoritative if an old or busy node cannot be woken.
    let _ = tokio::time::timeout_at(deadline, client.wake_node_reconciliation(request)).await;
}

impl CoordinatorRpc {
    async fn upgrade_session(&self) -> Session {
        self.0.state.lock().await.session.clone()
    }

    async fn checked_node(
        &self,
        node: &StoredNode,
        environment: &str,
    ) -> Result<(
        pb::node_service_client::NodeServiceClient<adx_transport::rpc::RpcChannel>,
        pb::NodeUpgradeInspection,
    )> {
        let channel = self
            .0
            .node_tls
            .endpoint(&node.address)
            .map_err(|_| Error::Conflict)?
            .connect_timeout(self.0.timeout)
            .timeout(Duration::from_secs(600))
            .connect()
            .await
            .map_err(|_| {
                Error::Unavailable("upgrade node unavailable; query before retry".into())
            })?;
        let mut client =
            pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel));
        let session = &node.session.as_ref().ok_or(Error::Conflict)?.id;
        let view = client
            .inspect_node_upgrade(pb::NodeUpgradeInspectRequest {
                node_session_id: session.clone(),
                environment_id: environment.into(),
                ..Default::default()
            })
            .await
            .map_err(adx_protocol::dependency_status)?
            .into_inner();
        if view.protocol_version != 3 || &view.node_session_id != session {
            return Err(Error::Invalid(
                "node upgrade protocol version 3 and matching process session required".into(),
            ));
        }
        Ok((client, view))
    }

    async fn node_view(
        &self,
        node: &StoredNode,
        environment: &str,
    ) -> Result<pb::NodeUpgradeInspection> {
        Ok(self.checked_node(node, environment).await?.1)
    }

    async fn upgrade_node(&self, id: &str) -> Result<StoredNode> {
        let state = self.0.state.lock().await;
        let node = state.nodes.get(id).ok_or(Error::NotFound)?.clone();
        let session = &node.session.as_ref().ok_or(Error::Conflict)?.id;
        if state.live.get(id).is_none_or(|live| {
            live.expired
                || &live.session != session
                || live.last_seen.elapsed() >= self.0.heartbeat_timeout
        }) {
            return Err(Error::Unavailable(
                "node process session is not live".into(),
            ));
        }
        Ok(node)
    }
    async fn save_task(&self, task: UpgradeTask) -> Result<UpgradeTask> {
        let revision = task.revision;
        self.upgrade_session()
            .await
            .save_upgrade(task, Some(revision))
            .await
    }
    async fn control(
        &self,
        task: &mut UpgradeTask,
        id: &str,
        phase: &str,
        grants: Vec<pb::NodeUpgradeGrant>,
    ) -> Result<pb::NodeUpgradeInspection> {
        self.control_with_phase(task, id, phase, phase, grants)
            .await
    }
    async fn control_with_phase(
        &self,
        task: &mut UpgradeTask,
        id: &str,
        phase: &str,
        workflow_phase: &str,
        grants: Vec<pb::NodeUpgradeGrant>,
    ) -> Result<pb::NodeUpgradeInspection> {
        let saved = self.upgrade_node(id).await?;
        // Compatibility must be confirmed before persisting any control intent.
        let (mut client, _) = self.checked_node(&saved, "").await?;
        let session = saved.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        let entry = task.nodes.get_mut(id).ok_or(Error::Conflict)?;
        if entry.node_session_id != session
            || (entry.delete_resource_version.is_some() && phase != "quiescing")
        {
            return Err(Error::Conflict);
        }
        // Persist revocation/authorization intent before it can affect execution.
        entry.control_revision = entry
            .control_revision
            .checked_add(1)
            .ok_or(Error::Conflict)?;
        entry.phase = workflow_phase.into();
        entry.permit_id = None;
        *task = self.save_task(task.clone()).await?;
        let entry = task.nodes.get(id).ok_or(Error::Conflict)?;
        let control = pb::NodeUpgradeControl {
            task_id: task.task_id.clone(),
            epoch: task.epoch,
            node_session_id: session,
            revision: entry.control_revision,
            phase: phase.into(),
            grants,
        };
        let response = client
            .apply_node_upgrade_control(control.clone())
            .await
            .map_err(adx_protocol::dependency_status)?
            .into_inner();
        if response.task_id != control.task_id
            || response.epoch != control.epoch
            || response.node_session_id != control.node_session_id
            || response.revision != control.revision
        {
            return Err(Error::Conflict);
        }
        Ok(response)
    }
    async fn inspect_upgrade(&self, id: &str) -> Result<Value> {
        let node = self.upgrade_node(id).await?;
        let session = node.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        let task = self.upgrade_session().await.upgrade_task().await?;
        let inspection = self.node_view(&node, "").await?;
        let bound = task
            .as_ref()
            .filter(|task| {
                task.active()
                    && task.task_id == inspection.task_id
                    && task.epoch == inspection.epoch
            })
            .and_then(|task| task.nodes.get(id))
            .filter(|entry| {
                entry.node_session_id == session && entry.control_revision == inspection.revision
            });
        let mut result = evidence(&inspection, bound, id);
        let state = self.0.state.lock().await;
        let (capacity, available) = state.scheduler.node_resources(id).ok_or(Error::Conflict)?;
        result["resources"] = json!({"capacity":capacity,"allocatable":available});
        Ok(result)
    }
    pub(crate) async fn upgrade_http(
        &self,
        method: &hyper::Method,
        path: &str,
        body: Value,
    ) -> Result<Value> {
        let _owner = self.0.upgrade_lock.lock().await;
        // Management dispatch contains several independent gRPC workflows. Keep
        // its future off caller/runtime worker stacks as these paths grow.
        self.upgrade_http_owned(method, path, body).await
    }

    fn upgrade_http_owned<'a>(
        &'a self,
        method: &'a hyper::Method,
        path: &'a str,
        body: Value,
    ) -> RpcFuture<'a, Value> {
        // Construct the large dispatch future outside the caller's poll frame.
        Box::pin(async move {
            let suffix = path
                .strip_prefix("/v1/node-upgrades/")
                .ok_or(Error::NotFound)?;
            if method == hyper::Method::GET && suffix == "capabilities" {
                return Ok(
                    json!({"version":2,"node_protocol_version":3,"planned_migration":true,"paused_transfer":true,
                "quiesce":true,"session_isolation":true,"recovery_compatibility":true,"runtime_rootfs_identity":true}),
                );
            }
            if method == hyper::Method::GET {
                if let Some(id) = suffix
                    .strip_prefix("nodes/")
                    .and_then(|s| s.strip_suffix("/inspect"))
                {
                    return self.inspect_upgrade(id).await;
                }
                if let Some(id) = suffix.strip_prefix("tasks/") {
                    let mut task = self
                        .upgrade_session()
                        .await
                        .upgrade_task()
                        .await?
                        .ok_or(Error::NotFound)?;
                    if task.task_id != id {
                        return Err(Error::NotFound);
                    }
                    if task.active() && task.protocol_version == 3 {
                        self.observe_upgrade_operations(&mut task).await?;
                    }
                    return serde_json::to_value(task).map_err(|_| Error::Conflict);
                }
            }
            if method != hyper::Method::POST {
                return Err(Error::NotFound);
            }
            if suffix == "tasks" {
                return self.begin_upgrade(parse(body)?).await;
            }
            let (id, action) = suffix
                .strip_prefix("tasks/")
                .and_then(|s| s.split_once('/'))
                .ok_or(Error::NotFound)?;
            let request: Action = parse(body)?;
            if request.task_id != id || request.operation_id.trim().is_empty() {
                return Err(Error::Conflict);
            }
            let mut task = self
                .upgrade_session()
                .await
                .upgrade_task()
                .await?
                .ok_or(Error::NotFound)?;
            task.require_protocol()?;
            if !task.active()
                && task.task_id == id
                && task.epoch == request.epoch
                && matches!(
                    (task.phase.as_str(), action),
                    ("Completed", "complete") | ("Aborted", "abort")
                )
            {
                return serde_json::to_value(task).map_err(|_| Error::Conflict);
            }
            task.fence(id, request.epoch)?;
            self.observe_upgrade_operations(&mut task).await?;
            if task.phase == "Aborting"
                && !matches!(action, "abort" | "revoke-delete")
                && !request.rollback
            {
                return Err(Error::Conflict);
            }
            self.upgrade_action(task, request, action).await
        })
    }

    fn upgrade_action<'a>(
        &'a self,
        mut task: UpgradeTask,
        request: Action,
        action: &'a str,
    ) -> RpcFuture<'a, Value> {
        // Select the workflow before constructing the other action branches.
        // Migration must not poll below their large debug dispatch frame.
        if action == "migrate" {
            return self.migrate_upgrade(task, request);
        }
        Box::pin(async move {
            match action {
                "prepare-node" => {
                    if request.register_source {
                        self.register_upgrade_source(&mut task, &request, false)
                            .await?;
                    }
                    self.bind_node(&mut task, &request, false).await?;
                    if task
                        .nodes
                        .get(&request.node_id)
                        .is_none_or(|n| !matches!(n.phase.as_str(), "pending" | "evacuating"))
                    {
                        return Err(Error::Conflict);
                    }
                    // Fence new wake demand before pausing scheduling or applying
                    // node control. The same durable CAS rejects already accepted
                    // resume intents without stranding them on a paused source.
                    if task.nodes[&request.node_id].phase == "pending" {
                        task.nodes
                            .get_mut(&request.node_id)
                            .ok_or(Error::Conflict)?
                            .phase = "evacuating".into();
                        task = self.save_task(task).await?;
                    }
                    self.set_upgrade_scheduling(&task, &request.node_id, true)
                        .await?;
                    let snapshot = self.upgrade_session().await.snapshot().await?;
                    let grants = snapshot
                        .environments
                        .values()
                        .filter(|e| e.assignment.node_id == request.node_id)
                        .map(|e| pb::NodeUpgradeGrant {
                            environment_id: e.spec.id.clone(),
                            assignment_generation: e.assignment.generation,
                            actions: vec!["pause".into(), "route".into()],
                        })
                        .collect();
                    self.control(&mut task, &request.node_id, "evacuating", grants)
                        .await?;
                }
                "quiesce" => {
                    if request.rollback && task.phase != "Aborting" {
                        return Err(Error::Conflict);
                    }
                    let current = self.upgrade_node(&request.node_id).await?;
                    let observation = self.node_view(&current, "").await?;
                    let replacement = replacement_binding(&task, &request, &current, &observation)?;
                    self.bind_node(&mut task, &request, replacement).await?;
                    self.set_upgrade_scheduling(&task, &request.node_id, true)
                        .await?;
                    if request.delete_intent {
                        let node = task.nodes.get(&request.node_id).ok_or(Error::Conflict)?;
                        if node.phase != "quiescing"
                            || node.permit_id.as_deref() != Some(&request.permit_id)
                            || request.pod_resource_version.is_empty()
                        {
                            return Err(Error::Conflict);
                        }
                        // Re-read local facts. No new grants are allowed after issuing a permit.
                        let inspection = self.inspect_upgrade(&request.node_id).await?;
                        if inspection["strict_empty"] != true {
                            return Err(Error::Conflict);
                        }
                        task.nodes
                            .get_mut(&request.node_id)
                            .ok_or(Error::Conflict)?
                            .delete_resource_version = Some(request.pod_resource_version.clone());
                        self.save_task(task).await?;
                        return Ok(inspection);
                    }
                    return self.quiesce_upgrade(&mut task, &request.node_id).await;
                }
                "revoke-delete" => {
                    if request.expected_delete_resource_version.is_empty()
                        || request.pod_resource_version.is_empty()
                        || request.pod_resource_version == request.expected_delete_resource_version
                    {
                        return Err(Error::Conflict);
                    }
                    self.bind_node(&mut task, &request, false).await?;
                    let entry = task
                        .nodes
                        .get_mut(&request.node_id)
                        .ok_or(Error::Conflict)?;
                    if let Some(old) = &entry.delete_resource_version {
                        if old != &request.expected_delete_resource_version {
                            return Err(Error::Conflict);
                        }
                        // The caller has CAS-patched this Pod to invalidate exactly
                        // the old DELETE. Replays cannot revoke a newer intent.
                        entry.delete_resource_version = None;
                        entry.permit_id = None;
                        task = self.save_task(task).await?;
                    }
                    // An already cleared intent is a same-binding no-op. In
                    // particular, retain any new permit not yet confirmed for DELETE.
                }
                "verify-node" => {
                    self.bind_node(&mut task, &request, false).await?;
                    let target = self.upgrade_node(&request.node_id).await?;
                    let inspection = self.node_view(&target, "").await?;
                    let scope = target.node.labels.get("ADX_CHECKPOINT_SCOPE");
                    if inspection.unknown
                        || target
                            .session
                            .as_ref()
                            .is_none_or(|session| !session.routable)
                        || target.checkpoint_gc_fenced
                        || scope.is_none_or(|scope| {
                            scope.is_empty() || !target.checkpoint_scopes.contains(scope)
                        })
                        || target.node.labels.get("ADX_RECOVERY_ROOTFS_SHA256")
                            != Some(&inspection.runtime_rootfs_sha256)
                        || target.node.labels.get("ADX_RECOVERY_ROOTFS_PATH")
                            != Some(&inspection.runtime_rootfs_path)
                        || !target.node.runtime_classes.iter().any(|runtime| {
                            crate::storage::recovery_profile_requirements(
                                runtime,
                                &target.node.labels,
                            )
                            .is_ok()
                        })
                    {
                        return Err(Error::Invalid(
                            "node recovery capabilities are unavailable or unverified".into(),
                        ));
                    }
                    let node = task.nodes.get(&request.node_id).ok_or(Error::Conflict)?;
                    if node.phase != "verified" {
                        if node.permit_id.as_deref() != Some(&request.permit_id)
                            || node.phase != "quiescing"
                        {
                            return Err(Error::Conflict);
                        }
                        let inspection = self.inspect_upgrade(&request.node_id).await?;
                        if inspection["strict_empty"] != true {
                            return Err(Error::Conflict);
                        }
                    }
                    self.control(&mut task, &request.node_id, "verified", vec![])
                        .await?;
                    self.set_upgrade_scheduling(&task, &request.node_id, false)
                        .await?;
                }
                "complete" => {
                    if task.phase != "Active"
                        || task.operations.values().any(|op| op.phase != "Completed")
                        || task.nodes.values().any(|n| n.phase != "verified")
                    {
                        return Err(Error::Conflict);
                    }
                    task = self.finish_upgrade(task, "Completed").await?;
                }
                "abort" if request.stage == "freeze" => {
                    task.phase = "Aborting".into();
                    task = self.save_task(task).await?;
                    self.cancel_prepared_operations(&mut task).await?;
                }
                "abort" if request.stage == "restore-original" => {
                    if task.phase != "Aborting" {
                        return Err(Error::Conflict);
                    }
                    if request.register_source {
                        self.register_upgrade_source(&mut task, &request, true)
                            .await?;
                    }
                    self.bind_node(&mut task, &request, false).await?;
                    let node = task
                        .nodes
                        .get(&request.node_id)
                        .ok_or(Error::Conflict)?
                        .clone();
                    if node.pod_uid != node.original_pod_uid
                        || node.node_session_id != node.original_session_id
                        || node.delete_resource_version.is_some()
                        || task.operations.values().any(|op| {
                            (op.source_node_id == request.node_id
                                || op.target_node_id == request.node_id)
                                && !op.settled()
                        })
                    {
                        return Err(Error::Conflict);
                    }
                    self.control_with_phase(
                        &mut task,
                        &request.node_id,
                        "released",
                        "RestoredOriginal",
                        vec![],
                    )
                    .await?;
                    self.set_upgrade_scheduling(&task, &request.node_id, false)
                        .await?;
                    return Ok(
                        json!({"restored_original":true,"node_id":node.node_id,"pod_uid":node.pod_uid,"node_session_id":node.node_session_id}),
                    );
                }
                "abort" if request.stage == "finalize" => {
                    // A Job must independently verify old template/generation and fresh Pod inventory.
                    if !request.template_restored
                        || task.operations.values().any(|op| !op.settled())
                        || task.nodes.values().any(|n| {
                            !matches!(
                                n.phase.as_str(),
                                "verified" | "pending" | "RestoredOriginal"
                            )
                        })
                    {
                        return Err(Error::Conflict);
                    }
                    task = self.finish_upgrade(task, "Aborted").await?;
                }
                _ => return Err(Error::Invalid("unknown upgrade action".into())),
            }
            serde_json::to_value(task).map_err(|_| Error::Conflict)
        })
    }

    async fn observe_upgrade_operations(&self, task: &mut UpgradeTask) -> Result<()> {
        let session = self.upgrade_session().await;
        let mut changed = false;
        for operation in task.operations.values_mut().filter(|op| !op.settled()) {
            let stored = match session.get(&operation.environment_id).await {
                Ok(stored) => stored,
                Err(Error::NotFound)
                    if session
                        .confirmed_deletion(&operation.environment_id, operation.source_generation)
                        .await? =>
                {
                    operation.phase = "Completed".into();
                    changed = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if stored.invalidated {
                continue;
            }
            let Some(record) = &stored.result else {
                continue;
            };
            if record.state == EnvironmentState::Deleted && !record.resources_held {
                operation.phase = "Completed".into();
                changed = true;
                continue;
            }
            let source_matches = stored.recovery.as_ref().is_some_and(|recovery| {
                recovery.source.node_id == operation.source_node_id
                    && recovery.source.generation == operation.source_generation
            });
            let complete = if operation.was_running {
                record.state == EnvironmentState::Running
                    && record.resources_held
                    && record.runtime.ip.is_some()
            } else {
                record.state == EnvironmentState::Paused && !record.resources_held
            };
            if stored.assignment.node_id == operation.target_node_id
                && source_matches
                && complete
                && self
                    .await_migration_adoption(
                        operation,
                        &stored.assignment,
                        operation.was_running,
                        false,
                    )
                    .await
                    .is_ok()
            {
                operation.phase = "Completed".into();
                changed = true;
            }
        }
        if changed {
            *task = self.save_task(task.clone()).await?;
        }
        Ok(())
    }
    async fn finish_upgrade(&self, mut task: UpgradeTask, phase: &str) -> Result<UpgradeTask> {
        for id in task.nodes.keys().cloned().collect::<Vec<_>>() {
            let workflow = task.nodes[&id].phase.clone();
            self.control_with_phase(&mut task, &id, "released", &workflow, vec![])
                .await?;
            self.set_upgrade_scheduling(&task, &id, false).await?;
        }
        task.phase = phase.into();
        self.save_task(task).await
    }

    async fn cancel_prepared_operations(&self, task: &mut UpgradeTask) -> Result<()> {
        if task.phase != "Aborting" {
            return Err(Error::Conflict);
        }
        let session = self.upgrade_session().await;
        let sources: Vec<_> = task
            .nodes
            .values()
            .filter(|node| node.phase == "evacuating")
            .map(|node| node.node_id.clone())
            .collect();
        for id in &sources {
            let node = &task.nodes[id];
            if node.pod_uid != node.original_pod_uid
                || node.node_session_id != node.original_session_id
            {
                return Err(Error::Conflict);
            }
            let snapshot = session.snapshot().await?;
            let grants = snapshot
                .environments
                .values()
                .filter(|stored| stored.assignment.node_id == *id)
                .map(|stored| pb::NodeUpgradeGrant {
                    environment_id: stored.spec.id.clone(),
                    assignment_generation: stored.assignment.generation,
                    actions: vec!["route".into()],
                })
                .collect();
            // Close delayed/queued task Pause calls while admitted work settles.
            // Routes remain available for a successful rollback on this source.
            self.control(task, id, "evacuating", grants).await?;
        }
        let pending: Vec<_> = task
            .operations
            .iter()
            .filter(|(_, op)| op.phase == "Prepared" && sources.contains(&op.source_node_id))
            .map(|(id, op)| (id.clone(), op.clone()))
            .collect();
        let mut changed = false;
        for (id, op) in pending {
            let node = self.upgrade_node(&op.source_node_id).await?;
            let binding = &task.nodes[&op.source_node_id];
            if binding.node_session_id != op.source_session_id
                || node
                    .session
                    .as_ref()
                    .is_none_or(|session| session.id != op.source_session_id)
            {
                return Err(Error::Conflict);
            }
            let stored = session.get(&op.environment_id).await?;
            let Some(record) = stored.result.as_ref() else {
                continue;
            };
            let original_state = if op.was_running {
                record.state == EnvironmentState::Running
                    && record.resources_held
                    && record.runtime.ip.is_some()
            } else {
                record.state == EnvironmentState::Paused && !record.resources_held
            };
            if stored.assignment.node_id != op.source_node_id
                || stored.assignment.generation != op.source_generation
                || stored.invalidated
                || stored.resume.is_some()
                || stored
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.pending)
                || record.restart_pending
                || record.revision < op.source_revision
                || !original_state
            {
                continue;
            }
            let view = self.node_view(&node, &op.environment_id).await?;
            if view.unknown
                || view.task_id != task.task_id
                || view.epoch != task.epoch
                || view.node_session_id != op.source_session_id
                || view.revision != binding.control_revision
            {
                continue;
            }
            let Some(local) = view.environment else {
                continue;
            };
            if local.durability != pb::Durability::Published as i32 {
                continue;
            }
            let Some(local) = local.record else {
                continue;
            };
            let local: EnvironmentRecord = local.try_into()?;
            if &local != record || session.get(&op.environment_id).await? != stored {
                continue;
            }
            task.operations.get_mut(&id).ok_or(Error::Conflict)?.phase = "Cancelled".into();
            changed = true;
        }
        if changed {
            *task = self.save_task(task.clone()).await?;
        }
        Ok(())
    }
    async fn begin_upgrade(&self, request: Begin) -> Result<Value> {
        let session = self.upgrade_session().await;
        if let Some(old) = session
            .upgrade_task()
            .await?
            .filter(|t| t.active() || t.task_id == request.task_id)
        {
            old.require_protocol()?;
            if old.task_id != request.task_id
                || old.epoch != request.epoch
                || old.daemonset_uid != request.daemonset_uid
                || old.target_image != request.target_image
                || old.namespace != request.namespace
            {
                return Err(Error::Conflict);
            }
            return serde_json::to_value(old).map_err(|_| Error::Conflict);
        }
        let mut nodes = BTreeMap::new();
        for mut item in request.nodes {
            let node = self.upgrade_node(&item.node_id).await?;
            if node.upgrade_pause.is_some()
                || node
                    .session
                    .as_ref()
                    .is_none_or(|s| s.id != item.node_session_id)
            {
                return Err(Error::Conflict);
            }
            self.inspect_upgrade(&item.node_id).await?;
            item.original_pod_uid = item.pod_uid.clone();
            item.original_session_id = item.node_session_id.clone();
            item.original_paused = node.scheduling_paused;
            item.phase = "pending".into();
            item.control_revision = 0;
            item.permit_id = None;
            item.delete_resource_version = None;
            if nodes.insert(item.node_id.clone(), item).is_some() {
                return Err(Error::Invalid("duplicate node".into()));
            }
        }
        let task = UpgradeTask {
            protocol_version: 3,
            task_id: request.task_id,
            epoch: request.epoch,
            namespace: request.namespace,
            daemonset_uid: request.daemonset_uid,
            target_image: request.target_image,
            revision: 0,
            phase: "Active".into(),
            nodes,
            operations: BTreeMap::new(),
        };
        serde_json::to_value(session.save_upgrade(task, None).await?).map_err(|_| Error::Conflict)
    }
    async fn bind_node(&self, task: &mut UpgradeTask, r: &Action, replacement: bool) -> Result<()> {
        let current = self.upgrade_node(&r.node_id).await?;
        let observation = self.node_view(&current, "").await?;
        let session = current.session.as_ref().ok_or(Error::Conflict)?.id.clone();
        if r.pod_uid.is_empty() {
            return Err(Error::Invalid("Pod UID required".into()));
        }
        if !r.node_session_id.is_empty() && r.node_session_id != session {
            return Err(Error::Conflict);
        }
        if let Some(entry) = task.nodes.get_mut(&r.node_id) {
            if entry.pod_uid != r.pod_uid || entry.node_session_id != session {
                if !replacement || entry.phase != "quiescing" || entry.permit_id.is_none() {
                    return Err(Error::Conflict);
                }
                if entry.pod_uid == r.pod_uid {
                    return Err(Error::Conflict);
                }
                entry.pod_uid = r.pod_uid.clone();
                entry.node_session_id = session;
                entry.permit_id = None;
                entry.delete_resource_version = None;

                entry.control_revision = entry.control_revision.max(observation.revision);
            }
        } else if replacement {
            task.nodes.insert(
                r.node_id.clone(),
                UpgradeNode {
                    node_id: r.node_id.clone(),
                    pod_uid: r.pod_uid.clone(),
                    original_pod_uid: String::new(),
                    original_session_id: String::new(),
                    node_session_id: session,
                    original_paused: self
                        .upgrade_session()
                        .await
                        .upgrade_original_paused(&task.task_id, task.epoch, &r.node_id)
                        .await?,
                    phase: "pending".into(),
                    control_revision: observation.revision,
                    permit_id: None,
                    delete_resource_version: None,
                },
            );
        } else {
            return Err(Error::Conflict);
        }
        *task = self.save_task(task.clone()).await?;
        Ok(())
    }
    async fn register_upgrade_source(
        &self,
        task: &mut UpgradeTask,
        r: &Action,
        restore_only: bool,
    ) -> Result<()> {
        if !matches!(
            (task.phase.as_str(), restore_only),
            ("Active", false) | ("Aborting", true)
        ) || r.pod_uid.is_empty()
            || r.node_session_id.is_empty()
        {
            return Err(Error::Conflict);
        }
        let node = self.upgrade_node(&r.node_id).await?;
        if node
            .session
            .as_ref()
            .is_none_or(|session| session.id != r.node_session_id)
        {
            return Err(Error::Conflict);
        }
        if let Some(entry) = task.nodes.get(&r.node_id) {
            if entry.pod_uid != r.pod_uid || entry.node_session_id != r.node_session_id {
                return Err(Error::Conflict);
            }
            if !entry.original_pod_uid.is_empty() {
                return if entry.original_pod_uid == r.pod_uid
                    && entry.original_session_id == r.node_session_id
                {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            if entry.phase != "pending"
                || !entry.original_session_id.is_empty()
                || entry.permit_id.is_some()
                || entry.delete_resource_version.is_some()
            {
                return Err(Error::Conflict);
            }
        }
        let observation = self.node_view(&node, "").await?;
        if observation.node_session_id != r.node_session_id {
            return Err(Error::Conflict);
        }
        let original_paused = self
            .upgrade_session()
            .await
            .upgrade_original_paused(&task.task_id, task.epoch, &r.node_id)
            .await?;
        let revision = task
            .nodes
            .get(&r.node_id)
            .map_or(observation.revision, |entry| {
                entry.control_revision.max(observation.revision)
            });
        task.nodes.insert(
            r.node_id.clone(),
            UpgradeNode {
                node_id: r.node_id.clone(),
                pod_uid: r.pod_uid.clone(),
                original_pod_uid: r.pod_uid.clone(),
                original_session_id: r.node_session_id.clone(),
                node_session_id: r.node_session_id.clone(),
                original_paused,
                phase: "pending".into(),
                control_revision: revision,
                permit_id: None,
                delete_resource_version: None,
            },
        );
        *task = self.save_task(task.clone()).await?;
        Ok(())
    }
    async fn set_upgrade_scheduling(
        &self,
        task: &UpgradeTask,
        id: &str,
        closed: bool,
    ) -> Result<()> {
        let mut state = self.0.state.lock().await;
        let node = state
            .session
            .set_upgrade_node_scheduling(&task.task_id, task.epoch, id, closed)
            .await?;
        // Discard optional ownership commands admitted before the durable node gate.
        // The current claim path compares this token in the same write as ownership.
        if closed {
            state.session.claim_barrier().await?;
        }
        state.scheduler.register(node.node.clone())?;
        state.nodes.insert(id.into(), node);
        self.0.changed.notify_waiters();
        Ok(())
    }
    async fn quiesce_upgrade(&self, task: &mut UpgradeTask, id: &str) -> Result<Value> {
        if task.operations.values().any(|operation| {
            !operation.settled()
                && (operation.source_node_id == id || operation.target_node_id == id)
        }) {
            return Err(Error::Unavailable(
                "migration outcome is unresolved; keep isolation".into(),
            ));
        }
        let inspection = self.control(task, id, "quiescing", vec![]).await?;
        let snapshot = self.upgrade_session().await.snapshot().await?;
        if !inspection.strict_empty
            || inspection.unknown
            || snapshot.environments.values().any(|e| {
                e.assignment.node_id == id
                    && e.result
                        .as_ref()
                        .is_none_or(|r| r.state != EnvironmentState::Deleted || r.resources_held)
            })
        {
            return Err(Error::Unavailable(
                "node is not strictly empty; keep isolation".into(),
            ));
        }
        let node = task.nodes.get_mut(id).ok_or(Error::Conflict)?;
        node.permit_id = Some(uuid::Uuid::new_v4().to_string());
        *task = self.save_task(task.clone()).await?;
        Ok(evidence(&inspection, task.nodes.get(id), id))
    }
}

impl CoordinatorRpc {
    async fn migration_source_node(&self, task: &UpgradeTask, r: &Action) -> Result<StoredNode> {
        let binding = task.nodes.get(&r.node_id).ok_or(Error::Conflict)?;
        let source = self.upgrade_node(&r.node_id).await?;
        if r.pod_uid != binding.pod_uid
            || r.node_session_id != binding.node_session_id
            || source
                .session
                .as_ref()
                .is_none_or(|session| session.id != binding.node_session_id)
        {
            return Err(Error::Conflict);
        }
        Ok(source)
    }

    async fn require_matching_rootfs(
        &self,
        source: &StoredNode,
        target: &StoredNode,
        spec: Option<&EnvironmentSpec>,
    ) -> Result<()> {
        let (source, target) =
            tokio::try_join!(self.node_view(source, ""), self.node_view(target, ""))?;
        matching_rootfs(&source, &target, spec)
    }

    async fn migration_nodes(&self, op: &UpgradeOperation) -> Result<(StoredNode, StoredNode)> {
        let source = self.upgrade_node(&op.source_node_id).await?;
        let target = self.upgrade_node(&op.target_node_id).await?;
        if source
            .session
            .as_ref()
            .is_none_or(|session| session.id != op.source_session_id)
            || target
                .session
                .as_ref()
                .is_none_or(|session| session.id != op.target_session_id)
        {
            return Err(Error::Conflict);
        }
        Ok((source, target))
    }

    fn migrate_upgrade(&self, mut task: UpgradeTask, r: Action) -> RpcFuture<'_, Value> {
        Box::pin(async move {
            if r.environment_id.is_empty() {
                return Err(Error::Invalid("environment_id required".into()));
            }
            let session = self.upgrade_session().await;
            let mut stored = session.get(&r.environment_id).await?;
            if !task.operations.contains_key(&r.operation_id) {
                let source_node = self.migration_source_node(&task, &r).await?;
                if stored.assignment.node_id != r.node_id
                    || stored.invalidated
                    || stored.resume.is_some()
                    || stored.recovery.as_ref().is_some_and(|op| op.pending)
                {
                    return Err(Error::Conflict);
                }
                if task
                    .nodes
                    .get(&r.node_id)
                    .is_none_or(|n| n.phase != "evacuating")
                {
                    return Err(Error::Conflict);
                }
                let source = stored.result.as_ref().ok_or(Error::Conflict)?;
                if r.expected_source_generation
                    .is_some_and(|generation| generation != stored.assignment.generation)
                    || r.expected_source_revision
                        .is_some_and(|revision| revision != source.revision)
                {
                    return Err(Error::Conflict);
                }
                if !matches!(
                    source.state,
                    EnvironmentState::Running | EnvironmentState::Paused
                ) || source.restart_pending
                {
                    return Err(Error::Conflict);
                }
                let target = self.migration_target(&stored, &r).await?;
                let target_node = self.upgrade_node(&target).await?;
                self.require_matching_rootfs(&source_node, &target_node, Some(&stored.spec))
                    .await?;
                task.operations.insert(
                    r.operation_id.clone(),
                    UpgradeOperation {
                        environment_id: r.environment_id.clone(),
                        source_node_id: r.node_id.clone(),
                        target_node_id: target.clone(),
                        source_generation: stored.assignment.generation,
                        source_revision: source.revision,
                        source_session_id: source_node.session.ok_or(Error::Conflict)?.id,
                        target_session_id: self
                            .upgrade_node(&target)
                            .await?
                            .session
                            .ok_or(Error::Conflict)?
                            .id,
                        was_running: source.state == EnvironmentState::Running,
                        phase: "Prepared".into(),
                    },
                );
                task = self.save_task(task).await?;
            }
            let op = task
                .operations
                .get(&r.operation_id)
                .ok_or(Error::Conflict)?
                .clone();
            if op.environment_id != r.environment_id
                || op.source_node_id != r.node_id
                || (!r.target_node_id.is_empty() && r.target_node_id != op.target_node_id)
            {
                return Err(Error::Conflict);
            }
            if op.phase == "Completed" {
                return self.migration_result(&stored).await;
            }
            if op.phase == "Cancelled" {
                return Err(Error::Conflict);
            }
            self.migration_source_node(&task, &r).await?;
            let (source_node, target_node) = self.migration_nodes(&op).await?;
            self.require_matching_rootfs(&source_node, &target_node, Some(&stored.spec))
                .await?;
            if stored.assignment.node_id == op.source_node_id {
                let source = stored.result.clone().ok_or(Error::Conflict)?;
                if stored.assignment.generation != op.source_generation {
                    return Err(Error::Conflict);
                }
                if source.state == EnvironmentState::Running {
                    let paused = self
                        .checked_node(&source_node, "")
                        .await?
                        .0
                        .pause_environment(pb::PauseEnvironmentRequest {
                            assignment: Some(stored.assignment.clone().try_into()?),
                            caller: Some(pb::CallerContext {
                                tenant_id: stored.spec.tenant_id.clone(),
                                administrator: true,
                            }),
                            operation_id: format!("task:{}:{}", task.task_id, r.operation_id),
                            expected_revision: op.source_revision,
                            node_session_id: op.source_session_id.clone(),
                            timeout_seconds: 300,
                            retention: Some(adx_core::lifecycle::RetentionPolicy::Forever.into()),
                        })
                        .await
                        .map_err(adx_protocol::dependency_status)?
                        .into_inner();
                    if paused.durability != pb::Durability::Published as i32 {
                        return Err(Error::Unavailable(
                            "source pause publication pending".into(),
                        ));
                    }
                    let record: EnvironmentRecord =
                        paused.record.ok_or(Error::Conflict)?.try_into()?;
                    if record.state != EnvironmentState::Paused || record.resources_held {
                        return Err(Error::Conflict);
                    }
                    self.commit(record, source_node.session.ok_or(Error::Conflict)?.id)
                        .await?;
                    stored = session.get(&r.environment_id).await?;
                }
                let paused = stored.result.as_ref().ok_or(Error::Conflict)?;
                if paused.state != EnvironmentState::Paused || paused.resources_held {
                    return Err(Error::Conflict);
                }
                // Both ordinary recovery and planned migration reserve through
                // the same scheduler round and durable assignment path.
                self.migration_nodes(&op).await?;
                let mut state = self.0.state.lock().await;
                state.recover_authoritative_state().await?;
                let point = stored
                    .result
                    .as_ref()
                    .and_then(|record| {
                        record
                            .checkpoint
                            .as_ref()
                            .or(record.snapshot_source.as_ref())
                    })
                    .ok_or(Error::Conflict)?;
                let spec = recovery::checkpoint_spec(
                    &stored,
                    Some(point),
                    None,
                    &BTreeSet::new(),
                    Some(&op.target_node_id),
                )?;
                if state
                    .scheduler
                    .requests
                    .get(&stored.spec.id)
                    .is_none_or(|(_, queued)| queued != &spec)
                {
                    state.scheduler.retire_claim(&stored.spec.id)?;
                    state.scheduler.submit_recovery(spec)?;
                }
                for _ in 0..state.scheduler.shards.len() {
                    state.drive().await?;
                }
                stored = session.get(&r.environment_id).await?;
                if stored.assignment.node_id == op.source_node_id {
                    return Err(Error::NoCapacity);
                }
                if stored.assignment.node_id != op.target_node_id {
                    return Err(Error::Conflict);
                }
                task.operations
                    .get_mut(&r.operation_id)
                    .ok_or(Error::Conflict)?
                    .phase = "Assigned".into();
                task = session
                    .save_upgrade(task.clone(), Some(task.revision))
                    .await?;
            }
            if stored.assignment.node_id != op.target_node_id {
                return Err(Error::Conflict);
            }
            if op.was_running
                && stored
                    .result
                    .as_ref()
                    .is_some_and(|record| record.state == EnvironmentState::Paused)
            {
                self.await_migration_adoption(&op, &stored.assignment, false, true)
                    .await?;
                let (_, node) = self.migration_nodes(&op).await?;
                self.node_view(&node, "").await?;
                self.recover_record(
                    node,
                    stored.result.clone().ok_or(Error::Conflict)?,
                    stored
                        .resume
                        .as_ref()
                        .and_then(|intent| intent.run_timeout_seconds),
                    Duration::from_secs(600),
                    true,
                )
                .await?;
                stored = session.get(&r.environment_id).await?;
            }
            if stored.invalidated || stored.assignment.node_id != op.target_node_id {
                return Err(Error::Conflict);
            }
            self.await_migration_adoption(&op, &stored.assignment, op.was_running, !op.was_running)
                .await?;
            let result = stored.result.as_ref().ok_or(Error::Conflict)?;
            if (op.was_running
                && (result.state != EnvironmentState::Running
                    || !result.resources_held
                    || result.runtime.ip.is_none()))
                || (!op.was_running
                    && (result.state != EnvironmentState::Paused || result.resources_held))
            {
                return Err(Error::Unavailable(
                    "migration target has not reached the expected durable state".into(),
                ));
            }
            task.operations
                .get_mut(&r.operation_id)
                .ok_or(Error::Conflict)?
                .phase = "Completed".into();
            self.save_task(task).await?;
            self.0.changed.notify_waiters();
            self.migration_result(&stored).await
        })
    }

    async fn await_migration_adoption(
        &self,
        op: &UpgradeOperation,
        assignment: &adx_core::Assignment,
        running: bool,
        mut wake_nodes: bool,
    ) -> Result<()> {
        let started = tokio::time::Instant::now();
        let deadline = started + self.0.timeout;
        let mut source_retired_ms = None;
        let mut target_adopted_ms = None;
        loop {
            let source = self.upgrade_node(&op.source_node_id).await?;
            let target = self.upgrade_node(&op.target_node_id).await?;
            if source
                .session
                .as_ref()
                .is_none_or(|s| s.id != op.source_session_id)
                || target
                    .session
                    .as_ref()
                    .is_none_or(|s| s.id != op.target_session_id)
            {
                return Err(Error::Conflict);
            }
            let ((mut source_client, source), (mut target_client, target)) = tokio::try_join!(
                self.checked_node(&source, &assignment.environment_id),
                self.checked_node(&target, &assignment.environment_id),
            )?;
            let retired = source
                .environment
                .as_ref()
                .and_then(|r| r.record.as_ref())
                .is_none_or(|record| {
                    record
                        .assignment
                        .as_ref()
                        .is_some_and(|a| a.generation != op.source_generation)
                });
            let adopted = target.environment.as_ref().is_some_and(|result| {
                result.durability == pb::Durability::Published as i32
                    && result.record.as_ref().is_some_and(|record| {
                        record.assignment.as_ref().is_some_and(|a| {
                            a.generation == assignment.generation && a.node_id == assignment.node_id
                        }) && record.state
                            == if running {
                                pb::EnvironmentState::Running as i32
                            } else {
                                pb::EnvironmentState::Paused as i32
                            }
                            && record.resources_held == running
                    })
            });
            let source_ready = retired && source.backends == 0 && !source.unknown;
            let target_ready = adopted && !target.unknown && (running || target.backends == 0);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
            if source_ready {
                source_retired_ms.get_or_insert(elapsed_ms);
            }
            if target_ready {
                target_adopted_ms.get_or_insert(elapsed_ms);
            }
            let complete = source_ready && target_ready;
            if complete || tokio::time::Instant::now() >= deadline {
                adx_observability::info!(event = "environment_migration_handoff",
                    environment_id = %assignment.environment_id, generation = assignment.generation,
                    source_node_id = %op.source_node_id, target_node_id = %op.target_node_id,
                    running, complete, ?source_retired_ms, ?target_adopted_ms,
                    total_ms = elapsed_ms, "observed migration handoff");
                return if complete {
                    Ok(())
                } else {
                    Err(Error::Unavailable(
                        "ownership transferred; local adoption or source retirement pending".into(),
                    ))
                };
            }
            if wake_nodes {
                wake_nodes = false;
                let wake_deadline =
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(250));
                tokio::join!(
                    wake_reconciliation(
                        &mut source_client,
                        &op.source_session_id,
                        source_ready,
                        wake_deadline
                    ),
                    wake_reconciliation(
                        &mut target_client,
                        &op.target_session_id,
                        target_ready,
                        wake_deadline
                    ),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn migration_result(&self, stored: &StoredEnvironment) -> Result<Value> {
        Ok(
            json!({"environment_id":stored.spec.id,"node_id":stored.assignment.node_id,
            "generation":stored.assignment.generation,"state":stored.result.as_ref().map(|r|format!("{:?}",r.state)),
            "durability":"Published","resources_held":stored.resources_held(),"outcome":"completed","completed":true}),
        )
    }
    async fn migration_target(&self, stored: &StoredEnvironment, r: &Action) -> Result<String> {
        // Device and node-local external mounts need an explicit transferable contract.
        let local = |source: &adx_core::execution::StorageSource| {
            matches!(source, adx_core::execution::StorageSource::Local(_))
        };
        if crate::storage::has_volumes(&stored.spec)
            || !stored.spec.scheduling.devices.is_empty()
            || stored
                .spec
                .execution
                .rootfs
                .as_ref()
                .is_some_and(|root| local(&root.source))
            || stored
                .spec
                .execution
                .mounts
                .iter()
                .any(|mount| mount.source.inline().is_ok_and(local))
            || stored
                .spec
                .lifecycle
                .pause
                .as_ref()
                .is_some_and(|p| p.storage == adx_core::lifecycle::CheckpointStorage::Local)
        {
            return Err(Error::Invalid(
                "migration requires transferable runtime resources and shared checkpoints".into(),
            ));
        }
        let mut state = self.0.state.lock().await;
        let source = state.nodes.get(&r.node_id).ok_or(Error::Conflict)?;
        if !r.checkpoint_storage_alias.is_empty()
            && source.node.labels.get("ADX_CHECKPOINT_STORAGE") != Some(&r.checkpoint_storage_alias)
        {
            return Err(Error::Conflict);
        }
        let point = stored
            .result
            .as_ref()
            .filter(|record| record.state == EnvironmentState::Paused)
            .and_then(|record| {
                record
                    .checkpoint
                    .as_ref()
                    .or(record.snapshot_source.as_ref())
            });
        let spec = recovery::checkpoint_spec(
            stored,
            point,
            Some(source),
            &BTreeSet::new(),
            (!r.target_node_id.is_empty()).then_some(r.target_node_id.as_str()),
        )?;
        state
            .scheduler
            .recovery_candidate(&spec)?
            .ok_or(Error::NoCapacity)
    }
}

// This admission check covers the deployment-owned local EROFS root only.
// Other roots need their own artifact compatibility contract before planned migration.
fn matching_rootfs(
    source: &pb::NodeUpgradeInspection,
    target: &pb::NodeUpgradeInspection,
    spec: Option<&EnvironmentSpec>,
) -> Result<()> {
    let digest = &source.runtime_rootfs_sha256;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || digest != &target.runtime_rootfs_sha256
        || !std::path::Path::new(&source.runtime_rootfs_path).is_absolute()
        || source.runtime_rootfs_path != target.runtime_rootfs_path
    {
        return Err(Error::Invalid(
            "migration requires matching verified default runtime rootfs bytes and paths".into(),
        ));
    }
    if spec.is_some_and(|spec| {
        !spec.image.is_empty()
            || spec.execution.rootfs.is_some()
            || spec.runtime_profile.as_ref().is_some_and(|profile| {
                profile.rootfs.r#type != "local"
                    || profile.rootfs.path != source.runtime_rootfs_path
                    || profile.bootstrap.r#type != "erofs"
                    || profile.bootstrap.root != source.runtime_rootfs_path
            })
    }) {
        return Err(Error::Invalid(
            "planned migration rootfs preflight supports only the deployment default local rootfs"
                .into(),
        ));
    }
    Ok(())
}

/// A fresh process must persist closed admission before its first catalog apply.
pub(super) fn registration_control(
    task: Option<&UpgradeTask>,
    request: &pb::RegisterNodeRequest,
) -> Result<Option<pb::NodeUpgradeControl>> {
    let Some(task) = task.filter(|task| task.active()) else {
        return Ok(None);
    };
    let previous = task.nodes.get(&request.node_id);
    if previous.is_some_and(|node| node.node_session_id == request.session_id) {
        return Ok(None);
    }
    Ok(Some(pb::NodeUpgradeControl {
        task_id: task.task_id.clone(),
        epoch: task.epoch,
        node_session_id: request.session_id.clone(),
        revision: previous
            .map_or(0, |node| node.control_revision)
            .checked_add(1)
            .ok_or(Error::Conflict)?,
        phase: "isolated".into(),
        grants: vec![],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_binding_requires_a_fresh_isolated_empty_session_and_old_delete_intent() {
        let task = UpgradeTask {
            protocol_version: 3,
            task_id: "task".into(),
            epoch: 1,
            phase: "Active".into(),
            nodes: [(
                "n".into(),
                UpgradeNode {
                    node_id: "n".into(),
                    pod_uid: "old".into(),
                    node_session_id: "old-session".into(),
                    phase: "quiescing".into(),
                    permit_id: Some("permit".into()),
                    delete_resource_version: Some("rv".into()),
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };
        let current: StoredNode = serde_json::from_value(json!({
            "node":{"id":"n","capacity":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0},"available":false,
                "labels":{},"devices":[],"runtime_classes":["runsc"]},"shard_id":0,"address":"n:1","proxy_address":"n:2",
            "session":{"id":"new-session","sequence":1,"routable":true},"scheduling_paused":true,
            "upgrade_pause":{"task_id":"task","epoch":1,"original_paused":false}
        })).unwrap();
        let request = Action {
            node_id: "n".into(),
            pod_uid: "new".into(),
            node_session_id: "new-session".into(),
            ..Default::default()
        };
        let observation = pb::NodeUpgradeInspection {
            protocol_version: 3,
            task_id: "task".into(),
            epoch: 1,
            node_session_id: "new-session".into(),
            phase: "isolated".into(),
            strict_empty: true,
            ..Default::default()
        };
        assert!(replacement_binding(&task, &request, &current, &observation).unwrap());
        for fault in [
            "delete", "permit", "busy", "unknown", "gate", "owner", "session", "uid", "phase",
        ] {
            let (mut task, mut request, mut current, mut observation) = (
                task.clone(),
                request.clone(),
                current.clone(),
                observation.clone(),
            );
            match fault {
                "delete" => task.nodes.get_mut("n").unwrap().delete_resource_version = None,
                "permit" => task.nodes.get_mut("n").unwrap().permit_id = None,
                "busy" => observation.strict_empty = false,
                "unknown" => observation.unknown = true,
                "gate" => current.scheduling_paused = false,
                "owner" => current.upgrade_pause = None,
                "session" => request.node_session_id = "stale".into(),
                "uid" => request.pod_uid = "old".into(),
                _ => observation.phase = "verified".into(),
            }
            assert!(
                replacement_binding(&task, &request, &current, &observation).is_err(),
                "{fault}"
            );
        }
        let mut added = task.clone();
        added.nodes.clear();
        assert!(replacement_binding(&added, &request, &current, &observation).unwrap());
    }
}
