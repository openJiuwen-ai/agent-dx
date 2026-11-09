use crate::clients::{authorize, Clients};
use adx_observability::trace;
use adx_protocol::control as pb;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::{Code, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Delete,
    Pause,
    Resume,
    Snapshot,
    Network,
    Reload,
}
impl Kind {
    fn code(self) -> i32 {
        (match self {
            Self::Pause => pb::LifecycleKind::Pause,
            Self::Resume => pb::LifecycleKind::Resume,
            Self::Snapshot => pb::LifecycleKind::Snapshot,
            Self::Network => pb::LifecycleKind::Network,
            Self::Reload => pb::LifecycleKind::Reload,
            Self::Delete => pb::LifecycleKind::Unspecified,
        }) as i32
    }
    fn state(self) -> i32 {
        (match self {
            Self::Delete => pb::EnvironmentState::Deleted,
            Self::Pause => pb::EnvironmentState::Paused,
            _ => pb::EnvironmentState::Running,
        }) as i32
    }
}
struct Operation {
    assignment: pb::Assignment,
    expected: u64,
    kind: Kind,
    body: Value,
    network: Option<pb::NetworkPolicy>,
}
struct OperationInput {
    body: Value,
    network: Option<pb::NetworkPolicy>,
}
type OperationKey = (String, String, String);
type PendingOperations = HashMap<OperationKey, Arc<Mutex<Operation>>>;
pub struct Operations {
    clients: Arc<Clients>,
    pending: Mutex<PendingOperations>,
}
impl Operations {
    pub fn new(clients: Arc<Clients>) -> Self {
        Self {
            clients,
            pending: Mutex::new(HashMap::new()),
        }
    }
    /// Supply a one-run duration through the same native owner recovery path.
    pub async fn resume_with_run_timeout(
        &self,
        id: &str,
        caller: &pb::CallerContext,
        seconds: u64,
    ) -> Result<Value, Status> {
        self.resume(id, caller, Some(seconds)).await
    }

    async fn resume(
        &self,
        id: &str,
        caller: &pb::CallerContext,
        run_timeout_seconds: Option<u64>,
    ) -> Result<Value, Status> {
        let observed = self.clients.owner(id, caller, false).await?;
        let expected_runtime_id = observed
            .record
            .as_ref()
            .and_then(|r| r.runtime.as_ref())
            .map_or_else(String::new, |r| r.id.clone());
        let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.clients.coordinator().await?,
        );
        let owner = self
            .clients
            .rpc_with_timeout(
                "apiserver.ensure_running",
                Duration::from_secs(300),
                coordinator.ensure_running(trace::inject(pb::EnsureRunningRequest {
                    run_timeout_seconds,
                    expected_runtime_id,
                    environment_id: id.into(),
                    caller: Some(caller.clone()),
                    timeout_seconds: 300,
                })),
            )
            .await?;
        authorize(caller, owner.record.as_ref())?;
        if owner
            .record
            .as_ref()
            .is_none_or(|r| r.state != pb::EnvironmentState::Running as i32)
        {
            return Err(Status::unavailable("resume has not committed Running"));
        }
        if owner.relay_address.is_empty()
            || owner
                .record
                .as_ref()
                .and_then(|r| r.spec.as_ref())
                .is_none_or(|s| s.id != id)
        {
            return Err(Status::data_loss(
                "invalid resume route or Environment identity",
            ));
        }
        let node = owner
            .record
            .as_ref()
            .and_then(|r| r.assignment.as_ref())
            .ok_or_else(|| Status::data_loss("resume assignment missing"))?
            .node_id
            .clone();
        let value = json!({"environmentId": id, "state": "running", "routeAddress": owner.relay_address,
            "functionProxyId": node, "nodeId": node, "portMappings": {}});
        self.clients.put_owner(owner).await?;
        Ok(value)
    }

    pub async fn execute(
        &self,
        kind: Kind,
        id: &str,
        request_id: &str,
        body: Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        if kind == Kind::Resume {
            return self.resume(id, caller, None).await;
        }
        let network = if kind == Kind::Network {
            crate::contract::network_policy(body.clone())?.map(Into::into)
        } else {
            None
        };
        self.execute_inner(
            kind,
            id,
            request_id,
            OperationInput { body, network },
            caller,
            None,
        )
        .await
    }

    /// Update a typed policy only if a fresh lookup still has `required_revision`.
    pub async fn update_network_at_revision(
        &self,
        id: &str,
        request_id: &str,
        network: Option<adx_core::execution::NetworkPolicy>,
        caller: &pb::CallerContext,
        required_revision: u64,
    ) -> Result<Value, Status> {
        if let Some(network) = &network {
            network.validate().map_err(adx_protocol::status)?;
        }
        self.execute_inner(
            Kind::Network,
            id,
            request_id,
            OperationInput {
                body: Value::Null,
                network: network.map(Into::into),
            },
            caller,
            Some(required_revision),
        )
        .await
    }

    async fn execute_inner(
        &self,
        kind: Kind,
        id: &str,
        request_id: &str,
        input: OperationInput,
        caller: &pb::CallerContext,
        required_revision: Option<u64>,
    ) -> Result<Value, Status> {
        let OperationInput { body, network } = input;
        let seconds = number(&body, "timeoutSeconds", 300, 3600)?;
        let name = body
            .get("name")
            .map(|n| {
                n.as_str()
                    .ok_or_else(|| Status::invalid_argument("invalid snapshot name"))
            })
            .transpose()?
            .unwrap_or("")
            .trim();
        if body
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty() && name.is_empty())
        {
            return Err(Status::invalid_argument("snapshot name cannot be blank"));
        }
        let clients = &self.clients;
        // Workload checkpoint acknowledges publication before the directory watch
        // necessarily catches up. Reload must start from the committed revision.
        let mut owner = clients
            .owner(
                id,
                caller,
                kind == Kind::Reload || required_revision.is_some(),
            )
            .await?;
        if kind == Kind::Delete
            && owner
                .record
                .as_ref()
                .is_some_and(|r| r.state == pb::EnvironmentState::Paused as i32)
        {
            let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(
                clients.coordinator().await?,
            );
            match clients
                .rpc(
                    "apiserver.retire_dormant",
                    coordinator.retire_dormant(trace::inject(pb::GetEnvironmentRequest {
                        environment_id: id.into(),
                        caller: Some(caller.clone()),
                    })),
                )
                .await
            {
                Ok(deleted) => {
                    owner.record = deleted.record;
                    clients.put_owner(owner).await?;
                    return Ok(Value::Null);
                }
                Err(error) if error.code() == Code::NotFound => return Ok(Value::Null),
                Err(error) if error.code() == Code::FailedPrecondition => {
                    owner = clients.owner(id, caller, true).await?;
                }
                Err(error) => return Err(error),
            }
        }
        let record = owner
            .record
            .as_ref()
            .ok_or_else(|| Status::unavailable("environment directory returned no record"))?;
        let default_retention = record
            .spec
            .as_ref()
            .and_then(|s| s.lifecycle.as_ref())
            .and_then(|l| l.pause.as_ref())
            .map(|p| {
                p.retention
                    .ok_or_else(|| Status::data_loss("pause retention missing"))
            })
            .transpose()?
            .map(TryInto::try_into)
            .transpose()
            .map_err(adx_protocol::status)?
            .unwrap_or_default();
        let retention = if kind == Kind::Pause {
            crate::contract::pause_retention(&body, default_retention)?
        } else {
            default_retention
        };
        if required_revision.is_some_and(|required| required != record.revision) {
            return Err(Status::aborted("sandbox revision changed"));
        }
        if kind == Kind::Delete
            && record.state == pb::EnvironmentState::Deleted as i32
            && !record.resources_held
        {
            return Ok(Value::Null);
        }
        let key = (caller.tenant_id.clone(), id.into(), request_id.into());
        let operation = {
            let mut pending = self.pending.lock().await;
            if let Some(operation) = pending.get(&key) {
                operation.clone()
            } else {
                // Retained retry identities are not admission slots. Completed
                // cache capacity must not reject an independent lifecycle request.
                let expected = record
                    .last_operation
                    .as_ref()
                    .filter(|operation| operation.id == request_id && operation.kind == kind.code())
                    .map_or(record.revision, |operation| operation.expected_revision);
                let operation = Arc::new(Mutex::new(Operation {
                    assignment: record.assignment.clone().ok_or_else(|| {
                        Status::unavailable("environment record returned no assignment")
                    })?,
                    expected,
                    kind,
                    body: body.clone(),
                    network: network.clone(),
                }));
                pending.insert(key.clone(), operation.clone());
                operation
            }
        };
        let operation = operation.lock().await;
        if operation.kind != kind
            || operation.body != body
            || operation.network != network
            || Some(&operation.assignment)
                != owner
                    .record
                    .as_ref()
                    .and_then(|record| record.assignment.as_ref())
        {
            return Err(Status::failed_precondition(
                "operation target or arguments changed",
            ));
        }
        for attempt in 0..2 {
            let mut node = pb::node_service_client::NodeServiceClient::new(
                clients.channel(&owner.node_address).await?,
            );
            let assignment = Some(operation.assignment.clone());
            let caller_context = Some(caller.clone());
            let mut snapshot = None;
            let result = match kind {
                Kind::Delete => {
                    clients
                        .rpc(
                            "apiserver.delete",
                            node.delete_environment(trace::inject(pb::DeleteEnvironmentRequest {
                                expected_revision: None,
                                failed_revision: None,
                                node_session_id: String::new(),
                                assignment,
                                caller: caller_context,
                            })),
                        )
                        .await
                }
                Kind::Pause => {
                    clients
                        .rpc_with_timeout(
                            "apiserver.pause",
                            clients.config.timeout() + Duration::from_secs(seconds),
                            node.pause_environment(trace::inject(pb::PauseEnvironmentRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                                retention: Some(retention.into()),
                                timeout_seconds: seconds,
                                node_session_id: String::new(),
                            })),
                        )
                        .await
                }
                Kind::Resume => Err(Status::internal("resume requires the Coordinator entry")),
                Kind::Snapshot => match clients
                    .rpc_with_timeout(
                        "apiserver.snapshot",
                        clients.config.timeout() + Duration::from_secs(seconds),
                        node.create_snapshot(trace::inject(pb::CreateSnapshotRequest {
                            node_session_id: String::new(),
                            assignment,
                            caller: caller_context,
                            operation_id: request_id.into(),
                            expected_revision: operation.expected,
                            names: if name.is_empty() {
                                vec![]
                            } else {
                                vec![name.into()]
                            },
                            timeout_seconds: seconds,
                            kind: pb::SnapshotKind::Regular as i32,
                        })),
                    )
                    .await
                {
                    Ok(result) => {
                        snapshot = result.snapshot;
                        result
                            .environment
                            .ok_or_else(|| Status::data_loss("missing snapshot result"))
                    }
                    Err(error) => Err(error),
                },
                Kind::Network => {
                    clients
                        .rpc(
                            "apiserver.update_network_policy",
                            node.update_network_policy(trace::inject(
                                pb::UpdateNetworkPolicyRequest {
                                    assignment,
                                    caller: caller_context,
                                    operation_id: request_id.into(),
                                    expected_revision: operation.expected,
                                    network: operation.network.clone(),
                                },
                            )),
                        )
                        .await
                }
                Kind::Reload => {
                    clients
                        .rpc(
                            "apiserver.reload",
                            node.reload_environment(trace::inject(pb::ReloadEnvironmentRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                            })),
                        )
                        .await
                }
            };
            match result {
                Ok(result) => {
                    let record = result
                        .record
                        .as_ref()
                        .ok_or_else(|| Status::unavailable("operation result missing"))?;
                    authorize(caller, Some(record))?;
                    if result.durability != pb::Durability::Published as i32
                        || record.assignment.as_ref() != Some(&operation.assignment)
                        || record.state != kind.state()
                    {
                        return Err(Status::unavailable("operation is not durably confirmed"));
                    }
                    if kind == Kind::Delete {
                        if record.resources_held {
                            return Err(Status::unavailable(
                                "deleted instance still holds resources",
                            ));
                        }
                    } else if record.last_operation.as_ref().is_none_or(|last| {
                        last.id != request_id
                            || last.kind != kind.code()
                            || last.expected_revision != operation.expected
                    }) {
                        return Err(Status::unavailable("operation result version mismatch"));
                    }
                    let value = match kind {
                        Kind::Delete => Value::Null,
                        Kind::Pause => {
                            let checkpoint = record
                                .checkpoint
                                .as_ref()
                                .filter(|checkpoint| {
                                    checkpoint.id == request_id
                                        && checkpoint.expires_at_unix_seconds != Some(0)
                                })
                                .ok_or_else(|| Status::data_loss("invalid recovery point"))?;
                            let size = checkpoint
                                .artifact
                                .as_ref()
                                .filter(|artifact| {
                                    artifact.size_bytes > 0
                                        && artifact.size_bytes <= i64::MAX as u64
                                })
                                .ok_or_else(|| Status::data_loss("invalid checkpoint artifact"))?
                                .size_bytes;
                            json!({
                                "environmentId": id,
                                "snapshotId": checkpoint.id,
                                "size": size,
                                "state": "paused",
                                "expiresAt": checkpoint.expires_at_unix_seconds,
                            })
                        }
                        Kind::Resume => {
                            return Err(Status::internal("resume requires the Coordinator entry"))
                        }
                        Kind::Snapshot => {
                            let snapshot = snapshot
                                .as_ref()
                                .ok_or_else(|| Status::data_loss("invalid snapshot source"))?;
                            let saved: adx_core::snapshots::Snapshot =
                                snapshot.clone().try_into().map_err(adx_protocol::status)?;
                            let source: adx_core::EnvironmentSpec = record
                                .spec
                                .clone()
                                .ok_or_else(|| Status::data_loss("snapshot source missing"))?
                                .try_into()
                                .map_err(adx_protocol::status)?;
                            if saved.state != adx_core::snapshots::SnapshotState::Ready
                                || !saved.matches_template(&source)
                            {
                                return Err(Status::data_loss("invalid snapshot source"));
                            }
                            snapshot_value(snapshot, caller)?
                        }
                        Kind::Network => json!({"success": true}),
                        Kind::Reload => json!({"success": true}),
                    };
                    owner.record = result.record;
                    clients.put_owner(owner).await?;
                    self.pending.lock().await.remove(&key);
                    return Ok(value);
                }
                Err(error) => {
                    let retryable =
                        matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded)
                            || (kind == Kind::Delete
                                && matches!(
                                    error.code(),
                                    Code::NotFound | Code::FailedPrecondition
                                ));
                    if !retryable {
                        self.pending.lock().await.remove(&key);
                        return Err(error);
                    }
                    if attempt == 1 {
                        return Err(error);
                    }
                    owner = clients.owner(id, caller, true).await?;
                    if owner
                        .record
                        .as_ref()
                        .and_then(|record| record.assignment.as_ref())
                        != Some(&operation.assignment)
                    {
                        self.pending.lock().await.remove(&key);
                        return Err(Status::failed_precondition(
                            "ownership changed; original operation cannot be replayed",
                        ));
                    }
                }
            }
        }
        unreachable!()
    }
}
pub fn number(value: &Value, key: &str, default: u64, max: u64) -> Result<u64, Status> {
    let value = match value.get(key) {
        None | Some(Value::Null) => default,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Status::invalid_argument(format!("invalid {key}")))?,
    };
    let value = if value == 0 { default } else { value };
    if value > max {
        return Err(Status::invalid_argument(format!("invalid {key}")));
    }
    Ok(value)
}

pub fn snapshot_value(
    s: &pb::ReusableSnapshot,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let spec = s
        .template
        .as_ref()
        .ok_or_else(|| Status::data_loss("invalid snapshot metadata"))?;
    if s.id.is_empty() || spec.tenant_id.is_empty() {
        return Err(Status::data_loss("invalid snapshot identity"));
    }
    if !caller.administrator && spec.tenant_id != caller.tenant_id {
        return Err(Status::permission_denied(
            "snapshot belongs to another tenant",
        ));
    }
    let kind = match pb::SnapshotKind::try_from(s.kind) {
        Ok(pb::SnapshotKind::Regular) => "regular",
        Ok(pb::SnapshotKind::Golden) => "golden",
        Err(_) => return Err(Status::data_loss("invalid snapshot kind")),
    };
    let state = match pb::SnapshotState::try_from(s.state) {
        Ok(pb::SnapshotState::Ready) => "ready",
        Ok(pb::SnapshotState::Deleting) => "deleting",
        Ok(pb::SnapshotState::Deleted) => "deleted",
        _ => return Err(Status::data_loss("invalid snapshot state")),
    };
    let created_at = i64::try_from(s.created_at_unix_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| Status::data_loss("invalid snapshot creation time"))?;
    Ok(json!({
        "snapshotId": s.id, "names": s.names, "kind": kind, "state": state,
        "createdAt": created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "image": spec.image, "runtime": spec.runtime_class
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_summary_exposes_metadata_without_private_capture_inputs() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            ..Default::default()
        };
        let snapshot = pb::ReusableSnapshot {
            id: "snapshot".into(),
            names: vec!["base".into()],
            kind: pb::SnapshotKind::Golden as i32,
            state: pb::SnapshotState::Ready as i32,
            created_at_unix_ms: 1_730_000_000_123,
            template: Some(pb::EnvironmentSpec {
                tenant_id: "tenant".into(),
                image: "image".into(),
                runtime_class: "firecracker".into(),
                env: std::collections::HashMap::from([("TOKEN".into(), "private-env".into())]),
                ..Default::default()
            }),
            artifact: Some(pb::CheckpointArtifact {
                location: "private-artifact".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            snapshot_value(&snapshot, &caller).unwrap(),
            json!({
                "snapshotId":"snapshot", "names":["base"], "kind":"golden", "state":"ready",
                "createdAt":"2024-10-27T03:33:20.123Z", "image":"image", "runtime":"firecracker"
            })
        );
        let mut invalid = snapshot.clone();
        invalid.kind = 42;
        assert_eq!(
            snapshot_value(&invalid, &caller).unwrap_err().code(),
            Code::DataLoss
        );
        invalid = snapshot.clone();
        invalid.state = 0;
        assert_eq!(
            snapshot_value(&invalid, &caller).unwrap_err().code(),
            Code::DataLoss
        );
        invalid = snapshot.clone();
        invalid.created_at_unix_ms = u64::MAX;
        assert_eq!(
            snapshot_value(&invalid, &caller).unwrap_err().code(),
            Code::DataLoss
        );
        let foreign = pb::CallerContext {
            tenant_id: "other".into(),
            ..Default::default()
        };
        assert_eq!(
            snapshot_value(&snapshot, &foreign).unwrap_err().code(),
            Code::PermissionDenied
        );
    }
}
