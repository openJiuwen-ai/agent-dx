use crate::clients::{authorize, Clients};
use adx_observability::trace;
use adx_protocol::control as pb;
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
}
impl Kind {
    fn code(self) -> i32 {
        (match self {
            Self::Pause => pb::LifecycleKind::Pause,
            Self::Resume => pb::LifecycleKind::Resume,
            Self::Snapshot => pb::LifecycleKind::Snapshot,
            Self::Delete => pb::LifecycleKind::Unspecified,
        }) as i32
    }
    fn state(self) -> i32 {
        (match self {
            Self::Delete => pb::InstanceState::Deleted,
            Self::Pause => pb::InstanceState::Paused,
            _ => pb::InstanceState::Running,
        }) as i32
    }
}
struct Operation {
    assignment: pb::Assignment,
    expected: u64,
    kind: Kind,
    body: Value,
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
    pub async fn execute(
        &self,
        kind: Kind,
        id: &str,
        request_id: &str,
        body: Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let ttl = number(&body, "ttlSeconds", 90000, u64::MAX)?;
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
        let mut owner = clients.owner(id, caller, false).await?;
        let record = owner
            .record
            .as_ref()
            .ok_or_else(|| Status::unavailable("instance directory returned no record"))?;
        if kind == Kind::Delete
            && record.state == pb::InstanceState::Deleted as i32
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
                if pending.len() >= clients.config.cache_entries {
                    return Err(Status::resource_exhausted(
                        "pending operation budget exhausted",
                    ));
                }
                let expected = record
                    .last_operation
                    .as_ref()
                    .filter(|operation| operation.id == request_id && operation.kind == kind.code())
                    .map_or(record.revision, |operation| operation.expected_revision);
                let operation = Arc::new(Mutex::new(Operation {
                    assignment: record.assignment.clone().ok_or_else(|| {
                        Status::unavailable("instance record returned no assignment")
                    })?,
                    expected,
                    kind,
                    body: body.clone(),
                }));
                pending.insert(key.clone(), operation.clone());
                operation
            }
        };
        let operation = operation.lock().await;
        if operation.kind != kind
            || operation.body != body
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
                            "api_server.delete",
                            node.delete_instance(trace::inject(pb::DeleteInstanceRequest {
                                assignment,
                                caller: caller_context,
                            })),
                        )
                        .await
                }
                Kind::Pause => {
                    clients
                        .rpc_with_timeout(
                            "api_server.pause",
                            clients.config.timeout() + Duration::from_secs(seconds),
                            node.pause_instance(trace::inject(pb::PauseInstanceRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                                ttl_seconds: ttl,
                                timeout_seconds: seconds,
                            })),
                        )
                        .await
                }
                Kind::Resume => {
                    clients
                        .rpc(
                            "api_server.resume",
                            node.resume_instance(trace::inject(pb::ResumeInstanceRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                            })),
                        )
                        .await
                }
                Kind::Snapshot => match clients
                    .rpc_with_timeout(
                        "api_server.snapshot",
                        clients.config.timeout() + Duration::from_secs(seconds),
                        node.create_snapshot(trace::inject(pb::CreateSnapshotRequest {
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
                        })),
                    )
                    .await
                {
                    Ok(result) => {
                        snapshot = result.snapshot;
                        result
                            .instance
                            .ok_or_else(|| Status::data_loss("missing snapshot result"))
                    }
                    Err(error) => Err(error),
                },
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
                                        && checkpoint.expires_at_unix_seconds > 0
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
                                "sandboxId": id,
                                "snapshotId": checkpoint.id,
                                "size": size,
                                "state": "paused",
                                "expiresAt": checkpoint.expires_at_unix_seconds,
                            })
                        }
                        Kind::Resume => {
                            if owner.node_proxy_address.is_empty() {
                                return Err(Status::data_loss("missing node route"));
                            }
                            json!({
                                "sandboxId": id,
                                "state": "running",
                                "routeAddress": owner.node_proxy_address,
                                "functionProxyId": operation.assignment.node_id,
                                "nodeId": operation.assignment.node_id,
                                "portMappings": {},
                            })
                        }
                        Kind::Snapshot => {
                            let snapshot = snapshot
                                .as_ref()
                                .filter(|snapshot| {
                                    snapshot.state == pb::SnapshotState::Ready as i32
                                        && snapshot.template == record.spec
                                })
                                .ok_or_else(|| Status::data_loss("invalid snapshot source"))?;
                            snapshot_value(snapshot, caller)?
                        }
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
    Ok(json!({"snapshotId":s.id,"names":s.names}))
}
