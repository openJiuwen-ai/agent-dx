//! Sandbox application service shared by HTTP and in-process callers.
use crate::{
    clients::{authorize, Clients},
    contract,
    operations::{Kind, Operations},
    ownership::Cache,
};
use adx_protocol::control as pb;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::{Code, Status};

type CreateKey = (String, String);

struct CreateOperation {
    digest: Vec<u8>,
    spec: pb::EnvironmentSpec,
    result: Option<Result<Value, Status>>,
    created_at: Instant,
    unknown_since: Option<Instant>,
}

type SharedCreate = Arc<Mutex<CreateOperation>>;

struct CreateReplays {
    pending: HashMap<CreateKey, SharedCreate>,
    completed: Cache<CreateKey, SharedCreate>,
}

impl CreateReplays {
    fn expire_pending(&mut self, now: Instant, retention: Duration) -> Vec<(String, CreateKey)> {
        let mut expired = Vec::new();
        self.pending.retain(|key, shared| {
            // A caller may already have obtained this identity and be waiting
            // for its operation lock. Keep that identity until callers finish.
            if Arc::strong_count(shared) > 1 {
                return true;
            }
            let Ok(operation) = shared.try_lock() else {
                return true;
            };
            let since = operation.unknown_since.unwrap_or(operation.created_at);
            if now.saturating_duration_since(since) < retention {
                return true;
            }
            // created_at also bounds contexts abandoned by a cancelled caller,
            // whose future never reached finish_create. No backend state changes.
            expired.push((operation.spec.id.clone(), key.clone()));
            false
        });
        expired
    }
}

/// Owns Sandbox lifecycle semantics independently of any transport adapter.
pub struct SandboxService {
    clients: Arc<Clients>,
    operations: Operations,
    creates: Mutex<CreateReplays>,
    names: Mutex<HashMap<String, CreateKey>>,
}

impl SandboxService {
    pub fn new(clients: Arc<Clients>) -> Arc<Self> {
        let retention = Duration::from_secs(clients.config.create_unknown_retention_seconds);
        let period = retention.min(Duration::from_secs(60));
        let service = Arc::new(Self {
            operations: Operations::new(clients.clone()),
            creates: Mutex::new(CreateReplays {
                pending: HashMap::new(),
                completed: Cache::new(clients.config.cache_entries),
            }),
            clients,
            names: Mutex::default(),
        });
        let weak = Arc::downgrade(&service);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(service) = weak.upgrade() else {
                    break;
                };
                let mut creates = service.creates.lock().await;
                let expired = creates.expire_pending(Instant::now(), retention);
                let mut names = service.names.lock().await;
                for (id, key) in expired {
                    if names.get(&id) == Some(&key) {
                        names.remove(&id);
                    }
                }
            }
        });
        service
    }

    pub async fn execute(
        &self,
        kind: Kind,
        id: &str,
        request_id: &str,
        body: Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        self.operations
            .execute(kind, id, request_id, body, caller)
            .await
    }

    /// Inspect the caller-visible Environment through the same versioned
    /// ownership directory used by REST lifecycle requests.
    pub async fn inspect(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<pb::GetEnvironmentResponse>, Status> {
        let caller = pb::CallerContext {
            tenant_id: tenant.to_owned(),
            administrator: false,
        };
        match self.clients.owner(id, &caller, false).await {
            Ok(owner) => Ok(Some(owner)),
            Err(error) if error.code() == Code::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Build a Platform specification from the public Sandbox request contract.
    pub fn prepare_create(
        &self,
        input: Value,
        caller: &pb::CallerContext,
    ) -> Result<pb::EnvironmentSpec, Status> {
        contract::create_spec_with_environment(
            input,
            caller,
            self.clients.config.runtime_profile.as_ref(),
        )
    }

    /// Shared admission for HTTP and in-process callers; accepts no Agent-specific types.
    pub async fn create_request(
        &self,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let spec = self.prepare_create(input.clone(), caller)?;
        Box::pin(self.create(spec, input, request_id, caller)).await
    }

    pub async fn create(
        &self,
        spec: pb::EnvironmentSpec,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        if request_id.len() > 256 {
            return Err(Status::invalid_argument("request ID too long"));
        }
        let create_key = (caller.tenant_id.clone(), request_id.to_string());
        let request_digest = Sha256::digest(
            serde_json::to_vec(&input).expect("serde_json::Value serialization is infallible"),
        )
        .to_vec();
        let (operation, retry_pending) = {
            let mut creates = self.creates.lock().await;
            if let Some(operation) = creates.pending.get(&create_key) {
                (operation.clone(), true)
            } else if let Some(operation) = creates.completed.get(&create_key) {
                (operation, false)
            } else {
                let operation = Arc::new(Mutex::new(CreateOperation {
                    digest: request_digest.clone(),
                    spec,
                    result: None,
                    created_at: Instant::now(),
                    unknown_since: None,
                }));
                creates
                    .pending
                    .insert(create_key.clone(), operation.clone());
                (operation, false)
            }
        };
        let mut operation = operation.lock_owned().await;
        if operation.digest != request_digest {
            return Err(Status::already_exists(
                "request ID reused with different arguments",
            ));
        }
        if let Some(result) = &operation.result {
            return result.clone();
        }
        let competing_create = {
            let mut names = self.names.lock().await;
            if let Some(existing) = names.get(&operation.spec.id) {
                if existing != &create_key {
                    Some(existing.clone())
                } else {
                    None
                }
            } else {
                names.insert(operation.spec.id.clone(), create_key.clone());
                None
            }
        };
        if let Some(competing_key) = competing_create {
            let other = {
                let mut creates = self.creates.lock().await;
                creates
                    .pending
                    .get(&competing_key)
                    .cloned()
                    .or_else(|| creates.completed.get(&competing_key))
            };
            if let Some(other) = other {
                let other = other.lock().await;
                if !matches_spec(&operation.spec, &other.spec) {
                    let result = Err(Status::already_exists(
                        "environment create in progress with different arguments",
                    ));
                    self.finish_create(&create_key, &mut operation, &result)
                        .await;
                    return result;
                }
            }
            let result = self
                .reuse_existing(&operation.spec, &input, request_id, caller)
                .await;
            self.finish_create(&create_key, &mut operation, &result)
                .await;
            return result;
        }
        let result = self
            .perform_create(&operation.spec, &input, request_id, caller, retry_pending)
            .await;
        self.names.lock().await.remove(&operation.spec.id);
        self.finish_create(&create_key, &mut operation, &result)
            .await;
        result
    }

    async fn finish_create(
        &self,
        key: &CreateKey,
        operation: &mut CreateOperation,
        result: &Result<Value, Status>,
    ) {
        // Unknown outcomes retain their identity for a finite retry window.
        // Repeated inconclusive retries do not renew that window.
        if result.as_ref().is_err_and(|error| {
            matches!(
                error.code(),
                Code::Cancelled
                    | Code::Unknown
                    | Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::Internal
            )
        }) {
            operation.unknown_since.get_or_insert_with(Instant::now);
            return;
        }
        operation.result = Some(result.clone());
        let mut creates = self.creates.lock().await;
        if let Some(shared) = creates.pending.remove(key) {
            creates
                .completed
                .insert(key.clone(), shared, Duration::from_secs(600));
        }
    }

    async fn perform_create(
        &self,
        spec: &pb::EnvironmentSpec,
        input: &Value,
        request_id: &str,
        caller: &pb::CallerContext,
        retry_pending: bool,
    ) -> Result<Value, Status> {
        // A pending retry bypasses a potentially stale subscription. Absence
        // still retries the same identity through atomic ownership, never a new ID.
        match self.clients.owner(&spec.id, caller, retry_pending).await {
            Ok(owner) => {
                let record = owner
                    .record
                    .ok_or_else(|| Status::data_loss("environment directory returned no record"))?;
                return existing_running_response(spec, input, request_id, &record);
            }
            Err(error) if error.code() == Code::NotFound => {}
            Err(error) => return Err(map_create_owner_error(error)),
        }

        let timeouts = contract::create_timeouts(input)?;
        let budget = Duration::from_secs(timeouts.create_seconds);
        // The RPC future is large in debug builds; keep it off the nested
        // embedded HTTP/Activator polling stack while preserving cancellation.
        let result = Box::pin(self.clients.create_environment(
            pb::CreateEnvironmentRequest {
                spec: Some(spec.clone()),
                caller: Some(caller.clone()),
                schedule_timeout_seconds: timeouts.schedule_seconds,
                create_timeout_seconds: timeouts.create_seconds,
            },
            budget,
        ))
        .await?;
        authorize(caller, result.record.as_ref())?;
        let record = result
            .record
            .ok_or_else(|| Status::unavailable("create returned no environment record"))?;
        if result.durability != pb::Durability::Published as i32 {
            return Err(Status::unavailable("create is not durably confirmed"));
        }
        let response = existing_running_response(spec, input, request_id, &record)?;

        // Node-local success seeds the versioned in-memory directory from the
        // trusted node result. Central fallback and startup races use one
        // authoritative read; ordinary local-first creates do not.
        match self.clients.owner(&spec.id, caller, false).await {
            Ok(owner) if owner_covers_create(&owner, &record)? => {}
            Ok(_) => {
                let owner = self.clients.owner(&spec.id, caller, true).await?;
                if !owner_covers_create(&owner, &record)? {
                    return Err(Status::unavailable(
                        "environment directory has not observed the published create result",
                    ));
                }
            }
            Err(error) if matches!(error.code(), Code::NotFound | Code::Unavailable) => {
                let owner = self.clients.owner(&spec.id, caller, true).await?;
                if !owner_covers_create(&owner, &record)? {
                    return Err(Status::unavailable(
                        "environment directory has not observed the published create result",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        Ok(response)
    }

    async fn reuse_existing(
        &self,
        spec: &pb::EnvironmentSpec,
        input: &Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let owner = match self.clients.owner(&spec.id, caller, false).await {
            Ok(owner) => owner,
            Err(error) if error.code() == Code::NotFound => {
                self.clients.owner(&spec.id, caller, true).await?
            }
            Err(error) => return Err(map_create_owner_error(error)),
        };
        let record = owner
            .record
            .ok_or_else(|| Status::data_loss("environment directory returned no record"))?;
        existing_running_response(spec, input, request_id, &record)
    }
}

fn existing_running_response(
    spec: &pb::EnvironmentSpec,
    input: &Value,
    request_id: &str,
    record: &pb::EnvironmentRecord,
) -> Result<Value, Status> {
    let confirmed_spec = record
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment record returned no spec"))?;
    if !matches_spec(spec, confirmed_spec) {
        return Err(Status::already_exists(
            "environment already exists with different arguments",
        ));
    }
    if record.state != pb::EnvironmentState::Running as i32 {
        return Err(Status::unavailable("environment create is not yet running"));
    }
    let mut response = json!({
        "sandboxId": confirmed_spec.id,
        "instanceId": confirmed_spec.id,
        "status": "running",
        "requestId": request_id,
    });
    if input.pointer("/tunnel/enabled").and_then(Value::as_bool) == Some(true) {
        let port = confirmed_spec
            .env
            .get("EXECD_TUNNEL_HTTP_PORT")
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|port| *port > 1)
            .unwrap_or(8766);
        let safe_id = confirmed_spec
            .id
            .replace('@', "-at-")
            .replace(['/', '.', '_'], "-");
        let path = if port == 8766 {
            format!("/tunnel/{safe_id}")
        } else {
            format!("/tunnel/{safe_id}/{}", port - 1)
        };
        response["tunnel"] = json!({
            "url": path,
            "path": path,
            "wsPath": path,
            "proxyUrl": format!("http://127.0.0.1:{port}"),
            "proxyPort": port,
        });
    }
    Ok(response)
}

fn owner_covers_create(
    owner: &pb::GetEnvironmentResponse,
    created: &pb::EnvironmentRecord,
) -> Result<bool, Status> {
    let cached = owner
        .record
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory returned no record"))?;
    let cached_assignment = cached
        .assignment
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment directory returned no assignment"))?;
    let created_assignment = created
        .assignment
        .as_ref()
        .ok_or_else(|| Status::data_loss("create returned no assignment"))?;
    Ok(
        cached_assignment.environment_id == created_assignment.environment_id
            && cached_assignment.node_id == created_assignment.node_id
            && cached_assignment.generation == created_assignment.generation
            && cached.revision >= created.revision
            && cached.state == pb::EnvironmentState::Running as i32,
    )
}

fn map_create_owner_error(error: Status) -> Status {
    if error.code() == Code::PermissionDenied {
        Status::already_exists("environment name is already owned by another tenant")
    } else {
        error
    }
}

fn matches_spec(want: &pb::EnvironmentSpec, got: &pb::EnvironmentSpec) -> bool {
    if want.snapshot_id.is_none() {
        return want == got;
    }
    want.id == got.id
        && want.tenant_id == got.tenant_id
        && want.snapshot_id == got.snapshot_id
        && (!got.image.is_empty() || got.runtime_profile.is_some())
        && !got.runtime_class.is_empty()
        && (want.image.is_empty() || want.image == got.image)
        && (want.runtime_class.is_empty() || want.runtime_class == got.runtime_class)
        && want.resources.as_ref().is_none_or(|want_resources| {
            got.resources.as_ref().is_some_and(|got_resources| {
                (want_resources.cpu_millis == 0
                    || want_resources.cpu_millis == got_resources.cpu_millis)
                    && (want_resources.memory_bytes == 0
                        || want_resources.memory_bytes == got_resources.memory_bytes)
                    && (want_resources.disk_bytes == 0
                        || want_resources.disk_bytes == got_resources.disk_bytes)
            })
        })
        && want
            .env
            .iter()
            .all(|(key, value)| got.env.get(key) == Some(value))
        && want.priority == got.priority
        && want.lifecycle == got.lifecycle
}

/// Embedded Ingress uses the same tenant-filtered directory as the public instance API.
/// No runtime credentials or EnvironmentSpec are returned to business adapters.
#[async_trait::async_trait]
impl data_plane_gateway::ingress::sandbox_files::SandboxDirectory for SandboxService {
    async fn authorize(
        &self,
        tenant: &str,
        sandbox_id: &str,
    ) -> Result<(), data_plane_gateway::ingress::sandbox_files::ReadError> {
        use data_plane_gateway::ingress::sandbox_files::ReadError;
        if tenant.trim().is_empty() || sandbox_id.trim().is_empty() {
            return Err(ReadError::Identity);
        }
        let response = self
            .inspect(tenant, sandbox_id)
            .await
            .map_err(|error| match error.code() {
                Code::NotFound | Code::PermissionDenied => ReadError::NotFound,
                _ => ReadError::Runtime,
            })?
            .ok_or(ReadError::NotFound)?;
        let record = response.record.ok_or(ReadError::Runtime)?;
        let spec = record.spec.ok_or(ReadError::Runtime)?;
        if spec.tenant_id != tenant || spec.id != sandbox_id {
            return Err(ReadError::NotFound);
        }
        if record.state != pb::EnvironmentState::Running as i32 {
            return Err(ReadError::Runtime);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_gc_preserves_waiters_and_collects_abandoned_contexts() {
        let now = Instant::now();
        let old = now - Duration::from_secs(30);
        let key = ("tenant".into(), "request".into());
        let operation = Arc::new(Mutex::new(CreateOperation {
            digest: Vec::new(),
            spec: pb::EnvironmentSpec {
                id: "environment".into(),
                ..Default::default()
            },
            result: None,
            created_at: old,
            unknown_since: None,
        }));
        let mut replays = CreateReplays {
            pending: HashMap::from([(key.clone(), operation.clone())]),
            completed: Cache::new(1),
        };
        // A retry has taken a shared identity but has not acquired its lock yet.
        assert!(replays
            .expire_pending(now, Duration::from_secs(10))
            .is_empty());
        drop(operation);
        assert_eq!(
            replays.expire_pending(now, Duration::from_secs(10)),
            vec![("environment".into(), key)]
        );
        assert!(replays.pending.is_empty());
    }

    #[test]
    fn unknown_retention_starts_at_first_unknown_result() {
        let now = Instant::now();
        let key = ("tenant".into(), "request".into());
        let mut replays = CreateReplays {
            pending: HashMap::from([(
                key.clone(),
                Arc::new(Mutex::new(CreateOperation {
                    digest: Vec::new(),
                    spec: pb::EnvironmentSpec {
                        id: "environment".into(),
                        ..Default::default()
                    },
                    result: None,
                    created_at: now - Duration::from_secs(30),
                    unknown_since: Some(now),
                })),
            )]),
            completed: Cache::new(1),
        };
        assert!(replays
            .expire_pending(now, Duration::from_secs(10))
            .is_empty());
        assert_eq!(
            replays.expire_pending(now + Duration::from_secs(10), Duration::from_secs(10)),
            vec![("environment".into(), key)]
        );
    }

    #[test]
    fn existing_running_name_converges_only_for_identical_spec() {
        let spec = pb::EnvironmentSpec {
            id: "tenant-worker".into(),
            tenant_id: "tenant".into(),
            runtime_class: "runsc".into(),
            ..Default::default()
        };
        let record = pb::EnvironmentRecord {
            spec: Some(spec.clone()),
            state: pb::EnvironmentState::Running as i32,
            ..Default::default()
        };
        let response = existing_running_response(&spec, &json!({}), "create-a", &record).unwrap();
        assert_eq!(response["instanceId"], "tenant-worker");
        assert_eq!(response["requestId"], "create-a");

        let mut different = spec.clone();
        different.runtime_class = "firecracker".into();
        assert_eq!(
            existing_running_response(&different, &json!({}), "create-b", &record)
                .unwrap_err()
                .code(),
            Code::AlreadyExists
        );
        let mut pending = record;
        pending.state = pb::EnvironmentState::Starting as i32;
        assert_eq!(
            existing_running_response(&spec, &json!({}), "create-c", &pending)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
    }

    #[test]
    fn custom_reverse_tunnel_port_is_encoded_in_public_path() {
        let mut spec = pb::EnvironmentSpec {
            id: "tenant-worker".into(),
            tenant_id: "tenant".into(),
            ..Default::default()
        };
        spec.env
            .insert("EXECD_TUNNEL_HTTP_PORT".into(), "18766".into());
        let record = pb::EnvironmentRecord {
            spec: Some(spec.clone()),
            state: pb::EnvironmentState::Running as i32,
            ..Default::default()
        };
        let response = existing_running_response(
            &spec,
            &json!({"tunnel":{"enabled":true,"proxyPort":18766}}),
            "create-a",
            &record,
        )
        .unwrap();
        assert_eq!(response["tunnel"]["path"], "/tunnel/tenant-worker/18765");
    }

    #[test]
    fn create_owner_cache_must_cover_the_published_running_revision() {
        let record = pb::EnvironmentRecord {
            spec: Some(pb::EnvironmentSpec {
                id: "tenant-worker".into(),
                tenant_id: "tenant".into(),
                ..Default::default()
            }),
            assignment: Some(pb::Assignment {
                environment_id: "tenant-worker".into(),
                node_id: "node-a".into(),
                generation: 7,
                ..Default::default()
            }),
            state: pb::EnvironmentState::Running as i32,
            revision: 9,
            ..Default::default()
        };
        let owner = |state, generation, revision, node_id: &str| pb::GetEnvironmentResponse {
            record: Some(pb::EnvironmentRecord {
                spec: record.spec.clone(),
                assignment: Some(pb::Assignment {
                    environment_id: "tenant-worker".into(),
                    node_id: node_id.into(),
                    generation,
                    ..Default::default()
                }),
                state,
                revision,
                ..Default::default()
            }),
            node_address: "node:9000".into(),
            relay_address: "node:9443".into(),
        };

        assert!(!owner_covers_create(
            &owner(pb::EnvironmentState::Starting as i32, 7, 8, "node-a"),
            &record
        )
        .unwrap());
        assert!(!owner_covers_create(
            &owner(pb::EnvironmentState::Running as i32, 6, 20, "node-a"),
            &record
        )
        .unwrap());
        assert!(!owner_covers_create(
            &owner(pb::EnvironmentState::Running as i32, 7, 9, "node-b"),
            &record
        )
        .unwrap());
        assert!(owner_covers_create(
            &owner(pb::EnvironmentState::Running as i32, 7, 9, "node-a"),
            &record
        )
        .unwrap());
        assert!(owner_covers_create(
            &owner(pb::EnvironmentState::Running as i32, 7, 10, "node-a"),
            &record
        )
        .unwrap());
    }

    #[test]
    fn create_maps_hidden_foreign_ownership_to_name_conflict() {
        let error = map_create_owner_error(Status::permission_denied(
            "environment belongs to another tenant",
        ));
        assert_eq!(error.code(), Code::AlreadyExists);
        assert_eq!(
            map_create_owner_error(Status::unavailable("directory unavailable")).code(),
            Code::Unavailable
        );
    }
}
