//! Environment application service shared by HTTP and in-process callers.
use crate::{
    clients::{authorize, Clients},
    contract,
    metrics::Attempt,
    operations::{Kind, Operations},
    ownership::Cache,
    workspace_affinity,
};
use adx_protocol::control as pb;
use adx_transport::deadline::Deadline;
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
    spec: pb::EnvironmentSpec,
    result: Option<Result<Value, Status>>,
    created_at: Instant,
    deadline: Deadline,
    assignment: Option<pb::Assignment>,
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

/// Owns Environment lifecycle semantics independently of any transport adapter.
pub struct EnvironmentService {
    pub(crate) creation_metrics: crate::metrics::CreationMetrics,
    pub(crate) clients: Arc<Clients>,
    operations: Operations,
    creates: Mutex<CreateReplays>,
    names: Mutex<HashMap<String, CreateKey>>,
    #[cfg(test)]
    cleanup_started: [tokio::sync::Notify; 2],
}

impl EnvironmentService {
    pub fn new(clients: Arc<Clients>) -> Arc<Self> {
        let retention = Duration::from_secs(clients.config.create_unknown_retention_seconds);
        let period = retention.min(Duration::from_secs(60));
        let service = Arc::new(Self {
            creation_metrics: Default::default(),
            operations: Operations::new(clients.clone()),
            creates: Mutex::new(CreateReplays {
                pending: HashMap::new(),
                completed: Cache::new(clients.config.cache_entries),
            }),
            clients,
            names: Mutex::default(),
            #[cfg(test)]
            cleanup_started: Default::default(),
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

    /// Resume with the duration of the newly started run, preserving Spec defaults.
    pub async fn resume_with_run_timeout(
        &self,
        id: &str,
        caller: &pb::CallerContext,
        seconds: u64,
    ) -> Result<Value, Status> {
        self.operations
            .resume_with_run_timeout(id, caller, seconds)
            .await
    }

    /// Execute a read-merge-write operation only at the inspected revision.
    pub async fn update_network_at_revision(
        &self,
        id: &str,
        request_id: &str,
        network: Option<adx_core::execution::NetworkPolicy>,
        caller: &pb::CallerContext,
        required_revision: u64,
    ) -> Result<Value, Status> {
        self.operations
            .update_network_at_revision(id, request_id, network, caller, required_revision)
            .await
    }

    pub fn metrics(&self) -> String {
        self.creation_metrics.render()
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

    /// Build a Platform specification from the public Environment request contract.
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
    /// Preserve the public request identity before optimistic local admission.
    pub fn prepare_create_with_request(
        &self,
        mut input: Value,
        caller: &pb::CallerContext,
        request_id: &str,
    ) -> Result<pb::EnvironmentSpec, Status> {
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(Status::invalid_argument(
                "request ID must contain 1 to 256 bytes",
            ));
        }
        if input
            .get("name")
            .is_none_or(|name| name.as_str().is_some_and(str::is_empty))
        {
            if let Some(object) = input.as_object_mut() {
                let identity = serde_json::to_vec(&(caller.tenant_id.as_str(), request_id))
                    .map_err(|_| Status::internal("request identity encoding failed"))?;
                object.insert(
                    "name".into(),
                    Value::String(format!("environment-{:x}", Sha256::digest(identity))),
                );
            }
        }
        self.prepare_create(input, caller)
    }

    /// Create admission with the Workspace owner as a soft node preference.
    /// A missing Workspace mount leaves the request unchanged.
    pub async fn prepare_create_with_workspace_affinity(
        &self,
        input: Value,
        caller: &pb::CallerContext,
        request_id: &str,
    ) -> Result<pb::EnvironmentSpec, Status> {
        let mut spec = self.prepare_create_with_request(input, caller, request_id)?;
        let Some(resolver) = &self.clients.workspace else {
            return Ok(spec);
        };
        let mounts = spec
            .execution
            .as_ref()
            .map(|execution| execution.mounts.as_slice())
            .unwrap_or_default();
        let owner = match workspace_affinity::derive_owner(resolver.root(), mounts) {
            Ok(Some(owner)) => owner,
            Ok(None) => return Ok(spec),
            Err(error) => return self.fall_back_without_affinity(spec, resolver, None, error),
        };
        let node_id = match resolver.node_id(&owner).await {
            Ok(node_id) => node_id,
            Err(error) => {
                return self.fall_back_without_affinity(spec, resolver, Some(&owner), error)
            }
        };
        match workspace_affinity::prefer_node(
            spec.scheduling.get_or_insert_with(Default::default),
            &node_id,
        ) {
            Ok(()) => Ok(spec),
            Err(error) => self.fall_back_without_affinity(spec, resolver, Some(&owner), error),
        }
    }

    /// Drop affinity and keep the caller's placement. Only `fail_closed` errors.
    fn fall_back_without_affinity(
        &self,
        spec: pb::EnvironmentSpec,
        resolver: &workspace_affinity::WorkspaceAffinityResolver,
        owner: Option<&str>,
        error: Status,
    ) -> Result<pb::EnvironmentSpec, Status> {
        if !resolver.may_fall_back(&error) {
            adx_observability::warn!(
                owner = owner.unwrap_or_default(),
                error = %error,
                "Workspace affinity rejected environment create"
            );
            return Err(error);
        }
        self.creation_metrics.workspace_affinity_fallback();
        adx_observability::warn!(
            owner = owner.unwrap_or_default(),
            error = %error,
            "Workspace affinity skipped; scheduling without the owner preference"
        );
        Ok(spec)
    }

    pub async fn create_request(
        &self,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        self.creation_metrics.embedded_request();
        let spec = self
            .prepare_create_with_workspace_affinity(input.clone(), caller, request_id)
            .await
            .inspect_err(|error| self.creation_metrics.rejection(error))?;
        Box::pin(self.create(spec, input, request_id, caller)).await
    }

    pub async fn create(
        &self,
        spec: pb::EnvironmentSpec,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let trace = adx_observability::trace::Trace::child("environment.create");
        trace.attribute("environment.id", spec.id.clone());
        // Single writer for the terminal outcome: `finish` records the actual
        // value, while the guard records "cancelled" if this future is dropped
        // before completion. The OTel SDK appends duplicate keys, so seeding a
        // fallback attribute and overwriting it later is not an option.
        let mut outcome_guard = trace.attribute_guard("environment.create.outcome", "cancelled");
        let tenant_id = caller.tenant_id.clone();
        let environment_id = spec.id.clone();
        let mut attempt = self.creation_metrics.attempt();
        let result = trace
            .scope_result(self.create_inner(spec, input, request_id, caller, &mut attempt))
            .await;
        attempt.finish(&result);
        outcome_guard.finish(attempt.outcome());
        trace.attribute("user.id", tenant_id);
        match &result {
            Ok(value) => {
                trace.attribute("result", "success".into());
                if let Some(id) = value.get("environmentId").and_then(Value::as_str) {
                    trace.attribute("instance_id", id.to_owned());
                }
                if let Some(newly) = value.get("newlyCreated").and_then(Value::as_bool) {
                    trace.attribute("newly_created", newly.to_string());
                }
            }
            Err(error) => {
                trace.attribute("result", "fail".into());
                trace.attribute("error.code", i32::from(error.code()).to_string());
                trace.attribute("error.message", error.message().to_owned());
                // Sets Error status and records the bounded `exception` event;
                // the terminal attributes above stay unchanged.
                trace.record_error("create", error.message().to_owned());
                trace.scope(|| write_retry_status_attributes(error));
            }
        }
        // Emit the completion log under the `environment.create` span. This
        // keeps the terminal log attributed to the create span even when the
        // caller (e.g. the activator) has no outer request span.
        let duration_ms = attempt.elapsed().as_secs_f64() * 1_000.0;
        let outcome = attempt.outcome();
        let status_code = result
            .as_ref()
            .err()
            .map_or("OK", |error| error.code().description());
        trace.scope(|| {
            adx_observability::info!(
                event = "environment_create_completed",
                request_id,
                environment_id = %environment_id,
                duration_ms,
                outcome,
                succeeded = result.is_ok(),
                status_code,
                status_message = result.as_ref().err().map_or("", |error| error.message()),
                "Environment create completed"
            );
        });
        result
    }

    async fn create_inner(
        &self,
        spec: pb::EnvironmentSpec,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
        attempt: &mut Attempt<'_>,
    ) -> Result<Value, Status> {
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(Status::invalid_argument(
                "request ID must contain 1 to 256 bytes",
            ));
        }
        let create_key = (caller.tenant_id.clone(), request_id.to_string());
        let (operation, retry_pending) = {
            let mut creates = self.creates.lock().await;
            if let Some(operation) = creates.pending.get(&create_key) {
                (operation.clone(), true)
            } else if let Some(operation) = creates.completed.get(&create_key) {
                (operation, false)
            } else {
                let operation = Arc::new(Mutex::new(CreateOperation {
                    spec,
                    result: None,
                    created_at: Instant::now(),
                    deadline: Deadline::after(Duration::from_secs(contract::create_timeout(
                        &input,
                    )?))?,
                    unknown_since: None,
                    assignment: None,
                }));
                creates
                    .pending
                    .insert(create_key.clone(), operation.clone());
                (operation, false)
            }
        };
        let mut operation = operation.lock_owned().await;
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
                let _ = &other.spec;
            }
        }
        // The typed Environment payload includes workload probe configuration.
        // Keep the RPC orchestration future off callers' stack frames.
        let result = tokio::time::timeout_at(
            operation.deadline.instant() + adx_transport::deadline::CREATE_RESPONSE_GRACE,
            Box::pin(self.perform_create(
                &mut operation,
                &input,
                request_id,
                caller,
                retry_pending,
                attempt,
            )),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Status::deadline_exceeded(
                "create response deadline exceeded while waiting for scheduling/node initialization result; query the same environment",
            ))
        });
        #[cfg(test)]
        self.cleanup_started[0].notify_one();
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
        if result.as_ref().is_err_and(crate::metrics::unknown) {
            operation.unknown_since.get_or_insert_with(Instant::now);
            return;
        }
        operation.result = Some(result.clone());
        #[cfg(test)]
        self.cleanup_started[1].notify_one();
        let mut creates = self.creates.lock().await;
        if let Some(shared) = creates.pending.remove(key) {
            creates
                .completed
                .insert(key.clone(), shared, Duration::from_secs(600));
        }
    }

    async fn perform_create(
        &self,
        operation: &mut CreateOperation,
        input: &Value,
        request_id: &str,
        caller: &pb::CallerContext,
        retry_pending: bool,
        attempt: &mut Attempt<'_>,
    ) -> Result<Value, Status> {
        let spec = &operation.spec;
        let deadline = operation.deadline;
        let expected_assignment = &mut operation.assignment;
        // A pending retry bypasses a potentially stale subscription. Absence
        // still retries the same identity through atomic ownership, never a new ID.
        match self.clients.owner(&spec.id, caller, retry_pending).await {
            Ok(owner) => {
                let record = owner
                    .record
                    .ok_or_else(|| Status::data_loss("environment directory returned no record"))?;
                if retry_pending
                    && !matches!(
                        pb::EnvironmentState::try_from(record.state),
                        Ok(pb::EnvironmentState::Pending
                            | pb::EnvironmentState::Starting
                            | pb::EnvironmentState::Running)
                    )
                {
                    write_create_target(&record, "replay");
                    return existing_running_response(spec, input, request_id, &record);
                }
                let assignment = record
                    .assignment
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("owner assignment missing"))?;
                *expected_assignment = Some(assignment.clone());
            }
            Err(error) if error.code() == Code::NotFound => {}
            Err(error) => return Err(map_create_owner_error(error)),
        };

        let timeouts = contract::create_timeouts(input)?;
        let budget = deadline.remaining().map_err(|_| {
            Status::deadline_exceeded("create deadline exceeded during ownership lookup")
        })?;
        // The RPC future is large in debug builds; keep it off the nested
        // embedded HTTP/Activator polling stack while preserving cancellation.
        let (result, source) = Box::pin(self.clients.create_environment(
            pb::CreateEnvironmentRequest {
                request_id: request_id.to_owned(),
                spec: Some(spec.clone()),
                caller: Some(caller.clone()),
                schedule_timeout_seconds: timeouts.schedule_seconds,
                create_timeout_seconds: timeouts.create_seconds,
                create_deadline_unix_millis: deadline.unix_millis(),
                expected_assignment: expected_assignment.clone(),
            },
            budget,
        ))
        .await?;
        authorize(caller, result.record.as_ref())?;
        let record = result
            .record
            .clone()
            .ok_or_else(|| Status::unavailable("create returned no environment record"))?;
        if result.durability != pb::Durability::Published as i32 {
            return Err(Status::unavailable("create is not durably confirmed"));
        }
        let source = if retry_pending { "replay" } else { source };
        write_create_target(&record, source);
        write_retry_result_attributes(&result);
        let mut response = existing_running_response(spec, input, request_id, &record)?;
        // This bit comes from the node's creation result, never requestId echo
        // or a pre-create existence query. Cached retries retain the same bit.
        response["newlyCreated"] = Value::Bool(result.newly_created);

        // Node-local success seeds the versioned in-memory directory from the
        // trusted node result. Central fallback and startup races use one
        // authoritative read; ordinary local-first creates do not.
        let confirmed_id = record
            .spec
            .as_ref()
            .ok_or_else(|| Status::data_loss("confirmed spec missing"))?
            .id
            .as_str();
        match self.clients.owner(confirmed_id, caller, false).await {
            Ok(owner) if owner_covers_create(&owner, &record)? => {}
            Ok(_) => {
                let owner = self.clients.owner(confirmed_id, caller, true).await?;
                if !owner_covers_create(&owner, &record)? {
                    return Err(Status::unavailable(
                        "environment directory has not observed the published create result",
                    ));
                }
            }
            Err(error) if matches!(error.code(), Code::NotFound | Code::Unavailable) => {
                let owner = self.clients.owner(confirmed_id, caller, true).await?;
                if !owner_covers_create(&owner, &record)? {
                    return Err(Status::unavailable(
                        "environment directory has not observed the published create result",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        if result.newly_created && !retry_pending {
            attempt.successful_create(attempt.elapsed());
        }
        Ok(response)
    }
}

fn existing_running_response(
    _spec: &pb::EnvironmentSpec,
    input: &Value,
    request_id: &str,
    record: &pb::EnvironmentRecord,
) -> Result<Value, Status> {
    let confirmed_spec = record
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("environment record returned no spec"))?;
    if record.state == pb::EnvironmentState::Failed as i32 {
        if let Some(failure) = &record.failure {
            let failure: adx_core::RuntimeFailure =
                failure.clone().try_into().map_err(adx_protocol::status)?;
            if failure.reason == adx_core::RuntimeFailureReason::DeadlineExceeded {
                return Err(adx_protocol::status(adx_core::Error::DeadlineExceeded(
                    failure.message,
                )));
            }
            if let Some(probe_failure) = failure.probe_failure {
                return Err(adx_protocol::status(adx_core::Error::ProbeFailed(
                    probe_failure,
                )));
            }
        }
    }
    if record.state != pb::EnvironmentState::Running as i32 {
        if !record.restart_pending
            && (record.state == pb::EnvironmentState::Failed as i32
                || record.state == pb::EnvironmentState::Deleted as i32)
        {
            return Err(Status::failed_precondition(
                "environment create has a terminal result; query its failure",
            ));
        }
        return Err(Status::unavailable("environment create is not yet running"));
    }
    let mut response = json!({
        "environmentId": confirmed_spec.id,
        "status": "running",
        "requestId": request_id,
        "newlyCreated": false,
    });
    if input.pointer("/tunnel/enabled").and_then(Value::as_bool) == Some(true) {
        let port = confirmed_spec
            .execution
            .as_ref()
            .and_then(|options| options.tunnel_http_port)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port > 1)
            .ok_or_else(|| Status::data_loss("environment tunnel configuration is missing"))?;
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

/// Records which node owns the created environment and how the request was
/// routed. Each key is written once per create span.
fn write_create_target(record: &pb::EnvironmentRecord, source: &str) {
    if let Some(assignment) = &record.assignment {
        adx_observability::trace::attribute("node", assignment.node_id.clone());
    }
    adx_observability::trace::attribute("source", source.to_owned());
}

fn write_retry_result_attributes(result: &pb::EnvironmentResult) {
    adx_observability::trace::attribute("schedule.attempts", result.create_attempts.to_string());
    adx_observability::trace::attribute("retry.count", result.retry_count.to_string());
    if !result.retry_first_error_code.is_empty() {
        adx_observability::trace::attribute(
            "retry.first_error_code",
            result.retry_first_error_code.clone(),
        );
    }
    if !result.retry_first_error_message.is_empty() {
        adx_observability::trace::attribute(
            "retry.first_error_message",
            result.retry_first_error_message.clone(),
        );
    }
}

fn write_retry_status_attributes(status: &Status) {
    let value = |key: &str| {
        status
            .metadata()
            .get(key)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    if let Some(value) = value("adx-create-attempts") {
        adx_observability::trace::attribute("schedule.attempts", value);
    }
    if let Some(value) = value("adx-retry-count") {
        adx_observability::trace::attribute("retry.count", value);
    }
    if let Some(value) = value("adx-retry-first-error-code") {
        adx_observability::trace::attribute("retry.first_error_code", value);
    }
    if let Some(value) = value("adx-retry-first-error-message") {
        adx_observability::trace::attribute("retry.first_error_message", value);
    }
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

/// Embedded Ingress uses the same tenant-filtered directory as the public instance API.
/// No runtime credentials or EnvironmentSpec are returned to business adapters.
#[async_trait::async_trait]
impl data_plane_gateway::ingress::environment_files::EnvironmentDirectory for EnvironmentService {
    async fn authorize(
        &self,
        tenant: &str,
        environment_id: &str,
    ) -> Result<(), data_plane_gateway::ingress::environment_files::ReadError> {
        use data_plane_gateway::ingress::environment_files::ReadError;
        if tenant.trim().is_empty() || environment_id.trim().is_empty() {
            return Err(ReadError::Identity);
        }
        let response = self
            .inspect(tenant, environment_id)
            .await
            .map_err(|error| match error.code() {
                Code::NotFound | Code::PermissionDenied => ReadError::NotFound,
                _ => ReadError::Runtime,
            })?
            .ok_or(ReadError::NotFound)?;
        let record = response.record.ok_or(ReadError::Runtime)?;
        let spec = record.spec.ok_or(ReadError::Runtime)?;
        if spec.tenant_id != tenant || spec.id != environment_id {
            return Err(ReadError::NotFound);
        }
        if !matches!(
            pb::EnvironmentState::try_from(record.state),
            Ok(pb::EnvironmentState::Running
                | pb::EnvironmentState::Pausing
                | pb::EnvironmentState::Paused
                | pb::EnvironmentState::Resuming)
        ) {
            return Err(ReadError::Runtime);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod create_cleanup {
        use super::*;
        use crate::{config::Config, http::Api};
        use std::sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        };
        use tokio::sync::Notify;
        use tonic::{Request, Response};

        include!("../tests/support/create_fixture.rs");

        #[tokio::test]
        async fn confirmed_create_waits_for_cleanup_before_committing_success_latency() {
            for (phase, cancel) in [(0, true), (1, true), (0, false), (1, false)] {
                let fixture = Fixture::new().await;
                fixture.state.block_response.store(true, Ordering::SeqCst);
                let service = fixture.api.environment_service.clone();
                let pending = tokio::spawn(async move {
                    service
                        .create(
                            spec("cleanup"),
                            json!({"name":"cleanup"}),
                            "cleanup",
                            &caller(),
                        )
                        .await
                });
                fixture.wait_starts(1).await;
                let names = if phase == 0 {
                    Some(fixture.service.names.lock().await)
                } else {
                    None
                };
                let creates = if phase == 1 {
                    Some(fixture.service.creates.lock().await)
                } else {
                    None
                };
                fixture.state.block_response.store(false, Ordering::SeqCst);
                fixture.state.release.notify_one();
                tokio::time::timeout(
                    Duration::from_secs(2),
                    fixture.service.cleanup_started[phase].notified(),
                )
                .await
                .unwrap();
                let count = |suffix: &str, n| {
                    let metrics = fixture.service.metrics();
                    assert!(
                        metrics.contains(&format!("adx_environment_create_{suffix} {n}\n")),
                        "{metrics}"
                    );
                };
                count("success_call_duration_seconds_count", 0);
                count("responses_total{outcome=\"success\"}", 0);
                count("in_flight", 1);
                if cancel {
                    pending.abort();
                    assert!(pending.await.unwrap_err().is_cancelled());
                    drop((names, creates));
                } else {
                    drop((names, creates));
                    pending.await.unwrap().unwrap();
                }
                count("responses_total{outcome=\"cancelled\"}", u64::from(cancel));
                count("responses_total{outcome=\"success\"}", u64::from(!cancel));
                count("duration_seconds_count", 1);
                count("in_flight", 0);
                assert_eq!(
                    fixture
                        .clients
                        .owner("cleanup", &caller(), true)
                        .await
                        .unwrap()
                        .record
                        .unwrap()
                        .state,
                    pb::EnvironmentState::Running as i32
                );
                for request in ["cleanup", "other"] {
                    fixture.create("cleanup", request).await.unwrap();
                    count("success_call_duration_seconds_count", u64::from(!cancel));
                }
            }
        }
    }

    #[test]
    fn unknown_gc_preserves_waiters_and_collects_abandoned_contexts() {
        let now = Instant::now();
        let old = now - Duration::from_secs(30);
        let key = ("tenant".into(), "request".into());
        let operation = Arc::new(Mutex::new(CreateOperation {
            spec: pb::EnvironmentSpec {
                id: "environment".into(),
                ..Default::default()
            },
            result: None,
            created_at: old,
            deadline: Deadline::after(Duration::from_secs(90)).unwrap(),
            assignment: None,
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
                    spec: pb::EnvironmentSpec {
                        id: "environment".into(),
                        ..Default::default()
                    },
                    result: None,
                    created_at: now - Duration::from_secs(30),
                    deadline: Deadline::after(Duration::from_secs(90)).unwrap(),
                    assignment: None,
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
    fn existing_running_name_reuses_the_confirmed_spec() {
        let spec = pb::EnvironmentSpec {
            id: "tenant-worker".into(),
            tenant_id: "tenant".into(),
            runtime_class: "runsc".into(),
            ..Default::default()
        };
        let record = pb::EnvironmentRecord {
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
            spec: Some(spec.clone()),
            state: pb::EnvironmentState::Running as i32,
            ..Default::default()
        };
        let response = existing_running_response(&spec, &json!({}), "create-a", &record).unwrap();
        assert_eq!(response["environmentId"], "tenant-worker");
        assert_eq!(response["requestId"], "create-a");

        let mut different = spec.clone();
        different.runtime_class = "firecracker".into();
        let replay =
            existing_running_response(&different, &json!({}), "create-b", &record).unwrap();
        assert_eq!(replay["environmentId"], "tenant-worker");
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
        spec.execution = Some(pb::ExecutionOptions {
            tunnel_http_port: Some(18766),
            ..Default::default()
        });
        // Raw environment values cannot redirect the public response either.
        spec.env
            .insert("EXECD_TUNNEL_HTTP_PORT".into(), "12345".into());
        let record = pb::EnvironmentRecord {
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
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
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
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
                observation: None,
                execution_artifacts: vec![],
                snapshot_source: None,
                failure: None,
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
