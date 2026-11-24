//! Agent-side conversion to the public Environment management protocol.
//! No Platform model or RPC client is used here.
use adx_agent_core::{
    environment::*,
    lifecycle::RetentionPolicy,
    limits,
    network::{PlatformEgress, TemplateNetwork},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub(crate) fn validate_identity(value: &str) -> Result<(), EnvironmentError> {
    if value.is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || matches!(value, "." | "..")
        || value
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(EnvironmentError::Invalid(
            "invalid Environment identity".into(),
        ));
    }
    Ok(())
}
pub(crate) fn check_golden(
    value: &Value,
    id: &str,
    image: &str,
    runtime: &str,
) -> Result<bool, EnvironmentError> {
    if value["snapshotId"] != id || value["state"] != "ready" {
        return Err(EnvironmentError::Unavailable(
            "ready snapshot metadata has not been confirmed".into(),
        ));
    }
    Ok(value["kind"] == "golden" && value["image"] == image && value["runtime"] == runtime)
}
pub(crate) fn observation(
    tenant: &str,
    id: &str,
    phase: EnvironmentPhase,
) -> EnvironmentObservation {
    EnvironmentObservation {
        id: id.into(),
        tenant: tenant.into(),
        ready: phase == EnvironmentPhase::Running,
        phase,
        runtime_id: None,
        message: None,
    }
}
pub fn validate_timeout(timeout: Duration) -> Result<(), EnvironmentError> {
    if timeout.is_zero() || i64::try_from(timeout.as_secs()).is_err() {
        return Err(EnvironmentError::Invalid(
            "invalid Environment timeout".into(),
        ));
    }
    Ok(())
}
pub fn validate_execution(execution: &ExecutionSpec) -> Result<(), EnvironmentError> {
    execution.validate().map_err(EnvironmentError::Invalid)?;
    if !execution.inherit_entrypoint {
        return Err(EnvironmentError::Unsupported(
            "API Server adapter requires image process inheritance".into(),
        ));
    }
    if execution
        .env
        .keys()
        .any(|key| key.starts_with("ADX_") || key == "EXECD_HTTP_TOKEN")
    {
        return Err(EnvironmentError::Invalid("reserved environment key".into()));
    }
    if execution
        .resources
        .cpu_limit_millis
        .unwrap_or(execution.resources.cpu_millis)
        > i64::MAX as u64
        || execution
            .resources
            .memory_limit_mib
            .unwrap_or(execution.resources.memory_mib)
            > (i64::MAX as u64 / 1048576)
    {
        return Err(EnvironmentError::Invalid(
            "Environment resources exceed API Server limits".into(),
        ));
    }
    Ok(())
}
/// Both HTTP and local adapters submit the same request body. Deadline is transport metadata.
pub fn create_input(
    request: &CreateEnvironment,
    timeout: Duration,
    platform_egress: Option<&PlatformEgress>,
) -> Result<Value, EnvironmentError> {
    validate_execution(&request.execution)?;
    validate_identity(&request.tenant)?;
    validate_identity(&request.id)?;
    validate_timeout(timeout)?;
    // AgentBinding generations use adx-<UUID>. The public API builds namespace-name IDs.
    let name = request
        .id
        .strip_prefix("adx-")
        .filter(|name| !name.is_empty() && name.len() <= 256)
        .ok_or_else(|| {
            EnvironmentError::Invalid("managed Environment ID must be adx-<name>".into())
        })?;
    let execution = &request.execution;
    // Keep the submitted specification stable across retries. The caller's absolute
    // deadline bounds HTTP waiting; API Server owns its independent create budget.
    let mut input = json!({
        "namespace":"adx", "name":name,
        "cpu":execution.resources.cpu_millis, "memory":execution.resources.memory_mib,
        "inheritEntrypoint":execution.snapshot_id.is_none(), "env":execution.env,
        "ports":execution.service.iter().map(|service| service.port.to_string()).collect::<std::collections::BTreeSet<_>>(),
        "createTimeoutSeconds":timeout.as_secs().max(1),
        "restartPolicy":{"maxAttempts":3,"initialBackoffSeconds":5,"maxBackoffSeconds":60},
    });
    if let Some(snapshot_id) = &execution.snapshot_id {
        input["snapshotId"] = json!(snapshot_id);
    } else {
        input["image"] = json!(execution.image);
        input["runtime"] = json!(execution.isolation_runtime);
    }
    if let Some(policy) = egress_input(execution.network.as_ref(), platform_egress) {
        input["network"] = policy;
    }
    if !execution.lifecycle.is_default() {
        input["idleTimeoutSeconds"] = json!(execution.lifecycle.idle_timeout_seconds);
        input["idleAction"] = json!(execution.lifecycle.idle_action);
        if let Some(pause) = &execution.lifecycle.pause {
            let retention = match pause.retention {
                RetentionPolicy::Ttl { ttl_seconds } => {
                    json!({"kind":"ttl","ttlSeconds":ttl_seconds})
                }
                RetentionPolicy::Forever {} => json!({"kind":"forever"}),
            };
            input["pauseOptions"] = json!({"retention":retention,"timeoutSeconds":pause.timeout_seconds,"storage":pause.storage});
        }
    }
    if !execution.mounts.is_empty() {
        input["mounts"] = json!(execution.mounts);
    }
    if let Some(seconds) = execution.init_timeout_seconds {
        input["initCallTimeoutSeconds"] = json!(seconds);
    }
    // The captured process owns both probes and its rootfs. Sending an image on
    // restore constructs a new rootfs request which may conflict with the source.
    if execution.snapshot_id.is_none() {
        if let Some(probe) = &execution.readiness_probe {
            input["readinessProbe"] = readiness_probe_input(probe)?;
        }
        if let Some(healthcheck) = &execution.healthcheck {
            let mut probe = readiness_probe_input(&healthcheck.probe)?;
            probe["failureThreshold"] = json!(healthcheck.failure_threshold);
            input["healthcheckProbe"] = probe;
        }
    }
    if let Some(limit) = execution.resources.cpu_limit_millis {
        input["cpu_limit"] = json!(limit);
    }
    if let Some(limit) = execution.resources.memory_limit_mib {
        input["mem_limit"] = json!(limit);
    }
    Ok(input)
}

pub(crate) fn seed_input(
    tenant: &str,
    id: &str,
    seed: &adx_agent_core::prewarm::SeedExecution,
    timeout: Duration,
    platform_egress: Option<&PlatformEgress>,
) -> Result<Value, EnvironmentError> {
    if let Some(input) = &seed.native_input {
        return Ok(input.clone());
    }
    let request = CreateEnvironment {
        tenant: tenant.into(),
        id: format!("adx-{id}"),
        execution: seed.execution.clone(),
        deadline_unix_ms: None,
        creation_attempt: 0,
    };
    let mut environment = create_input(&request, timeout, platform_egress)?;
    environment["idleTimeoutSeconds"] = json!(0);
    Ok(json!({"environment":environment, "volume":seed.volume}))
}

fn readiness_probe_input(
    probe: &adx_agent_core::probes::ReadinessProbe,
) -> Result<Value, EnvironmentError> {
    use adx_agent_core::probes::ProbeAction;
    let action = match probe.action().map_err(EnvironmentError::Invalid)? {
        ProbeAction::HttpGet { port, path } => json!({"httpGet":{"port":port,"path":path}}),
        ProbeAction::TcpSocket { port } => json!({"tcpSocket":{"port":port}}),
        ProbeAction::Exec { command } => json!({"exec":{"command":command}}),
    };
    let mut input = action;
    input["initialDelaySeconds"] = json!(probe.initial_delay_seconds);
    input["periodSeconds"] = json!(probe.period_seconds);
    input["timeoutSeconds"] = json!(probe.timeout_seconds);
    Ok(input)
}
pub fn operation_id(action: &str, tenant: &str, id: &str) -> Result<String, EnvironmentError> {
    validate_identity(tenant)?;
    validate_identity(id)?;
    let identity = serde_json::to_vec(&(tenant, id))
        .map_err(|_| EnvironmentError::Invalid("invalid Environment identity".into()))?;
    Ok(format!("activator-{action}-{:x}", Sha256::digest(identity)))
}
/// Stable within one creation attempt, including retries after response loss.
pub fn creation_request_id(request: &CreateEnvironment) -> Result<String, EnvironmentError> {
    creation_operation_id(&request.tenant, &request.id, request.creation_attempt)
}
pub(crate) fn creation_operation_id(
    tenant: &str,
    id: &str,
    attempt: u64,
) -> Result<String, EnvironmentError> {
    if attempt == 0 {
        return operation_id("create", tenant, id);
    }
    validate_identity(tenant)?;
    validate_identity(id)?;
    let identity = serde_json::to_vec(&(tenant, id, attempt))
        .map_err(|_| EnvironmentError::Invalid("invalid creation identity".into()))?;
    Ok(format!("activator-create-{:x}", Sha256::digest(identity)))
}

/// Only the structured, confirmed pre-assignment cancellation authorizes another attempt.
pub(crate) fn scheduling_cancellation(value: &Value) -> Option<EnvironmentError> {
    let error = value.get("error")?;
    if error["code"] != "DEADLINE_EXCEEDED"
        || error["retry"] != "never"
        || error["outcome"] != "not_started"
    {
        return None;
    }
    let failure: adx_core::scheduling::SchedulingFailure =
        serde_json::from_value(error.get("schedulingFailure")?.clone()).ok()?;
    Some(EnvironmentError::SchedulingCancelled(Box::new(failure)))
}

pub(crate) fn terminal_creation(value: &Value, id: &str, request_id: &str) -> bool {
    let error = &value["error"];
    value["message"] == adx_error::CREATE_TERMINAL_MESSAGE
        && error["code"] == "CONFLICT"
        && error["retry"] == "never"
        && error["outcome"] == "not_started"
        && error["environmentId"] == id
        && error["requestId"] == request_id
}

pub(crate) fn created(
    tenant: &str,
    id: &str,
    value: &Value,
) -> Result<EnvironmentObservation, EnvironmentError> {
    if value["environmentId"] != id || value["status"] != "running" {
        return Err(EnvironmentError::OutcomeUnknown(
            "invalid API Server create response".into(),
        ));
    }
    Ok(observation(tenant, id, EnvironmentPhase::Running))
}
pub(crate) fn observed(
    tenant: &str,
    id: &str,
    row: &Value,
) -> Result<EnvironmentObservation, EnvironmentError> {
    if row["id"] != id {
        return Err(EnvironmentError::Unavailable(
            "API Server instance identity mismatch".into(),
        ));
    }
    let phase = match row["status"].as_str() {
        Some("running") => EnvironmentPhase::Running,
        Some("pending" | "starting") => EnvironmentPhase::Creating,
        Some("failed") => EnvironmentPhase::Failed,
        Some("pausing") => EnvironmentPhase::Pausing,
        Some("paused") => EnvironmentPhase::Paused,
        Some("resuming") => EnvironmentPhase::Resuming,
        Some("deleting") => {
            return Err(EnvironmentError::Conflict(
                "Environment is not available for Agent activation".into(),
            ));
        }
        _ => {
            return Err(EnvironmentError::Unavailable(
                "unknown API Server instance state".into(),
            ));
        }
    };
    Ok(observation(tenant, id, phase))
}

/// Deterministic pause operation identity for a stable tenant/id. The `pause-` prefix
/// satisfies the Platform `x-adx-request-id` lifecycle contract; the hash makes retries
/// replay the authoritative result instead of conflicting with a prior pause.
pub fn pause_operation_id(tenant: &str, id: &str) -> Result<String, EnvironmentError> {
    validate_identity(tenant)?;
    validate_identity(id)?;
    let identity = serde_json::to_vec(&(tenant, id))
        .map_err(|_| EnvironmentError::Invalid("invalid Environment identity".into()))?;
    Ok(format!("pause-activator-{:x}", Sha256::digest(identity)))
}

/// Pause submits only the checkpoint capture budget. Retention is intentionally omitted so
/// the Environment inherits the create-time template pause policy.
pub fn pause_input(timeout_seconds: u64) -> Value {
    json!({ "timeoutSeconds": timeout_seconds })
}

/// Validate a durable Paused response. The Platform acknowledges pause only after the
/// checkpoint is published, so a mismatched identity or non-paused state is an unknown
/// outcome: the caller re-queries the original ID rather than assuming success.
pub fn paused(
    tenant: &str,
    id: &str,
    value: &Value,
) -> Result<EnvironmentObservation, EnvironmentError> {
    if value["environmentId"] != id || value["state"] != "paused" {
        return Err(EnvironmentError::OutcomeUnknown(
            "invalid API Server pause response".into(),
        ));
    }
    Ok(observation(tenant, id, EnvironmentPhase::Paused))
}

pub(crate) fn validate_platform_egress(
    egress: Option<&PlatformEgress>,
) -> Result<(), EnvironmentError> {
    if let Some(egress) = egress {
        egress.validate().map_err(EnvironmentError::Invalid)?;
    }
    Ok(())
}

fn egress_input(
    template: Option<&TemplateNetwork>,
    platform: Option<&PlatformEgress>,
) -> Option<Value> {
    let platform_rules = platform
        .and_then(|settings| settings.egress.as_ref())
        .into_iter()
        .flat_map(|egress| {
            egress
                .iter()
                .map(|(action, rule)| (action, &rule.destination, rule.priority))
        });
    let priority = platform
        .and_then(|settings| settings.template_priority)
        .unwrap_or(1);
    let template_rules = template.into_iter().flat_map(|network| {
        network
            .egress
            .iter()
            .map(|(action, rule)| (action, rule, priority))
    });
    let rules: Vec<_> = platform_rules
        .chain(template_rules)
        .map(|(action, rule, priority)| {
            let mut peer = json!({"cidr": rule.cidr.trunc().to_string()});
            if let Some(ports) = &rule.ports {
                peer["portRange"] = json!(ports);
            }
            json!({"action":action,"direction":"egress","protocol":rule.protocol,"peer":peer,"priority":priority})
        })
        .collect();
    (!rules.is_empty()).then(|| {
        json!({"schemaVersion":2,"traffic":{
            "ingressDefaultAction":"deny","egressDefaultAction":"deny","mode":"stateful","rules":rules
        }})
    })
}

/// The persisted definition wins; only its stable reference enters the execution request.
pub(crate) fn volume_reference(
    value: &Value,
    tenant: &str,
    id: &str,
) -> Result<adx_agent_core::volumes::VolumeRef, EnvironmentError> {
    if value["spec"]["tenant_id"] != tenant || value["spec"]["id"] != id {
        return Err(EnvironmentError::Conflict(
            "Volume ensure identity mismatch".into(),
        ));
    }
    let uid = value["uid"]
        .as_str()
        .filter(|uid| !uid.is_empty())
        .ok_or_else(|| EnvironmentError::OutcomeUnknown("Volume ensure UID missing".into()))?;
    Ok(adx_agent_core::volumes::VolumeRef {
        id: id.into(),
        uid: uid.into(),
    })
}
