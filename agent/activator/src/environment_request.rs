//! Agent-side conversion to the public Environment management protocol.
//! No Platform model or RPC client is used here.
use adx_agent_core::{
    environment::*,
    lifecycle::RetentionPolicy,
    limits,
    network::{EgressProtocol, RuntimeNetworkPolicy, SandboxNetworkPolicy},
};
use ipnet::IpNet;
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
    network: &SandboxNetworkPolicy,
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
    // Validate the complete deployment configuration even for an explicit opt-out.
    let network_request = network_input(network)?;
    // Keep the submitted specification stable across retries. The caller's absolute
    // deadline bounds HTTP waiting; API Server owns its independent create budget.
    let mut input = json!({
        "namespace":"adx", "name":name, "image":execution.image, "runtime":execution.isolation_runtime,
        "cpu":execution.resources.cpu_millis, "memory":execution.resources.memory_mib,
        "inheritEntrypoint":true, "env":execution.env,
        "ports":execution.service.iter().map(|service| service.port.to_string()).collect::<std::collections::BTreeSet<_>>(),
        "createTimeoutSeconds":timeout.as_secs().max(1),
    });
    if network.runtime_policies.get(&execution.isolation_runtime)
        != Some(&RuntimeNetworkPolicy::Unrestricted)
    {
        input["network"] = network_request;
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
    if let Some(probe) = &execution.readiness_probe {
        input["readinessProbe"] = readiness_probe_input(probe)?;
    }
    if let Some(healthcheck) = &execution.healthcheck {
        let mut probe = readiness_probe_input(&healthcheck.probe)?;
        probe["failureThreshold"] = json!(healthcheck.failure_threshold);
        input["healthcheckProbe"] = probe;
    }
    if let Some(limit) = execution.resources.cpu_limit_millis {
        input["cpu_limit"] = json!(limit);
    }
    if let Some(limit) = execution.resources.memory_limit_mib {
        input["mem_limit"] = json!(limit);
    }
    Ok(input)
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

/// Compile deployment policy to the public Environment network contract.
/// The sandboxd backend accepts IPv4 only. Unsupported CIDRs, overlapping service
/// exceptions and invalid port ranges fail closed before creating an Environment.
pub fn network_input(config: &SandboxNetworkPolicy) -> Result<Value, EnvironmentError> {
    if config.runtime_policies.len() > 64
        || config.runtime_policies.keys().any(|runtime| {
            runtime.is_empty()
                || runtime.len() > 64
                || !runtime
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        })
    {
        return Err(EnvironmentError::Invalid(
            "runtime network policies require at most 64 valid runtime names".into(),
        ));
    }
    fn cidr(value: &str) -> Result<IpNet, EnvironmentError> {
        value.parse::<ipnet::Ipv4Net>().map(IpNet::V4).map_err(|_| {
            EnvironmentError::Invalid("Environment network requires an IPv4 CIDR".into())
        })
    }
    if config.sandbox_cidrs.is_empty()
        || config.sandbox_cidrs.len() + config.internal_cidrs.len() + config.egress_allow.len()
            > 256
    {
        return Err(EnvironmentError::Invalid(
            "Environment network requires address pools and at most 256 configured rules".into(),
        ));
    }
    let pools = config
        .sandbox_cidrs
        .iter()
        .map(|v| cidr(v))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rules = Vec::new();
    let mut rule = |action: &str, direction: &str, protocol: Value, peer: Value, priority: u32| {
        rules.push(json!({"action":action,"direction":direction,"protocol":protocol,"peer":peer,"priority":priority}));
    };
    if config.public_egress {
        rule(
            "allow",
            "egress",
            json!("any"),
            json!({"cidr":"0.0.0.0/0"}),
            100,
        );
    }
    // Non-public and special-use destinations are never included in public egress.
    // Deployment-specific globally addressed cluster networks must be listed too.
    for net in [
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
    ]
    .into_iter()
    .chain(config.internal_cidrs.iter().map(String::as_str))
    {
        let net = cidr(net)?;
        rule(
            "deny",
            "egress",
            json!("any"),
            json!({"cidr":net.to_string()}),
            200,
        );
    }
    for allow in &config.egress_allow {
        let net = cidr(&allow.cidr)?;
        if pools
            .iter()
            .any(|pool| pool.contains(&net.network()) || net.contains(&pool.network()))
        {
            return Err(EnvironmentError::Invalid(
                "egress exception overlaps an Environment address pool".into(),
            ));
        }
        match (&allow.ports, allow.protocol) {
            (Some(ports), EgressProtocol::Tcp | EgressProtocol::Udp)
                if ports.first > 0 && ports.first <= ports.last => {}
            (None, EgressProtocol::Icmp) => (),
            _ => {
                return Err(EnvironmentError::Invalid(
                    "TCP/UDP exceptions require a valid port range; ICMP does not accept ports"
                        .into(),
                ));
            }
        }
        let mut peer = json!({"cidr":net.to_string()});
        if let Some(ports) = &allow.ports {
            peer["portRange"] = json!(ports);
        }
        rule("allow", "egress", json!(allow.protocol), peer, 300);
    }
    for net in pools {
        rule(
            "deny",
            "both",
            json!("any"),
            json!({"cidr":net.to_string()}),
            400,
        );
    }
    Ok(
        json!({"schemaVersion":2,"traffic":{"ingressDefaultAction":"deny","egressDefaultAction":"deny","mode":"stateful","rules":rules}}),
    )
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
