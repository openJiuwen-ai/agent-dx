//! Public JSON contract converted directly to typed Capsule RPC payloads.
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use tonic::Status;

#[derive(Default, Deserialize)]
#[serde(default)]
struct Create {
    name: String,
    namespace: String,
    image: String,
    runtime: String,
    rootfs: Value,
    cpu: i64,
    memory: i64,
    cpu_limit: i64,
    mem_limit: i64,
    #[serde(rename = "storageMb")]
    storage: Option<i64>,
    storage_limit_mb: i64,
    env: HashMap<String, String>,
    labels: HashMap<String, String>,
    #[serde(rename = "snapshotId")]
    snapshot_id: String,
    #[serde(rename = "idleTimeoutSeconds")]
    idle: i64,
    #[serde(rename = "restartPolicy")]
    restart: Option<Restart>,
    #[serde(rename = "scheduleAffinities")]
    affinities: Vec<Affinity>,
    #[serde(rename = "createTimeoutSeconds")]
    create_timeout: i64,
    #[serde(rename = "scheduleTimeoutSeconds")]
    schedule_timeout: i64,
    #[serde(rename = "initCallTimeoutSeconds")]
    init_timeout: i64,
    #[serde(rename = "inheritEntrypoint")]
    inherit: bool,
    failover: bool,
    xpu: String,
    ports: Vec<String>,
    mounts: Vec<Value>,
    extra_config: HashMap<String, Value>,
    network: Value,
    #[serde(rename = "dataPlane")]
    data_plane: Value,
    tunnel: Tunnel,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Tunnel {
    enabled: bool,
    proxy_port: i64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Restart {
    max_attempts: u32,
    initial_backoff_seconds: u64,
    max_backoff_seconds: u64,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Rootfs {
    runtime: String,
    r#type: String,
    imageurl: String,
    readonly: Option<bool>,
    path: String,
    storage_info: Option<S3>,
}
#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct S3 {
    endpoint: String,
    bucket: String,
    object: String,
    access_key: String,
    secret_key: String,
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct Mount {
    r#type: String,
    target: String,
    options: Vec<String>,
    #[serde(alias = "imageUrl")]
    image_url: String,
    #[serde(alias = "s3Config")]
    s3_config: Option<S3>,
    #[serde(alias = "hostPath")]
    host_path: String,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct PortRange {
    first: u32,
    last: u32,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Peer {
    address: String,
    port: u32,
    cidr: String,
    domain: String,
    port_range: Option<PortRange>,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Rule {
    action: String,
    direction: String,
    protocol: String,
    peer: Peer,
    sandbox_port: u32,
    sandbox_port_range: Option<PortRange>,
    priority: u32,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Traffic {
    ingress_default_action: String,
    egress_default_action: String,
    rules: Vec<Rule>,
    mode: String,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct DnsRule {
    action: String,
    pattern: String,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Dns {
    default_action: String,
    rules: Vec<DnsRule>,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Network {
    block_network: bool,
    dns_blacklist: Vec<String>,
    schema_version: u32,
    traffic: Option<Traffic>,
    dns: Option<Dns>,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct DataPlane {
    tunnel_security_mode: String,
    port_forward_security_mode: String,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Affinity {
    kind: i32,
    affinity: i32,
    weight: i64,
    preferred_priority: bool,
    preferred_anti_other_labels: bool,
    label_ops: Vec<LabelOp>,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct LabelOp {
    r#type: i32,
    label_key: String,
    label_values: Vec<String>,
}
fn invalid(message: &str) -> Status {
    Status::invalid_argument(message)
}
fn active(v: &Value) -> bool {
    !v.is_null() && v.as_object().is_none_or(|o| !o.is_empty())
}
fn positive(value: i64, scale: u64) -> Result<u64, Status> {
    u64::try_from(value)
        .ok()
        .and_then(|v| v.checked_mul(scale))
        .filter(|v| *v <= (1u64 << 53))
        .ok_or_else(|| invalid("invalid resource amount"))
}
fn s3(value: S3) -> Result<adx_core::sandbox::S3Source, Status> {
    let source = adx_core::sandbox::S3Source {
        endpoint: value.endpoint,
        bucket: value.bucket,
        object: value.object,
        access_key_id: value.access_key,
        access_key_secret: value.secret_key,
    };
    source
        .validate()
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(source)
}

fn validate_rootfs_object(
    fields: &serde_json::Map<String, Value>,
    top_level_image: &str,
) -> Result<(), Status> {
    if let Some(runtime) = fields.get("runtime") {
        if runtime.as_str().is_none_or(|value| value.trim().is_empty()) {
            return Err(invalid("rootfs runtime must be a non-empty string"));
        }
    }
    if fields
        .get("readonly")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(invalid("rootfs readonly must be a boolean"));
    }

    let source_fields = ["imageurl", "path", "storageInfo"];
    let has_source = source_fields.iter().any(|key| fields.contains_key(*key));
    let Some(kind) = fields.get("type") else {
        if has_source {
            return Err(invalid("rootfs source fields require type"));
        }
        return Ok(());
    };
    let kind = kind
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("rootfs type must be a non-empty string"))?;
    if !top_level_image.trim().is_empty() {
        return Err(invalid("image and rootfs source are mutually exclusive"));
    }
    let non_empty_string = |key: &str| {
        fields
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    };
    match kind {
        "image" => {
            if fields.contains_key("path") || fields.contains_key("storageInfo") {
                return Err(invalid("image rootfs cannot contain path or storageInfo"));
            }
            if !non_empty_string("imageurl") {
                return Err(invalid("image rootfs requires non-empty imageurl"));
            }
        }
        "s3" => {
            if fields.contains_key("path") || fields.contains_key("imageurl") {
                return Err(invalid("S3 rootfs cannot contain path or imageurl"));
            }
            if !fields.get("storageInfo").is_some_and(Value::is_object) {
                return Err(invalid("S3 rootfs requires an object storageInfo"));
            }
        }
        "local" => {
            if fields.contains_key("imageurl") || fields.contains_key("storageInfo") {
                return Err(invalid(
                    "local rootfs cannot contain imageurl or storageInfo",
                ));
            }
            if !non_empty_string("path") {
                return Err(invalid("local rootfs requires non-empty path"));
            }
        }
        _ => return Err(invalid("unsupported rootfs type")),
    }
    Ok(())
}

fn rootfs(value: Value, top_level_image: &str) -> Result<Rootfs, Status> {
    let value = match value {
        Value::Null => return Ok(Rootfs::default()),
        Value::String(value) if value.trim().starts_with('{') => {
            serde_json::from_str(&value).map_err(|_| invalid("invalid rootfs"))?
        }
        Value::String(value) => {
            if value.trim().is_empty() || !top_level_image.trim().is_empty() {
                return Err(invalid("invalid or conflicting rootfs image"));
            }
            return Ok(Rootfs {
                r#type: "image".into(),
                imageurl: value,
                ..Default::default()
            });
        }
        value => value,
    };
    let fields = value
        .as_object()
        .ok_or_else(|| invalid("rootfs overlay must be an object"))?;
    validate_rootfs_object(fields, top_level_image)?;
    serde_json::from_value(value).map_err(|_| invalid("invalid rootfs"))
}

fn storage_source(root: &Rootfs) -> Result<Option<adx_core::sandbox::StorageSource>, Status> {
    use adx_core::sandbox::StorageSource;
    match root.r#type.as_str() {
        "" => Ok(None),
        "image" => Ok(Some(StorageSource::Image(root.imageurl.clone()))),
        "s3" => Ok(Some(StorageSource::S3(s3(root
            .storage_info
            .clone()
            .ok_or_else(|| invalid("S3 rootfs storageInfo is required"))?)?))),
        "local" => Err(invalid("public local rootfs is not allowed")),
        _ => Err(invalid("unsupported rootfs type")),
    }
}
fn mount(value: Value) -> Result<adx_core::sandbox::Mount, Status> {
    use adx_core::sandbox::StorageSource;
    let value: Mount = serde_json::from_value(value).map_err(|_| invalid("invalid mount"))?;
    let source = match (
        value.image_url.is_empty(),
        value.s3_config,
        value.host_path.is_empty(),
    ) {
        (false, None, true) => StorageSource::Image(value.image_url),
        (true, Some(value), true) => StorageSource::S3(s3(value)?),
        (true, None, false) => return Err(invalid("public host mounts are not allowed")),
        _ => return Err(invalid("mount requires exactly one source")),
    };
    let result = adx_core::sandbox::Mount {
        kind: value.r#type,
        target: value.target,
        options: value.options,
        source,
    };
    result
        .validate()
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(result)
}
fn network_action(value: &str) -> Result<adx_core::sandbox::NetworkAction, Status> {
    match value {
        "allow" => Ok(adx_core::sandbox::NetworkAction::Allow),
        "deny" => Ok(adx_core::sandbox::NetworkAction::Deny),
        _ => Err(invalid("invalid network action")),
    }
}
fn network_direction(value: &str) -> Result<adx_core::sandbox::NetworkDirection, Status> {
    match value {
        "ingress" => Ok(adx_core::sandbox::NetworkDirection::Ingress),
        "egress" => Ok(adx_core::sandbox::NetworkDirection::Egress),
        "both" => Ok(adx_core::sandbox::NetworkDirection::Both),
        _ => Err(invalid("invalid network direction")),
    }
}
fn network_protocol(value: &str) -> Result<adx_core::sandbox::NetworkProtocol, Status> {
    match value {
        "any" => Ok(adx_core::sandbox::NetworkProtocol::Any),
        "tcp" => Ok(adx_core::sandbox::NetworkProtocol::Tcp),
        "udp" => Ok(adx_core::sandbox::NetworkProtocol::Udp),
        "icmp" => Ok(adx_core::sandbox::NetworkProtocol::Icmp),
        _ => Err(invalid("invalid network protocol")),
    }
}
fn port_range(value: PortRange) -> adx_core::sandbox::PortRange {
    adx_core::sandbox::PortRange {
        first: value.first,
        last: value.last,
    }
}
pub(crate) fn network_policy(
    value: Value,
) -> Result<Option<adx_core::sandbox::NetworkPolicy>, Status> {
    use adx_core::sandbox as model;
    if !active(&value) {
        return Ok(None);
    }
    let value: Network =
        serde_json::from_value(value).map_err(|_| invalid("invalid network policy"))?;
    if value.block_network && (!value.dns_blacklist.is_empty() || value.schema_version != 0) {
        return Err(invalid("legacy network policies cannot be combined"));
    }
    let mut policy = model::NetworkPolicy::default();
    if value.block_network {
        policy.traffic = Some(model::TrafficPolicy {
            ingress_default_action: model::NetworkAction::Deny,
            egress_default_action: model::NetworkAction::Deny,
            rules: vec![],
            mode: model::TrafficMode::Stateful,
        });
    } else if !value.dns_blacklist.is_empty() {
        policy.dns = Some(model::DnsPolicy {
            default_action: model::NetworkAction::Allow,
            rules: value
                .dns_blacklist
                .into_iter()
                .map(|pattern| model::DnsRule {
                    action: model::NetworkAction::Deny,
                    pattern,
                })
                .collect(),
        });
    } else {
        if value.schema_version != 2 {
            return Err(invalid("network schemaVersion must be 2"));
        }
        policy.traffic = value
            .traffic
            .map(|traffic| {
                Ok(model::TrafficPolicy {
                    ingress_default_action: network_action(&traffic.ingress_default_action)?,
                    egress_default_action: network_action(&traffic.egress_default_action)?,
                    rules: traffic
                        .rules
                        .into_iter()
                        .map(|rule| {
                            Ok(model::NetworkRule {
                                action: network_action(&rule.action)?,
                                direction: network_direction(&rule.direction)?,
                                protocol: network_protocol(&rule.protocol)?,
                                peer: model::NetworkPeer {
                                    address: rule.peer.address,
                                    port: rule.peer.port,
                                    cidr: rule.peer.cidr,
                                    domain: rule.peer.domain,
                                    port_range: rule.peer.port_range.map(port_range),
                                },
                                sandbox_port: rule.sandbox_port,
                                sandbox_port_range: rule.sandbox_port_range.map(port_range),
                                priority: rule.priority,
                            })
                        })
                        .collect::<Result<_, Status>>()?,
                    mode: match traffic.mode.as_str() {
                        "stateless" => model::TrafficMode::Stateless,
                        "stateful" => model::TrafficMode::Stateful,
                        _ => return Err(invalid("invalid traffic policy mode")),
                    },
                })
            })
            .transpose()?;
        policy.dns = value
            .dns
            .map(|dns| -> Result<model::DnsPolicy, Status> {
                Ok(model::DnsPolicy {
                    default_action: network_action(&dns.default_action)?,
                    rules: dns
                        .rules
                        .into_iter()
                        .map(|rule| {
                            Ok(model::DnsRule {
                                action: network_action(&rule.action)?,
                                pattern: rule.pattern,
                            })
                        })
                        .collect::<Result<_, Status>>()?,
                })
            })
            .transpose()?;
    }
    policy
        .validate()
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(Some(policy))
}
fn security(value: &str) -> Result<adx_core::sandbox::DataPlaneSecurityMode, Status> {
    match value {
        "" => Ok(adx_core::sandbox::DataPlaneSecurityMode::Inherit),
        "tls" => Ok(adx_core::sandbox::DataPlaneSecurityMode::Tls),
        "tls-token" => Ok(adx_core::sandbox::DataPlaneSecurityMode::TlsToken),
        _ => Err(invalid("invalid data-plane security mode")),
    }
}
pub fn create_spec(body: Value, caller: &pb::CallerContext) -> Result<pb::CapsuleSpec, Status> {
    create_spec_with_environment(body, caller, None)
}
pub fn create_spec_with_environment(
    body: Value,
    caller: &pb::CallerContext,
    environment: Option<&adx_core::environment::EnvironmentSpec>,
) -> Result<pb::CapsuleSpec, Status> {
    if let Some(e) = environment {
        e.validate().map_err(|e| invalid(&e.to_string()))?;
    }
    let mut r: Create =
        serde_json::from_value(body).map_err(|_| invalid("invalid create request"))?;
    if caller.tenant_id.is_empty() {
        return Err(Status::unauthenticated("verified identity required"));
    }
    let rootfs_value = std::mem::take(&mut r.rootfs);
    let mut root = rootfs(rootfs_value, &r.image)?;
    if !r.runtime.is_empty() && !root.runtime.is_empty() && r.runtime != root.runtime {
        return Err(invalid("conflicting runtime selections"));
    }
    if root.runtime.is_empty() {
        root.runtime = r.runtime;
    }
    if root.imageurl.is_empty() {
        root.imageurl = r.image;
    }
    if root.r#type.is_empty() && !root.imageurl.is_empty() {
        root.r#type = "image".into();
    }
    let snapshot = (!r.snapshot_id.trim().is_empty()).then(|| r.snapshot_id.trim().to_string());
    if snapshot.is_none()
        && environment.is_none()
        && root.imageurl.trim().is_empty()
        && root.r#type != "s3"
    {
        return Err(invalid("image required"));
    }
    if snapshot.is_none() && root.runtime.is_empty() {
        root.runtime = environment
            .map(|e| e.rootfs.runtime_class.clone())
            .unwrap_or_else(|| "runsc".into());
    }
    resolve_create_timeout(r.create_timeout, r.schedule_timeout, r.init_timeout)?;
    if r.create_timeout < 0 || r.schedule_timeout < 0 || r.init_timeout < 0 || r.idle < 0 {
        return Err(invalid("timeouts cannot be negative"));
    }
    if r.storage.is_some_and(|v| v <= 0) || r.storage_limit_mb < 0 {
        return Err(invalid("storageMb must be positive"));
    }
    let cpu = positive(
        if r.cpu == 0 && snapshot.is_none() {
            1000
        } else {
            r.cpu
        },
        1,
    )?;
    let memory = positive(
        if r.memory == 0 && snapshot.is_none() {
            2048
        } else {
            r.memory
        },
        1048576,
    )?;
    let disk = positive(r.storage.unwrap_or(0), 1048576)?;
    if r.cpu_limit < 0 || r.mem_limit < 0 {
        return Err(invalid("limits cannot be negative"));
    }
    let policy = affinities(r.affinities, r.labels)?;
    if r.env
        .keys()
        .any(|k| k.starts_with("ADX_") || k == "RRT_HTTP_TOKEN")
    {
        return Err(invalid("reserved environment key"));
    }
    let restart = r
        .restart
        .map(|v| {
            if v.max_attempts == 0
                || v.initial_backoff_seconds == 0
                || v.max_backoff_seconds < v.initial_backoff_seconds
            {
                return Err(invalid("invalid restart policy"));
            }
            Ok(pb::RestartPolicy {
                max_attempts: v.max_attempts,
                initial_backoff_seconds: v.initial_backoff_seconds,
                max_backoff_seconds: v.max_backoff_seconds,
            })
        })
        .transpose()?;
    // Forwarded ports do not allocate host ports: Edge and Node Proxy route
    // authenticated traffic directly to the Capsule IP. Keep the public
    // declaration validated while the originating SDK handle uses it to guard
    // get_port_url().
    let ports = validate_ports(&r.ports)?;
    let cpu_limit = if r.cpu_limit == 0 {
        cpu
    } else {
        positive(r.cpu_limit, 1)?
    };
    let memory_limit = if r.mem_limit == 0 {
        memory
    } else {
        positive(r.mem_limit, 1048576)?
    };
    let disk_limit = if r.storage_limit_mb == 0 {
        disk
    } else {
        positive(r.storage_limit_mb, 1048576)?
    };
    if cpu_limit < cpu || memory_limit < memory || disk_limit < disk {
        return Err(invalid("resource limits cannot be below requests"));
    }
    let readonly = root
        .readonly
        .or_else(|| environment.map(|value| value.rootfs.readonly))
        .unwrap_or(false);
    let rootfs =
        storage_source(&root)?.map(|source| adx_core::sandbox::Rootfs { readonly, source });
    let mounts = r
        .mounts
        .into_iter()
        .map(mount)
        .collect::<Result<Vec<_>, _>>()?;
    let network = network_policy(r.network)?;
    let data_plane: DataPlane = if active(&r.data_plane) {
        serde_json::from_value(r.data_plane)
            .map_err(|_| invalid("invalid data-plane security policy"))?
    } else {
        DataPlane::default()
    };
    let data_plane = adx_core::sandbox::DataPlanePolicy {
        tunnel: security(&data_plane.tunnel_security_mode)?,
        port_forward: security(&data_plane.port_forward_security_mode)?,
    };
    let extra_config = if r.extra_config.is_empty() {
        String::new()
    } else {
        serde_json::to_string(&r.extra_config)
            .map_err(|_| invalid("invalid sandbox extra_config"))?
    };
    let sandbox = adx_core::sandbox::SandboxOptions {
        rootfs,
        mounts,
        network,
        data_plane,
        ports,
        failover: r.failover,
        inherit_entrypoint: r.inherit,
        limits: adx_core::sandbox::ResourceLimits {
            cpu_millis: cpu_limit,
            memory_bytes: memory_limit,
            disk_bytes: disk_limit,
        },
        extra_config,
    };
    sandbox
        .validate(&adx_core::Resources {
            cpu_millis: cpu,
            memory_bytes: memory,
            disk_bytes: disk,
        })
        .map_err(|error| invalid(&error.to_string()))?;
    let mut policy = policy;
    if !r.xpu.is_empty() {
        let parts: Vec<_> = r.xpu.split(':').collect();
        if parts.len() != 3 {
            return Err(invalid("xpu must be kind:model:count"));
        }
        let kind = match parts[0].to_ascii_lowercase().as_str() {
            "gpu" => pb::DeviceKind::Gpu,
            "npu" => pb::DeviceKind::Npu,
            _ => return Err(invalid("unknown device kind")),
        };
        let count = parts[2]
            .parse::<u32>()
            .ok()
            .filter(|c| *c > 0)
            .ok_or_else(|| invalid("whole-card count required"))?;
        policy.devices.push(pb::DeviceRequest {
            kind: kind as i32,
            model: (!parts[1].is_empty()).then(|| parts[1].into()),
            count,
        });
    }
    if r.name.is_empty() {
        r.name = format!("sandbox-{}", uuid::Uuid::new_v4());
    }
    if r.namespace.is_empty() {
        r.namespace = "default".into();
    }
    if [&r.name, &r.namespace]
        .iter()
        .any(|v| v.len() > 256 || v.chars().any(|c| c.is_control() || c == '/' || c == '\\'))
    {
        return Err(invalid("invalid instance name or namespace"));
    }
    r.env.insert("RRT_HTTP_PORT".into(), "50090".into());
    for (key, default) in [
        ("RRT_COMMAND_RESULT_TTL_SECS", "3600"),
        ("RRT_COMMAND_STDOUT_LIMIT_BYTES", "4194304"),
        ("RRT_COMMAND_STDERR_LIMIT_BYTES", "4194304"),
        ("RRT_COMMAND_REGISTRY_MAX_RECORDS", "4096"),
        ("RRT_COMMAND_REGISTRY_MAX_BYTES", "268435456"),
        (
            "RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES",
            "201326592",
        ),
        ("RRT_COMMAND_ACTIVITY_HEARTBEAT_SECS", "10"),
        ("RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS", "4096"),
        ("RRT_COMMAND_WATCH_MAX_FRAME_BYTES", "1048576"),
    ] {
        let name = match key {
            "RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS" => {
                "ADX_COMMAND_WATCH_MAX_SUBSCRIPTIONS_PER_CONNECTION".into()
            }
            "RRT_COMMAND_WATCH_MAX_FRAME_BYTES" => "ADX_COMMAND_WATCH_MAX_FRAME_BYTES".into(),
            _ => format!("ADX_{key}"),
        };
        r.env.insert(
            key.into(),
            std::env::var(name)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or(default.into()),
        );
    }
    if r.tunnel.enabled {
        let port = if r.tunnel.proxy_port > 1 {
            r.tunnel.proxy_port
        } else {
            8766
        };
        if port > 65535 {
            return Err(invalid("invalid tunnel port"));
        }
        r.env
            .insert("RRT_TUNNEL_HTTP_PORT".into(), port.to_string());
        r.env
            .insert("RRT_TUNNEL_WS_PORT".into(), (port - 1).to_string());
    }
    let environment = if snapshot.is_none() {
        environment.cloned().map(|mut value| {
            value.rootfs.runtime_class.clone_from(&root.runtime);
            if let Some(readonly) = root.readonly {
                value.rootfs.readonly = readonly;
            }
            value.into()
        })
    } else {
        None
    };
    Ok(pb::CapsuleSpec {
        environment,
        id: format!("{}-{}", r.namespace, r.name),
        tenant_id: caller.tenant_id.clone(),
        image: root.imageurl.trim().into(),
        runtime_class: root.runtime,
        resources: Some(pb::Resources {
            cpu_millis: cpu,
            memory_bytes: memory,
            disk_bytes: disk,
        }),
        priority: 0,
        scheduling: Some(policy),
        env: r.env,
        lifecycle: Some(pb::LifecyclePolicy {
            idle_timeout_seconds: r.idle as u64,
            restart,
        }),
        snapshot_id: snapshot,
        sandbox: Some(sandbox.into()),
    })
}

fn validate_ports(ports: &[String]) -> Result<Vec<u16>, Status> {
    let mut unique = std::collections::BTreeSet::new();
    for value in ports {
        let port = value
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| invalid("invalid forwarded port"))?;
        if !unique.insert(port) {
            return Err(invalid("duplicate forwarded port"));
        }
    }
    Ok(unique.into_iter().collect())
}
fn affinities(
    input: Vec<Affinity>,
    labels: HashMap<String, String>,
) -> Result<pb::SchedulingPolicy, Status> {
    let mut p = pb::SchedulingPolicy {
        labels,
        ..Default::default()
    };
    if input.len() > 256 {
        return Err(invalid("too many affinity alternatives"));
    }
    for a in input {
        if !(0..=1).contains(&a.kind)
            || !(0..=3).contains(&a.affinity)
            || !(0..=1000).contains(&a.weight)
            || a.label_ops.is_empty()
            || a.label_ops.len() > 64
        {
            return Err(invalid("invalid affinity"));
        }
        if a.preferred_anti_other_labels {
            return Err(Status::unimplemented(
                "preferredAntiOtherLabels is not supported",
            ));
        }
        let mut requirements = Vec::new();
        for op in a.label_ops {
            if op.label_key.trim().is_empty()
                || !(0..=3).contains(&op.r#type)
                || (op.r#type < 2 && op.label_values.is_empty())
            {
                return Err(invalid("invalid affinity expression"));
            }
            let kind = match op.r#type {
                0 => pb::SelectorOp::In,
                1 => pb::SelectorOp::NotIn,
                2 => pb::SelectorOp::Exists,
                _ => pb::SelectorOp::DoesNotExist,
            };
            if op.r#type == 1 {
                requirements.push(pb::LabelRequirement {
                    key: op.label_key.clone(),
                    op: pb::SelectorOp::Exists as i32,
                    values: vec![],
                });
            }
            requirements.push(pb::LabelRequirement {
                key: op.label_key,
                op: kind as i32,
                values: op.label_values,
            });
        }
        let target = if a.kind == 0 {
            pb::PlacementTarget::Node
        } else {
            pb::PlacementTarget::Capsule
        } as i32;
        let required = a.affinity >= 2;
        let anti = a.affinity % 2 == 1;
        let index = p
            .placement_groups
            .iter()
            .position(|g| g.target == target && g.required == required && g.anti == anti);
        let index = if let Some(i) = index {
            i
        } else {
            p.placement_groups.push(pb::PlacementGroup {
                target,
                required,
                anti,
                ordered: a.preferred_priority,
                terms: vec![],
            });
            p.placement_groups.len() - 1
        };
        let group = &mut p.placement_groups[index];
        if group.ordered != a.preferred_priority {
            return Err(invalid("inconsistent preference ordering"));
        }
        group.terms.push(pb::WeightedSelector {
            weight: a.weight.max(1) as u32,
            selector: Some(pb::LabelSelector {
                expressions: requirements,
                ..Default::default()
            }),
        });
    }
    Ok(p)
}

/// Compatibility budget validation. Scheduling itself remains memory-only.
pub fn resolve_create_timeout(create: i64, schedule: i64, init: i64) -> Result<u64, Status> {
    if create < 0 || schedule < 0 || init < 0 {
        return Err(invalid("timeouts cannot be negative"));
    }
    let reserve = (if init == 0 { 30 } else { init })
        .checked_add(30)
        .ok_or_else(|| invalid("timeout overflow"))?;
    let add = |s: i64| {
        s.checked_add(reserve)
            .ok_or_else(|| invalid("timeout overflow"))
    };
    let result = match (create, schedule) {
        (0, 0) => std::env::var("ADX_SANDBOX_CREATE_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(90)
            .max(add(30)?),
        (0, s) => add(s)?,
        (c, 0) if c > 30 => add(c - 30)?,
        (c, s) if s > 0 && c >= s && c - s >= 30 => c.max(add(s)?),
        _ => {
            return Err(invalid(
                "create budget must reserve at least 30 seconds beyond scheduling",
            ))
        }
    };
    Ok(result as u64)
}
pub fn create_timeout(body: &Value) -> Result<u64, Status> {
    Ok(create_timeouts(body)?.create_seconds)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateTimeouts {
    pub create_seconds: u64,
    pub schedule_seconds: u64,
}

pub fn create_timeouts(body: &Value) -> Result<CreateTimeouts, Status> {
    let r: Create =
        serde_json::from_value(body.clone()).map_err(|_| invalid("invalid create request"))?;
    let create_seconds =
        resolve_create_timeout(r.create_timeout, r.schedule_timeout, r.init_timeout)?;
    let schedule_seconds = if r.schedule_timeout == 0 {
        30
    } else {
        u64::try_from(r.schedule_timeout).map_err(|_| invalid("invalid schedule timeout"))?
    };
    Ok(CreateTimeouts {
        create_seconds,
        schedule_seconds,
    })
}
