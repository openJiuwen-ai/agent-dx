//! Public JSON contract converted directly to typed Instance RPC payloads.
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
pub fn create_spec(body: Value, caller: &pb::CallerContext) -> Result<pb::InstanceSpec, Status> {
    create_spec_with_environment(body, caller, None)
}
pub fn create_spec_with_environment(
    body: Value,
    caller: &pb::CallerContext,
    environment: Option<&adx_core::environment::RuntimeEnvironment>,
) -> Result<pb::InstanceSpec, Status> {
    if let Some(e) = environment {
        e.validate().map_err(|e| invalid(&e.to_string()))?;
    }
    let mut r: Create =
        serde_json::from_value(body).map_err(|_| invalid("invalid create request"))?;
    if caller.tenant_id.is_empty() {
        return Err(Status::unauthenticated("verified identity required"));
    }
    let mut root = match r.rootfs {
        Value::Null => Rootfs::default(),
        Value::String(s) => {
            if s.trim().starts_with('{') {
                serde_json::from_str(&s).map_err(|_| invalid("invalid rootfs"))?
            } else {
                Rootfs {
                    imageurl: s,
                    ..Default::default()
                }
            }
        }
        v => serde_json::from_value(v).map_err(|_| invalid("invalid rootfs"))?,
    };
    if !r.runtime.is_empty() && !root.runtime.is_empty() && r.runtime != root.runtime {
        return Err(invalid("conflicting runtime selections"));
    }
    if root.runtime.is_empty() {
        root.runtime = r.runtime;
    }
    if root.imageurl.is_empty() {
        root.imageurl = r.image;
    }
    let snapshot = (!r.snapshot_id.trim().is_empty()).then(|| r.snapshot_id.trim().to_string());
    if snapshot.is_none() && environment.is_none() && root.imageurl.trim().is_empty() {
        return Err(invalid("image required"));
    }
    if snapshot.is_none() && root.runtime.is_empty() {
        root.runtime = environment
            .map(|e| e.rootfs.runtime.clone())
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
    if r.failover
        || r.inherit
        || !r.mounts.is_empty()
        || !r.extra_config.is_empty()
        || active(&r.network)
        || active(&r.data_plane)
        || !r.ports.is_empty()
        || (!root.r#type.is_empty() && root.r#type != "image")
    {
        return Err(Status::unimplemented(
            "requested create option is not supported",
        ));
    }
    if (r.cpu_limit != 0 && positive(r.cpu_limit, 1)? != cpu)
        || (r.mem_limit != 0 && positive(r.mem_limit, 1048576)? != memory)
        || (r.storage_limit_mb != 0 && positive(r.storage_limit_mb, 1048576)? != disk)
    {
        return Err(Status::unimplemented(
            "independent resource limits are not supported",
        ));
    }
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
    Ok(pb::InstanceSpec {
        runtime_environment: if snapshot.is_none() {
            environment.cloned().map(Into::into)
        } else {
            None
        },
        id: format!("{}-{}", r.namespace, r.name),
        tenant_id: caller.tenant_id.clone(),
        image: root.imageurl.trim().into(),
        runtime: root.runtime,
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
    })
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
            pb::PlacementTarget::Instance
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
    let r: Create =
        serde_json::from_value(body.clone()).map_err(|_| invalid("invalid create request"))?;
    resolve_create_timeout(r.create_timeout, r.schedule_timeout, r.init_timeout)
}
