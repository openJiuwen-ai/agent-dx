//! Private authenticated node-group preparation used by supervisor startup.
use crate::{
    config::{merge_value, Deployment, Process, Role, ServiceOverride},
    rollout::{artifact, hex, Mode, RemoteArchive},
    Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

const MAX_INPUT: u64 = 4 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Declaration {
    schema: u32,
    mode: Mode,
    components: Components,
    ready: Ready,
    #[serde(default)]
    upgrade_components: Vec<Role>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    components: Components,
}

// Named fields reject missing, duplicate and additional roles during deserialization.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Components {
    adxlet: Component,
    relay: Component,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Component {
    version: String,
    release_id: String,
    artifact: ArchiveDescriptor,
    config: ConfigInput,
}

/// External declaration wire form of a remote package: `{url, sha256}`.
#[derive(PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArchiveDescriptor {
    url: String,
    sha256: String,
}

impl ArchiveDescriptor {
    /// The only conversion to the internal `remote_archive.archive_sha256` form.
    fn remote(&self) -> RemoteArchive {
        RemoteArchive {
            url: self.url.clone(),
            archive_sha256: self.sha256.clone(),
        }
    }
}

use crate::config::ConfigInput;

pub(crate) struct PreparedComponent {
    pub(crate) role: Role,
    pub(crate) version: String,
    pub(crate) release_id: String,
    pub(crate) binary_sha256: String,
    pub(crate) raw_config_sha256: String,
    pub(crate) effective_config_sha256: String,
}

pub(crate) struct PreparedGroup {
    pub(crate) mode: Mode,
    pub(crate) processes: Vec<Process>,
    pub(crate) components: Vec<PreparedComponent>,
}

pub(crate) struct Prepared {
    pub(crate) target: PreparedGroup,
    pub(crate) ready: PreparedGroup,
}

fn budget(deadline: Instant, stopped: &AtomicBool) -> Result<()> {
    if stopped.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Err("external preparation stopped or deadline exceeded".into());
    }
    Ok(())
}

fn read_pinned(file: File, deadline: Instant, stopped: &AtomicBool) -> Result<Vec<u8>> {
    if !file.metadata()?.is_file() {
        return Err("external input must be regular".into());
    }
    let mut file = file.take(MAX_INPUT + 1);
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        budget(deadline, stopped)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    if bytes.len() > MAX_INPUT as usize {
        return Err("external input exceeds 4 MiB".into());
    }
    Ok(bytes)
}

fn open_input(path: &Path) -> Result<File> {
    // Explicit projected paths may traverse ..data symlinks. fstat and all reads
    // use this one descriptor, never a second resolution of the mount path.
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?)
}

pub(crate) fn config_snapshot(
    input: &ConfigInput,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<ServiceOverride> {
    input.validate()?;
    let bytes = read_pinned(open_input(&input.mount_path)?, deadline, stopped)?;
    if format!("{:x}", Sha256::digest(&bytes)) != input.sha256 {
        return Err("external config digest mismatch".into());
    }
    let patch: ServiceOverride =
        serde_json::from_slice(&bytes).map_err(|_| "invalid external ServiceOverride JSON")?;
    if patch
        .env
        .keys()
        .any(|key| key == "ADX_POD_UID" || key.starts_with("ADX_SUPERVISOR_"))
    {
        return Err("external override contains parent-owned environment keys".into());
    }
    Ok(patch)
}

fn validate_components(components: &Components, mode: Mode) -> Result<()> {
    for component in [&components.adxlet, &components.relay] {
        if !hex(&component.version, 40) || component.release_id.trim().is_empty() {
            return Err("external version or release label is invalid".into());
        }
        component.artifact.remote().validate()?;
        component.config.validate()?;
    }
    if mode == Mode::Embedded
        && (components.adxlet.version != components.relay.version
            || components.adxlet.release_id != components.relay.release_id
            || components.adxlet.artifact != components.relay.artifact)
    {
        return Err("embedded external components must select the same full package".into());
    }
    Ok(())
}

impl Declaration {
    fn validate(&self) -> Result<()> {
        if self.schema != 1 {
            return Err("unsupported external declaration schema".into());
        }
        if self.upgrade_components.len() > 1
            || self
                .upgrade_components
                .iter()
                .any(|role| !matches!(role, Role::Adxlet | Role::Relay))
            || (self.mode == Mode::Embedded && !self.upgrade_components.is_empty())
        {
            return Err("invalid external upgrade components".into());
        }
        validate_components(&self.components, self.mode)?;
        validate_components(&self.ready.components, self.mode)
    }
}

/// Reads one pinned declaration only; never acquires component configuration or packages.
pub(crate) fn read_declaration(
    path: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<(Declaration, String)> {
    budget(deadline, stopped)?;
    if !path.is_absolute() {
        return Err("external declaration path must be absolute".into());
    }
    let bytes = read_pinned(open_input(path)?, deadline, stopped)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid external declaration UTF-8")?;
    let declaration: Declaration =
        serde_saphyr::from_str(text).map_err(|_| "invalid external declaration YAML")?;
    declaration.validate()?;
    budget(deadline, stopped)?;
    Ok((declaration, format!("{:x}", Sha256::digest(&bytes))))
}

fn apply_embedded(
    owner: &mut crate::config::Service,
    adxlet: ServiceOverride,
    relay: ServiceOverride,
) -> Result<()> {
    if adxlet
        .env
        .iter()
        .any(|(key, value)| relay.env.get(key).is_some_and(|other| other != value))
    {
        return Err("embedded Relay and Adxlet environment conflict".into());
    }
    apply(owner, adxlet)?;
    owner.env.extend(relay.env);
    Ok(())
}

fn apply(service: &mut crate::config::Service, patch: ServiceOverride) -> Result<()> {
    if let Some(config) = patch.config {
        if !config.is_object() {
            return Err("external config override must be an object".into());
        }
        merge_value(&mut service.config, config);
    }
    service.env.extend(patch.env);
    Ok(())
}

fn relay_payload(patch: &ServiceOverride) -> Result<()> {
    if patch
        .config
        .as_ref()
        .is_some_and(|value| !value.as_object().is_some_and(serde_json::Map::is_empty))
    {
        return Err("Relay external override accepts env only".into());
    }
    Ok(())
}

fn rendered_config(directory: &Path, process: &Process) -> Result<serde_json::Value> {
    serde_json::from_slice(&fs::read(directory.join(format!("{}.json", process.id)))?)
        .map_err(|_| "invalid private rendered config".into())
}

pub(crate) fn protected_boundary(
    base: &serde_json::Value,
    actual: &serde_json::Value,
    baseline: &Process,
    process: &Process,
) -> Result<()> {
    let proxy_mode = |value: &serde_json::Value| {
        value
            .get("proxy_mode")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("embedded")
            .to_owned()
    };
    if proxy_mode(base) != proxy_mode(actual) {
        return Err("external override changes proxy mode".into());
    }
    for key in [
        "node_id",
        "listen",
        "advertised_address",
        "proxy_address",
        "admin_socket",
        "proxy_socket",
        "sandboxd_socket",
        "storage_mounts",
        "execd_command",
        "execd_env",
        "metrics_listen",
        "jiuwenbox",
        "runtime_logs",
        "runtime_control",
        "runtime_profile",
        "checkpoint_dir",
        "checkpoint_storage",
        "degradation_journal",
        "discovery",
        "tls",
        "execd_port",
        "checkpoint_runtime_binaries",
    ] {
        if base.get(key) != actual.get(key) {
            return Err("external override changes protected node boundary".into());
        }
    }
    let controlled = |env: &BTreeMap<String, String>| {
        env.iter()
            .filter(|(key, _)| {
                matches!(
                    key.as_str(),
                    "ADX_NODE_ID"
                        | "ADX_POD_UID"
                        | "ADX_DATA_PLANE_RELAY_BIND"
                        | "ADX_DATA_PLANE_RELAY_HEALTH_BIND"
                        | "ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS"
                        | "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR"
                        | "ADX_DATA_PLANE_RELAY_ROUTE_SOCKET"
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    if baseline.admin_socket != process.admin_socket
        || controlled(&baseline.env) != controlled(&process.env)
    {
        return Err("external override changes protected control environment".into());
    }
    Ok(())
}

fn prepare_group(
    deployment: &Deployment,
    components: Components,
    mode: Mode,
    directory: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
    runtime: &tokio::runtime::Handle,
) -> Result<PreparedGroup> {
    budget(deadline, stopped)?;
    fs::DirBuilder::new().mode(0o700).create(directory)?;
    let mut candidate = deployment.clone();
    // Render only the owned node group, avoiding unrelated services and their files.
    candidate
        .services
        .retain(|service| matches!(service.role, Role::Adxlet | Role::Relay));
    if candidate
        .services
        .iter()
        .filter(|service| service.role == Role::Adxlet)
        .count()
        != 1
        || candidate
            .services
            .iter()
            .filter(|service| service.role == Role::Relay)
            .count()
            != usize::from(mode == Mode::Standalone)
    {
        return Err("external preparation requires one complete local node group".into());
    }
    let owner = candidate
        .services
        .iter()
        .find(|service| service.role == Role::Adxlet)
        .ok_or("missing node owner")?;
    if candidate.rollout_mode(owner) != mode {
        return Err("external mode differs from deployment".into());
    }
    let baseline_directory = directory.join("baseline");
    let baseline = candidate.render(&baseline_directory)?;
    let adxlet_patch = config_snapshot(&components.adxlet.config, deadline, stopped)?;
    let relay_patch = config_snapshot(&components.relay.config, deadline, stopped)?;
    relay_payload(&relay_patch)?;
    let owner = candidate
        .services
        .iter_mut()
        .find(|service| service.role == Role::Adxlet)
        .ok_or("missing node owner")?;
    if mode == Mode::Embedded {
        apply_embedded(owner, adxlet_patch, relay_patch)?;
    } else {
        apply(owner, adxlet_patch)?;
        let relay = candidate
            .services
            .iter_mut()
            .find(|service| service.role == Role::Relay)
            .ok_or("missing Relay")?;
        apply(relay, relay_patch)?;
    }
    let rendered = directory.join("rendered");
    let mut processes = candidate.render(&rendered)?;
    for process in &processes {
        let base = baseline
            .iter()
            .find(|base| base.role == process.role)
            .ok_or("missing baseline process")?;
        protected_boundary(
            &rendered_config(&baseline_directory, base)?,
            &rendered_config(&rendered, process)?,
            base,
            process,
        )?;
    }
    let mut identities = Vec::new();
    let mut owned_binary_sha256 = None;
    for (role, component) in [
        (Role::Adxlet, components.adxlet),
        (Role::Relay, components.relay),
    ] {
        budget(deadline, stopped)?;
        let acquisition = directory.join(role.name());
        let binary = if mode == Mode::Embedded && role == Role::Relay {
            artifact::PreparedBinary {
                commit: component.version.clone(),
                sha256: owned_binary_sha256
                    .clone()
                    .ok_or("missing owned Adxlet digest")?,
            }
        } else {
            fs::DirBuilder::new().mode(0o700).create(&acquisition)?;
            artifact::remote::prepare_binary(
                &acquisition,
                &component.artifact.remote(),
                artifact::ExpectedBinary {
                    role,
                    commit: &component.version,
                    sha256: None,
                },
                deadline,
                stopped,
                runtime,
            )?
        };
        if role == Role::Adxlet {
            owned_binary_sha256 = Some(binary.sha256.clone());
        }
        let process = processes
            .iter_mut()
            .find(|process| {
                process.role == role || (mode == Mode::Embedded && process.role == Role::Adxlet)
            })
            .ok_or("missing prepared process")?;
        if role == Role::Adxlet || mode == Mode::Standalone {
            process.binary = acquisition.join("executable");
        }
        process.sources.insert(
            role,
            crate::config::ProcessSource {
                version: Some(binary.commit.clone()),
                archive_sha256: Some(component.artifact.sha256.clone()),
                raw_config_sha256: Some(component.config.sha256.clone()),
            },
        );
        identities.push(PreparedComponent {
            role,
            version: binary.commit,
            release_id: component.release_id,
            binary_sha256: binary.sha256,
            raw_config_sha256: component.config.sha256,
            effective_config_sha256: process.configuration_sha256.clone(),
        });
    }
    Ok(PreparedGroup {
        mode,
        processes,
        components: identities,
    })
}

/// Called by the startup worker, not from the supervisor actor or async executor.
/// All acquisitions share this first-preparation deadline and the caller's stop flag.
pub(crate) fn prepare(
    path: &Path,
    deployment: &Deployment,
    directory: &Path,
    stopped: &AtomicBool,
    runtime: &tokio::runtime::Handle,
) -> Result<Prepared> {
    let deadline = Instant::now() + Duration::from_secs(900);
    let (declaration, _) = read_declaration(path, deadline, stopped)?;
    fs::DirBuilder::new().mode(0o700).create(directory)?;
    let target = prepare_group(
        deployment,
        declaration.components,
        declaration.mode,
        &directory.join("target"),
        deadline,
        stopped,
        runtime,
    )?;
    let ready = prepare_group(
        deployment,
        declaration.ready.components,
        declaration.mode,
        &directory.join("ready"),
        deadline,
        stopped,
        runtime,
    )?;
    budget(deadline, stopped)?;
    Ok(Prepared { target, ready })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_require_complete_roles_and_embedded_same_package() {
        let component = serde_json::json!({"version":"a".repeat(40), "release_id":"release-label",
            "artifact":{"url":"https://example.invalid/package.tar.gz", "sha256":"b".repeat(64)},
            "config":{"mount_path":"/mounted/config", "sha256":"c".repeat(64)}});
        assert!(
            serde_json::from_value::<Components>(serde_json::json!({"adxlet":component})).is_err()
        );
        let mut components: Components =
            serde_json::from_value(serde_json::json!({"adxlet":component,"relay":component}))
                .unwrap();
        validate_components(&components, Mode::Embedded).unwrap();
        components.relay.release_id = "different-label".into();
        assert!(validate_components(&components, Mode::Embedded).is_err());
        validate_components(&components, Mode::Standalone).unwrap();
        let groups = serde_json::json!({"adxlet":component,"relay":component});
        let mut declaration: Declaration = serde_json::from_value(serde_json::json!({
            "schema":1,"mode":"standalone","components":groups,
            "ready":{"components":groups},"upgrade_components":["adxlet"]
        }))
        .unwrap();
        declaration.validate().unwrap();
        let official = serde_json::to_value(&declaration).unwrap();
        assert_eq!(
            official["components"]["adxlet"]["artifact"],
            component["artifact"]
        );
        assert!(!serde_json::to_string(&official)
            .unwrap()
            .contains("archive_sha256"));
        serde_json::from_value::<Declaration>(official)
            .unwrap()
            .validate()
            .unwrap();
        declaration.upgrade_components.push(Role::Relay);
        assert!(declaration.validate().is_err());
        declaration.upgrade_components = vec![Role::Relay];
        declaration.mode = Mode::Embedded;
        assert!(declaration.validate().is_err());
        declaration.upgrade_components.clear();
        declaration.validate().unwrap();
        declaration.ready.components.relay.config.mount_path = "relative".into();
        assert!(declaration.validate().is_err());
        let mut invalid = component.clone();
        invalid["artifact"]
            .as_object_mut()
            .unwrap()
            .remove("sha256");
        invalid["artifact"]["archive_sha256"] = serde_json::json!("b".repeat(64));
        assert!(serde_json::from_value::<Component>(invalid).is_err());
        let mut unknown = component.clone();
        unknown["artifact"]["extra"] = serde_json::json!("x");
        assert!(serde_json::from_value::<Component>(unknown).is_err());
        let parsed: Component = serde_json::from_value(component.clone()).unwrap();
        let remote = parsed.artifact.remote();
        assert_eq!(remote.url, "https://example.invalid/package.tar.gz");
        assert_eq!(remote.archive_sha256, "b".repeat(64));
    }

    #[test]
    fn protected_runtime_and_relay_environment_cannot_change() {
        let base = Process {
            sources: BTreeMap::new(),
            configuration_sha256: "e".repeat(64),
            id: "node".into(),
            role: Role::Adxlet,
            binary: "/bin/adxlet".into(),
            args: vec![],
            env: BTreeMap::from([("ADX_DATA_PLANE_RELAY_BIND".into(), "127.0.0.1:1".into())]),
            admin_socket: Some("/run/admin.sock".into()),
            health_url: None,
        };
        let config =
            serde_json::json!({"node_id":"node", "runtime_control":{"listen":"127.0.0.1:2"}});
        let mut changed = config.clone();
        changed["runtime_control"]["listen"] = serde_json::json!("0.0.0.0:3");
        assert!(protected_boundary(&config, &changed, &base, &base).is_err());
        for (key, original, replacement) in [
            (
                "storage_mounts",
                serde_json::json!({"volume":{"backend_id":"old","root":"/old"}}),
                serde_json::json!({"volume":{"backend_id":"new","root":"/new"}}),
            ),
            (
                "execd_command",
                serde_json::json!(["/old/execd"]),
                serde_json::json!(["/new/execd"]),
            ),
            (
                "execd_env",
                serde_json::json!({"BACKEND":"old"}),
                serde_json::json!({"BACKEND":"new"}),
            ),
            (
                "metrics_listen",
                serde_json::json!("127.0.0.1:1"),
                serde_json::json!("0.0.0.0:2"),
            ),
        ] {
            let mut original_config = config.clone();
            original_config[key] = original;
            let mut replacement_config = original_config.clone();
            replacement_config[key] = replacement;
            assert!(
                protected_boundary(&original_config, &replacement_config, &base, &base).is_err()
            );
        }
        let mut process = base.clone();
        process
            .env
            .insert("ADX_DATA_PLANE_RELAY_BIND".into(), "0.0.0.0:4".into());
        assert!(protected_boundary(&config, &config, &base, &process).is_err());
    }

    #[test]
    fn embedded_relay_overrides_baseline_but_not_conflicting_explicit_patch() {
        let mut owner = crate::config::Service {
            id: "node".into(),
            role: Role::Adxlet,
            config: serde_json::json!({}),
            env: BTreeMap::from([("LIMIT".into(), "old".into())]),
        };
        let patch = |value| serde_json::from_value::<ServiceOverride>(value).unwrap();
        apply_embedded(
            &mut owner,
            patch(serde_json::json!({})),
            patch(serde_json::json!({"env":{"LIMIT":"new"}})),
        )
        .unwrap();
        assert_eq!(owner.env.get("LIMIT").map(String::as_str), Some("new"));
        assert!(apply_embedded(
            &mut owner,
            patch(serde_json::json!({"env":{"LIMIT":"adxlet"}})),
            patch(serde_json::json!({"env":{"LIMIT":"relay"}}))
        )
        .is_err());
        assert_eq!(owner.env.get("LIMIT").map(String::as_str), Some("new"));
    }

    #[test]
    fn projected_input_remains_pinned_after_symlink_replacement() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("old"), b"old").unwrap();
        fs::write(root.path().join("new"), b"new").unwrap();
        let path = root.path().join("config");
        std::os::unix::fs::symlink("old", &path).unwrap();
        let pinned = open_input(&path).unwrap();
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("new", &path).unwrap();
        assert_eq!(
            read_pinned(
                pinned,
                Instant::now() + Duration::from_secs(1),
                &AtomicBool::new(false)
            )
            .unwrap(),
            b"old"
        );
    }

    #[test]
    fn config_requires_authenticated_bytes_and_relay_payload_is_not_ignored() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config");
        let bytes = br#"{"env":{"EXAMPLE":"effective"}}"#;
        fs::write(&path, bytes).unwrap();
        let mut input = ConfigInput {
            mount_path: path,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        };
        let patch = config_snapshot(
            &input,
            Instant::now() + Duration::from_secs(1),
            &AtomicBool::new(false),
        )
        .unwrap();
        relay_payload(&patch).unwrap();
        let mut service = crate::config::Service {
            id: "relay".into(),
            role: Role::Relay,
            config: serde_json::Value::Null,
            env: BTreeMap::new(),
        };
        apply(&mut service, patch).unwrap();
        assert_eq!(
            service.env.get("EXAMPLE").map(String::as_str),
            Some("effective")
        );
        input.sha256 = "0".repeat(64);
        assert!(config_snapshot(
            &input,
            Instant::now() + Duration::from_secs(1),
            &AtomicBool::new(false)
        )
        .is_err());
        let invalid: ServiceOverride =
            serde_json::from_value(serde_json::json!({"config":{"ignored":true}})).unwrap();
        assert!(relay_payload(&invalid).is_err());
        for key in ["ADX_POD_UID", "ADX_SUPERVISOR_OWNER_SERVICE"] {
            let bytes = serde_json::to_vec(&serde_json::json!({"env":{(key):"forged"}})).unwrap();
            fs::write(&input.mount_path, &bytes).unwrap();
            input.sha256 = format!("{:x}", Sha256::digest(&bytes));
            assert!(config_snapshot(
                &input,
                Instant::now() + Duration::from_secs(1),
                &AtomicBool::new(false)
            )
            .is_err());
        }
    }
}
