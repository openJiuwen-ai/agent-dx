//! Conservative evidence for default-rootfs runsc recovery. Backend paths are
//! deployment-owned declarations; their actual ELF bytes are measured here.
//! runsc validates checkpoint CPU requirements when restoring; host release strings
//! and CPU model equality cannot establish runtime compatibility.
use crate::RuntimeRootfsIdentity;
use adx_core::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

pub(super) fn profiles(
    rootfs: Option<&RuntimeRootfsIdentity>,
    binaries: &BTreeMap<String, PathBuf>,
) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
    let mut profiles = BTreeMap::new();
    for (class, path) in binaries {
        if class != "runsc" || !path.is_absolute() {
            return Err(Error::Invalid(
                "checkpoint runtime binaries require an absolute runsc backend path".into(),
            ));
        }
    }
    let Some(rootfs) = rootfs.filter(|root| Path::new(&root.path).is_absolute()) else {
        return Ok(profiles);
    };
    let release = release_identity(&rootfs.path)?;
    for (class, path) in binaries {
        let mut profile = BTreeMap::from([
            (
                format!("ADX_RECOVERY_RUNTIME_{class}"),
                binary_digest(path, std::env::consts::ARCH)?,
            ),
            ("ADX_RECOVERY_ROOTFS_SHA256".into(), rootfs.sha256.clone()),
            ("ADX_RECOVERY_ROOTFS_PATH".into(), rootfs.path.clone()),
        ]);
        if let Some(release) = &release {
            profile.insert("ADX_RECOVERY_RELEASE".into(), release.clone());
        }
        profiles.insert(class.clone(), profile);
    }
    Ok(profiles)
}

/// ADX release identity that produced the runtime rootfs.
///
/// The packaged runtime rootfs lives in `<package>/runtime/`, so the release
/// manifest is two levels above it. A missing manifest leaves the release
/// unqualified; a manifest that exists but cannot be parsed fails closed.
fn release_identity(rootfs_path: &str) -> Result<Option<String>> {
    let Some(package) = Path::new(rootfs_path).parent().and_then(Path::parent) else {
        return Ok(None);
    };
    let manifest = package.join("manifest.json");
    if !manifest.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&manifest).map_err(super::unavailable)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Invalid("invalid ADX release manifest".into()))?;
    let commit = value
        .get("commit")
        .and_then(serde_json::Value::as_str)
        .filter(|commit| !commit.is_empty())
        .ok_or_else(|| Error::Invalid("ADX release manifest requires a commit".into()))?;
    Ok(Some(format!(
        "{commit}@sha256:{:x}",
        Sha256::digest(&bytes)
    )))
}

fn binary_digest(path: &Path, arch: &str) -> Result<String> {
    use std::os::unix::fs::PermissionsExt;
    let mut file = std::fs::File::open(path).map_err(super::unavailable)?;
    let before = file.metadata().map_err(super::unavailable)?;
    if !before.is_file() || before.permissions().mode() & 0o111 == 0 {
        return Err(Error::Invalid(
            "checkpoint runtime binary must be an executable regular ELF file".into(),
        ));
    }
    let mut header = [0; 20];
    file.read_exact(&mut header).map_err(super::unavailable)?;
    let machine = match arch {
        "x86_64" => 62,
        "aarch64" => 183,
        _ => {
            return Err(Error::Invalid(
                "unsupported checkpoint runtime architecture".into(),
            ))
        }
    };
    if &header[..6] != b"\x7fELF\x02\x01" || u16::from_le_bytes([header[18], header[19]]) != machine
    {
        return Err(Error::Invalid(
            "checkpoint runtime ELF architecture mismatch".into(),
        ));
    }
    let mut digest = Sha256::new();
    digest.update(header);
    let mut buffer = [0; 65536];
    loop {
        let count = file.read(&mut buffer).map_err(super::unavailable)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let after = file.metadata().map_err(super::unavailable)?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return Err(Error::Unavailable(
            "checkpoint runtime binary changed while hashing".into(),
        ));
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn driver_rejects_rootfs_bytes_changed_after_profile_publication() {
        use super::super::{rootfs_identity, Config, Sandboxd};
        use crate::RuntimeDriver;
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root.img");
        let runsc = directory.path().join("runsc");
        std::fs::write(&root, b"first-rootfs").unwrap();
        let mut binary = vec![0; 64];
        binary[..6].copy_from_slice(b"\x7fELF\x02\x01");
        let machine = if cfg!(target_arch = "x86_64") {
            62u16
        } else {
            183u16
        };
        binary[18..20].copy_from_slice(&machine.to_le_bytes());
        std::fs::write(&runsc, binary).unwrap();
        std::fs::set_permissions(&runsc, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = Config {
            runtime_profile: Some(serde_json::from_value(serde_json::json!({
                "rootfs":{"runtime_class":"runsc", "type":"local", "path":root, "readonly":true},
                "bootstrap":{"type":"erofs", "root":root, "target":"/__adx",
                    "entrypoint":["/__adx/usr/local/bin/adx-execd"]}
            })).unwrap()),
            checkpoint_runtime_binaries: BTreeMap::from([("runsc".into(), runsc)]),
            ..Config::default()
        };
        let rootfs_identity = rootfs_identity(config.runtime_profile.as_ref()).unwrap();
        let published = profiles(
            rootfs_identity.as_ref(),
            &config.checkpoint_runtime_binaries,
        )
        .unwrap();
        let expected = published
            .get("runsc")
            .expect("runtime and rootfs must provide recovery evidence")
            .clone();
        assert_eq!(
            expected.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "ADX_RECOVERY_ROOTFS_PATH",
                "ADX_RECOVERY_ROOTFS_SHA256",
                "ADX_RECOVERY_RUNTIME_runsc",
            ]
        );
        let driver = Sandboxd {
            client: super::super::proto::sandbox_service_client::SandboxServiceClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            config,
            rootfs_identity,
            recovery_profiles: published,
            starts: Default::default(),
            backend_ids: Default::default(),
            overlaybd_connector: None,
        };
        let spec = serde_json::from_value(serde_json::json!({
            "id":"environment", "tenant_id":"tenant", "runtime_class":"runsc", "image":"",
            "priority":0, "runtime_profile":driver.config.runtime_profile,
            "resources":{"cpu_millis":1000, "memory_bytes":1024, "disk_bytes":0}
        }))
        .unwrap();
        assert_eq!(driver.recovery_profile(&spec).unwrap(), expected);
        std::fs::write(&root, b"other-rootfs").unwrap();
        assert!(matches!(
            driver.recovery_profile(&spec),
            Err(Error::Unavailable(_))
        ));
    }

    #[test]
    fn runtime_profile_uses_actual_executable_bytes_and_rejects_unknown_inputs() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runsc");
        let mut binary = vec![0; 64];
        binary[..6].copy_from_slice(b"\x7fELF\x02\x01");
        binary[18..20].copy_from_slice(&62u16.to_le_bytes());
        std::fs::write(&path, &binary).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let first = binary_digest(&path, "x86_64").unwrap();
        binary[63] = 1;
        std::fs::write(&path, binary).unwrap();
        assert_ne!(first, binary_digest(&path, "x86_64").unwrap());
        assert!(binary_digest(&path, "aarch64").is_err());
        assert!(binary_digest(&directory.path().join("missing"), "x86_64").is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(binary_digest(&path, "x86_64").is_err());
    }

    #[test]
    fn release_identity_requires_a_parsable_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let package = directory.path().join("release");
        std::fs::create_dir_all(package.join("runtime")).unwrap();
        let rootfs = package.join("runtime").join("adx-runtime-rootfs.img");
        std::fs::write(&rootfs, b"image").unwrap();
        let manifest = package.join("manifest.json");
        let path = rootfs.to_str().unwrap();

        assert_eq!(release_identity(path).unwrap(), None);
        std::fs::write(&manifest, br#"{"commit":"abc123"}"#).unwrap();
        let identity = release_identity(path).unwrap().unwrap();
        assert!(identity.starts_with("abc123@sha256:"));
        std::fs::write(&manifest, b"not json").unwrap();
        assert!(release_identity(path).is_err());
        std::fs::write(&manifest, br#"{"commit":""}"#).unwrap();
        assert!(release_identity(path).is_err());
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn profile_includes_release_identity_from_package_manifest() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let package = directory.path().join("release");
        std::fs::create_dir_all(package.join("runtime")).unwrap();
        let rootfs = package.join("runtime").join("adx-runtime-rootfs.img");
        std::fs::write(&rootfs, b"rootfs").unwrap();
        std::fs::write(package.join("manifest.json"), br#"{"commit":"abc123"}"#).unwrap();
        let runsc = package.join("runsc");
        let mut binary = vec![0; 64];
        binary[..6].copy_from_slice(b"\x7fELF\x02\x01");
        let machine = if cfg!(target_arch = "x86_64") {
            62u16
        } else {
            183u16
        };
        binary[18..20].copy_from_slice(&machine.to_le_bytes());
        std::fs::write(&runsc, binary).unwrap();
        std::fs::set_permissions(&runsc, std::fs::Permissions::from_mode(0o755)).unwrap();

        let identity = RuntimeRootfsIdentity {
            path: rootfs.to_str().unwrap().to_owned(),
            sha256: "rootfs-sha".into(),
        };
        let published = profiles(
            Some(&identity),
            &BTreeMap::from([("runsc".to_owned(), runsc)]),
        )
        .unwrap();
        let profile = &published["runsc"];
        assert!(profile["ADX_RECOVERY_RELEASE"].starts_with("abc123@sha256:"));
        assert_eq!(profile["ADX_RECOVERY_ROOTFS_SHA256"], "rootfs-sha");
    }
}
