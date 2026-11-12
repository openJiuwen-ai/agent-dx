//! Conservative evidence for default-rootfs runsc recovery. Backend paths are
//! deployment-owned declarations; their actual ELF bytes are measured here.
use crate::RuntimeRootfsIdentity;
use adx_core::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
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
    if binaries.is_empty() {
        return Ok(profiles);
    }
    let host = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .zip(std::fs::read_to_string("/proc/cpuinfo").ok())
        .and_then(|(kernel, cpu)| host_fingerprint(std::env::consts::ARCH, kernel.trim(), &cpu));
    let Some(host) = host else {
        return Ok(profiles);
    };
    for (class, path) in binaries {
        profiles.insert(
            class.clone(),
            BTreeMap::from([
                ("ADX_RECOVERY_HOST".into(), host.clone()),
                (
                    format!("ADX_RECOVERY_RUNTIME_{class}"),
                    binary_digest(path, std::env::consts::ARCH)?,
                ),
                ("ADX_RECOVERY_ROOTFS_SHA256".into(), rootfs.sha256.clone()),
                ("ADX_RECOVERY_ROOTFS_PATH".into(), rootfs.path.clone()),
            ]),
        );
    }
    Ok(profiles)
}

fn host_fingerprint(arch: &str, kernel: &str, cpu: &str) -> Option<String> {
    let required: &[&str] = match arch {
        "x86_64" => &["vendor_id", "cpu family", "model", "stepping", "flags"],
        "aarch64" => &[
            "CPU implementer",
            "CPU architecture",
            "CPU variant",
            "CPU part",
            "CPU revision",
            "Features",
        ],
        _ => return None,
    };
    if kernel.is_empty() {
        return None;
    }
    let mut processors = BTreeSet::new();
    for block in cpu.split("\n\n").filter(|b| !b.trim().is_empty()) {
        let values: BTreeMap<_, _> = block
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| {
                (
                    key.trim(),
                    value.split_whitespace().collect::<BTreeSet<_>>(),
                )
            })
            .collect();
        let mut processor = Vec::new();
        for key in required {
            let value = values.get(key).filter(|value| !value.is_empty())?;
            processor.push(format!(
                "{key}:{}",
                value.iter().copied().collect::<Vec<_>>().join(" ")
            ));
        }
        processors.insert(processor.join("\n"));
    }
    if processors.len() != 1 {
        return None;
    }
    Some(format!(
        "{:x}",
        Sha256::digest(format!(
            "{arch}\n{kernel}\n{}",
            processors.into_iter().collect::<Vec<_>>().join("\n\n")
        ))
    ))
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
            .expect("Linux host must provide recovery evidence")
            .clone();
        let driver = Sandboxd {
            client: super::super::proto::sandbox_service_client::SandboxServiceClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            config,
            rootfs_identity,
            recovery_profiles: published,
            starts: Default::default(),
            backend_ids: Default::default(),
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
    fn host_profile_requires_complete_stable_cpu_and_kernel_evidence() {
        let cpu =
            "vendor_id: GenuineIntel\ncpu family: 6\nmodel: 85\nstepping: 7\nflags: sse avx\n";
        let profile = host_fingerprint("x86_64", "6.1-test", cpu).unwrap();
        assert_eq!(
            Some(profile.clone()),
            host_fingerprint("x86_64", "6.1-test", &cpu.replace("sse avx", "avx sse"))
        );
        assert_ne!(
            Some(profile.clone()),
            host_fingerprint("x86_64", "6.2-test", cpu)
        );
        assert_ne!(
            Some(profile),
            host_fingerprint("x86_64", "6.1-test", &cpu.replace("avx", "avx2"))
        );
        assert!(host_fingerprint("x86_64", "6.1-test", "flags: avx").is_none());
        assert!(host_fingerprint(
            "x86_64",
            "6.1-test",
            &format!("{cpu}\n{}", cpu.replace("avx", "avx2"))
        )
        .is_none());
        assert!(host_fingerprint("x86_64", "", cpu).is_none());
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
}
