//! Node-local handling of explicitly requested runsc file-handle semantics.
use super::{proto, unavailable};
use adx_core::{Error, Result};

const HANDLE_OPTION: &str = "disable_file_handle_sharing";

#[derive(Clone, Copy)]
// Live filesystem observation is Linux-only; other hosts still build the adapter.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum Filesystem {
    Remote,
    Other,
    Autofs,
}

pub(super) async fn normalize(request: &mut proto::StartRequest) -> Result<()> {
    if request.runtime != "runsc" || !request.mounts.iter().any(marked) {
        return Ok(());
    }
    let mut mounts = request.mounts.clone();
    request.mounts = tokio::task::spawn_blocking(move || {
        normalize_mounts(&mut mounts, filesystem)?;
        Ok::<_, Error>(mounts)
    })
    .await
    .map_err(unavailable)??;
    Ok(())
}

fn marked(mount: &proto::Mount) -> bool {
    mount.options.iter().any(|option| option == HANDLE_OPTION)
}

fn normalize_mounts(
    mounts: &mut [proto::Mount],
    mut observe: impl FnMut(&str) -> Result<Filesystem>,
) -> Result<()> {
    for mount in mounts.iter_mut().filter(|mount| marked(mount)) {
        let source = match (&*mount.r#type, &mount.source) {
            ("bind", Some(proto::mount::Source::HostPath(path))) => path,
            _ => {
                return Err(Error::Invalid(format!(
                    "{HANDLE_OPTION} requires a host bind mount for runsc"
                )))
            }
        };
        let filesystem = observe(source)?;
        if matches!(filesystem, Filesystem::Autofs) {
            return Err(Error::Unavailable(format!(
                "runsc mount source is still autofs after opening: {source}"
            )));
        }
        // TEMPORARY compatibility shim for akernel-dev/gvisor@49590af731d7:
        // AdjustMountOptions unconditionally appends this flag on NFS/FUSE,
        // while restore validation compares option multiplicity. Preserve the
        // caller's requested semantics with exactly one EFFECTIVE flag: let
        // runsc supply it remotely, and keep one copy on other filesystems.
        // Remove this statfs workaround once the packaged runsc makes its
        // automatic append idempotent. Never inject it into unmarked mounts,
        // modify the persisted Environment spec, or apply it to other runtimes.
        mount.options.retain(|option| option != HANDLE_OPTION);
        if matches!(filesystem, Filesystem::Other) {
            mount.options.push(HANDLE_OPTION.into());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn filesystem(source: &str) -> Result<Filesystem> {
    use std::{
        fs::OpenOptions,
        os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    };
    // Opening the source triggers autofs before observing the backing mount.
    // The node and sandboxd must share the same source-path mount view.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(source)
        .map_err(|error| {
            Error::Unavailable(format!("open runsc mount source {source}: {error}"))
        })?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: file owns a live descriptor and stat is writable, correctly sized
    // storage for fstatfs; its contents are read only after a successful call.
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(Error::Unavailable(format!(
            "fstatfs runsc mount source {source}: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: fstatfs returned success and initialized the statfs structure.
    let stat = unsafe { stat.assume_init() };
    Ok(match stat.f_type {
        libc::NFS_SUPER_MAGIC | libc::FUSE_SUPER_MAGIC => Filesystem::Remote,
        libc::AUTOFS_SUPER_MAGIC => Filesystem::Autofs,
        _ => Filesystem::Other,
    })
}

#[cfg(not(target_os = "linux"))]
fn filesystem(_source: &str) -> Result<Filesystem> {
    Err(Error::Unavailable(
        "explicit runsc handle-option normalization requires a Linux node".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount(options: &[&str]) -> proto::Mount {
        proto::Mount {
            r#type: "bind".into(),
            target: "/workspace".into(),
            source: Some(proto::mount::Source::HostPath("/users/alice".into())),
            options: options.iter().map(|option| (*option).into()).collect(),
        }
    }

    #[test]
    fn explicit_semantics_survive_local_and_remote_effective_options() {
        for filesystem in [Filesystem::Other, Filesystem::Remote] {
            let mut mounts = [mount(&["rw", HANDLE_OPTION, HANDLE_OPTION, "nosuid"])];
            normalize_mounts(&mut mounts, |_| Ok(filesystem)).unwrap();
            // Model the pinned runsc's subsequent NFS/FUSE append.
            let mut effective = mounts[0].options.clone();
            if matches!(filesystem, Filesystem::Remote) {
                effective.push(HANDLE_OPTION.into());
            }
            assert_eq!(
                effective
                    .iter()
                    .filter(|value| value.as_str() == HANDLE_OPTION)
                    .count(),
                1
            );
            assert!(effective.contains(&"rw".into()));
            assert!(effective.contains(&"nosuid".into()));
        }
    }

    #[test]
    fn unresolved_autofs_and_observation_errors_are_not_treated_as_local() {
        let mut mounts = [mount(&["rw", HANDLE_OPTION])];
        assert!(normalize_mounts(&mut mounts, |_| Ok(Filesystem::Autofs)).is_err());
        assert!(normalize_mounts(&mut mounts, |_| Err(Error::Unavailable(
            "statfs failed".into()
        )))
        .is_err());
    }

    #[tokio::test]
    async fn unmarked_sources_and_other_runtimes_are_not_observed() {
        let mut request = proto::StartRequest {
            runtime: "runsc".into(),
            mounts: vec![mount(&["rw"])],
            ..Default::default()
        };
        normalize(&mut request).await.unwrap();
        assert_eq!(request.mounts[0].options, ["rw"]);
        request.runtime = "runc".into();
        request.mounts[0].options.push(HANDLE_OPTION.into());
        normalize(&mut request).await.unwrap();
        assert_eq!(request.mounts[0].options, ["rw", HANDLE_OPTION]);
    }
}
