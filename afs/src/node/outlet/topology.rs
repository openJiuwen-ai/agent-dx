//! 只读挂载拓扑诊断。
//!
//! 纯解析：输入 `/proc/self/mountinfo` 的字节，输出一份判定结论。不做 IO、
//! 不做决策、不参与收敛（spec H3）——诊断只用于让"挂载配错了"这件事在日志
//! 里可见，而不是让进程据此改变行为。名字面在挂载缺失时仍然工作（发布 symlink
//! 不需要 target 存在），因此诊断绝不能反过来阻塞或回滚发布。

use std::path::Path;

/// One mount, reduced to the fields the diagnosis needs.
///
/// `mount_point` is decoded back to bytes (octal escapes resolved) so callers can
/// compare it against a path without re-encoding every candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mount {
    pub mount_point: Vec<u8>,
    pub fs_type: String,
    pub source: String,
}

/// Parse every line of a mountinfo buffer.
///
/// Lines that do not look like mountinfo are skipped rather than failing the
/// whole read: the file is a kernel interface that may grow fields, and a
/// diagnostic must not break on a shape it does not recognise.
pub fn parse(mountinfo: &[u8]) -> Vec<Mount> {
    mountinfo
        .split(|byte| *byte == b'\n')
        .filter_map(parse_line)
        .collect()
}

fn parse_line(line: &[u8]) -> Option<Mount> {
    let split = line.windows(3).position(|window| window == b" - ")?;
    let fields = &line[..split];
    let fs_fields = &line[split + 3..];
    let encoded_point = fields.split(|byte| *byte == b' ').nth(4)?;
    let mut fs_iter = fs_fields.split(|byte| *byte == b' ');
    let fs_type = fs_iter.next()?;
    let source = fs_iter.next()?;
    Some(Mount {
        mount_point: decode_octal(encoded_point),
        fs_type: String::from_utf8_lossy(fs_type).into_owned(),
        source: String::from_utf8_lossy(source).into_owned(),
    })
}

/// Resolve mountinfo's `\040`-style escapes back to bytes.
fn decode_octal(encoded: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index] == b'\\' && index + 3 < encoded.len() {
            let digits = &encoded[index + 1..index + 4];
            if digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
                let value = digits
                    .iter()
                    .fold(0u16, |acc, byte| acc * 8 + u16::from(byte - b'0'));
                out.push(value as u8);
                index += 4;
                continue;
            }
        }
        out.push(encoded[index]);
        index += 1;
    }
    out
}

/// A single topology finding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Finding {
    /// No mount under the expected prefix at all.
    PathMissing { mount_point: String },
    /// A mount exists but its type is not what the deployment contract expects.
    PathDrift {
        mount_point: String,
        fs_type: String,
        expected: &'static str,
    },
}

impl Finding {
    /// Stable label used as the log event suffix.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PathMissing { .. } => "path_missing",
            Self::PathDrift { .. } => "path_drift",
        }
    }

    #[must_use]
    pub fn mount_point(&self) -> &str {
        match self {
            Self::PathMissing { mount_point } | Self::PathDrift { mount_point, .. } => mount_point,
        }
    }
}

/// Diagnose the import topology for one node.
///
/// For each home under `<import_root>/` the contract expects
/// `<import_root>/<node>/users` to be a mount of its own, and to be either a
/// local filesystem (the home's own tree) or an `nfs*` mount (a remote home).
/// Anything else means the data path will fail at use time with an error that
/// points at the workspace rather than at the mount, which is why the drift is
/// reported here instead.
///
/// `homes` is the set of node ids whose directory exists under `import_root`.
/// The caller enumerates the directory: a home that has never been mounted is
/// absent from mountinfo but may still have an (empty) directory created by the
/// deployer, and that case is worth reporting too.
pub fn diagnose_imports(import_root: &Path, homes: &[String], mounts: &[Mount]) -> Vec<Finding> {
    let mut findings = Vec::new();
    for home in homes {
        let expected = import_root.join(home).join("users");
        let expected_bytes = expected.as_os_str().as_encoded_bytes();
        // The mount serving this path is the longest mount point that is a
        // prefix of it: nested mounts (a bind inside the tree) take precedence.
        let served = mounts
            .iter()
            .filter(|mount| expected_bytes.starts_with(&mount.mount_point))
            .max_by_key(|mount| mount.mount_point.len());
        let Some(mount) = served else {
            findings.push(Finding::PathMissing {
                mount_point: expected.to_string_lossy().into_owned(),
            });
            continue;
        };
        // Only a mount whose point *is* the users directory tells us about the
        // workspace layout. A parent mount (the import root itself) satisfies
        // the prefix test but says nothing about per-home mounting, so it
        // reports as missing rather than passing.
        if mount.mount_point != expected_bytes {
            findings.push(Finding::PathMissing {
                mount_point: expected.to_string_lossy().into_owned(),
            });
            continue;
        }
        let fs_type = mount.fs_type.as_str();
        let acceptable = fs_type.starts_with("nfs")
            || matches!(
                fs_type,
                "ext4" | "xfs" | "btrfs" | "zfs" | "virtiofs" | "fuse"
            );
        if !acceptable {
            findings.push(Finding::PathDrift {
                mount_point: expected.to_string_lossy().into_owned(),
                fs_type: mount.fs_type.clone(),
                expected: "local filesystem or nfs",
            });
        }
    }
    findings
}

/// Diagnose the public (name) layer.
///
/// Only the tmpfs requirement is checked at runtime. The startup gate already
/// refuses to begin without it; repeating the check here catches a mount that
/// disappeared or was replaced while the process was running, which otherwise
/// shows up only as failing renames.
pub fn diagnose_public(public_root: &Path, mounts: &[Mount]) -> Vec<Finding> {
    let expected = public_root.as_os_str().as_encoded_bytes();
    let served = mounts
        .iter()
        .filter(|mount| expected.starts_with(&mount.mount_point))
        .max_by_key(|mount| mount.mount_point.len());
    let Some(mount) = served else {
        return vec![Finding::PathMissing {
            mount_point: public_root.to_string_lossy().into_owned(),
        }];
    };
    if mount.mount_point != expected {
        // The directory is covered only by a parent mount: it is not its own
        // tmpfs, so names written there would persist across restarts.
        return vec![Finding::PathMissing {
            mount_point: public_root.to_string_lossy().into_owned(),
        }];
    }
    if mount.fs_type != "tmpfs" {
        return vec![Finding::PathDrift {
            mount_point: public_root.to_string_lossy().into_owned(),
            fs_type: mount.fs_type.clone(),
            expected: "tmpfs",
        }];
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(pairs: &[(&str, &str)]) -> Vec<Mount> {
        pairs
            .iter()
            .map(|(point, fs)| Mount {
                mount_point: point.as_bytes().to_vec(),
                fs_type: (*fs).to_owned(),
                source: "src".into(),
            })
            .collect()
    }

    #[test]
    fn octal_escapes_are_decoded() {
        let mounts = parse(b"36 1 0:32 / /mnt/ad\\040x rw - tmpfs tmpfs rw\n");
        assert_eq!(mounts[0].mount_point, b"/mnt/ad x");
        assert_eq!(mounts[0].fs_type, "tmpfs");
    }

    #[test]
    fn malformed_lines_are_skipped_not_fatal() {
        let mounts = parse(b"garbage\n36 1 0:32 / /a rw - tmpfs tmpfs rw\n\n");
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].mount_point, b"/a");
    }

    #[test]
    fn missing_home_mount_is_reported() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into()],
            &entries(&[("/imports", "nfs4")]),
        );
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind(), "path_missing");
        assert_eq!(findings[0].mount_point(), "/imports/node-a/users");
    }

    #[test]
    fn nfs_home_mount_passes() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into()],
            &entries(&[("/imports/node-a/users", "nfs4")]),
        );
        assert!(findings.is_empty());
    }

    /// 本机 home 用本地文件系统也合法。
    #[test]
    fn local_filesystem_home_mount_passes() {
        for fs in ["ext4", "xfs", "btrfs"] {
            let findings = diagnose_imports(
                Path::new("/imports"),
                &["node-a".into()],
                &entries(&[("/imports/node-a/users", fs)]),
            );
            assert!(findings.is_empty(), "{fs} should be accepted");
        }
    }

    #[test]
    fn unexpected_filesystem_is_drift() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into()],
            &entries(&[("/imports/node-a/users", "overlay")]),
        );
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind(), "path_drift");
        match &findings[0] {
            Finding::PathDrift { fs_type, .. } => assert_eq!(fs_type, "overlay"),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// 只用父挂载"碰巧"覆盖路径不算通过：那不是 per-home 挂载。
    #[test]
    fn parent_mount_does_not_satisfy_the_home_mount_requirement() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into()],
            &entries(&[("/imports", "ext4")]),
        );
        assert_eq!(findings[0].kind(), "path_missing");
    }

    #[test]
    fn multiple_homes_are_checked_independently() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into(), "node-b".into()],
            &entries(&[
                ("/imports/node-a/users", "nfs4"),
                ("/imports/node-b/users", "overlay"),
            ]),
        );
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].mount_point(), "/imports/node-b/users");
    }

    #[test]
    fn public_root_must_be_its_own_tmpfs_mount() {
        assert!(
            diagnose_public(
                Path::new("/mnt/adx/public"),
                &entries(&[("/mnt/adx/public", "tmpfs")])
            )
            .is_empty()
        );

        // 只被父挂载覆盖 → 名字会落到持久盘上。
        let findings = diagnose_public(
            Path::new("/mnt/adx/public"),
            &entries(&[("/mnt/adx", "ext4")]),
        );
        assert_eq!(findings[0].kind(), "path_missing");

        let findings = diagnose_public(
            Path::new("/mnt/adx/public"),
            &entries(&[("/mnt/adx/public", "ext4")]),
        );
        assert_eq!(findings[0].kind(), "path_drift");
    }

    /// 嵌套挂载取最长前缀：在 users 树里 bind 了别的盘时以更具体者为准。
    #[test]
    fn the_longest_matching_mount_wins() {
        let findings = diagnose_imports(
            Path::new("/imports"),
            &["node-a".into()],
            &entries(&[("/imports", "nfs4"), ("/imports/node-a/users", "ext4")]),
        );
        assert!(
            findings.is_empty(),
            "the nested mount should be authoritative"
        );
    }
}
