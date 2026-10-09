//! Single-run, local diagnostic witness. Never participates in filesystem results.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};

use super::{InFlightCommit, InodeWriteState, PendingFileCommit};

const MAX_INPUT_BYTES: u64 = 16 * 1024;
const MAX_RECORDS: u64 = 64;
const MAX_BYTES: u64 = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    enabled: bool,
    nonce: String,
    namespace_id: String,
    node_id: String,
    output_path: PathBuf,
    arm_path: PathBuf,
    config_path: PathBuf,
    config_sha256: String,
    executable_sha256: String,
    source_binding_path: PathBuf,
    source_binding_sha256: String,
    meta_endpoint: String,
    mount_identity: String,
    max_records: u64,
    max_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    schema: String,
    nonce: String,
    namespace_id: String,
    node_id: String,
    session_id: String,
    inode_id: String,
}

pub(super) struct PendingTrace {
    config: Config,
    binding: Value,
    started: Instant,
    state: Mutex<Writer>,
}

struct Writer {
    file: File,
    index: u64,
    bytes: u64,
    disabled: bool,
    armed_inode: Option<String>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}

fn hash_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn read_owned(path: &Path, owner: u32) -> io::Result<Vec<u8>> {
    require(path.is_absolute(), "trace input must be absolute")?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file()
            && metadata.uid() == owner
            && metadata.mode() & 0o022 == 0
            && metadata.len() <= MAX_INPUT_BYTES,
        "trace input must be a bounded, owned, non-writable regular file",
    )?;
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_BYTES + 1).read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 <= MAX_INPUT_BYTES,
        "trace input grew beyond bound",
    )?;
    Ok(bytes)
}

// There is no SHA256 implementation/direct dependency in this crate. Cache these
// three startup hashes; no command, SHA256 or payload is used on commit events.
fn sha256_file(path: &Path) -> io::Result<String> {
    let mut child = Command::new("/usr/bin/sha256sum")
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                require(status.success(), "trace SHA256 command failed")?;
                let mut output = String::new();
                if let Some(stdout) = child.stdout.take() {
                    stdout.take(4096).read_to_string(&mut output)?;
                }
                let hash = output
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| invalid("missing trace SHA256"))?;
                require(hash_valid(hash), "invalid trace SHA256")?;
                return Ok(hash.to_owned());
            }
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(2)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(invalid("trace SHA256 command timed out or failed"));
            }
        }
    }
}

impl PendingTrace {
    #[cfg(not(test))]
    pub(super) fn from_env(namespace: &str, node: &str, session: &str) -> Option<Self> {
        let path = std::env::var_os("AFS_DFS_PENDING_TRACE_CONFIG")?;
        match Self::open(Path::new(&path), namespace, node, session) {
            Ok(trace) => Some(trace),
            Err(error) => {
                eprintln!("DFS pending trace disabled: {error}");
                None
            }
        }
    }

    fn open(path: &Path, namespace: &str, node: &str, session: &str) -> io::Result<Self> {
        let pid = std::process::id();
        let owner = fs::metadata(format!("/proc/{pid}"))?.uid();
        let config: Config = serde_json::from_slice(&read_owned(path, owner)?)?;
        require(
            config.schema == "afs-dfs-pending-trace-config-v1" && config.enabled,
            "trace requires explicit enabled schema",
        )?;
        require(
            config.namespace_id == namespace && config.node_id == node && !session.is_empty(),
            "trace instance identity mismatch",
        )?;
        require(
            !config.nonce.is_empty()
                && config.nonce.len() <= 128
                && config
                    .nonce
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid trace nonce",
        )?;
        require(
            config.max_records > 0
                && config.max_records <= MAX_RECORDS
                && config.max_bytes > 0
                && config.max_bytes <= MAX_BYTES,
            "invalid trace bounds",
        )?;
        require(
            config.arm_path.is_absolute()
                && config.output_path.is_absolute()
                && config.config_path.is_absolute(),
            "trace paths must be absolute",
        )?;
        require(
            !config.meta_endpoint.is_empty() && !config.mount_identity.is_empty(),
            "trace profile binding missing",
        )?;
        let argv = fs::read(format!("/proc/{pid}/cmdline"))?;
        let arguments: Vec<_> = argv.split(|b| *b == 0).collect();
        let config_arg = config.config_path.as_os_str().as_encoded_bytes();
        require(
            arguments
                .windows(2)
                .any(|pair| pair[0] == b"--config" && pair[1] == config_arg)
                || arguments
                    .iter()
                    .any(|arg| arg.strip_prefix(b"--config=") == Some(config_arg)),
            "trace config is not actual process config argument",
        )?;
        let exe_sha = sha256_file(Path::new(&format!("/proc/{pid}/exe")))?;
        let config_sha = sha256_file(&config.config_path)?;
        let node_config: toml::Value = toml::from_str(&fs::read_to_string(&config.config_path)?)
            .map_err(|_| invalid("trace actual Node config cannot be parsed"))?;
        require(
            node_config.get("id").and_then(toml::Value::as_str) == Some(node)
                && node_config
                    .get("meta_endpoint")
                    .and_then(toml::Value::as_str)
                    == Some(&config.meta_endpoint)
                && node_config.get("dfs_mount").and_then(toml::Value::as_str)
                    == Some(&config.mount_identity),
            "trace profile does not match actual config",
        )?;
        let source_bytes = read_owned(&config.source_binding_path, owner)?;
        let source_sha = sha256_file(&config.source_binding_path)?;
        let source: Value = serde_json::from_slice(&source_bytes)?;
        require(
            read_owned(&config.source_binding_path, owner)? == source_bytes,
            "trace source binding changed during verification",
        )?;
        require(
            exe_sha == config.executable_sha256
                && config_sha == config.config_sha256
                && source_sha == config.source_binding_sha256,
            "trace artifact hash mismatch",
        )?;
        require(
            source["schema"] == "afs-dfs-pending-trace-source-v1"
                && source["nonce"] == config.nonce
                && source["node_executable_sha256"] == exe_sha
                && source["source_sha256"].as_str().is_some_and(hash_valid)
                && source["candidate_json_sha256"]
                    .as_str()
                    .is_some_and(hash_valid),
            "trace source/run binding mismatch",
        )?;
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let start_ticks = stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().nth(19))
            .and_then(|field| field.parse::<u64>().ok())
            .ok_or_else(|| invalid("trace process stat invalid"))?;
        let binding = json!({"pid":pid, "start_ticks":start_ticks,
            "boot_id":fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim(),
            "executable":fs::read_link("/proc/self/exe")?, "executable_sha256":exe_sha,
            "config_path":config.config_path, "config_sha256":config_sha,
            "source_binding_path":config.source_binding_path, "source_binding_sha256":source_sha,
            "source_sha256":source["source_sha256"], "node_id":node,"session_id":session,
            "candidate_json_sha256":source["candidate_json_sha256"],
            "actual_argv":arguments.iter().filter(|arg| !arg.is_empty()).map(|arg| String::from_utf8_lossy(arg).into_owned()).collect::<Vec<_>>(),
            "namespace_id":namespace,"meta_endpoint":config.meta_endpoint,"mount_identity":config.mount_identity});
        Self::create(config, binding)
    }

    fn create(config: Config, binding: Value) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&config.output_path)?;
        require(file.metadata()?.is_file(), "trace output must be regular")?;
        let trace = Self {
            config,
            binding,
            started: Instant::now(),
            state: Mutex::new(Writer {
                file,
                index: 0,
                bytes: 0,
                disabled: false,
                armed_inode: None,
            }),
        };
        trace.write(
            json!({"event":"dfs_pending_trace_header","arm_required":true,
            "max_records":trace.config.max_records,"max_bytes":trace.config.max_bytes}),
        );
        Ok(trace)
    }

    fn write_locked(&self, writer: &mut Writer, mut record: Value) {
        if writer.disabled {
            return;
        }
        record["schema"] = json!("afs-dfs-pending-trace-v1");
        record["nonce"] = json!(self.config.nonce);
        record["event_index"] = json!(writer.index + 1);
        record["monotonic_elapsed_ns"] =
            json!(self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        record["binding"] = self.binding.clone();
        let Ok(mut bytes) = serde_json::to_vec(&record) else {
            writer.disabled = true;
            return;
        };
        bytes.push(b'\n');
        if writer.index >= self.config.max_records
            || writer.bytes + bytes.len() as u64 > self.config.max_bytes
        {
            writer.disabled = true;
            return;
        }
        if writer.file.write_all(&bytes).is_err() {
            writer.disabled = true;
            return;
        }
        writer.index += 1;
        writer.bytes += bytes.len() as u64;
    }

    fn write(&self, record: Value) {
        if let Ok(mut writer) = self.state.lock() {
            self.write_locked(&mut writer, record);
        }
    }

    fn arm(&self, writer: &mut Writer) -> bool {
        if writer.disabled {
            return false;
        }
        if writer.armed_inode.is_none() {
            let owner = match writer.file.metadata() {
                Ok(m) => m.uid(),
                Err(_) => {
                    writer.disabled = true;
                    return false;
                }
            };
            match read_owned(&self.config.arm_path, owner) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => return false,
                Ok(bytes) => {
                    let arm = serde_json::from_slice::<Arm>(&bytes);
                    let Ok(arm) = arm else {
                        writer.disabled = true;
                        return false;
                    };
                    if arm.schema != "afs-dfs-pending-trace-arm-v1"
                        || arm.nonce != self.config.nonce
                        || arm.namespace_id != self.config.namespace_id
                        || arm.node_id != self.config.node_id
                        || arm.session_id != self.binding["session_id"]
                        || arm.inode_id.is_empty()
                    {
                        writer.disabled = true;
                        return false;
                    }
                    writer.armed_inode = Some(arm.inode_id);
                }
                Err(_) => {
                    writer.disabled = true;
                    return false;
                }
            }
        }
        true
    }

    pub(super) fn observe(
        &self,
        event: &str,
        branch: &str,
        state: &InodeWriteState,
        sent: &PendingFileCommit,
        error: Option<&afs_error::Error>,
    ) {
        let Ok(mut writer) = self.state.lock() else {
            return;
        };
        if writer.disabled {
            return;
        }
        let cleared = event == "dfs_pending_commit_success_cleared"
            || event == "dfs_pending_commit_definite_rejection_cleared";
        let pending = if cleared {
            if state.in_flight.is_some() {
                writer.disabled = true;
                return;
            }
            if event == "dfs_pending_commit_success_cleared"
                && (state.base_version.as_ref().map(|v| &v.id)
                    != Some(&sent.batch.commit.file_version.id)
                    || state.durable_write_seq != sent.batch.through_seq
                    || state.committed_write_seq != sent.batch.through_seq)
            {
                writer.disabled = true;
                return;
            }
            sent
        } else {
            match state.in_flight.as_ref() {
                Some(InFlightCommit::File(live))
                    if live.batch.commit == sent.batch.commit
                        && live.batch.through_seq == sent.batch.through_seq
                        && live.frozen.through_seq == sent.frozen.through_seq
                        && live
                            .trace_send_attempt
                            .as_ref()
                            .zip(sent.trace_send_attempt.as_ref())
                            .is_some_and(|(live, sent)| Arc::ptr_eq(live, sent)) =>
                {
                    live
                }
                _ => {
                    writer.disabled = true;
                    return;
                }
            }
        };
        let Some(counter) = pending.trace_send_attempt.as_ref() else {
            writer.disabled = true;
            return;
        };
        // Count actual commit-call boundaries, including sends before arming.
        // Prepared pending reuse alone is not evidence of a previous send.
        let send_attempt = if event == "dfs_pending_commit_send" {
            match counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |attempt| {
                attempt.checked_add(1)
            }) {
                Ok(previous) => previous + 1,
                Err(_) => {
                    writer.disabled = true;
                    return;
                }
            }
        } else {
            counter.load(Ordering::Relaxed)
        };
        if send_attempt == 0 {
            writer.disabled = true;
            return;
        }
        let event = if event == "dfs_pending_commit_send" {
            if send_attempt == 1 {
                "dfs_pending_commit_first_send"
            } else {
                "dfs_pending_commit_retry_send"
            }
        } else {
            event
        };
        if !self.arm(&mut writer)
            || writer.armed_inode.as_deref() != Some(&sent.batch.commit.inode_id.0)
        {
            return;
        }
        let commit = &pending.batch.commit;
        let Ok(bytes) = serde_json::to_vec(commit) else {
            writer.disabled = true;
            return;
        };
        let Ok(receipts) = serde_json::to_vec(&commit.chunk_receipts) else {
            writer.disabled = true;
            return;
        };
        let mut record = json!({"event":event,"branch":branch,"send_attempt":send_attempt,"locked_state_sample":true,
            "in_flight":if cleared { "None" } else { "File" },
            "send_argument_matches_live_pending":!cleared,"cleared":cleared,
            "operation":"DfsCommitFileVersion","caller_id":self.config.node_id,
            "inode_id":commit.inode_id,"operation_id":commit.operation_id,
            "request_digest":blake3::hash(&bytes).to_hex().to_string(),"request_serialized_bytes":bytes.len(),
            "write_lease":commit.write_lease,"expected_inode_revision":commit.expected_inode_revision,
            "expected_head_version":commit.expected_head_version,"file_version_id":commit.file_version.id,
            "parent_version_id":commit.file_version.parent_version,"file_length":commit.file_version.length,
            "layout_root_id":commit.layout_root.id,"layout_file_length":commit.layout_root.file_length,
            "chunk_receipt_count":commit.chunk_receipts.len(),"chunk_receipts_digest":blake3::hash(&receipts).to_hex().to_string()});
        let state_record = json!({
            "metadata_mode":commit.metadata_delta.mode,"kill_suidgid":commit.metadata_delta.kill_suidgid,
            "frozen_through_seq":pending.frozen.through_seq,"batch_through_seq":pending.batch.through_seq,
            "visible_write_seq":state.visible_write_seq,"durable_write_seq":state.durable_write_seq,
            "committed_write_seq":state.committed_write_seq,"logical_length":state.logical_length,
            "commit_busy":state.commit_busy,"operation_busy":state.operation_busy,
            "dirty_extent_count":state.dirty_extents.extents.len(),"open_writers":state.open_writers,
            "actual_inode_head_version":state.inode.head_version,"actual_base_version":state.base_version.as_ref().map(|v| &v.id),
            "definite_commit_rejection":error.map(super::is_definite_commit_rejection),
            "error":error.map(|error| json!({"code":error.code().raw(),"kind":format!("{:?}",error.kind()),"message":error.message()}))});
        if let (Some(record), Some(fields)) = (record.as_object_mut(), state_record.as_object()) {
            record.extend(fields.clone());
        }
        self.write_locked(&mut writer, record);
    }

    #[cfg(test)]
    pub(super) fn test_gate(
        path: &Path,
        namespace: &str,
        node: &str,
        session: &str,
        inode: &str,
    ) -> Self {
        let trace = Self::create(
            Config {
                schema: "test".into(),
                enabled: true,
                nonce: "test-pending-trace".into(),
                namespace_id: namespace.into(),
                node_id: node.into(),
                output_path: path.into(),
                arm_path: path.with_extension("arm"),
                config_path: PathBuf::new(),
                config_sha256: String::new(),
                executable_sha256: String::new(),
                source_binding_path: PathBuf::new(),
                source_binding_sha256: String::new(),
                meta_endpoint: "test".into(),
                mount_identity: "test".into(),
                max_records: MAX_RECORDS,
                max_bytes: MAX_BYTES,
            },
            json!({"test_fixture":true,"session_id":session}),
        )
        .unwrap();
        trace.state.lock().unwrap().armed_inode = Some(inode.into());
        trace
    }

    #[cfg(test)]
    pub(super) fn test_exhaust_records(&self) {
        self.state.lock().unwrap().index = MAX_RECORDS;
    }

    #[cfg(test)]
    pub(super) fn test_unarm(&self) {
        self.state.lock().unwrap().armed_inode = None;
    }

    #[cfg(test)]
    pub(super) fn test_arm_target(&self, inode: &str) {
        use std::os::unix::fs::PermissionsExt;
        let arm = json!({"schema":"afs-dfs-pending-trace-arm-v1",
            "nonce":self.config.nonce,"namespace_id":self.config.namespace_id,
            "node_id":self.config.node_id,"session_id":self.binding["session_id"],"inode_id":inode});
        fs::write(&self.config.arm_path, serde_json::to_vec(&arm).unwrap()).unwrap();
        fs::set_permissions(&self.config.arm_path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn pending_trace_exclusive_output_limits_and_write_failure() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("trace.jsonl");
        let mut trace = PendingTrace::test_gate(&path, "default", "node-a", "session-a", "inode");
        let before = fs::read(&path).unwrap();
        assert!(
            std::panic::catch_unwind(|| PendingTrace::test_gate(
                &path,
                "default",
                "node-a",
                "session-a",
                "inode"
            ))
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), before);
        trace.config.max_bytes = before.len() as u64;
        trace.write(json!({"event":"must-not-fit"}));
        assert!(trace.state.lock().unwrap().disabled);
        assert_eq!(fs::read(&path).unwrap(), before);
        let path2 = temp.path().join("write-failure.jsonl");
        let trace2 = PendingTrace::test_gate(&path2, "default", "node-a", "session-a", "inode");
        trace2.state.lock().unwrap().file = File::open(&path2).unwrap();
        trace2.write(json!({"event":"cannot-write"}));
        assert!(trace2.state.lock().unwrap().disabled);
    }

    #[test]
    fn pending_trace_owned_inputs_and_actual_instance_guard() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        let owner = fs::metadata(format!("/proc/{}", std::process::id()))
            .unwrap()
            .uid();
        let value = json!({"schema":"afs-dfs-pending-trace-config-v1","enabled":true,"nonce":"run-161",
            "namespace_id":"default","node_id":"wrong-node","output_path":temp.path().join("output"),
            "arm_path":temp.path().join("arm"),"config_path":temp.path().join("node.toml"),"config_sha256":"0".repeat(64),
            "executable_sha256":"0".repeat(64),"source_binding_path":temp.path().join("source.json"),
            "source_binding_sha256":"0".repeat(64),"meta_endpoint":"http://meta","mount_identity":"/mount",
            "max_records":64,"max_bytes":262144});
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let error = PendingTrace::open(&path, "default", "node-a", "session-a")
            .err()
            .unwrap();
        assert!(error.to_string().contains("instance identity"));
        assert!(!temp.path().join("output").exists());
        let link = temp.path().join("symlink");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_owned(&link, owner).is_err());
        assert!(hash_valid(&"a".repeat(64)));
        assert!(!hash_valid(&"A".repeat(64)));
        // Hash the caller process, never /proc/self/exe inside sha256sum's child.
        assert_eq!(
            sha256_file(Path::new(&format!("/proc/{}/exe", std::process::id()))).unwrap(),
            sha256_file(&std::env::current_exe().unwrap()).unwrap()
        );
        assert_ne!(
            sha256_file(&std::env::current_exe().unwrap()).unwrap(),
            sha256_file(Path::new("/usr/bin/sha256sum")).unwrap()
        );
    }

    #[test]
    fn pending_trace_arm_matches_actual_session_and_latches_once() {
        let temp = tempfile::tempdir().unwrap();
        for valid in [false, true] {
            let trace = PendingTrace::test_gate(
                &temp.path().join(format!("{valid}.jsonl")),
                "default",
                "node-a",
                "session-a",
                "unused",
            );
            trace.state.lock().unwrap().armed_inode = None;
            let mut arm = json!({"schema":"afs-dfs-pending-trace-arm-v1","nonce":"test-pending-trace",
                "namespace_id":"default","node_id":"node-a","session_id":if valid {"session-a"} else {"stale-session"}, "inode_id":"inode-a"});
            fs::write(&trace.config.arm_path, serde_json::to_vec(&arm).unwrap()).unwrap();
            fs::set_permissions(&trace.config.arm_path, fs::Permissions::from_mode(0o600)).unwrap();
            let mut writer = trace.state.lock().unwrap();
            assert_eq!(trace.arm(&mut writer), valid);
            if valid {
                arm["inode_id"] = json!("inode-b");
                fs::write(&trace.config.arm_path, serde_json::to_vec(&arm).unwrap()).unwrap();
                assert!(trace.arm(&mut writer));
                assert_eq!(writer.armed_inode.as_deref(), Some("inode-a"));
            } else {
                assert!(writer.disabled);
            }
        }
    }
}
