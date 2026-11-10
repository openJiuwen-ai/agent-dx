//! Private protocol faultpoints: compiled into unit-test executables only.
//! Real child processes are SIGKILLed; this is not physical power-loss evidence.
use super::{
    MetaRead, MetaStore, NodeSessionLease, OperationResult, RequestKey, RequestOutcome, Store,
    StoreRevision, TxnOutcome, WatchBatch,
    journal::MetaLog,
    local_log::{LocalLog, LocalLogOptions},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

const CHILD: &str = "AFS_P3_PRIVATE_CRASH_CHILD";
const POINT: &str = "AFS_P3_PRIVATE_CRASH_POINT";
const DIRECTORY: &str = "AFS_P3_PRIVATE_CRASH_DIRECTORY";
const TEST: &str = "meta::store::crash_tests::p3_process_crash_matrix";
const CHECKPOINT_TEST: &str = "meta::store::crash_tests::p3_checkpoint_process_crash_matrix";

pub(super) fn active(point: &str) -> bool {
    std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1"))
        && std::env::var(POINT).ok().as_deref() == Some(point)
        && std::env::var_os(DIRECTORY).is_some_and(|dir| PathBuf::from(dir).join("armed").is_file())
}

pub(super) fn pause(point: &str, detail: &str) {
    if !active(point) {
        return;
    }
    let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
    fs::write(directory.join("reached.tmp"), format!("{point}: {detail}")).unwrap();
    fs::rename(directory.join("reached.tmp"), directory.join("reached")).unwrap();
    // Only the supervising test may end this deliberately paused child.
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

#[derive(Deserialize, Serialize)]
struct Witness {
    original: RequestOutcome,
    identity: Vec<u8>,
    wal_bytes: u64,
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn lease(session: &str) -> NodeSessionLease {
    NodeSessionLease {
        node_id: "node-a".into(),
        session_id: session.into(),
        grpc_addr: "grpc".into(),
        data_addr: "data".into(),
        rest_addr: "rest".into(),
        ownerfs_enabled: true,
        storage_devices: Vec::new(),
        lease_ttl: Duration::from_secs(60),
    }
}
async fn child(directory: &Path) {
    let root = directory.join("meta");
    let store = Store::open_log(Arc::new(
        LocalLog::open(&root, LocalLogOptions::default()).unwrap(),
    ))
    .await
    .unwrap();
    let seed = RequestKey::new("node-a", "seed");
    assert!(matches!(
        store
            .register_node_session(seed.clone(), lease("seed"))
            .await
            .unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let witness = Witness {
        original: store
            .read(MetaRead::RequestOutcome(seed))
            .await
            .unwrap()
            .request_outcome
            .unwrap(),
        identity: fs::read(root.join("IDENTITY")).unwrap(),
        wal_bytes: fs::metadata(root.join("wal-00000000000000000001.log"))
            .unwrap()
            .len(),
    };
    fs::write(
        directory.join("ack.json"),
        serde_json::to_vec(&witness).unwrap(),
    )
    .unwrap();
    fs::write(directory.join("armed"), b"seed ACK complete").unwrap();
    // A current-thread runtime enqueues both requests before the commit task
    // can collect them, so the paused append really contains a two-record group.
    let (one, two) = tokio::join!(
        store.register_node_session(RequestKey::new("node-a", "one"), lease("one")),
        store.register_node_session(RequestKey::new("node-a", "two"), lease("two")),
    );
    fs::write(directory.join("unexpected-ack"), format!("{one:?} {two:?}")).unwrap();
    panic!("selected faultpoint was not reached");
}

#[tokio::test(flavor = "current_thread")]
async fn p3_process_crash_matrix() {
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        child(&PathBuf::from(std::env::var_os(DIRECTORY).unwrap())).await;
        return;
    }
    for point in [
        "partial_second",
        "after_append_before_sync",
        "after_sync_before_publish",
    ] {
        // Preserve the directory on any assertion failure, including the child's
        // log and exact WAL; only a successful scenario removes its own fixture.
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!("P3 crash case={point} evidence={}", directory.display());
        let child_log = File::create(directory.join("child.log")).unwrap();
        let mut process = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("reached").is_file() {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited before {point}: {}",
                fs::read_to_string(directory.join("child.log")).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "child failed to reach {point}; evidence {}",
                directory.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let reached = fs::read_to_string(directory.join("reached")).unwrap();
        assert!(reached.starts_with(point));
        if point == "after_sync_before_publish" {
            assert!(reached.contains("visible=1 staged=3"), "{reached}");
        }
        assert!(!directory.join("unexpected-ack").exists());
        process.0.kill().unwrap();
        let status = process.0.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let witness: Witness =
            serde_json::from_slice(&fs::read(directory.join("ack.json")).unwrap()).unwrap();
        let root = directory.join("meta");
        let wal = root.join("wal-00000000000000000001.log");
        let before = fs::read(&wal).unwrap();
        let log = Arc::new(LocalLog::open(&root, LocalLogOptions::default()).unwrap());
        let store = Store::open_log(log.clone()).await.unwrap();
        let count = if point == "partial_second" { 2 } else { 3 };
        assert_eq!(log.recover().await.unwrap().tip.sequence, count);
        let current = store
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into(),
            })
            .await
            .unwrap();
        assert_eq!(current.revision, StoreRevision(count));
        let Some(super::MetaEntity::NodeSession(session)) = current.entity else {
            panic!("missing recovered session");
        };
        assert_eq!(session.lease_epoch, count);
        assert_eq!(session.session_id, if count == 2 { "one" } else { "two" });
        assert_eq!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new("node-a", "seed")))
                .await
                .unwrap()
                .request_outcome,
            Some(witness.original)
        );
        for request in ["seed", "one"] {
            let key = RequestKey::new("node-a", request);
            let original = store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .unwrap();
            let TxnOutcome::ConditionFailed {
                existing_outcome: Some(replayed),
                ..
            } = store
                .register_node_session(key, lease("must-not-replace"))
                .await
                .unwrap()
            else {
                panic!("retry applied twice");
            };
            assert_eq!(replayed, original);
        }
        if count == 3 {
            let key = RequestKey::new("node-a", "two");
            let original = store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .unwrap();
            assert!(matches!(&original.result, OperationResult::NodeSession(_)));
            let TxnOutcome::ConditionFailed {
                existing_outcome: Some(replayed),
                ..
            } = store
                .register_node_session(key, lease("must-not-replace"))
                .await
                .unwrap()
            else {
                panic!("second unknown result applied twice");
            };
            assert_eq!(replayed, original);
        } else {
            assert!(
                store
                    .read(MetaRead::RequestOutcome(RequestKey::new("node-a", "two")))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_none()
            );
        }
        let after = fs::read(&wal).unwrap();
        assert!(after.len() as u64 > witness.wal_bytes);
        if count == 2 {
            assert!(after.len() < before.len());
            let saved = fs::read_dir(&root)
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| {
                    p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("incomplete-tail-")
                })
                .unwrap();
            assert_eq!(fs::read(saved).unwrap(), before[after.len()..]);
        } else {
            assert_eq!(after, before);
        }
        assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), witness.identity);
        let watch = store.watch(StoreRevision::ZERO, 100).await.unwrap();
        watch.validate_order().unwrap();
        let WatchBatch::Events { events, .. } = watch else {
            panic!("unexpected compaction");
        };
        assert!(events.iter().all(|event| event.revision.0 <= count));
        assert!(events.iter().any(|event| event.revision.0 == count));
        assert_eq!(fs::read(&wal).unwrap(), after, "retries cannot append");
        eprintln!(
            "P3 crash PASS case={point} signal={} seq={count} epoch={} recovered_bytes={} retained_tail_bytes={}",
            libc::SIGKILL,
            session.lease_epoch,
            after.len(),
            before.len() - after.len()
        );
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        drop(process);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn p3_initialization_process_crash() {
    const INITIALIZATION_TEST: &str = "meta::store::crash_tests::p3_initialization_process_crash";
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        fs::write(directory.join("armed"), b"no listener or business ACK").unwrap();
        LocalLog::open(directory.join("meta"), LocalLogOptions::default()).unwrap();
        panic!("initialization faultpoint was not reached");
    }
    let directory = tempfile::tempdir().unwrap().keep();
    eprintln!("P3 initialization crash evidence={}", directory.display());
    let output = File::create(directory.join("child.log")).unwrap();
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                INITIALIZATION_TEST,
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .env(POINT, "initialization_wal_installed")
            .env(DIRECTORY, &directory)
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while !directory.join("reached").is_file() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "initialization child exited: {}",
            fs::read_to_string(directory.join("child.log")).unwrap()
        );
        assert!(Instant::now() < deadline, "initialization child timeout");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let root = directory.join("meta");
    let identity = fs::read(root.join("IDENTITY")).unwrap();
    assert!(root.join("INITIALIZING").exists());
    let log = Arc::new(LocalLog::open(&root, LocalLogOptions::default()).unwrap());
    assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), identity);
    assert!(!root.join("INITIALIZING").exists());
    let store = Store::open_log(log.clone()).await.unwrap();
    let key = RequestKey::new("node-a", "after-initialization");
    assert!(matches!(
        store
            .register_node_session(key.clone(), lease("after-initialization"))
            .await
            .unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let original = store
        .read(MetaRead::RequestOutcome(key.clone()))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&log) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(log);
    let store = Store::open_log(Arc::new(
        LocalLog::open(&root, LocalLogOptions::default()).unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(
        store
            .read(MetaRead::RequestOutcome(key))
            .await
            .unwrap()
            .request_outcome,
        Some(original)
    );
    assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), identity);
    drop(store);
    drop(child);
    // Keep failures intact; only a successful fixture removes its own data.
    fs::remove_dir_all(directory).unwrap();
}

fn checkpoint_log_limits() -> LocalLogOptions {
    LocalLogOptions {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 32,
    }
}

fn checkpoint_image_limits() -> super::local_log::LocalCheckpointLimits {
    super::local_log::LocalCheckpointLimits {
        image_max_bytes: 64 * 1024,
        validation_charge_max_bytes: 256 * 1024,
        disk_max_bytes: 512 * 1024,
        files_max: 8,
    }
}

#[derive(Deserialize, Serialize)]
struct CheckpointWitness {
    tip: super::journal::LogTip,
    identity: Vec<u8>,
    state: Vec<u8>,
}

async fn checkpoint_child(directory: &Path) {
    let root = directory.join("meta");
    let log = LocalLog::open_with_checkpoint_limits(
        &root,
        checkpoint_log_limits(),
        checkpoint_image_limits(),
    )
    .unwrap();
    let mut state = super::state::State::default();
    let mut tip = super::journal::LogTip::EMPTY;
    let mut records = super::journal::test_records(13).into_iter();
    for number in 1..=12 {
        let record = records.next().unwrap();
        state = state.replay_record(&record).unwrap();
        tip = log
            .append_and_sync(tip.at(log.term()), vec![record])
            .await
            .unwrap();
        if number == 4 || number == 8 {
            log.install_checkpoint(
                tip,
                super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                    .unwrap(),
            )
            .await
            .unwrap();
        }
    }
    let witness = CheckpointWitness {
        tip,
        identity: fs::read(root.join("IDENTITY")).unwrap(),
        state: state.snapshot_bytes_with_batch(0).unwrap(),
    };
    fs::write(
        directory.join("ack.json"),
        serde_json::to_vec(&witness).unwrap(),
    )
    .unwrap();
    fs::write(
        directory.join("armed"),
        b"twelve durable decisions and two checkpoints",
    )
    .unwrap();
    if std::env::var(POINT).unwrap().starts_with("rotation_") {
        log.append_and_sync(tip.at(log.term()), vec![records.next().unwrap()])
            .await
            .unwrap();
    } else {
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
    }
    panic!("selected checkpoint/rotation faultpoint was not reached");
}

fn disk_accounting(root: &Path) -> (u64, u64, usize) {
    use std::os::unix::fs::MetadataExt;
    let mut bytes = 0;
    let mut blocks = 0;
    let mut files = 0;
    for entry in fs::read_dir(root).unwrap() {
        let metadata = entry.unwrap().metadata().unwrap();
        bytes += metadata.len();
        blocks += metadata.blocks() * 512;
        files += 1;
    }
    (bytes, blocks, files)
}

#[tokio::test(flavor = "current_thread")]
async fn p3_checkpoint_process_crash_matrix() {
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        checkpoint_child(&PathBuf::from(std::env::var_os(DIRECTORY).unwrap())).await;
        return;
    }
    for point in [
        "checkpoint_partial",
        "checkpoint_image_installed",
        "checkpoint_selected",
        "checkpoint_during_delete",
        "rotation_prepared",
        "rotation_selected",
    ] {
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!(
            "P3 checkpoint crash case={point} evidence={}",
            directory.display()
        );
        let child_log = File::create(directory.join("child.log")).unwrap();
        let mut process = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    CHECKPOINT_TEST,
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("reached").is_file() {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited before {point}: {}",
                fs::read_to_string(directory.join("child.log")).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "child failed to reach {point}; evidence {}",
                directory.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!directory.join("unexpected-ack").exists());
        let root = directory.join("meta");
        let before = disk_accounting(&root);
        // Small deterministic fixture budget, not a production peak/RSS claim.
        let disk_bound =
            checkpoint_log_limits().wal_max_bytes + checkpoint_image_limits().disk_max_bytes + 8192;
        assert!(before.0 <= disk_bound && before.1 <= disk_bound);
        assert!(
            before.2
                <= checkpoint_log_limits().segments_max + checkpoint_image_limits().files_max + 2
        );
        process.0.kill().unwrap();
        assert_eq!(process.0.wait().unwrap().signal(), Some(libc::SIGKILL));
        let witness: CheckpointWitness =
            serde_json::from_slice(&fs::read(directory.join("ack.json")).unwrap()).unwrap();
        let partial = root.join("checkpoint.tmp");
        let partial_bytes = if point == "checkpoint_partial" {
            Some(fs::read(&partial).unwrap())
        } else {
            None
        };
        let started = Instant::now();
        let log = Arc::new(
            LocalLog::open_with_checkpoint_limits(
                &root,
                checkpoint_log_limits(),
                checkpoint_image_limits(),
            )
            .unwrap(),
        );
        let recovered = log.recover().await.unwrap();
        assert_eq!(
            recovered.tip, witness.tip,
            "no interrupted operation was appended"
        );
        let selected = recovered.checkpoint.unwrap();
        let expected = if matches!(point, "checkpoint_selected" | "checkpoint_during_delete") {
            12
        } else {
            8
        };
        assert_eq!(selected.covered.sequence, expected);
        let mut restored = super::log::StateImage::decode(
            &selected.bytes,
            checkpoint_image_limits().image_max_bytes,
        )
        .unwrap();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            witness.state
        );
        assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), witness.identity);
        let store = Store::open_log(log.clone()).await.unwrap();
        let original = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "log-test",
                "request-0",
            )))
            .await
            .unwrap()
            .request_outcome
            .unwrap();
        let TxnOutcome::ConditionFailed {
            existing_outcome: Some(replayed),
            ..
        } = store
            .register_node_session(
                RequestKey::new("log-test", "request-0"),
                lease("must-not-replace"),
            )
            .await
            .unwrap()
        else {
            panic!("ACK retry applied twice")
        };
        assert_eq!(original, replayed);
        assert_eq!(log.recover().await.unwrap().tip, witness.tip);
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The exact empty prepared segment must be reusable; a partial image
        // must remain intact, and later installation may quarantine it safely.
        if let Some(bytes) = &partial_bytes {
            assert_eq!(&fs::read(&partial).unwrap(), bytes);
        }
        let record = super::journal::test_records(13).pop().unwrap();
        restored = restored.replay_record(&record).unwrap();
        let tip = log
            .append_and_sync(witness.tip.at(log.term()), vec![record])
            .await
            .unwrap();
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&restored, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
        if let Some(bytes) = partial_bytes {
            assert!(!partial.exists());
            assert!(fs::read_dir(&root).unwrap().any(|entry| {
                let path = entry.unwrap().path();
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("orphan-checkpoint.tmp-")
                    && fs::read(path).unwrap() == bytes
            }));
        }
        let after = disk_accounting(&root);
        assert!(after.0 <= disk_bound && after.1 <= disk_bound);
        eprintln!(
            "P3 checkpoint PASS case={point} signal={} selected={} ACK=12 final={} bytes_before={} blocks_before={} files_before={} bytes_after={} blocks_after={} files_after={} recovery_and_followup_us={}",
            libc::SIGKILL,
            expected,
            tip.sequence,
            before.0,
            before.1,
            before.2,
            after.0,
            after.1,
            after.2,
            started.elapsed().as_micros()
        );
        drop(log);
        drop(process);
        fs::remove_dir_all(directory).unwrap();
    }
}

fn restored_checkpoint_state(recovered: &super::journal::RecoveredLog) -> super::state::State {
    let mut state = recovered
        .checkpoint
        .as_ref()
        .map(|c| {
            super::log::StateImage::decode(&c.bytes, checkpoint_image_limits().image_max_bytes)
                .unwrap()
        })
        .unwrap_or_default();
    for record in &recovered.records {
        state = state.replay_record(record).unwrap();
    }
    state
}

#[tokio::test(flavor = "current_thread")]
async fn p3_repeated_unselected_checkpoint_crashes_remain_recoverable() {
    const REPEATED_TEST: &str =
        "meta::store::crash_tests::p3_repeated_unselected_checkpoint_crashes_remain_recoverable";
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        let root = directory.join("meta");
        let log = LocalLog::open_with_checkpoint_limits(
            &root,
            checkpoint_log_limits(),
            checkpoint_image_limits(),
        )
        .unwrap();
        let recovered = log.recover().await.unwrap();
        let mut state = restored_checkpoint_state(&recovered);
        let record = super::journal::test_records(recovered.tip.sequence as usize + 1)
            .pop()
            .unwrap();
        state = state.replay_record(&record).unwrap();
        let tip = log
            .append_and_sync(recovered.tip.at(log.term()), vec![record])
            .await
            .unwrap();
        fs::write(
            directory.join("ack.json"),
            serde_json::to_vec(&CheckpointWitness {
                tip,
                identity: fs::read(root.join("IDENTITY")).unwrap(),
                state: state.snapshot_bytes_with_batch(0).unwrap(),
            })
            .unwrap(),
        )
        .unwrap();
        fs::write(
            directory.join("armed"),
            b"next committed tip; before selector",
        )
        .unwrap();
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
        panic!("unselected-image faultpoint not reached");
    }
    let directory = tempfile::tempdir().unwrap().keep();
    eprintln!(
        "P3 repeated checkpoint crash evidence={}",
        directory.display()
    );
    let root = directory.join("meta");
    for round in 0..6 {
        let reached = directory.join("reached");
        if reached.exists() {
            fs::remove_file(&reached).unwrap();
        }
        let output = File::create(directory.join(format!("child-{round}.log"))).unwrap();
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", REPEATED_TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .env(POINT, "checkpoint_image_installed")
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(output.try_clone().unwrap()))
                .stderr(Stdio::from(output))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !reached.is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "checkpoint round {round} exited before faultpoint: {}",
                fs::read_to_string(directory.join(format!("child-{round}.log"))).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "checkpoint round {round} timed out"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
        let witness: CheckpointWitness =
            serde_json::from_slice(&fs::read(directory.join("ack.json")).unwrap()).unwrap();
        let log = LocalLog::open_with_checkpoint_limits(
            &root,
            checkpoint_log_limits(),
            checkpoint_image_limits(),
        )
        .unwrap();
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, witness.tip);
        let mut state = restored_checkpoint_state(&recovered);
        assert_eq!(state.snapshot_bytes_with_batch(0).unwrap(), witness.state);
        assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), witness.identity);
        // Advance before installing: reselecting the same image would hide the
        // orphan accumulation rather than reproduce repeated interrupted work.
        let record = super::journal::test_records(witness.tip.sequence as usize + 1)
            .pop()
            .unwrap();
        state = state.replay_record(&record).unwrap();
        let tip = log
            .append_and_sync(witness.tip.at(log.term()), vec![record])
            .await
            .unwrap();
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
        eprintln!(
            "P3 repeated checkpoint PASS round={round} tip={} disk={:?}",
            tip.sequence,
            disk_accounting(&root)
        );
        drop(log);
        drop(child);
    }
    let images = fs::read_dir(&root)
        .unwrap()
        .filter(|e| {
            let n = e.as_ref().unwrap().file_name();
            let n = n.to_string_lossy();
            n.starts_with("checkpoint-") && n.ends_with(".bin")
        })
        .count();
    assert!(
        images <= 2,
        "only selected current/previous images may remain"
    );
    fs::remove_dir_all(directory).unwrap();
}

/// A writer open selects its term in CURRENT before returning. Killed before
/// or after that CURRENT write, the next open recovers every acknowledged
/// record and selects a term above every term ever made durable.
#[tokio::test(flavor = "current_thread")]
async fn p3_open_term_process_crash() {
    const OPEN_TERM_TEST: &str = "meta::store::crash_tests::p3_open_term_process_crash";
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        fs::write(directory.join("armed"), b"term-1 writer closed").unwrap();
        LocalLog::open(directory.join("meta"), LocalLogOptions::default()).unwrap();
        panic!("open term faultpoint was not reached");
    }
    let records = super::journal::test_records(2);
    for (point, durable_term) in [("open_term_prepared", 1), ("open_term_selected", 2)] {
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!(
            "P3 open-term crash case={point} evidence={}",
            directory.display()
        );
        let root = directory.join("meta");
        let log = LocalLog::open(&root, LocalLogOptions::default()).unwrap();
        assert_eq!(log.term(), 1);
        let tip = log
            .append_and_sync(super::journal::LogTip::EMPTY.at(1), records[..1].to_vec())
            .await
            .unwrap();
        drop(log);
        let output = File::create(directory.join("child.log")).unwrap();
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", OPEN_TERM_TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(output.try_clone().unwrap()))
                .stderr(Stdio::from(output))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("reached").is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "open-term child exited before {point}: {}",
                fs::read_to_string(directory.join("child.log")).unwrap()
            );
            assert!(Instant::now() < deadline, "open-term child timeout");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
        let log = LocalLog::open(&root, LocalLogOptions::default()).unwrap();
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.term, durable_term + 1, "{point}");
        assert_eq!(recovered.tip, tip, "{point}: acknowledged record kept");
        let next = log
            .append_and_sync(recovered.cursor(), records[1..].to_vec())
            .await
            .unwrap();
        drop(log);
        let log = LocalLog::open(&root, LocalLogOptions::default()).unwrap();
        let again = log.recover().await.unwrap();
        assert!(again.term > recovered.term, "{point}: term stays monotonic");
        assert_eq!(again.tip, next);
        assert_eq!(again.records.len(), 2);
        drop(log);
        drop(child);
        fs::remove_dir_all(directory).unwrap();
    }
}

fn r86_commit(number: u64) -> super::MetaTxn {
    use super::{MetaEntity, MetaTxn, StoreOperation, TxnCondition, TxnMutation};
    use crate::dfs::{ChunkId, Extent, FileVersion, FileVersionId, LayoutRoot, LayoutRootId};

    let fixtures = super::codec::tests::entity_fixtures();
    let MetaEntity::DfsInode(mut inode) = fixtures[7].clone() else {
        panic!("expected an inode fixture");
    };
    let version = FileVersionId::new(format!("r86-version-{number}"));
    let layout = LayoutRootId::new(format!("r86-layout-{number}"));
    inode.head_version = Some(version.clone());
    inode.revision = number;
    let request = RequestKey::new("r86", format!("append-{number}"));
    let operation = StoreOperation::DfsCommitFileVersion;
    let mut txn = MetaTxn::new(request.clone(), operation);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
            id: version,
            inode_id: inode.inode_id.clone(),
            parent_version: (number > 1)
                .then(|| FileVersionId::new(format!("r86-version-{}", number - 1))),
            length: number * 4096,
            layout_root: layout.clone(),
            created_at_unix_ms: number,
        })),
        TxnMutation::Put(MetaEntity::DfsLayoutRoot(Arc::new(LayoutRoot {
            id: layout,
            file_length: number * 4096,
            inline_extents: (0..number)
                .map(|index| Extent {
                    file_offset: index * 4096,
                    length: 4096,
                    chunk_id: ChunkId::new(format!("r86-chunk-{index}")),
                    chunk_offset: 0,
                })
                .collect(),
        }))),
        TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation,
            result: OperationResult::DfsInode(inode),
        }),
    ]);
    txn
}

#[tokio::test]
async fn r86_overwrite_keeps_full_layout_between_append_records() {
    use super::{MetaEntity, TxnMutation, log::Record};
    use crate::dfs::ChunkId;

    let directory = tempfile::tempdir().unwrap();
    let log = Arc::new(LocalLog::open(directory.path(), LocalLogOptions::default()).unwrap());
    let store = Store::open_log(log.clone()).await.unwrap();
    let mut expected = Vec::new();
    for number in 1..=4 {
        let mut txn = r86_commit(number);
        if number >= 3 {
            let layout = txn
                .mutations
                .iter_mut()
                .find_map(|mutation| match mutation {
                    TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)) => Some(layout),
                    _ => None,
                })
                .unwrap();
            Arc::make_mut(layout).inline_extents[0].chunk_id = ChunkId::new("r86-overwritten");
            expected.push(layout.clone());
        }
        assert!(matches!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
    }
    let recovered = log.recover().await.unwrap();
    assert!(matches!(
        recovered.records.as_slice(),
        [
            Record::Decision { .. },
            Record::AppendLayout { .. },
            Record::Decision { .. },
            Record::AppendLayout { .. }
        ]
    ));
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&log) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(log);
    let log = Arc::new(LocalLog::open(directory.path(), LocalLogOptions::default()).unwrap());
    let restored = Store::open_log(log).await.unwrap();
    for layout in expected {
        let actual = restored
            .read(MetaRead::DfsLayoutRoot(layout.id.clone()))
            .await
            .unwrap();
        assert_eq!(actual.entity, Some(MetaEntity::DfsLayoutRoot(layout)));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn r86_layout_append_process_crash() {
    const NAME: &str = "meta::store::crash_tests::r86_layout_append_process_crash";

    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        let log =
            Arc::new(LocalLog::open(directory.join("meta"), LocalLogOptions::default()).unwrap());
        let store = Store::open_log(log.clone()).await.unwrap();
        let point = std::env::var(POINT).unwrap();
        assert!(matches!(
            store.compare_and_commit(r86_commit(1)).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
        if point.starts_with("checkpoint_") {
            store.checkpoint().await.unwrap();
            assert!(matches!(
                store.compare_and_commit(r86_commit(2)).await.unwrap(),
                TxnOutcome::Committed { .. }
            ));
            let recovered = log.recover().await.unwrap();
            assert!(matches!(
                recovered.records.last(),
                Some(super::log::Record::AppendLayout { .. })
            ));
            fs::write(directory.join("armed"), b"append ACK complete").unwrap();
            store.checkpoint().await.unwrap();
        } else {
            fs::write(directory.join("armed"), b"base ACK complete").unwrap();
            store.compare_and_commit(r86_commit(2)).await.unwrap();
        }
        panic!("selected layout append crash point was not reached");
    }

    for point in [
        "after_append_before_sync",
        "after_sync_before_publish",
        "checkpoint_image_installed",
        "checkpoint_selected",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let child_log = File::create(directory.path().join("child.log")).unwrap();
        let mut process = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, directory.path())
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        let reached = directory.path().join("reached");
        while !reached.is_file() {
            assert!(
                process.0.try_wait().unwrap().is_none() && Instant::now() < deadline,
                "did not reach {point}: {}",
                fs::read_to_string(directory.path().join("child.log")).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(fs::read_to_string(reached).unwrap().starts_with(point));
        process.0.kill().unwrap();
        assert_eq!(process.0.wait().unwrap().signal(), Some(libc::SIGKILL));
        let log = Arc::new(
            LocalLog::open(directory.path().join("meta"), LocalLogOptions::default()).unwrap(),
        );
        let store = Store::open_log(log.clone()).await.unwrap();
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip.sequence, 2);
        if !point.starts_with("checkpoint_") {
            assert!(matches!(
                recovered.records.last(),
                Some(super::log::Record::AppendLayout { .. })
            ));
        }
        let layout = store
            .read(MetaRead::DfsLayoutRoot(crate::dfs::LayoutRootId::new(
                "r86-layout-2",
            )))
            .await
            .unwrap()
            .entity
            .unwrap();
        let expected = r86_commit(2)
            .mutations
            .into_iter()
            .find_map(|mutation| match mutation {
                super::TxnMutation::Put(entity @ super::MetaEntity::DfsLayoutRoot(_)) => {
                    Some(entity)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(layout, expected, "{point}");
        for number in [1, 2] {
            assert!(
                store
                    .read(MetaRead::RequestOutcome(r86_commit(number).request))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_some(),
                "{point}"
            );
        }
        eprintln!("R86 recovered exact layout and outcomes after {point}");
    }
}
