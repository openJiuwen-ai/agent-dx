use super::*;
use crate::meta::store::journal::{encode_frame, test_records};

fn budget_retired_segment() -> SegmentRef {
    SegmentRef {
        base: LogTip::EMPTY,
        base_revision: StoreRevision::ZERO,
        sealed: Some((
            Tail {
                generation: 1,
                tip: LogTip {
                    sequence: 1,
                    record_hash: [1; 32],
                },
                end: LOG_HEADER_BYTES + 16,
            },
            StoreRevision(1),
        )),
    }
}
#[test]
fn wal_budget_retired_deletion_after_enumeration_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    let retired = budget_retired_segment();
    let retired_name = wal_name(retired.base).unwrap();
    fs::write(dir.path().join(&retired_name), [1u8; 16]).unwrap();
    fs::write(dir.path().join("wal-live.log"), [2u8; 64]).unwrap();
    let entries = fs::read_dir(dir.path()).unwrap().map(|entry| {
        let entry = entry.unwrap();
        if entry.file_name() == retired_name.as_str() {
            fs::remove_file(entry.path()).unwrap();
        }
        Ok(entry)
    });
    assert_eq!(
        wal_budget_entries(
            entries,
            LocalLogOptions::default(),
            11,
            0,
            &[retired],
            |path| { fs::symlink_metadata(path) }
        )
        .unwrap(),
        75
    );
}
#[test]
fn wal_budget_missing_live_and_unknown_entries_still_fail() {
    for name in ["wal-00000000000000000002.log", "wal-unrecognized.log"] {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(name), [1u8; 16]).unwrap();
        let entries = fs::read_dir(dir.path()).unwrap().map(|entry| {
            let entry = entry.unwrap();
            fs::remove_file(entry.path()).unwrap();
            Ok(entry)
        });
        let error = wal_budget_entries(
            entries,
            LocalLogOptions::default(),
            0,
            0,
            &[budget_retired_segment()],
            |path| fs::symlink_metadata(path),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::IO_NOT_FOUND);
    }
}
#[test]
fn wal_budget_retired_files_still_consume_capacity_until_removed() {
    let dir = tempfile::tempdir().unwrap();
    let retired = budget_retired_segment();
    fs::write(dir.path().join(wal_name(retired.base).unwrap()), [1u8; 16]).unwrap();
    let limits = LocalLogOptions {
        segments_max: 1,
        ..Default::default()
    };
    assert_eq!(
        wal_budget_entries(
            fs::read_dir(dir.path()).unwrap(),
            limits,
            0,
            0,
            &[retired],
            |path| fs::symlink_metadata(path)
        )
        .unwrap(),
        16
    );
    assert_eq!(
        wal_budget_entries(
            fs::read_dir(dir.path()).unwrap(),
            limits,
            0,
            1,
            &[retired],
            |path| fs::symlink_metadata(path)
        )
        .unwrap_err()
        .code(),
        afs_error::IO_CAPACITY
    );
    let errors = vec![Err(std::io::Error::from_raw_os_error(libc::EACCES))];
    assert_eq!(
        wal_budget_entries(errors, limits, 0, 0, &[retired], |path| {
            fs::symlink_metadata(path)
        })
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
}

#[test]
fn p3_lock_owner_release_is_not_delayed_by_inherited_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    // dup retains the same open-file-description lock, as a forked child
    // can do before CLOEXEC closes inherited descriptors. No unsafe fork.
    let inherited = log.inner._lock.try_clone().unwrap();
    let clone = log.clone();
    drop(log);
    assert!(
        LocalLog::open(dir.path(), LocalLogOptions::default()).is_err(),
        "another real LocalLog owner is still alive"
    );
    drop(clone);
    let reopened = LocalLog::open(dir.path(), LocalLogOptions::default());
    assert!(
        reopened.is_ok(),
        "last real owner release must unlock despite a non-authoritative duplicated descriptor"
    );
    let reopened = reopened.unwrap();
    drop(inherited);
    assert!(
        LocalLog::open(dir.path(), LocalLogOptions::default()).is_err(),
        "closing a stale duplicate must not unlock the new owner"
    );
    drop(reopened);
    LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
}
#[test]
fn p3_lock_owner_foreign_drop_does_not_unlock_parent_description() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let parent_description = log.inner._lock.try_clone().unwrap();
    Arc::get_mut(&mut log.inner).unwrap().owner_pid = std::process::id().wrapping_add(1);
    drop(log);
    assert!(
        LocalLog::open(dir.path(), LocalLogOptions::default()).is_err(),
        "dropping an inherited handle must not unlock the parent writer"
    );
    drop(parent_description);
    LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
}
#[tokio::test]
async fn p3_lock_owner_foreign_process_refuses_all_io_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let snapshot = || {
        fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(&path).unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let before = snapshot();
    let actual = std::process::id();
    Arc::get_mut(&mut log.inner).unwrap().owner_pid = actual.wrapping_add(1);
    let image =
        super::super::log::StateImage::encode(&super::super::state::State::default(), 1024 * 1024)
            .unwrap();
    let errors = [
        log.health().await.is_err(),
        log.status().await.is_err(),
        log.recover().await.is_err(),
        log.can_append(LogTip::EMPTY.at(log.term()), 4096)
            .await
            .is_err(),
        log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
            .await
            .is_err(),
        log.install_checkpoint(LogTip::EMPTY, image).await.is_err(),
    ];
    Arc::get_mut(&mut log.inner).unwrap().owner_pid = actual;
    assert!(
        errors.into_iter().all(|error| error),
        "an inherited runtime handle is not a new writer authority"
    );
    assert_eq!(
        snapshot(),
        before,
        "foreign process rejection must precede any storage mutation"
    );
}

async fn install_test_image(
    log: &LocalLog,
    records: Vec<Record>,
    previous: LogTip,
    state: &mut super::super::state::State,
) -> LogTip {
    for record in &records {
        *state = state.replay_record(record).unwrap();
    }
    let tip = log
        .append_and_sync(previous.at(log.term()), records)
        .await
        .unwrap();
    log.install_checkpoint(
        tip,
        super::super::log::StateImage::encode(state, 1024 * 1024).unwrap(),
    )
    .await
    .unwrap();
    tip
}

#[test]
fn p3_initialization_resumes_only_marked_empty_prefixes() {
    for phase in 0..6 {
        let dir = tempfile::tempdir().unwrap();
        let identity = StoreIdentity::new().unwrap();
        fs::write(dir.path().join("INITIALIZING"), []).unwrap();
        if phase == 1 {
            fs::write(
                dir.path().join("IDENTITY.tmp"),
                &identity_bytes(identity)[..12],
            )
            .unwrap();
        }
        if phase == 2 {
            fs::write(dir.path().join("IDENTITY.tmp"), identity_bytes(identity)).unwrap();
        }
        if phase >= 3 {
            fs::write(dir.path().join(IDENTITY), identity_bytes(identity)).unwrap();
            let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
            bytes[..STATIC_BYTES].copy_from_slice(&static_header(identity, LogTip::EMPTY));
            bytes[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
                generation: 0,
                tip: LogTip::EMPTY,
                end: LOG_HEADER_BYTES,
            }));
            fs::write(
                dir.path().join(WAL),
                if phase == 3 {
                    &bytes[..100]
                } else {
                    &bytes[..]
                },
            )
            .unwrap();
        }
        if phase == 4 {
            let current = encode_current(&Current::initial(identity)).unwrap();
            fs::write(dir.path().join("CURRENT.tmp"), &current[..17]).unwrap();
        }
        if phase == 5 {
            write_current(
                dir.path(),
                &Current::initial(identity),
                LocalCheckpointLimits::default(),
            )
            .unwrap();
        }
        let log = LocalLog::open(dir.path(), LocalLogOptions::default())
            .unwrap_or_else(|e| panic!("initialization phase {phase} must resume: {e}"));
        let found = log.identity();
        if phase >= 2 {
            assert_eq!(found, identity);
        }
        assert!(!dir.path().join("INITIALIZING").exists());
        // Initialization installs the term-0 initial manifest; the open
        // that follows selects the first writer term.
        assert_eq!(
            read_current(dir.path(), found).unwrap(),
            Some(Current {
                term: 1,
                ..Current::initial(found)
            })
        );
        assert_eq!(log.inner.state.lock().unwrap().tip, LogTip::EMPTY);
        drop(log);
        assert_eq!(
            LocalLog::open(dir.path(), LocalLogOptions::default())
                .unwrap()
                .identity(),
            found
        );
    }
}

#[tokio::test]
async fn p3_initialization_marker_never_adopts_committed_or_foreign_data() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
        .await
        .unwrap();
    drop(log);
    fs::write(dir.path().join("INITIALIZING"), []).unwrap();
    let original = fs::read(dir.path().join(WAL)).unwrap();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), original);
    assert_eq!(read_identity(dir.path()).unwrap(), identity);
    let foreign = tempfile::tempdir().unwrap();
    fs::write(foreign.path().join("INITIALIZING"), []).unwrap();
    fs::write(foreign.path().join("snapshot.json"), b"foreign input").unwrap();
    assert!(LocalLog::open(foreign.path(), LocalLogOptions::default()).is_err());
    assert_eq!(
        fs::read(foreign.path().join("snapshot.json")).unwrap(),
        b"foreign input"
    );
}

#[tokio::test]
async fn p3_orphan_image_cleanup_preserves_foreign_corrupt_and_unknown_files() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    let bytes =
        super::super::log::StateImage::encode(&super::super::state::State::default(), 65536)
            .unwrap();
    let (own_header, own_ref) = image_header(identity, LogTip::EMPTY, StoreRevision::ZERO, &bytes);
    fs::write(
        dir.path().join(image_name(own_ref)),
        [own_header.as_slice(), &bytes].concat(),
    )
    .unwrap();
    let (foreign_header, foreign_ref) = image_header(
        StoreIdentity::new().unwrap(),
        LogTip::EMPTY,
        StoreRevision::ZERO,
        &bytes,
    );
    let foreign = [foreign_header.as_slice(), &bytes].concat();
    fs::write(dir.path().join(image_name(foreign_ref)), &foreign).unwrap();
    let (bad_header, bad_ref) = image_header(
        identity,
        LogTip {
            sequence: 1,
            record_hash: [7; 32],
        },
        StoreRevision::ZERO,
        &bytes,
    );
    let mut bad = [bad_header.as_slice(), &bytes].concat();
    *bad.last_mut().unwrap() ^= 1;
    fs::write(dir.path().join(image_name(bad_ref)), &bad).unwrap();
    fs::write(
        dir.path().join("checkpoint-unknown.bin"),
        b"unknown original",
    )
    .unwrap();
    drop(log);
    let reopened = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert!(!dir.path().join(image_name(own_ref)).exists());
    assert_eq!(
        fs::read(dir.path().join(image_name(foreign_ref))).unwrap(),
        foreign
    );
    assert_eq!(fs::read(dir.path().join(image_name(bad_ref))).unwrap(), bad);
    assert_eq!(
        fs::read(dir.path().join("checkpoint-unknown.bin")).unwrap(),
        b"unknown original"
    );
    assert_eq!(reopened.recover().await.unwrap().tip, LogTip::EMPTY);
}

#[tokio::test]
async fn p3_recovery_missing_current_refuses_acknowledged_wal() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
        .await
        .unwrap();
    drop(log);
    fs::remove_file(dir.path().join(CURRENT)).unwrap();
    let original = fs::read(dir.path().join(WAL)).unwrap();
    assert!(
        LocalLog::open(dir.path(), LocalLogOptions::default()).is_err(),
        "missing CURRENT must not silently choose the initial manifest"
    );
    assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), original);
}
#[tokio::test]
async fn p3_recovery_rolled_current_refuses_unlisted_nonempty_segment() {
    let dir = tempfile::tempdir().unwrap();
    let limits = LocalLogOptions {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 32,
    };
    let log = LocalLog::open(dir.path(), limits).unwrap();
    let initial = fs::read(dir.path().join(CURRENT)).unwrap();
    let mut tip = LogTip::EMPTY;
    for record in test_records(12) {
        tip = log
            .append_and_sync(tip.at(log.term()), vec![record])
            .await
            .unwrap();
    }
    drop(log);
    let originals: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("wal-"))
        .map(|p| {
            let bytes = fs::read(&p).unwrap();
            (p, bytes)
        })
        .collect();
    assert!(
        originals.iter().any(
            |(p, bytes)| p.file_name().unwrap() != WAL && bytes.len() as u64 > LOG_HEADER_BYTES
        )
    );
    fs::write(dir.path().join(CURRENT), initial).unwrap();
    assert!(
        LocalLog::open(dir.path(), limits).is_err(),
        "rolled CURRENT must not hide the acknowledged suffix"
    );
    for (path, bytes) in originals {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn p3_maintenance_current_temporary_uses_manifest_bound_not_image_bound() {
    let dir = tempfile::tempdir().unwrap();
    let limits = LocalCheckpointLimits {
        image_max_bytes: 256,
        validation_charge_max_bytes: 1024,
        disk_max_bytes: 256 * 1024,
        files_max: 5,
    };
    limits.validate().unwrap();
    let bytes = vec![7u8; 1024];
    fs::write(dir.path().join("CURRENT.tmp"), &bytes).unwrap();
    quarantine_checkpoint_tmp(dir.path(), "CURRENT.tmp", limits)
        .expect("CURRENT.tmp is bounded by manifest size, not a small image cap");
    let evidence = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("orphan-CURRENT.tmp-")
        })
        .unwrap();
    assert_eq!(fs::read(evidence).unwrap(), bytes);
    fs::write(dir.path().join("checkpoint.tmp"), &bytes).unwrap();
    assert!(quarantine_checkpoint_tmp(dir.path(), "checkpoint.tmp", limits).is_err());
    assert_eq!(fs::read(dir.path().join("checkpoint.tmp")).unwrap(), bytes);
}

#[tokio::test]
async fn p3_segments_capacity_rejects_before_mutation_and_missing_seal_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let limits = LocalLogOptions {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 2,
    };
    let log = LocalLog::open(dir.path(), limits).unwrap();
    let snapshot = || {
        fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(&path).unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let mut tip = LogTip::EMPTY;
    let mut rejected = false;
    for record in test_records(24) {
        let before = snapshot();
        match log.append_and_sync(tip.at(log.term()), vec![record]).await {
            Ok(next) => tip = next,
            Err(error) => {
                assert!(error.to_string().contains("segment count"));
                assert_eq!(snapshot(), before);
                rejected = true;
                break;
            }
        }
    }
    assert!(rejected);
    log.health().await.unwrap();
    assert_eq!(log.recover().await.unwrap().tip, tip);
    let manifest = read_current(dir.path(), log.identity()).unwrap().unwrap();
    assert!(manifest.segments[0].sealed.is_some());
    let path = dir
        .path()
        .join(wal_name(manifest.segments[0].base).unwrap());
    drop(log);
    fs::rename(&path, dir.path().join("preserved-required-segment")).unwrap();
    assert!(LocalLog::open(dir.path(), limits).is_err());
}

#[test]
fn wal_budget_allows_retired_segment_cleanup_during_scan() {
    let dir = tempfile::tempdir().unwrap();
    let retired = dir.path().join("wal-00000000000000000001.log");
    let active = dir.path().join("wal-00000000000000000002.log");
    fs::write(&retired, [0; 32]).unwrap();
    fs::write(&active, [0; 64]).unwrap();
    let bytes = wal_budget_entries(
        fs::read_dir(dir.path()).unwrap(),
        LocalLogOptions::default(),
        8,
        0,
        &[budget_retired_segment()],
        |path| {
            if path == retired {
                fs::remove_file(path).unwrap();
            }
            fs::symlink_metadata(path)
        },
    )
    .unwrap();
    assert_eq!(bytes, 72);
    assert!(active.is_file());
}

#[test]
fn wal_budget_propagates_metadata_errors_other_than_disappearance() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("wal-00000000000000000001.log"), [0; 32]).unwrap();
    let result = wal_budget_entries(
        fs::read_dir(dir.path()).unwrap(),
        LocalLogOptions::default(),
        0,
        0,
        &[],
        |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "denied",
            ))
        },
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn p3_segments_rotate_reclaim_and_keep_previous_recovery_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let limits = LocalLogOptions {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 32,
    };
    let log = LocalLog::open(dir.path(), limits).unwrap();
    let identity = log.identity();
    let mut state = super::super::state::State::default();
    let mut records = test_records(12).into_iter();
    let mut tip = LogTip::EMPTY;
    for _ in 0..4 {
        let record = records.next().unwrap();
        state = state.replay_record(&record).unwrap();
        tip = log
            .append_and_sync(tip.at(log.term()), vec![record])
            .await
            .unwrap();
    }
    let wal_names = || {
        fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("wal-") && name.ends_with(".log"))
            .collect::<Vec<_>>()
    };
    assert!(
        wal_names().len() > 1,
        "WAL must rotate at the fixed small segment target"
    );
    log.install_checkpoint(
        tip,
        super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap(),
    )
    .await
    .unwrap();
    for _ in 0..2 {
        for _ in 0..4 {
            let record = records.next().unwrap();
            state = state.replay_record(&record).unwrap();
            tip = log
                .append_and_sync(tip.at(log.term()), vec![record])
                .await
                .unwrap();
        }
        log.install_checkpoint(
            tip,
            super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
    }
    assert!(
        !dir.path().join(WAL).exists(),
        "fully covered sealed initial segment should be reclaimed"
    );
    let manifest = read_current(dir.path(), identity).unwrap().unwrap();
    let image = dir.path().join(image_name(manifest.current.unwrap()));
    let mut corrupt = fs::read(&image).unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&image, &corrupt).unwrap();
    drop(log);
    let log = LocalLog::open(dir.path(), limits).unwrap();
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.tip, tip);
    let image = recovered.checkpoint.unwrap();
    assert_eq!(image.covered.sequence, 8);
    let mut restored = super::super::log::StateImage::decode(&image.bytes, 1024 * 1024).unwrap();
    for record in recovered.records {
        restored = restored.replay_record(&record).unwrap();
    }
    assert_eq!(
        restored.snapshot_bytes_with_batch(0).unwrap(),
        state.snapshot_bytes_with_batch(0).unwrap()
    );
    drop(log);
    let active = wal_names().into_iter().max().unwrap();
    fs::rename(
        dir.path().join(&active),
        dir.path().join("retained-missing-segment"),
    )
    .unwrap();
    assert!(
        LocalLog::open(dir.path(), limits).is_err(),
        "missing required active segment cannot be ignored"
    );
}

#[tokio::test]
async fn p3_checkpoint_bad_current_image_falls_back_without_losing_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    let mut state = super::super::state::State::default();
    let mut records = test_records(3);
    let tip1 = install_test_image(&log, vec![records.remove(0)], LogTip::EMPTY, &mut state).await;
    let tip2 = install_test_image(&log, vec![records.remove(0)], tip1, &mut state).await;
    let tip3 = log
        .append_and_sync(tip2.at(log.term()), records)
        .await
        .unwrap();
    let manifest = read_current(dir.path(), identity).unwrap().unwrap();
    assert_eq!(manifest.previous.unwrap().covered, tip1);
    let current = dir.path().join(image_name(manifest.current.unwrap()));
    let mut corrupt = fs::read(&current).unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&current, &corrupt).unwrap();
    drop(log);
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.tip, tip3);
    assert_eq!(recovered.checkpoint.unwrap().covered, tip1);
    assert_eq!(recovered.records.len(), 2);
    assert_eq!(
        fs::read(&current).unwrap(),
        corrupt,
        "corruption evidence retained"
    );
    drop(log);
    // A previous valid image never authorizes ignoring a broken ACK chain.
    let mut wal = fs::read(dir.path().join(WAL)).unwrap();
    *wal.last_mut().unwrap() ^= 1;
    fs::write(dir.path().join(WAL), &wal).unwrap();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), wal);
}

#[tokio::test]
async fn p3_checkpoint_identity_and_manifest_corruption_refuse_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    let mut state = super::super::state::State::default();
    install_test_image(&log, test_records(1), LogTip::EMPTY, &mut state).await;
    let manifest = read_current(dir.path(), identity).unwrap().unwrap();
    let image = dir.path().join(image_name(manifest.current.unwrap()));
    let original = fs::read(&image).unwrap();
    let mut foreign = original.clone();
    foreign[6] ^= 1;
    fs::write(&image, &foreign).unwrap();
    drop(log);
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    fs::write(&image, &original).unwrap();
    let current = dir.path().join(CURRENT);
    let mut broken = fs::read(&current).unwrap();
    broken[6] ^= 1;
    fs::write(&current, &broken).unwrap();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    assert_eq!(fs::read(current).unwrap(), broken);
}

#[test]
fn p3_checkpoint_quarantined_temporary_is_charged_and_symlink_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let limits = LocalCheckpointLimits {
        image_max_bytes: 256,
        validation_charge_max_bytes: 1024,
        disk_max_bytes: 256 * 1024,
        files_max: 5,
    };
    limits.validate().unwrap();
    fs::write(dir.path().join("checkpoint.tmp"), [1u8; 256]).unwrap();
    quarantine_checkpoint_tmp(dir.path(), "checkpoint.tmp", limits).unwrap();
    assert!(checkpoint_budget(dir.path(), 256 * 1024, 0, limits).is_err());
    let outside = dir.path().join("missing");
    std::os::unix::fs::symlink(&outside, dir.path().join(CURRENT)).unwrap();
    assert!(read_current(dir.path(), StoreIdentity::new().unwrap()).is_err());
}

#[tokio::test]
async fn p3_local_write_failure_latches_until_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    // Real write(2) EBADF, rather than a hook returning an invented error.
    log.inner.state.lock().unwrap().file = File::open(dir.path().join(WAL)).unwrap();
    assert!(
        log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
            .await
            .is_err()
    );
    assert!(log.inner.state.lock().unwrap().poisoned);
    assert!(log.health().await.is_err());
    assert!(log.recover().await.is_err());
    assert!(
        log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
            .await
            .is_err()
    );
    drop(log);
    let reopened = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(reopened.recover().await.unwrap().tip, LogTip::EMPTY);
}

#[tokio::test]
async fn p3_local_wal_budget_refuses_before_changing_file() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(
        dir.path(),
        LocalLogOptions {
            record_max_bytes: 1024,
            batch_max_bytes: 4096,
            wal_max_bytes: 8192,
            ..Default::default()
        },
    )
    .unwrap();
    let mut tip = LogTip::EMPTY;
    let mut exhausted = false;
    for record in test_records(64) {
        let before = fs::read(dir.path().join(WAL)).unwrap();
        match log.append_and_sync(tip.at(log.term()), vec![record]).await {
            Ok(next) => tip = next,
            Err(error) => {
                assert!(error.to_string().contains("WAL byte budget"));
                assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), before);
                exhausted = true;
                break;
            }
        }
    }
    assert!(exhausted);
    assert!(tip.sequence > 0);
    assert!(fs::metadata(dir.path().join(WAL)).unwrap().len() <= 8192);
    assert_eq!(log.recover().await.unwrap().tip, tip);
    log.health().await.unwrap();
}

#[tokio::test]
async fn p3_local_group_reopens_exact_records_and_stable_identity() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    let original = test_records(2);
    let expected = original
        .iter()
        .map(|r| crate::meta::store::codec::encode_bounded(r, 1024 * 1024).unwrap())
        .collect::<Vec<_>>();
    let tip = log
        .append_and_sync(LogTip::EMPTY.at(log.term()), original)
        .await
        .unwrap();
    log.health().await.unwrap();
    drop(log);
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(log.identity(), identity);
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.tip, tip);
    assert_eq!(
        recovered
            .records
            .iter()
            .map(|r| crate::meta::store::codec::encode_bounded(r, 1024 * 1024).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn p3_unacknowledged_complete_record_replays_and_incomplete_tail_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let identity = log.identity();
    drop(log);
    let record = test_records(1).pop().unwrap();
    // Written as the closed writer (term 1) would have written it.
    let (frame, tip) = encode_frame(identity, LogTip::EMPTY.at(1), record, 1024 * 1024).unwrap();
    let mut file = OpenOptions::new()
        .append(true)
        .open(dir.path().join(WAL))
        .unwrap();
    file.write_all(&frame).unwrap();
    file.write_all(&[4, 5, 6]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(log.recover().await.unwrap().tip, tip);
    assert_eq!(
        fs::metadata(dir.path().join(WAL)).unwrap().len(),
        LOG_HEADER_BYTES + frame.len() as u64
    );
    let evidence = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("incomplete-tail-")
        })
        .unwrap();
    assert_eq!(fs::read(evidence).unwrap(), [4, 5, 6]);
    drop(log);
    assert_eq!(
        LocalLog::open(dir.path(), LocalLogOptions::default())
            .unwrap()
            .recover()
            .await
            .unwrap()
            .tip,
        tip
    );
}

#[tokio::test]
async fn p3_acknowledged_truncation_is_not_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
        .await
        .unwrap();
    drop(log);
    let path = dir.path().join(WAL);
    let file = open_file(&path, false).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = fs::read(&path).unwrap();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[tokio::test]
async fn p3_complete_tail_corruption_is_not_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
        .await
        .unwrap();
    drop(log);
    let path = dir.path().join(WAL);
    let mut file = open_file(&path, false).unwrap();
    file.seek(SeekFrom::Start(LOG_HEADER_BYTES + 8)).unwrap();
    file.write_all(&[255]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = fs::read(&path).unwrap();
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn p3_identity_mix_missing_wal_and_legacy_are_rejected() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    drop(LocalLog::open(a.path(), LocalLogOptions::default()).unwrap());
    drop(LocalLog::open(b.path(), LocalLogOptions::default()).unwrap());
    fs::copy(b.path().join(IDENTITY), a.path().join(IDENTITY)).unwrap();
    assert!(LocalLog::open(a.path(), LocalLogOptions::default()).is_err());
    fs::rename(b.path().join(WAL), b.path().join("preserved-wal")).unwrap();
    assert!(LocalLog::open(b.path(), LocalLogOptions::default()).is_err());
    for marker in ["snapshot", "wal"] {
        let old = tempfile::tempdir().unwrap();
        fs::write(old.path().join(marker), b"legacy").unwrap();
        let error = match LocalLog::open(old.path(), LocalLogOptions::default()) {
            Err(error) => error,
            Ok(_) => panic!("old {marker} directory was accepted"),
        };
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains("empty directory"), "{error}");
        assert!(error.to_string().contains("original binary"), "{error}");
        assert_eq!(fs::read(old.path().join(marker)).unwrap(), b"legacy");
        assert!(!old.path().join(IDENTITY).exists());
        assert!(!old.path().join(WAL).exists());
    }
}

#[tokio::test]
async fn p3_live_health_detects_replaced_file_and_root() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    fs::rename(dir.path().join(WAL), dir.path().join("original-wal")).unwrap();
    fs::copy(dir.path().join("original-wal"), dir.path().join(WAL)).unwrap();
    assert!(log.health().await.is_err());
    assert!(
        log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
            .await
            .is_err()
    );
    let other = tempfile::tempdir().unwrap();
    drop(LocalLog::open(other.path(), LocalLogOptions::default()).unwrap());
    fs::copy(other.path().join(IDENTITY), dir.path().join(IDENTITY)).unwrap();
    assert!(log.health().await.is_err());
}

#[test]
fn p3_damaged_tail_marker_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    drop(LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap());
    let mut file = open_file(&dir.path().join(WAL), false).unwrap();
    file.seek(SeekFrom::Start(STATIC_BYTES as u64)).unwrap();
    file.write_all(&[1]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(LocalLog::open(dir.path(), LocalLogOptions::default()).is_err());
}

fn current_term(dir: &Path) -> u64 {
    read_current(dir, read_identity(dir).unwrap())
        .unwrap()
        .unwrap()
        .term
}

/// Terms of every complete frame in the first WAL segment, in order.
fn wal_terms(dir: &Path) -> Vec<u64> {
    let identity = read_identity(dir).unwrap();
    let bytes = fs::read(dir.join(WAL)).unwrap();
    let mut offset = LOG_HEADER_BYTES as usize;
    let mut tip = LogTip::EMPTY;
    let mut terms = Vec::new();
    while offset + 8 <= bytes.len() {
        let size = frame_length(&bytes[offset..], 1024 * 1024).unwrap();
        let (_, next, term) =
            decode_frame(identity, tip, &bytes[offset..offset + size], 1024 * 1024).unwrap();
        terms.push(term);
        tip = next;
        offset += size;
    }
    terms
}

#[tokio::test]
async fn term_increases_on_every_open_and_frames_carry_it() {
    let dir = tempfile::tempdir().unwrap();
    let records = test_records(3);
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(current_term(dir.path()), 1, "a new log starts at term 1");
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.term, 1);
    let tip = log
        .append_and_sync(recovered.cursor(), records[..1].to_vec())
        .await
        .unwrap();
    drop(log);
    assert_eq!(
        current_term(dir.path()),
        1,
        "close does not change the term"
    );
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(current_term(dir.path()), 2);
    assert_eq!(log.recover().await.unwrap().term, 2);
    let tip = log
        .append_and_sync(tip.at(2), records[1..].to_vec())
        .await
        .unwrap();
    assert_eq!(wal_terms(dir.path()), [1, 2, 2]);
    drop(log);
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let recovered = log.recover().await.unwrap();
    assert_eq!((recovered.term, recovered.tip), (3, tip));
    assert_eq!(recovered.records.len(), 3);
}

/// The directory lock still excludes a second writer, in this process and
/// from another process, and a refused open never advances the term.
#[tokio::test]
async fn flock_still_excludes_writers_and_refused_open_keeps_term() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let before = fs::read(dir.path().join(CURRENT)).unwrap();
    let error = LocalLog::open(dir.path(), LocalLogOptions::default())
        .err()
        .expect("a second writer must be refused");
    assert!(error.to_string().contains("already locked"), "{error}");
    let other_process = std::process::Command::new("flock")
        .arg("--nonblock")
        .arg(dir.path().join("LOCK"))
        .arg("true")
        .status()
        .unwrap();
    assert!(!other_process.success(), "another process acquired LOCK");
    assert_eq!(fs::read(dir.path().join(CURRENT)).unwrap(), before);
    assert_eq!(log.recover().await.unwrap().term, 1);
    drop(log);
    let after = std::process::Command::new("flock")
        .arg("--nonblock")
        .arg(dir.path().join("LOCK"))
        .arg("true")
        .status()
        .unwrap();
    assert!(after.success(), "released lock must be available");
}

/// Damaged data whose chain terms decrease, or exceed the CURRENT term, is
/// refused at open and left in place.
#[tokio::test]
async fn open_refuses_decreasing_or_future_frame_terms() {
    for (terms, marker) in [
        ([1, 0], "decreases below previous term 1"),
        ([1, 2], "exceeds current term 1"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
        let identity = log.identity();
        drop(log);
        let mut tip = LogTip::EMPTY;
        let mut file = OpenOptions::new()
            .append(true)
            .open(dir.path().join(WAL))
            .unwrap();
        for (record, term) in test_records(2).into_iter().zip(terms) {
            let (frame, next) = encode_frame(identity, tip.at(term), record, 1024 * 1024).unwrap();
            file.write_all(&frame).unwrap();
            tip = next;
        }
        file.sync_all().unwrap();
        drop(file);
        let before = (
            fs::read(dir.path().join(WAL)).unwrap(),
            fs::read(dir.path().join(CURRENT)).unwrap(),
        );
        let error = LocalLog::open(dir.path(), LocalLogOptions::default())
            .err()
            .expect("bad chain terms must be refused");
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains(marker), "{error}");
        assert_eq!(
            (
                fs::read(dir.path().join(WAL)).unwrap(),
                fs::read(dir.path().join(CURRENT)).unwrap(),
            ),
            before,
            "refused open changed the evidence"
        );
    }
}

/// A real v2 CURRENT (the same manifest without the term field) is
/// refused with a rebuild instruction and left in place.
#[test]
fn open_refuses_v2_current() {
    let dir = tempfile::tempdir().unwrap();
    drop(LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap());
    let v3 = fs::read(dir.path().join(CURRENT)).unwrap();
    assert_eq!(v3[..6], *CURRENT_MAGIC);
    // postcard: 48 identity bytes, then the one-byte term varint (term 1).
    assert_eq!(v3[6 + 48], 1);
    let mut v2 = b"AFSM\0\x02".to_vec();
    v2.extend_from_slice(&v3[6..6 + 48]);
    v2.extend_from_slice(&v3[6 + 49..v3.len() - 36]);
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.current.v1");
    hash.update(&v2);
    v2.extend_from_slice(hash.finalize().as_bytes());
    let crc = crc32c::crc32c(&v2);
    v2.extend_from_slice(&crc.to_be_bytes());
    fs::write(dir.path().join(CURRENT), &v2).unwrap();
    let error = LocalLog::open(dir.path(), LocalLogOptions::default())
        .err()
        .expect("v2 CURRENT must be refused");
    assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
    let message = error.to_string();
    for part in [
        "Meta CURRENT manifest",
        "CURRENT",
        "version 2",
        "supports version 3",
        "rebuild the data directory",
    ] {
        assert!(message.contains(part), "{part}: {message}");
    }
    assert_eq!(fs::read(dir.path().join(CURRENT)).unwrap(), v2);
}

/// Same-process takeover: a handle that lost its lock is fenced by the
/// newer CURRENT term before it can write anything.
#[tokio::test]
async fn stale_handle_is_fenced_by_newer_current_term() {
    let dir = tempfile::tempdir().unwrap();
    let old = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let tip = old
        .append_and_sync(LogTip::EMPTY.at(1), test_records(1))
        .await
        .unwrap();
    old.release_lock_for_takeover().unwrap();
    let new = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    assert_eq!(current_term(dir.path()), 2);
    let wal = fs::read(dir.path().join(WAL)).unwrap();
    let error = old
        .append_and_sync(tip.at(1), test_records(2)[1..].to_vec())
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_LOG_FENCED);
    assert!(
        error.to_string().contains("found term 1, current term 2"),
        "{error}"
    );
    assert_eq!(
        fs::read(dir.path().join(WAL)).unwrap(),
        wal,
        "no byte written"
    );
    new.append_and_sync(tip.at(2), test_records(2)[1..].to_vec())
        .await
        .unwrap();
    assert_eq!(wal_terms(dir.path()), [1, 2]);
}
