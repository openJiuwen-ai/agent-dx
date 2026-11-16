use super::*;
use std::os::unix::fs::MetadataExt;

#[derive(Default)]
struct State {
    pending: Option<RootReservation>,
    active: Option<RootLocation>,
    activations: usize,
    aborts: usize,
    fail_before_activate: bool,
    lose_activate_reply: bool,
    fail_abort: bool,
}

#[derive(Default)]
struct Meta(Mutex<State>);

impl RootMeta for Meta {
    fn reserve_root(&self, _: &RootId, _: &str) -> Result<RootReservation> {
        Err(invalid_grant("not used by Home preparation tests"))
    }

    fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant> {
        let mut state = self.0.lock().unwrap();
        assert_eq!(state.pending.as_ref(), Some(prepared.reservation()));
        state.activations += 1;
        if state.fail_before_activate {
            return Err(unavailable_grant("activation unavailable"));
        }
        let reservation = state.pending.take().unwrap();
        state.active = Some(RootLocation {
            id: reservation.id.clone(),
            epoch: reservation.epoch,
            home_node_id: reservation.home_node_id.clone(),
            home_session_id: reservation.session_id.clone(),
        });
        if state.lose_activate_reply {
            return Err(unavailable_grant("activation reply lost"));
        }
        Ok(RootGrant {
            id: reservation.id,
            epoch: reservation.epoch,
            home_node_id: reservation.home_node_id.clone(),
            home_session_id: reservation.session_id.clone(),
            holder_node_id: reservation.home_node_id,
            session_id: reservation.session_id,
            access_generation: 1,
            rights: vec![RootRight::Lookup, RootRight::Write],
            fencing_token: "grant-token".into(),
        })
    }

    fn abort_root(&self, reservation: &RootReservation) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        state.aborts += 1;
        if state.fail_abort {
            return Err(unavailable_grant("abort unavailable"));
        }
        assert_eq!(state.pending.as_ref(), Some(reservation));
        state.pending = None;
        Ok(())
    }

    fn deactivate_root(&self, _: &RootId, _: u64, _: &str, _: &str) -> Result<()> {
        Err(invalid_grant("not used by creation tests"))
    }
    fn lookup_root(&self, _: &RootId) -> Result<Option<RootLocation>> {
        Ok(self.0.lock().unwrap().active.clone())
    }
    fn lookup_root_state(&self, _: &RootId) -> Result<RootLookupState> {
        let state = self.0.lock().unwrap();
        Ok(RootLookupState {
            location: state.active.clone(),
            reservation: state.pending.clone(),
        })
    }
    fn list_owner_roots(&self, home: &str) -> Result<OwnerRootInventory> {
        let state = self.0.lock().unwrap();
        Ok(OwnerRootInventory {
            active: state
                .active
                .clone()
                .filter(|root| root.home_node_id == home)
                .into_iter()
                .collect(),
            pending: state
                .pending
                .clone()
                .filter(|root| root.home_node_id == home)
                .into_iter()
                .collect(),
        })
    }
    fn acquire_root(&self, _: &RootId, _: RootRight) -> Result<RootGrant> {
        Err(invalid_grant("not used by creation tests"))
    }
    fn validate_root_access(&self, _: &PresentedRootAccess, _: &str) -> Result<RootGrant> {
        Err(invalid_grant("not used by creation tests"))
    }
    fn recover_root(&self, _: &LocalRootRecord, _: &str) -> Result<RootGrant> {
        Err(invalid_grant("not used by creation tests"))
    }
}

struct Fixture {
    temp: tempfile::TempDir,
    meta: Arc<Meta>,
    manager: RootManager,
    reservation: RootReservation,
    params: RootCreateParams,
}

#[test]
fn root_ids_encode_names_losslessly() {
    assert_eq!(
        root_id_from_name(OsStr::new("agent-a")).unwrap().0,
        "root-6167656e742d61"
    );
    assert_eq!(
        root_id_from_name(OsStr::from_bytes(&[0xff, b'a', 0x80]))
            .unwrap()
            .0,
        "root-ff6180"
    );
}

/// `/public` 下由 outlet 自用的两个目录名不能作为 workspace 名。
///
/// `staging_path` 把暂存文件放进 `<public_root>/.pub/`，也就是"以 workspace 名
/// 命名的目录"里。一旦允许 `.pub` 成为 workspace，发布就会与隔离互相搬运：
/// 每一轮先把它挪进 `.quarantine`，发布又用 `create_dir_all` 把它建回来，
/// `rename` 撞目录失败，下一轮重来。实测单节点 1 个隔离目录/秒、无限增长，
/// tmpfs 堆满后整个名字层的发布一起失败。
#[test]
fn reserved_name_layer_directories_cannot_become_roots() {
    for name in [".pub", ".quarantine"] {
        let error = validate_root_name(OsStr::new(name)).unwrap_err();
        assert_eq!(
            error.code(),
            afs_error::NODE_VFS_INVALID,
            "{name} 是名字层的保留名，必须拒绝创建"
        );
    }
}

/// 只拒保留名，不拒所有点开头的名字：`.cache` 这类仍是合法的 workspace 名。
#[test]
fn a_dot_prefixed_name_that_is_not_reserved_still_works() {
    assert!(validate_root_name(OsStr::new(".cache")).is_ok());
    assert!(validate_root_name(OsStr::new("pub")).is_ok());
    assert!(validate_root_name(OsStr::new("demo")).is_ok());
}

/// 保留名必须在**问 Meta 之前**就被拒掉。
///
/// 这条断言用错误码区分来源：夹具里的 `reserve_root` 只会返回"授权不可用"，
/// 所以拿到 `NODE_VFS_INVALID` 就证明这次调用根本没走到 Meta。
#[test]
fn reserving_a_reserved_name_is_refused_before_meta_is_asked() {
    let temp = tempfile::tempdir().unwrap();
    let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
    let manager = RootManager::open(
        "home".into(),
        "home-session".into(),
        Arc::new(Meta::default()),
        disk,
    )
    .unwrap();

    let error = manager.reserve_workspace(OsStr::new(".pub")).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
}

impl Fixture {
    fn new(mode: u32) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let owner = temp.path().metadata().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let meta = Arc::new(Meta::default());
        let manager =
            RootManager::open("home".into(), "home-session".into(), meta.clone(), disk).unwrap();
        let id = root_id_from_name(OsStr::new("agent-a")).unwrap();
        let reservation = RootReservation {
            create_intent_id: format!("entry:entry-session:{}", id.0),
            id,
            epoch: 1,
            home_node_id: "home".into(),
            session_id: "home-session".into(),
            requester_node_id: "entry".into(),
            requester_session_id: "entry-session".into(),
            prepare_token: "prepare-token".into(),
        };
        meta.0.lock().unwrap().pending = Some(reservation.clone());
        let params = RootCreateParams {
            name: b"agent-a".to_vec(),
            uid: owner.uid(),
            gid: owner.gid(),
            mode,
        };
        Self {
            temp,
            meta,
            manager,
            reservation,
            params,
        }
    }
    fn path(&self) -> std::path::PathBuf {
        self.temp
            .path()
            .join(root_data_dir(OsStr::new("agent-a"), 1).unwrap().as_path())
    }
    fn prepare(&self) -> Result<()> {
        self.manager
            .prepare_owner_root("entry", &self.reservation, &self.params)
    }
}

#[test]
fn prepared_directory_reuse_and_duplicate_preserve_original_permissions() {
    for mode in [0o750, 0] {
        let fixture = Fixture::new(mode);
        fixture.meta.0.lock().unwrap().fail_before_activate = true;
        assert!(fixture.prepare().is_err());
        assert_eq!(fixture.meta.0.lock().unwrap().aborts, 0);
        assert_eq!(fixture.manager.catalog.scan_roots().unwrap().len(), 1);
        fixture.meta.0.lock().unwrap().fail_before_activate = false;
        fixture.prepare().unwrap();
        let error = fixture.prepare().unwrap_err();
        assert_eq!(error.code(), afs_error::IO_ALREADY_EXISTS);
        let stat = fixture.path().metadata().unwrap();
        assert_eq!(stat.mode() & 0o7777, mode);
        assert_eq!(
            (stat.uid(), stat.gid()),
            (fixture.params.uid, fixture.params.gid)
        );
        assert_eq!(fixture.meta.0.lock().unwrap().activations, 2);
        assert!(
            fixture
                .manager
                .enter_root(&fixture.reservation.id, RootRight::Write)
                .is_ok()
        );
    }
}

#[test]
fn prepared_directory_retry_rejects_changed_permissions() {
    for (mode, retry_mode, change_uid, change_gid) in [
        (0o755, 0o700, false, false),
        (0o700, 0o777, false, false),
        (0o755, 0o755, true, false),
        (0o755, 0o755, false, true),
    ] {
        let fixture = Fixture::new(mode);
        fixture.meta.0.lock().unwrap().fail_before_activate = true;
        assert!(fixture.prepare().is_err());
        let records = fixture.manager.catalog.scan_roots().unwrap();
        assert_eq!(records.len(), 1);
        fixture.meta.0.lock().unwrap().fail_before_activate = false;
        let mut changed = fixture.params.clone();
        changed.mode = retry_mode;
        changed.uid += u32::from(change_uid);
        changed.gid += u32::from(change_gid);
        let error = fixture
            .manager
            .prepare_owner_root("entry", &fixture.reservation, &changed)
            .unwrap_err();
        assert_eq!(error.code(), afs_error::IO_INVALID);
        assert_eq!(crate::error::errno(&error), libc::EINVAL);
        let stat = fixture.path().metadata().unwrap();
        assert_eq!(stat.mode() & 0o7777, mode);
        assert_eq!(
            (stat.uid(), stat.gid()),
            (fixture.params.uid, fixture.params.gid)
        );
        assert_eq!(fixture.manager.catalog.scan_roots().unwrap(), records);
        {
            let state = fixture.meta.0.lock().unwrap();
            assert_eq!(state.activations, 1);
            assert_eq!(state.aborts, 0);
            assert_eq!(state.pending.as_ref(), Some(&fixture.reservation));
            assert!(state.active.is_none());
        }
        fixture.prepare().unwrap();
        assert_eq!(fixture.meta.0.lock().unwrap().activations, 2);
    }
}

#[test]
fn lost_activation_reply_remains_uncertain_and_keeps_durable_evidence() {
    let fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().lose_activate_reply = true;
    assert_eq!(
        fixture.prepare().unwrap_err().message(),
        "activation reply lost"
    );
    let state = fixture.meta.0.lock().unwrap();
    assert_eq!(state.activations, 1);
    assert_eq!(state.aborts, 0);
    assert!(state.active.is_some());
    assert!(fixture.path().is_dir());
    assert_eq!(fixture.manager.catalog.scan_roots().unwrap().len(), 1);
    assert!(
        fixture
            .manager
            .enter_root(&fixture.reservation.id, RootRight::Write)
            .is_err()
    );
}

#[test]
fn stale_token_and_wrong_peer_do_not_touch_the_directory() {
    let fixture = Fixture::new(0o750);
    let mut replaced = fixture.reservation.clone();
    replaced.prepare_token = "new-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(replaced.clone());
    assert!(fixture.prepare().is_err());
    assert!(
        fixture
            .manager
            .prepare_owner_root("other-node", &replaced, &fixture.params)
            .is_err()
    );
    assert!(!fixture.path().exists());
    assert_eq!(
        fixture.meta.0.lock().unwrap().pending.as_ref(),
        Some(&replaced)
    );
}

#[test]
fn preparation_failure_attempts_abort_once_and_keeps_nonempty_residual() {
    let fixture = Fixture::new(0o750);
    std::fs::create_dir(fixture.path()).unwrap();
    std::fs::write(fixture.path().join("payload"), b"keep").unwrap();
    fixture.meta.0.lock().unwrap().fail_abort = true;
    assert_eq!(
        fixture.prepare().unwrap_err().code(),
        afs_error::IO_DIRECTORY_NOT_EMPTY
    );
    let state = fixture.meta.0.lock().unwrap();
    assert_eq!(state.aborts, 1);
    assert_eq!(state.activations, 0);
    assert_eq!(state.pending.as_ref(), Some(&fixture.reservation));
    assert_eq!(
        std::fs::read(fixture.path().join("payload")).unwrap(),
        b"keep"
    );
}

#[test]
fn concurrent_notifications_prepare_and_activate_only_once() {
    let fixture = Fixture::new(0o750);
    let results = std::thread::scope(|scope| {
        let first = scope.spawn(|| fixture.prepare());
        let second = scope.spawn(|| fixture.prepare());
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let error = results.into_iter().find_map(Result::err).unwrap();
    assert_eq!(error.code(), afs_error::IO_ALREADY_EXISTS);
    assert_eq!(fixture.meta.0.lock().unwrap().activations, 1);
    assert_eq!(fixture.manager.catalog.scan_roots().unwrap().len(), 1);
}

#[test]
fn replacement_cleans_obsolete_empty_preparation_without_home_restart() {
    for directory_already_removed in [false, true] {
        let mut fixture = Fixture::new(0o750);
        fixture.meta.0.lock().unwrap().fail_before_activate = true;
        assert!(fixture.prepare().is_err());
        let old = fixture.reservation.clone();
        let old_path = fixture.path();
        if directory_already_removed {
            std::fs::remove_dir(&old_path).unwrap();
        }
        fixture.reservation.epoch += 1;
        fixture.reservation.prepare_token = "replacement-token".into();
        {
            let mut state = fixture.meta.0.lock().unwrap();
            state.pending = Some(fixture.reservation.clone());
            state.fail_before_activate = false;
        }

        fixture.prepare().unwrap();
        assert!(!old_path.exists());
        let records = fixture.manager.catalog.scan_roots().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].epoch, fixture.reservation.epoch);
        assert!(
            fixture
                .temp
                .path()
                .join(records[0].data_dir.as_path())
                .is_dir()
        );
        assert!(
            fixture
                .manager
                .prepare_owner_root("entry", &old, &fixture.params)
                .is_err()
        );
        assert_eq!(fixture.manager.catalog.scan_roots().unwrap(), records);
    }
}

#[test]
fn replacement_preserves_nonempty_obsolete_preparation() {
    let mut fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().fail_before_activate = true;
    assert!(fixture.prepare().is_err());
    std::fs::write(fixture.path().join("payload"), b"keep").unwrap();
    fixture.reservation.epoch += 1;
    fixture.reservation.prepare_token = "replacement-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(fixture.reservation.clone());

    fixture.meta.0.lock().unwrap().fail_before_activate = false;
    fixture.prepare().unwrap();
    assert_eq!(
        std::fs::read(fixture.path().join("payload")).unwrap(),
        b"keep"
    );
    let records = fixture.manager.catalog.scan_roots().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].epoch, fixture.reservation.epoch);
    assert!(
        fixture
            .temp
            .path()
            .join(records[0].data_dir.as_path())
            .is_dir()
    );
}

#[test]
fn same_epoch_conflict_does_not_remove_preparation() {
    let mut fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().fail_before_activate = true;
    assert!(fixture.prepare().is_err());
    let records = fixture.manager.catalog.scan_roots().unwrap();
    fixture.reservation.prepare_token = "conflicting-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(fixture.reservation.clone());

    assert!(fixture.prepare().is_err());
    assert!(fixture.path().is_dir());
    assert_eq!(fixture.manager.catalog.scan_roots().unwrap(), records);
}

#[test]
fn replacement_does_not_remove_a_noncanonical_catalog_directory() {
    let mut fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().fail_before_activate = true;
    assert!(fixture.prepare().is_err());
    let mut record = fixture.manager.catalog.scan_roots().unwrap().remove(0);
    fixture
        .manager
        .catalog
        .remove_prepared_root(&record)
        .unwrap();
    record.data_dir = StoragePath::new("foreign-directory").unwrap();
    let foreign = fixture.temp.path().join(record.data_dir.as_path());
    std::fs::create_dir(&foreign).unwrap();
    fixture
        .manager
        .catalog
        .persist_prepared_root(&record)
        .unwrap();
    fixture.reservation.epoch += 1;
    fixture.reservation.prepare_token = "replacement-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(fixture.reservation.clone());

    assert!(fixture.prepare().is_err());
    assert!(foreign.is_dir());
    assert!(fixture.path().is_dir());
    assert_eq!(fixture.manager.catalog.scan_roots().unwrap(), vec![record]);
}

#[test]
fn startup_cleans_replaced_pending_preparation() {
    let fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().fail_before_activate = true;
    assert!(fixture.prepare().is_err());
    let old_path = fixture.path();
    let mut replacement = fixture.reservation.clone();
    replacement.epoch += 1;
    replacement.prepare_token = "replacement-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(replacement);
    drop(fixture.manager);

    let disk = Arc::new(LocalFs::open(fixture.temp.path()).unwrap());
    let manager = RootManager::open(
        "home".into(),
        "new-home-session".into(),
        fixture.meta.clone(),
        disk,
    )
    .expect("startup must reconcile a pending takeover with older local evidence");
    assert!(manager.cached_local_roots().unwrap().is_empty());
    assert!(manager.catalog.scan_roots().unwrap().is_empty());
    assert!(!old_path.exists());
    assert!(fixture.meta.0.lock().unwrap().pending.is_none());
    assert_eq!(fixture.meta.0.lock().unwrap().aborts, 1);
}

#[test]
fn startup_preserves_nonempty_replaced_preparation() {
    let fixture = Fixture::new(0o750);
    fixture.meta.0.lock().unwrap().fail_before_activate = true;
    assert!(fixture.prepare().is_err());
    let payload = fixture.path().join("payload");
    std::fs::write(&payload, b"keep").unwrap();
    let mut replacement = fixture.reservation.clone();
    replacement.epoch += 1;
    replacement.prepare_token = "replacement-token".into();
    fixture.meta.0.lock().unwrap().pending = Some(replacement);
    drop(fixture.manager);

    let disk = Arc::new(LocalFs::open(fixture.temp.path()).unwrap());
    let manager = RootManager::open(
        "home".into(),
        "new-home-session".into(),
        fixture.meta,
        disk.clone(),
    )
    .expect("fenced nonempty preparation must not block startup");
    assert_eq!(std::fs::read(payload).unwrap(), b"keep");
    assert!(manager.catalog.scan_roots().unwrap().is_empty());
}
