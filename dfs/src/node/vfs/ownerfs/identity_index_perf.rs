use std::{
    hint::black_box,
    time::{Instant, UNIX_EPOCH},
};

use serde_json::json;

use super::*;

const HIT_COUNT: usize = 256;
const ROUNDS: usize = 6;
const SCALES: [usize; 3] = [100, 1000, 10000];

fn path(index: usize) -> StoragePath {
    StoragePath::new(format!("file-{index:05}")).unwrap()
}

fn child_path(parent: &str, child: &str) -> StoragePath {
    StoragePath::new(format!("{parent}/{child}")).unwrap()
}

fn root_id(name: &str) -> RootId {
    RootId(name.to_owned())
}

fn identity(index: usize) -> files::FileIdentity {
    files::FileIdentity(index.to_le_bytes().to_vec())
}

fn attrs(kind: FileKind) -> FileAttributes {
    FileAttributes {
        kind,
        size: 0,
        blocks: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
    }
}

fn state_with_entries(scale: usize) -> (OwnerState, RootId) {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    for index in 0..scale {
        let inode = state.inode_for_path(
            root.clone(),
            path(index),
            identity(index),
            attrs(FileKind::Regular),
            FileKind::Regular,
        );
        assert_eq!(inode, OWNERFS_ROOT_INODE + 1 + index as u64);
    }
    assert_eq!(state.paths.len(), scale);
    assert_eq!(state.identities.len(), scale);
    assert_eq!(state.inodes.len(), scale);
    (state, root)
}

fn emit_sample(method: &str, scale: usize, round: usize, elapsed_ns: u128) {
    let row = json!({
        "method": method,
        "scale": scale,
        "round": round,
        "hits": HIT_COUNT,
        "elapsed_ns": elapsed_ns,
    });
    println!("G2_INDEX_SAMPLE {row}");
}

#[test]
#[ignore = "diagnostic benchmark; run on Linux ARM64 with --ignored --nocapture --test-threads=1"]
fn ignored_owner_identity_index_benchmark() {
    for scale in SCALES {
        for round in 0..ROUNDS {
            let (mut state, root) = state_with_entries(scale);
            let target = scale / 2;
            let target_path = path(target);
            let target_identity = identity(target);
            let inode = *state
                .paths
                .get(&(root.clone(), target_path.clone()))
                .expect("target path is present");
            let started = Instant::now();
            for hit in 0..HIT_COUNT {
                let mut attributes = attrs(FileKind::Regular);
                attributes.size = hit as u64;
                let observed = state.inode_for_path(
                    black_box(root.clone()),
                    black_box(target_path.clone()),
                    black_box(target_identity.clone()),
                    black_box(attributes),
                    black_box(FileKind::Regular),
                );
                assert_eq!(black_box(observed), inode);
            }
            emit_sample("inode_for_path", scale, round, started.elapsed().as_nanos());
            assert_eq!(state.paths.len(), scale);
            assert_eq!(state.identities.len(), scale);
            assert_eq!(state.inodes.len(), scale);
        }

        for round in 0..ROUNDS {
            let (mut state, root) = state_with_entries(scale);
            let target = scale / 2;
            let target_identity = identity(target);
            let inode = *state
                .identities
                .get(&(root, target_identity.clone().0))
                .expect("target identity is present");
            let started = Instant::now();
            for hit in 0..HIT_COUNT {
                let mut attributes = attrs(FileKind::Regular);
                attributes.size = hit as u64;
                state
                    .update_record(
                        black_box(inode),
                        black_box(target_identity.clone()),
                        black_box(attributes),
                        black_box(FileKind::Regular),
                    )
                    .unwrap();
            }
            emit_sample("update_record", scale, round, started.elapsed().as_nanos());
            assert_eq!(state.paths.len(), scale);
            assert_eq!(state.identities.len(), scale);
            assert_eq!(state.inodes.len(), scale);
        }
    }
}

#[test]
fn g2_identity_replacement_removes_stale_identity() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let old = state.inode_for_path(
        root.clone(),
        path(1),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let new = state.inode_for_path(
        root.clone(),
        path(1),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_ne!(old, new);
    assert_eq!(state.paths.get(&(root.clone(), path(1))), Some(&new));
    assert!(!state.identities.contains_key(&(root, identity(1).0)));
}

#[test]
fn g2_identity_replacement_keeps_remaining_hardlink_alias() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let old_identity = identity(1);
    let old = state.inode_for_path(
        root.clone(),
        path(1),
        old_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            path(2),
            old_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        old
    );

    let new = state.inode_for_path(
        root.clone(),
        path(1),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    assert_ne!(old, new);
    assert_eq!(state.paths.get(&(root.clone(), path(1))), Some(&new));
    assert_eq!(state.paths.get(&(root.clone(), path(2))), Some(&old));
    assert_eq!(state.inodes[&old].relative, path(2));
    assert_eq!(state.identities.get(&(root, old_identity.0)), Some(&old));
}

#[test]
fn g2_identity_remaining_hardlink_alias_survives_remove() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let inode = state.inode_for_path(
        root.clone(),
        path(1),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            path(2),
            identity(1),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        inode
    );

    let removed = state.remove_path(&root, path(1)).unwrap();
    assert_eq!(removed.0, inode);
    assert_eq!(removed.1, Some(path(2)));
    assert_eq!(
        state.identities.get(&(root.clone(), identity(1).0)),
        Some(&inode)
    );
    assert_eq!(state.inodes[&inode].relative, path(2));
}

#[test]
fn g2_identity_removed_path_retains_old_record() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let old_path = path(1);
    let old_identity = identity(1);
    let inode = state.inode_for_path(
        root.clone(),
        old_path.clone(),
        old_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    let removed = state.remove_path(&root, old_path.clone()).unwrap();
    assert_eq!(removed, (inode, None));
    assert!(!state.paths.contains_key(&(root.clone(), old_path.clone())));
    assert!(!state.identities.contains_key(&(root, old_identity.0)));
    assert_eq!(state.inodes[&inode].relative, old_path);
}

#[test]
fn g2_identity_directory_rename_descendants() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let dir = state.inode_for_path(
        root.clone(),
        StoragePath::new("old").unwrap(),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let child = state.inode_for_path(
        root.clone(),
        child_path("old", "child"),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(
        root.clone(),
        StoragePath::new("old").unwrap(),
        StoragePath::new("new").unwrap(),
    );

    assert_eq!(
        state
            .paths
            .get(&(root.clone(), StoragePath::new("new").unwrap())),
        Some(&dir)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), child_path("new", "child"))),
        Some(&child)
    );
    assert!(
        !state
            .paths
            .contains_key(&(root.clone(), child_path("old", "child")))
    );
    assert_eq!(
        state.inodes[&dir].relative,
        StoragePath::new("new").unwrap()
    );
    assert_eq!(state.inodes[&child].relative, child_path("new", "child"));
}

#[test]
fn g2_identity_cross_root_distinct_identity() {
    let mut state = OwnerState::new();
    let left_root = root_id("g2-left-root");
    let right_root = root_id("g2-right-root");
    let shared_identity = identity(1);
    let left = state.inode_for_path(
        left_root.clone(),
        path(1),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let right = state.inode_for_path(
        right_root.clone(),
        path(1),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    assert_ne!(left, right);
    assert_eq!(
        state
            .identities
            .get(&(left_root, shared_identity.clone().0)),
        Some(&left)
    );
    assert_eq!(
        state.identities.get(&(right_root, shared_identity.0)),
        Some(&right)
    );
}

#[test]
fn g2_identity_update_record_identity_changed() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let old_identity = identity(1);
    let new_identity = identity(2);
    let inode = state.inode_for_path(
        root.clone(),
        path(1),
        old_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state
        .update_record(
            inode,
            new_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();

    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), old_identity.0))
    );
    assert_eq!(
        state.identities.get(&(root, new_identity.clone().0)),
        Some(&inode)
    );
    assert_eq!(state.inodes[&inode].identity, new_identity);
}
