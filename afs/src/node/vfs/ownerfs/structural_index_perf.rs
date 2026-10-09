use std::{
    hint::black_box,
    time::{Instant, UNIX_EPOCH},
};

use serde_json::json;

use super::*;

const OPERATIONS: usize = 256;
const ROUNDS: usize = 6;
const SCALES: [usize; 2] = [1000, 10000];

fn path(index: usize) -> StoragePath {
    StoragePath::new(format!("file-{index:05}")).unwrap()
}

fn alias_path(index: usize) -> StoragePath {
    StoragePath::new(format!("alias-{index:05}")).unwrap()
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

fn state_with_entries(scale: usize) -> (OwnerState, RootId, Vec<u64>) {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let mut inodes = Vec::with_capacity(scale);
    for index in 0..scale {
        let inode = state.inode_for_path(
            root.clone(),
            path(index),
            identity(index),
            attrs(FileKind::Regular),
            FileKind::Regular,
        );
        assert_eq!(inode, OWNERFS_ROOT_INODE + 1 + index as u64);
        inodes.push(inode);
    }
    assert_eq!(state.paths.len(), scale);
    assert_eq!(state.identities.len(), scale);
    assert_eq!(state.inodes.len(), scale);
    (state, root, inodes)
}

fn target_index(operation: usize) -> usize {
    operation * 2
}

fn emit_sample(method: &str, scale: usize, round: usize, elapsed_ns: u128) {
    let row = json!({
        "scale": scale,
        "method": method,
        "round": round,
        "operations": OPERATIONS,
        "elapsed_ns": elapsed_ns,
    });
    println!("G2_STRUCTURAL_SAMPLE {row}");
}

#[test]
#[ignore = "diagnostic benchmark; run on Linux ARM64 with --ignored --nocapture --test-threads=1"]
fn ignored_owner_structural_index_benchmark() {
    for scale in SCALES {
        for round in 0..ROUNDS {
            let (mut state, root, inodes) = state_with_entries(scale);
            let targets: Vec<_> = (0..OPERATIONS)
                .map(|operation| {
                    let index = target_index(operation);
                    (index, path(index))
                })
                .collect();
            for (index, target_path) in &targets {
                assert_eq!(
                    state.paths.get(&(root.clone(), target_path.clone())),
                    Some(&inodes[*index])
                );
            }
            let mut removed_results = Vec::with_capacity(OPERATIONS);

            let started = Instant::now();
            for (_, target_path) in &targets {
                removed_results
                    .push(state.remove_path(black_box(&root), black_box(target_path.clone())));
            }
            let elapsed_ns = started.elapsed().as_nanos();

            for ((index, _), removed) in targets.iter().zip(removed_results) {
                assert_eq!(removed, Some((inodes[*index], None)));
            }
            emit_sample("remove_path", scale, round, elapsed_ns);

            assert_eq!(state.paths.len(), scale - OPERATIONS);
            assert_eq!(state.identities.len(), scale - OPERATIONS);
            assert_eq!(state.inodes.len(), scale);
            for (index, target_path) in &targets {
                assert!(
                    !state
                        .paths
                        .contains_key(&(root.clone(), target_path.clone()))
                );
                assert!(
                    !state
                        .identities
                        .contains_key(&(root.clone(), identity(*index).0))
                );
                assert_eq!(&state.inodes[&inodes[*index]].relative, target_path);
            }
        }

        for round in 0..ROUNDS {
            let (mut state, root, inodes) = state_with_entries(scale);
            let targets: Vec<_> = (0..OPERATIONS)
                .map(|operation| {
                    let index = target_index(operation);
                    (
                        index,
                        alias_path(index),
                        identity(index),
                        attrs(FileKind::Regular),
                    )
                })
                .collect();
            for (index, _, target_identity, _) in &targets {
                assert_eq!(
                    state
                        .identities
                        .get(&(root.clone(), target_identity.clone().0)),
                    Some(&inodes[*index])
                );
            }
            let mut alias_results = Vec::with_capacity(OPERATIONS);

            let started = Instant::now();
            for (_, target_path, target_identity, target_attrs) in &targets {
                alias_results.push(state.inode_for_path(
                    black_box(root.clone()),
                    black_box(target_path.clone()),
                    black_box(target_identity.clone()),
                    black_box(target_attrs.clone()),
                    black_box(FileKind::Regular),
                ));
            }
            let elapsed_ns = started.elapsed().as_nanos();

            for ((index, _, _, _), inode) in targets.iter().zip(alias_results) {
                assert_eq!(inode, inodes[*index]);
            }
            emit_sample("inode_for_path_alias", scale, round, elapsed_ns);

            assert_eq!(state.paths.len(), scale + OPERATIONS);
            assert_eq!(state.identities.len(), scale);
            assert_eq!(state.inodes.len(), scale);
            for (index, target_path, target_identity, _) in &targets {
                assert_eq!(
                    state.paths.get(&(root.clone(), target_path.clone())),
                    Some(&inodes[*index])
                );
                assert_eq!(
                    state
                        .identities
                        .get(&(root.clone(), target_identity.clone().0)),
                    Some(&inodes[*index])
                );
                assert_eq!(state.inodes[&inodes[*index]].relative, path(*index));
            }
        }
    }
}

#[test]
fn g2_structural_sole_path_removal_retires_active_identity_but_retains_record() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
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
    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), old_identity.0))
    );
    assert_eq!(state.inodes[&inode].root_id, root);
    assert_eq!(state.inodes[&inode].relative, old_path);
}

#[test]
fn g2_structural_remaining_alias_rebinds_then_last_alias_retires_identity() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let first = path(1);
    let second = path(2);
    let old_identity = identity(1);
    let inode = state.inode_for_path(
        root.clone(),
        first.clone(),
        old_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            second.clone(),
            old_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        inode
    );

    assert_eq!(
        state.remove_path(&root, first.clone()).unwrap(),
        (inode, Some(second.clone()))
    );
    assert!(!state.paths.contains_key(&(root.clone(), first)));
    assert_eq!(
        state.paths.get(&(root.clone(), second.clone())),
        Some(&inode)
    );
    assert_eq!(
        state
            .identities
            .get(&(root.clone(), old_identity.clone().0)),
        Some(&inode)
    );
    assert_eq!(state.inodes[&inode].relative, second.clone());

    assert_eq!(
        state.remove_path(&root, second.clone()).unwrap(),
        (inode, None)
    );
    assert!(!state.paths.contains_key(&(root.clone(), second.clone())));
    assert!(!state.identities.contains_key(&(root, old_identity.0)));
    assert_eq!(state.inodes[&inode].relative, second);
}

#[test]
fn g2_structural_same_path_replacement_preserves_retained_old_inode() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let shared_path = path(1);
    let old_identity = identity(1);
    let new_identity = identity(2);
    let old_inode = state.inode_for_path(
        root.clone(),
        shared_path.clone(),
        old_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    let new_inode = state.inode_for_path(
        root.clone(),
        shared_path.clone(),
        new_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    assert_ne!(old_inode, new_inode);
    assert_eq!(
        state.paths.get(&(root.clone(), shared_path.clone())),
        Some(&new_inode)
    );
    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), old_identity.0))
    );
    assert_eq!(
        state.identities.get(&(root, new_identity.0)),
        Some(&new_inode)
    );
    assert_eq!(state.inodes[&old_inode].relative, shared_path);
}

#[test]
fn g2_structural_same_path_replacement_by_active_identity_moves_alias_only() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let replaced_path = path(1);
    let surviving_path = path(2);
    let old_inode = state.inode_for_path(
        root.clone(),
        replaced_path.clone(),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let active_identity = identity(2);
    let active_inode = state.inode_for_path(
        root.clone(),
        surviving_path.clone(),
        active_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    let observed = state.inode_for_path(
        root.clone(),
        replaced_path.clone(),
        active_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    assert_eq!(observed, active_inode);
    assert_eq!(
        state.paths.get(&(root.clone(), replaced_path.clone())),
        Some(&active_inode)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), surviving_path.clone())),
        Some(&active_inode)
    );
    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), identity(1).0))
    );
    assert_eq!(
        state.identities.get(&(root.clone(), active_identity.0)),
        Some(&active_inode)
    );
    assert_eq!(state.inodes[&old_inode].relative, replaced_path);
    assert_eq!(state.inodes[&active_inode].relative, surviving_path);
}

#[test]
fn g2_structural_root_reincarnation_preserves_canonical_root_identity() {
    let mut state = OwnerState::new();
    let root_name = OsString::from("workspace");
    let root = root_id("g2-structural-root");
    let root_identity = identity(1);
    let root_attrs = attrs(FileKind::Directory);
    let original = state.insert_root(
        root_name.clone(),
        root.clone(),
        root_identity.clone(),
        root_attrs.clone(),
    );
    let reincarnated =
        state.insert_root(root_name, root.clone(), root_identity.clone(), root_attrs);

    assert_eq!(original, reincarnated);
    assert_eq!(
        state.paths.get(&(root.clone(), StoragePath::root())),
        Some(&original)
    );
    assert_eq!(
        state.identities.get(&(root, root_identity.0)),
        Some(&original)
    );
    assert_eq!(state.inodes[&original].relative, StoragePath::root());
}

#[test]
fn g2_structural_cross_root_identity_keeps_roots_independent() {
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
            .get(&(left_root.clone(), shared_identity.clone().0)),
        Some(&left)
    );
    assert_eq!(
        state
            .identities
            .get(&(right_root.clone(), shared_identity.0)),
        Some(&right)
    );
    assert_eq!(state.paths.get(&(left_root, path(1))), Some(&left));
    assert_eq!(state.paths.get(&(right_root, path(1))), Some(&right));
}

#[test]
fn g2_structural_directory_rename_updates_cached_descendants() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
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
fn g2_structural_overwritten_destination_keeps_surviving_alias_canonical() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let source = StoragePath::new("src").unwrap();
    let dest = StoragePath::new("dest").unwrap();
    let dest_alias = StoragePath::new("dest-alias").unwrap();
    let source_inode = state.inode_for_path(
        root.clone(),
        source.clone(),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let dest_identity = identity(2);
    let dest_inode = state.inode_for_path(
        root.clone(),
        dest.clone(),
        dest_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            dest_alias.clone(),
            dest_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        dest_inode
    );

    state.rename_path(root.clone(), source.clone(), dest.clone());

    assert!(!state.paths.contains_key(&(root.clone(), source)));
    assert_eq!(
        state.paths.get(&(root.clone(), dest.clone())),
        Some(&source_inode)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), dest_alias.clone())),
        Some(&dest_inode)
    );
    assert_eq!(state.inodes[&source_inode].relative, dest);
    assert_eq!(state.inodes[&dest_inode].relative, dest_alias);
    assert_eq!(
        state.identities.get(&(root, dest_identity.0)),
        Some(&dest_inode)
    );
}

#[test]
fn g2_structural_same_inode_rename_noop_preserves_aliases() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let from = path(1);
    let to = path(2);
    let file_identity = identity(1);
    let inode = state.inode_for_path(
        root.clone(),
        from.clone(),
        file_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            to.clone(),
            file_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        inode
    );

    state.rename_path(root.clone(), from.clone(), to.clone());

    assert_eq!(state.paths.get(&(root.clone(), from)), Some(&inode));
    assert_eq!(state.paths.get(&(root.clone(), to)), Some(&inode));
    assert_eq!(state.identities.get(&(root, file_identity.0)), Some(&inode));
}

#[test]
fn g2_structural_identity_change_rebuilds_identity_index() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
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
    assert_eq!(state.identities.get(&(root, new_identity.0)), Some(&inode));
}

#[test]
fn g2_structural_identity_collision_keeps_active_mapping_conservative() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let shared_identity = identity(1);
    let left = state.inode_for_path(
        root.clone(),
        path(1),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let right = state.inode_for_path(
        root.clone(),
        path(2),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state
        .update_record(
            left,
            shared_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();

    let mapped = *state
        .identities
        .get(&(root.clone(), shared_identity.clone().0))
        .expect("shared identity remains active");
    assert!(mapped == left || mapped == right);
    assert_eq!(state.inodes[&mapped].identity, shared_identity);
    assert!(state.paths.values().any(|inode| *inode == mapped));
}

#[test]
fn g2_structural_uncached_destination_subtree_cleanup_removes_cached_children() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let source = StoragePath::new("src").unwrap();
    let dest = StoragePath::new("dest").unwrap();
    let cached_child = child_path("dest", "child");
    let source_inode = state.inode_for_path(
        root.clone(),
        source.clone(),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let child_inode = state.inode_for_path(
        root.clone(),
        cached_child.clone(),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(root.clone(), source.clone(), dest.clone());

    assert_eq!(
        state.paths.get(&(root.clone(), dest.clone())),
        Some(&source_inode)
    );
    assert!(!state.paths.contains_key(&(root.clone(), source)));
    assert!(!state.paths.contains_key(&(root.clone(), cached_child)));
    assert!(!state.identities.contains_key(&(root, identity(2).0)));
    assert_eq!(state.inodes[&source_inode].relative, dest);
    assert_eq!(
        state.inodes[&child_inode].relative,
        child_path("dest", "child")
    );
}

#[test]
fn g2_structural_repeated_alias_churn_preserves_indexes() {
    let mut state = OwnerState::new();
    let root = root_id("g2-structural-root");
    let inodes: Vec<_> = (0..32)
        .map(|index| {
            state.inode_for_path(
                root.clone(),
                path(index),
                identity(index),
                attrs(FileKind::Regular),
                FileKind::Regular,
            )
        })
        .collect();

    for step in 0..64 {
        let index = step % inodes.len();
        let alias = StoragePath::new(format!("churn-{step:03}")).unwrap();
        assert_eq!(
            state.inode_for_path(
                root.clone(),
                alias.clone(),
                identity(index),
                attrs(FileKind::Regular),
                FileKind::Regular,
            ),
            inodes[index]
        );
        assert_eq!(
            state.remove_path(&root, alias).unwrap(),
            (inodes[index], Some(path(index)))
        );
        assert_eq!(state.inodes[&inodes[index]].relative, path(index));
    }

    assert_eq!(state.paths.len(), inodes.len());
    assert_eq!(state.identities.len(), inodes.len());
    assert_eq!(state.inodes.len(), inodes.len());
    for (index, inode) in inodes.into_iter().enumerate() {
        assert_eq!(state.paths.get(&(root.clone(), path(index))), Some(&inode));
        assert_eq!(
            state.identities.get(&(root.clone(), identity(index).0)),
            Some(&inode)
        );
        assert_eq!(state.inodes[&inode].relative, path(index));
    }
}
