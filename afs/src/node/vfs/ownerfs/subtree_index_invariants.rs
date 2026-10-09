use std::{
    collections::{BTreeSet, HashMap},
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    path::PathBuf,
    time::UNIX_EPOCH,
};

use super::*;

fn root_id(name: &str) -> RootId {
    RootId(name.to_owned())
}

fn path(value: impl AsRef<str>) -> StoragePath {
    StoragePath::new(value.as_ref()).unwrap()
}

fn non_utf8_path(bytes: &[u8]) -> StoragePath {
    StoragePath::new(PathBuf::from(OsString::from_vec(bytes.to_vec()))).unwrap()
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

fn assert_subtree_index(state: &OwnerState, prefixes: &[(RootId, StoragePath)]) {
    let mut expected: HashMap<RootId, BTreeSet<StoragePath>> = HashMap::new();
    for ((root_id, path), inode) in &state.paths {
        let record = state.inodes.get(inode).expect("active path has inode");
        assert_eq!(&record.root_id, root_id);
        expected
            .entry(root_id.clone())
            .or_default()
            .insert(path.clone());
    }
    assert_eq!(state.subtrees, expected);

    for (root_id, prefix) in prefixes {
        let indexed: BTreeSet<_> = state.subtree_paths(root_id, prefix).into_iter().collect();
        let reference: BTreeSet<_> = state
            .paths
            .keys()
            .filter_map(|(candidate_root, candidate_path)| {
                (candidate_root == root_id
                    && candidate_path
                        .as_path()
                        .strip_prefix(prefix.as_path())
                        .is_ok())
                .then_some(candidate_path.clone())
            })
            .collect();
        assert_eq!(indexed, reference, "prefix query mismatch for {prefix:?}");
    }
}

#[test]
fn g2_subtree_index_matches_full_scan_for_boundaries_non_utf8_and_root() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let other = root_id("g2-other-root");
    let non_utf8_dir = non_utf8_path(&[b'd', b'i', b'r', 0xff]);
    let non_utf8_child = non_utf8_path(&[b'd', b'i', b'r', 0xff, b'/', b'c']);
    for (index, candidate) in [
        path("dir"),
        path("dir/child"),
        path("dir-/child"),
        path("directory"),
        non_utf8_dir.clone(),
        non_utf8_child.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        state.inode_for_path(
            root.clone(),
            candidate,
            identity(index),
            attrs(FileKind::Regular),
            FileKind::Regular,
        );
    }
    state.inode_for_path(
        other.clone(),
        path("dir/child"),
        identity(99),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    assert_subtree_index(
        &state,
        &[
            (root.clone(), StoragePath::root()),
            (root.clone(), path("dir")),
            (root.clone(), path("dir-")),
            (root.clone(), path("directory")),
            (root.clone(), non_utf8_dir),
            (other.clone(), path("dir")),
        ],
    );
}

#[test]
fn g2_subtree_index_tracks_rename_destination_purge_and_churn() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let source_dir = state.inode_for_path(
        root.clone(),
        path("src"),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let source_child = state.inode_for_path(
        root.clone(),
        path("src/child"),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let stale_dest_child = state.inode_for_path(
        root.clone(),
        path("dest/stale"),
        identity(3),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let sibling = state.inode_for_path(
        root.clone(),
        path("src-keep/child"),
        identity(4),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(root.clone(), path("src"), path("dest"));

    assert_eq!(
        state.paths.get(&(root.clone(), path("dest"))),
        Some(&source_dir)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("dest/child"))),
        Some(&source_child)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("src-keep/child"))),
        Some(&sibling)
    );
    assert!(!state.aliases.contains_key(&stale_dest_child));
    assert_subtree_index(
        &state,
        &[
            (root.clone(), StoragePath::root()),
            (root.clone(), path("src")),
            (root.clone(), path("src-keep")),
            (root.clone(), path("dest")),
        ],
    );

    state.remove_path(&root, path("dest/child")).unwrap();
    assert_subtree_index(&state, &[(root.clone(), path("dest"))]);
}

#[test]
fn g2_subtree_index_survives_root_replacement_and_direct_rebuild() {
    let mut state = OwnerState::new();
    let name = OsString::from("workspace");
    let root = root_id("g2-root");
    let first = state.insert_root(
        name.clone(),
        root.clone(),
        identity(1),
        attrs(FileKind::Directory),
    );
    let second = state.insert_root(name, root.clone(), identity(2), attrs(FileKind::Directory));

    assert_ne!(first, second);
    assert_subtree_index(&state, &[(root.clone(), StoragePath::root())]);

    state
        .paths
        .insert((root.clone(), path("manual-alias")), second);
    state.rebuild_identity_index();

    assert_eq!(
        state.paths.get(&(root.clone(), path("manual-alias"))),
        Some(&second)
    );
    assert_subtree_index(
        &state,
        &[
            (root.clone(), StoragePath::root()),
            (root.clone(), path("manual-alias")),
        ],
    );
}

#[test]
fn g2_subtree_index_survives_multi_root_rename_and_collision_rebuild() {
    let mut state = OwnerState::new();
    let left_root = root_id("left-root");
    let right_root = root_id("right-root");
    let shared_identity = identity(7);
    let left = state.inode_for_path(
        left_root.clone(),
        path("dir/file"),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let right = state.inode_for_path(
        right_root.clone(),
        path("dir/file"),
        identity(8),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let collider = state.inode_for_path(
        left_root.clone(),
        path("collision"),
        identity(9),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    state
        .update_record(
            collider,
            shared_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();
    let collision_key = (left_root.clone(), shared_identity.0.clone());
    assert!(
        state
            .identities
            .get(&collision_key)
            .is_some_and(|inode| *inode == left || *inode == collider)
    );

    state.rename_path(left_root.clone(), path("dir"), path("renamed"));

    assert_eq!(
        state.paths.get(&(left_root.clone(), path("renamed/file"))),
        Some(&left)
    );
    assert_eq!(
        state.paths.get(&(right_root.clone(), path("dir/file"))),
        Some(&right)
    );
    assert_subtree_index(
        &state,
        &[
            (left_root.clone(), path("dir")),
            (left_root.clone(), path("renamed")),
            (right_root.clone(), path("dir")),
        ],
    );
}
