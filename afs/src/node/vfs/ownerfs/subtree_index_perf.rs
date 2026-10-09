//! Draft G2-B3 private OwnerState subtree fixture.
//!
//! This file is intentionally outside `source/` and is not wired into cfg(test).
//! After the B2 parent is frozen, root may copy/adapt this as a private test
//! module next to OwnerFs internals and capture Linux ARM64 baseline/candidate
//! evidence. Timed intervals below are limited to production `rename_path` calls.

use std::{
    ffi::OsString,
    hint::black_box,
    os::unix::ffi::OsStringExt,
    path::PathBuf,
    time::{Instant, UNIX_EPOCH},
};

use serde_json::json;

use super::*;

const REGULAR_OPERATIONS: usize = 256;
const DIRECTORY_RENAMES: usize = 16;
const DIRECTORY_CHILDREN: usize = 16;
const ROUNDS: usize = 6;
const SCALES: [usize; 2] = [1000, 10000];

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

fn path(name: impl AsRef<str>) -> StoragePath {
    StoragePath::new(name.as_ref()).unwrap()
}

fn regular_path(index: usize) -> StoragePath {
    path(format!("file-{index:05}"))
}

fn regular_renamed_path(index: usize) -> StoragePath {
    path(format!("renamed-{index:05}"))
}

fn dir_path(index: usize) -> StoragePath {
    path(format!("dir-{index:03}"))
}

fn dir_renamed_path(index: usize) -> StoragePath {
    path(format!("renamed-dir-{index:03}"))
}

fn child_path(dir: usize, child: usize) -> StoragePath {
    path(format!("dir-{dir:03}/child-{child:03}"))
}

fn child_renamed_path(dir: usize, child: usize) -> StoragePath {
    path(format!("renamed-dir-{dir:03}/child-{child:03}"))
}

fn non_utf8_path(bytes: &[u8]) -> StoragePath {
    StoragePath::new(PathBuf::from(OsString::from_vec(bytes.to_vec()))).unwrap()
}

fn regular_target_index(operation: usize) -> usize {
    operation * 2
}

fn emit_sample(method: &str, scale: usize, round: usize, operations: usize, elapsed_ns: u128) {
    let row = json!({
        "scale": scale,
        "method": method,
        "round": round,
        "operations": operations,
        "elapsed_ns": elapsed_ns,
    });
    println!("G2_SUBTREE_SAMPLE {row}");
}

fn state_with_regular_entries(scale: usize) -> (OwnerState, RootId, Vec<u64>) {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let mut inodes = Vec::with_capacity(scale);
    for index in 0..scale {
        let inode = state.inode_for_path(
            root.clone(),
            regular_path(index),
            identity(index),
            attrs(FileKind::Regular),
            FileKind::Regular,
        );
        inodes.push(inode);
    }
    assert_eq!(state.paths.len(), scale);
    (state, root, inodes)
}

fn state_with_directory_entries(scale: usize) -> (OwnerState, RootId, Vec<u64>, Vec<Vec<u64>>) {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let mut next_identity = 0;
    let mut dir_inodes = Vec::with_capacity(DIRECTORY_RENAMES);
    let mut child_inodes = Vec::with_capacity(DIRECTORY_RENAMES);
    for dir in 0..DIRECTORY_RENAMES {
        let inode = state.inode_for_path(
            root.clone(),
            dir_path(dir),
            identity(next_identity),
            attrs(FileKind::Directory),
            FileKind::Directory,
        );
        next_identity += 1;
        dir_inodes.push(inode);
        let mut children = Vec::with_capacity(DIRECTORY_CHILDREN);
        for child in 0..DIRECTORY_CHILDREN {
            let child_inode = state.inode_for_path(
                root.clone(),
                child_path(dir, child),
                identity(next_identity),
                attrs(FileKind::Regular),
                FileKind::Regular,
            );
            next_identity += 1;
            children.push(child_inode);
        }
        child_inodes.push(children);
    }
    while state.paths.len() < scale {
        let index = next_identity;
        state.inode_for_path(
            root.clone(),
            path(format!("filler-{index:05}")),
            identity(index),
            attrs(FileKind::Regular),
            FileKind::Regular,
        );
        next_identity += 1;
    }
    assert_eq!(state.paths.len(), scale);
    (state, root, dir_inodes, child_inodes)
}

#[test]
#[ignore = "draft diagnostic benchmark; run only after B2 parent freeze on Linux ARM64"]
fn ignored_owner_subtree_index_benchmark() {
    for scale in SCALES {
        for round in 0..ROUNDS {
            let (mut state, root, inodes) = state_with_regular_entries(scale);
            let targets: Vec<_> = (0..REGULAR_OPERATIONS)
                .map(|operation| {
                    let index = regular_target_index(operation);
                    (index, regular_path(index), regular_renamed_path(index))
                })
                .collect();
            for (index, from, _) in &targets {
                assert_eq!(
                    state.paths.get(&(root.clone(), from.clone())),
                    Some(&inodes[*index])
                );
            }
            let commands: Vec<_> = targets
                .iter()
                .map(|(_, from, to)| (root.clone(), from.clone(), to.clone()))
                .collect();

            let started = Instant::now();
            for (command_root, from, to) in commands {
                state.rename_path(black_box(command_root), black_box(from), black_box(to));
            }
            let elapsed_ns = started.elapsed().as_nanos();

            for (index, from, to) in &targets {
                assert!(!state.paths.contains_key(&(root.clone(), from.clone())));
                assert_eq!(
                    state.paths.get(&(root.clone(), to.clone())),
                    Some(&inodes[*index])
                );
                assert_eq!(&state.inodes[&inodes[*index]].relative, to);
            }
            emit_sample(
                "rename_regular",
                scale,
                round,
                REGULAR_OPERATIONS,
                elapsed_ns,
            );
        }

        for round in 0..ROUNDS {
            let (mut state, root, dir_inodes, child_inodes) = state_with_directory_entries(scale);
            let targets: Vec<_> = (0..DIRECTORY_RENAMES)
                .map(|dir| (dir, dir_path(dir), dir_renamed_path(dir)))
                .collect();
            for (dir, from, _) in &targets {
                assert_eq!(
                    state.paths.get(&(root.clone(), from.clone())),
                    Some(&dir_inodes[*dir])
                );
            }
            let commands: Vec<_> = targets
                .iter()
                .map(|(_, from, to)| (root.clone(), from.clone(), to.clone()))
                .collect();

            let started = Instant::now();
            for (command_root, from, to) in commands {
                state.rename_path(black_box(command_root), black_box(from), black_box(to));
            }
            let elapsed_ns = started.elapsed().as_nanos();

            for (dir, from, to) in &targets {
                assert!(!state.paths.contains_key(&(root.clone(), from.clone())));
                assert_eq!(
                    state.paths.get(&(root.clone(), to.clone())),
                    Some(&dir_inodes[*dir])
                );
                assert_eq!(&state.inodes[&dir_inodes[*dir]].relative, to);
                for (child, &inode) in child_inodes[*dir].iter().enumerate() {
                    let old_child = child_path(*dir, child);
                    let new_child = child_renamed_path(*dir, child);
                    assert!(!state.paths.contains_key(&(root.clone(), old_child)));
                    assert_eq!(
                        state.paths.get(&(root.clone(), new_child.clone())),
                        Some(&inode)
                    );
                    assert_eq!(state.inodes[&inode].relative, new_child);
                }
            }
            emit_sample(
                "rename_directory",
                scale,
                round,
                DIRECTORY_RENAMES,
                elapsed_ns,
            );
        }
    }
}

#[test]
fn g2_subtree_component_boundary_keeps_sibling_prefixes() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let dir = state.inode_for_path(
        root.clone(),
        path("dir"),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let child = state.inode_for_path(
        root.clone(),
        path("dir/child"),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let sibling_dash = state.inode_for_path(
        root.clone(),
        path("dir-/child"),
        identity(3),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let sibling_joined = state.inode_for_path(
        root.clone(),
        path("directory"),
        identity(4),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );

    state.rename_path(root.clone(), path("dir"), path("new"));

    assert_eq!(state.paths.get(&(root.clone(), path("new"))), Some(&dir));
    assert_eq!(
        state.paths.get(&(root.clone(), path("new/child"))),
        Some(&child)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("dir-/child"))),
        Some(&sibling_dash)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("directory"))),
        Some(&sibling_joined)
    );
}

#[test]
fn g2_subtree_non_utf8_component_moves_only_component_subtree() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let old = non_utf8_path(&[b'd', b'i', b'r', 0xff]);
    let child = non_utf8_path(&[b'd', b'i', b'r', 0xff, b'/', b'c']);
    let sibling = non_utf8_path(&[b'd', b'i', b'r', 0xfe, b'/', b'c']);
    let new = path("new-nonutf8");
    let child_new = path("new-nonutf8/c");
    let dir_inode = state.inode_for_path(
        root.clone(),
        old.clone(),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let child_inode = state.inode_for_path(
        root.clone(),
        child.clone(),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let sibling_inode = state.inode_for_path(
        root.clone(),
        sibling.clone(),
        identity(3),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(root.clone(), old, new.clone());

    assert_eq!(state.paths.get(&(root.clone(), new)), Some(&dir_inode));
    assert_eq!(
        state.paths.get(&(root.clone(), child_new)),
        Some(&child_inode)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), sibling)),
        Some(&sibling_inode)
    );
}

#[test]
fn g2_subtree_uncached_source_purges_stale_destination_descendants() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let stale_child = state.inode_for_path(
        root.clone(),
        path("dest/stale-child"),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(root.clone(), path("missing-source"), path("dest"));

    assert!(
        !state
            .paths
            .contains_key(&(root.clone(), path("dest/stale-child")))
    );
    assert!(state.inodes.contains_key(&stale_child));
    assert!(!state.identities.contains_key(&(root, identity(1).0)));
}

#[test]
fn g2_subtree_cross_root_same_names_are_isolated() {
    let mut state = OwnerState::new();
    let left = root_id("left-root");
    let right = root_id("right-root");
    let left_dir = state.inode_for_path(
        left.clone(),
        path("dir"),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let right_child = state.inode_for_path(
        right.clone(),
        path("dir/child"),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );

    state.rename_path(left.clone(), path("dir"), path("new"));

    assert_eq!(
        state.paths.get(&(left.clone(), path("new"))),
        Some(&left_dir)
    );
    assert_eq!(
        state.paths.get(&(right.clone(), path("dir/child"))),
        Some(&right_child)
    );
}

#[test]
fn g2_subtree_directory_overwrite_keeps_hardlink_alias_canonical() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let source = state.inode_for_path(
        root.clone(),
        path("src"),
        identity(1),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    let destination_identity = identity(2);
    let dest = state.inode_for_path(
        root.clone(),
        path("dest"),
        destination_identity.clone(),
        attrs(FileKind::Directory),
        FileKind::Directory,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            path("dest-alias"),
            destination_identity.clone(),
            attrs(FileKind::Directory),
            FileKind::Directory,
        ),
        dest
    );

    state.rename_path(root.clone(), path("src"), path("dest"));

    assert_eq!(
        state.paths.get(&(root.clone(), path("dest"))),
        Some(&source)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("dest-alias"))),
        Some(&dest)
    );
    assert_eq!(state.inodes[&dest].relative, path("dest-alias"));
}

#[test]
fn g2_subtree_nested_cached_canonical_alias_renames_with_subtree() {
    let mut state = OwnerState::new();
    let root = root_id("g2-subtree-root");
    let file_identity = identity(2);
    let file = state.inode_for_path(
        root.clone(),
        path("old/a/file"),
        file_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            path("old/b/file-alias"),
            file_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        file
    );

    state.rename_path(root.clone(), path("old"), path("new"));

    assert_eq!(
        state.paths.get(&(root.clone(), path("new/a/file"))),
        Some(&file)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), path("new/b/file-alias"))),
        Some(&file)
    );
    assert!(state.aliases[&file].contains(&path("new/a/file")));
    assert!(state.aliases[&file].contains(&path("new/b/file-alias")));
    assert!(
        state.inodes[&file].relative == path("new/a/file")
            || state.inodes[&file].relative == path("new/b/file-alias")
    );
}
