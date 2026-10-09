use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    time::UNIX_EPOCH,
};

use super::*;

fn path(index: usize) -> StoragePath {
    StoragePath::new(format!("file-{index:05}")).unwrap()
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

fn assert_owner_indexes(state: &OwnerState) {
    let mut expected_aliases: HashMap<u64, HashSet<StoragePath>> = HashMap::new();
    for ((root_id, path), inode) in &state.paths {
        let record = state.inodes.get(inode).expect("active path has inode");
        assert_eq!(&record.root_id, root_id);
        expected_aliases
            .entry(*inode)
            .or_default()
            .insert(path.clone());
    }
    assert_eq!(state.aliases, expected_aliases);

    let mut active_by_identity: HashMap<(RootId, Vec<u8>), HashSet<u64>> = HashMap::new();
    for (inode, aliases) in &state.aliases {
        assert!(!aliases.is_empty(), "alias sets are active-only");
        let record = state.inodes.get(inode).expect("alias inode has record");
        assert!(
            aliases.contains(&record.relative),
            "canonical path must be an active alias"
        );
        active_by_identity
            .entry((record.root_id.clone(), record.identity.0.clone()))
            .or_default()
            .insert(*inode);
    }

    for (key, inode) in &state.identities {
        let record = state.inodes.get(inode).expect("identity inode has record");
        assert_eq!((&record.root_id, &record.identity.0), (&key.0, &key.1));
        assert!(
            state
                .aliases
                .get(inode)
                .is_some_and(|aliases| !aliases.is_empty()),
            "identity map must not publish retained inactive records"
        );
        assert!(
            active_by_identity
                .get(key)
                .is_some_and(|inodes| inodes.contains(inode)),
            "identity winner must be active and match the key"
        );
    }

    assert_eq!(&state.identity_members, &active_by_identity);
    for (key, active_inodes) in active_by_identity {
        if active_inodes.len() == 1 {
            let inode = *active_inodes.iter().next().unwrap();
            assert_eq!(state.identities.get(&key), Some(&inode));
        } else {
            let mapped = state
                .identities
                .get(&key)
                .expect("colliding active identity still has a winner");
            assert!(active_inodes.contains(mapped));
        }
    }
}

#[test]
fn g2_structural_indexes_track_alias_churn_and_retained_records() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let file_identity = identity(1);
    let inode = state.inode_for_path(
        root.clone(),
        path(1),
        file_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            path(2),
            file_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        inode
    );
    assert_owner_indexes(&state);

    assert_eq!(
        state.remove_path(&root, path(1)).unwrap(),
        (inode, Some(path(2)))
    );
    assert_owner_indexes(&state);

    assert_eq!(state.remove_path(&root, path(2)).unwrap(), (inode, None));
    assert!(state.inodes.contains_key(&inode));
    assert!(!state.aliases.contains_key(&inode));
    assert!(!state.identities.contains_key(&(root, file_identity.0)));
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_root_reincarnation_replaces_active_root_alias() {
    let mut state = OwnerState::new();
    let name = OsString::from("workspace");
    let root = root_id("g2-root");
    let first_identity = identity(1);
    let second_identity = identity(2);
    let first = state.insert_root(
        name.clone(),
        root.clone(),
        first_identity.clone(),
        attrs(FileKind::Directory),
    );
    let second = state.insert_root(
        name,
        root.clone(),
        second_identity.clone(),
        attrs(FileKind::Directory),
    );

    assert_ne!(first, second);
    assert_eq!(
        state.paths.get(&(root.clone(), StoragePath::root())),
        Some(&second)
    );
    assert!(state.inodes.contains_key(&first));
    assert!(!state.aliases.contains_key(&first));
    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), first_identity.0))
    );
    assert_eq!(
        state.identities.get(&(root, second_identity.0)),
        Some(&second)
    );
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_rename_overwrite_rebinds_surviving_destination_alias() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let source = StoragePath::new("src").unwrap();
    let destination = StoragePath::new("dest").unwrap();
    let destination_alias = StoragePath::new("dest-alias").unwrap();
    let source_inode = state.inode_for_path(
        root.clone(),
        source.clone(),
        identity(1),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let destination_identity = identity(2);
    let destination_inode = state.inode_for_path(
        root.clone(),
        destination.clone(),
        destination_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    assert_eq!(
        state.inode_for_path(
            root.clone(),
            destination_alias.clone(),
            destination_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        ),
        destination_inode
    );

    state.rename_path(root.clone(), source.clone(), destination.clone());

    assert_eq!(
        state.paths.get(&(root.clone(), destination.clone())),
        Some(&source_inode)
    );
    assert_eq!(
        state.paths.get(&(root.clone(), destination_alias.clone())),
        Some(&destination_inode)
    );
    assert_eq!(state.inodes[&source_inode].relative, destination);
    assert_eq!(state.inodes[&destination_inode].relative, destination_alias);
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_update_record_does_not_publish_inactive_identity() {
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
    assert_eq!(state.remove_path(&root, path(1)).unwrap(), (inode, None));

    state
        .update_record(
            inode,
            new_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();

    assert!(state.inodes.contains_key(&inode));
    assert!(!state.aliases.contains_key(&inode));
    assert!(
        !state
            .identities
            .contains_key(&(root.clone(), old_identity.0))
    );
    assert!(!state.identities.contains_key(&(root, new_identity.0)));
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_collision_rebuild_winner_is_active_and_valid() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let shared_identity = identity(7);
    let left = state.inode_for_path(
        root.clone(),
        path(1),
        identity(1),
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
        .get(&(root.clone(), shared_identity.0.clone()))
        .expect("collision keeps one active identity winner");
    assert!(mapped == left || mapped == right);
    assert_eq!(state.inodes[&mapped].root_id, root);
    assert_eq!(state.inodes[&mapped].identity, shared_identity);
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_collision_last_alias_of_winner_keeps_remaining_active_key() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let shared_identity = identity(9);
    let left = state.inode_for_path(
        root.clone(),
        path(1),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let right = state.inode_for_path(
        root.clone(),
        path(2),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    state
        .update_record(
            right,
            shared_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();
    let collision_key = (root.clone(), shared_identity.0.clone());
    let winner = *state
        .identities
        .get(&collision_key)
        .expect("collision has active winner");
    let retired_path = if winner == left { path(1) } else { path(2) };
    let remaining = if winner == left { right } else { left };

    assert_eq!(
        state.remove_path(&root, retired_path).unwrap(),
        (winner, None)
    );

    assert_eq!(state.identities.get(&collision_key), Some(&remaining));
    assert_eq!(state.inodes[&remaining].identity, shared_identity);
    assert_owner_indexes(&state);
}

#[test]
fn g2_structural_collision_winner_identity_change_restores_remaining_winner() {
    let mut state = OwnerState::new();
    let root = root_id("g2-index-root");
    let shared_identity = identity(11);
    let left = state.inode_for_path(
        root.clone(),
        path(1),
        shared_identity.clone(),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    let right = state.inode_for_path(
        root.clone(),
        path(2),
        identity(2),
        attrs(FileKind::Regular),
        FileKind::Regular,
    );
    state
        .update_record(
            right,
            shared_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();
    let collision_key = (root.clone(), shared_identity.0.clone());
    let winner = *state
        .identities
        .get(&collision_key)
        .expect("collision has active winner");
    let remaining = if winner == left { right } else { left };
    let replacement_identity = identity(12);

    state
        .update_record(
            winner,
            replacement_identity.clone(),
            attrs(FileKind::Regular),
            FileKind::Regular,
        )
        .unwrap();

    assert_eq!(state.identities.get(&collision_key), Some(&remaining));
    assert_eq!(state.inodes[&remaining].identity, shared_identity);
    assert_eq!(
        state
            .identities
            .get(&(root.clone(), replacement_identity.0)),
        Some(&winner)
    );
    assert_owner_indexes(&state);
}
