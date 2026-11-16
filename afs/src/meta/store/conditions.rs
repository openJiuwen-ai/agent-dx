//! Entity keys and transaction condition evaluation.

use super::*;

pub(super) fn invalid_contract(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_CATALOG_INVALID_REQUEST, message)
}

pub(in crate::meta) fn entity_key(entity: &MetaEntity) -> MetaKey {
    match entity {
        MetaEntity::RootEpochWatermark(_) => MetaKey::RootEpochWatermark,
        MetaEntity::DfsReference(reference) => MetaKey::DfsReference {
            node_id: reference.node_id.clone(),
            session_id: reference.session_id.clone(),
            reference_id: reference.reference_id.clone(),
        },
        MetaEntity::DfsGc(_) => MetaKey::DfsGc,
        MetaEntity::NodeSession(session) => MetaKey::NodeSession {
            node_id: session.node_id.clone(),
            session_id: session.session_id.clone(),
        },
        MetaEntity::RootReservation(reservation) => MetaKey::RootReservation {
            root_id: reservation.root_id.clone(),
        },
        MetaEntity::Root(root) => MetaKey::Root {
            root_id: root.root_id.clone(),
        },
        MetaEntity::RootGrant(grant) => MetaKey::RootGrant {
            root_id: grant.root_id.clone(),
            holder_node_id: grant.holder_node_id.clone(),
            holder_session_id: grant.holder_session_id.clone(),
        },
        MetaEntity::RootCommand(command) => MetaKey::RootCommand {
            command_id: command.command_id.clone(),
        },
        MetaEntity::RootCommandAck(ack) => MetaKey::RootCommandAck {
            command_id: ack.command_id.clone(),
            home_node_id: ack.node_id.clone(),
            home_session_id: ack.session_id.clone(),
        },
        MetaEntity::DfsDentry(dentry) => MetaKey::DfsDentry(dentry.key.clone()),
        MetaEntity::DfsInode(inode) => MetaKey::DfsInode(inode.inode_id.clone()),
        MetaEntity::DfsFileVersion(version) => MetaKey::DfsFileVersion(version.id.clone()),
        MetaEntity::DfsLayoutRoot(layout) => MetaKey::DfsLayoutRoot(layout.id.clone()),
        MetaEntity::DfsChunk(chunk) => MetaKey::DfsChunk(chunk.id.clone()),
        MetaEntity::DfsReplicationConfig(_) => MetaKey::DfsReplicationConfig,
        MetaEntity::DfsDurabilityPolicy(_) => MetaKey::DfsDurabilityPolicy,
        MetaEntity::DfsReplicaFailureConfig(_) => MetaKey::DfsReplicaFailureConfig,
        MetaEntity::DfsPlacement(placement) => MetaKey::DfsPlacement(placement.chunk_id.clone()),
        MetaEntity::DfsCopy(copy) => MetaKey::DfsCopy(copy.id.clone()),
        MetaEntity::DfsReplicationTask(task) => MetaKey::DfsReplicationTask(task.id.clone()),
        MetaEntity::DfsWriteLease(lease) => MetaKey::DfsWriteLease(lease.inode_id.clone()),
    }
}

pub(super) fn condition_matches(state: &State, condition: &TxnCondition) -> bool {
    match condition {
        TxnCondition::StoreRevisionEquals(revision) => state.revision == *revision,
        TxnCondition::RootEpochWatermarkEquals(expected) => {
            state.root_epoch_watermark() == *expected
        }
        TxnCondition::Missing(key) => !state.entities.contains_key(key),
        TxnCondition::RevisionEquals { key, revision } => state
            .entities
            .get(key)
            .is_some_and(|entity| entity.revision == *revision),
        TxnCondition::EntityEquals(entity) => state
            .entities
            .get(&entity_key(entity))
            .is_some_and(|current| current.entity == *entity),
        TxnCondition::NodeSessionCurrent {
            node_id,
            session_id,
        } => current_session_matches(state, node_id, session_id),
        TxnCondition::NodeSessionExpired {
            node_id,
            session_id,
        } => !current_session_matches(state, node_id, session_id),
        TxnCondition::NodeSessionUnavailable { node_id } => !node_has_live_session(state, node_id),
        TxnCondition::RootEpochEquals {
            root_id,
            root_epoch,
        } => state
            .entities
            .get(&MetaKey::Root {
                root_id: root_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::Root(root) if root.root_epoch == *root_epoch
                )
            }),
        TxnCondition::RootCommandAcked {
            command_id,
            node_id,
            session_id,
        } => state
            .entities
            .get(&MetaKey::RootCommandAck {
                command_id: command_id.clone(),
                home_node_id: node_id.clone(),
                home_session_id: session_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::RootCommandAck(ack) if ack.success
                )
            }),
        TxnCondition::RootCommandMatches {
            command_id,
            home_node_id,
            home_session_id,
            root_id,
            root_epoch,
            old_access_generation,
            command_type,
        } => state
            .entities
            .get(&MetaKey::RootCommand {
                command_id: command_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::RootCommand(command)
                        if command.home_node_id == *home_node_id
                            && command.home_session_id == *home_session_id
                            && command.root_id == *root_id
                            && command.root_epoch == *root_epoch
                            && command.old_access_generation == *old_access_generation
                            && command.command_type == *command_type
                )
            }),
        TxnCondition::RootCommandAckedExact {
            command_id,
            node_id,
            session_id,
            root_id,
            root_epoch,
            access_generation,
            command_type,
        } => {
            let acked = state
                .entities
                .get(&MetaKey::RootCommandAck {
                    command_id: command_id.clone(),
                    home_node_id: node_id.clone(),
                    home_session_id: session_id.clone(),
                })
                .is_some_and(|entity| {
                    matches!(
                        &entity.entity,
                        MetaEntity::RootCommandAck(ack)
                            if ack.success
                                && ack.root_id == *root_id
                                && ack.root_epoch == *root_epoch
                                && ack.access_generation == *access_generation
                    )
                });
            let command_matches = state
                .entities
                .get(&MetaKey::RootCommand {
                    command_id: command_id.clone(),
                })
                .is_some_and(|entity| {
                    matches!(
                        &entity.entity,
                        MetaEntity::RootCommand(command)
                            if command.home_node_id == *node_id
                                && command.home_session_id == *session_id
                                && command.root_id == *root_id
                                && command.root_epoch == *root_epoch
                                && command.old_access_generation == *access_generation
                                && command.command_type == *command_type
                    )
                });
            acked && command_matches
        }
        TxnCondition::RequestAbsent(key) => !state.requests.contains_key(key),
        TxnCondition::DfsDirectoryEmpty {
            namespace_id,
            parent_inode_id,
        } => dfs_directory_is_empty(state, namespace_id, parent_inode_id),
        TxnCondition::DfsNotDescendant {
            namespace_id,
            ancestor_inode_id,
            child_inode_id,
        } => !dfs_is_descendant_or_same(state, namespace_id, ancestor_inode_id, child_inode_id),
        TxnCondition::DfsGcPlanStillValid(guard) => dfs_gc_plan_still_valid(state, guard),
    }
}

/// Whether a GC plan made at `guard.since` still holds. It holds unless a change
/// since then touched what the plan writes or could make something in it alive
/// again. These are the inputs of `plan_collection` that can move that way:
///
/// - a reference admitted for an inode the plan removes or whose old versions it
///   removes, or a `write:` reference for the generation of a chunk it removes;
/// - a write lease on those inodes;
/// - a version on a removed inode or layout, or a layout using a removed chunk;
/// - a task, placement or copy of a removed chunk;
/// - a change in the set of live sessions, which decide the waiting nodes and the
///   references retired with this plan;
/// - any write to a key the plan writes. This covers inode changes of removed
///   inodes, and tasks, placements and copies of removed chunks.
///
/// Everything else only makes more things dead: releases, expiring leases,
/// claims and grace. A live inode's new head is a new version (commit requires
/// its id to be absent), so its old versions stay dead. History the check cannot
/// see counts as a change.
fn dfs_gc_plan_still_valid(state: &State, guard: &super::GcPlanGuard) -> bool {
    if state.compacted_to > guard.since {
        return false;
    }
    let sessions_changed = {
        let now_ms = now_unix_ms();
        let live = state
            .entities
            .starting_at(MetaKey::CurrentNodeSession {
                node_id: String::new(),
            })
            .take_while(|(key, _)| matches!(key, MetaKey::CurrentNodeSession { .. }))
            .filter_map(|(_, versioned)| match &versioned.entity {
                MetaEntity::NodeSession(session) if session.is_live_at_unix_ms(now_ms) => {
                    Some((session.node_id.clone(), session.session_id.clone()))
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        live != guard.live_sessions
    };
    if sessions_changed {
        return false;
    }
    if let Some(readers) = &guard.readers
        && readers
            .table
            .registered_since(readers.since, &readers.inodes)
    {
        return false;
    }
    !state.events.since(guard.since).any(|event| {
        if guard.written_keys.contains(&event.key) {
            return true;
        }
        let WatchChange::Put(entity) = &event.change else {
            return false;
        };
        match entity {
            MetaEntity::DfsReference(reference) => {
                reference.retired_at_unix_ms.is_none()
                    && (reference
                        .inode_id
                        .as_ref()
                        .is_some_and(|inode| guard.protected_by_reference.contains(inode))
                        || (reference.publication
                            && reference
                                .reference_id
                                .strip_prefix("write:")
                                .is_some_and(|generation| guard.generations.contains(generation))))
            }
            MetaEntity::DfsWriteLease(lease) => {
                guard.protected_by_reference.contains(&lease.inode_id)
            }
            MetaEntity::DfsFileVersion(version) => {
                guard.dead_inodes.contains(&version.inode_id)
                    || guard.dead_layouts.contains(&version.layout_root)
            }
            MetaEntity::DfsLayoutRoot(layout) => layout
                .inline_extents
                .iter()
                .any(|extent| guard.chunks.contains(&extent.chunk_id)),
            MetaEntity::DfsReplicationTask(task) => guard.chunks.contains(&task.chunk_id),
            MetaEntity::DfsCopy(copy) => guard.chunks.contains(&copy.chunk_id),
            MetaEntity::DfsPlacement(placement) => guard.chunks.contains(&placement.chunk_id),
            _ => false,
        }
    })
}

fn dfs_directory_is_empty(
    state: &State,
    namespace_id: &NamespaceId,
    parent_inode_id: &InodeId,
) -> bool {
    let first = DentryKey {
        namespace_id: namespace_id.clone(),
        parent_inode_id: parent_inode_id.clone(),
        name: Vec::new(),
    };
    !matches!(
        state.entities.starting_at(MetaKey::DfsDentry(first)).next(),
        Some((MetaKey::DfsDentry(key), _))
            if key.namespace_id == *namespace_id && key.parent_inode_id == *parent_inode_id
    )
}

fn dfs_is_descendant_or_same(
    state: &State,
    namespace_id: &NamespaceId,
    ancestor_inode_id: &InodeId,
    child_inode_id: &InodeId,
) -> bool {
    if ancestor_inode_id == child_inode_id {
        return true;
    }
    let mut current = child_inode_id.clone();
    let mut seen = std::collections::HashSet::new();
    while seen.insert(current.clone()) {
        let Some(parent) = dfs_parent_of(state, namespace_id, &current) else {
            return false;
        };
        if &parent == ancestor_inode_id {
            return true;
        }
        if parent.0 == "1" {
            return false;
        }
        current = parent;
    }
    true
}

fn dfs_parent_of(state: &State, namespace_id: &NamespaceId, inode_id: &InodeId) -> Option<InodeId> {
    match state
        .dentry_of_child(namespace_id, inode_id)
        .map(|versioned| &versioned.entity)
    {
        Some(MetaEntity::DfsDentry(dentry)) => Some(dentry.key.parent_inode_id.clone()),
        _ => None,
    }
}

/// Whether the node currently holds any live session, regardless of which one.
fn node_has_live_session(state: &State, node_id: &str) -> bool {
    let now_ms = now_unix_ms();
    state
        .entities
        .get(&MetaKey::CurrentNodeSession {
            node_id: node_id.to_owned(),
        })
        .is_some_and(|entity| {
            matches!(
                &entity.entity,
                MetaEntity::NodeSession(session) if session.is_live_at_unix_ms(now_ms)
            )
        })
}

pub(super) fn current_session_matches(state: &State, node_id: &str, session_id: &str) -> bool {
    let now_ms = now_unix_ms();
    state
        .entities
        .get(&MetaKey::CurrentNodeSession {
            node_id: node_id.to_owned(),
        })
        .is_some_and(|entity| {
            matches!(
                &entity.entity,
                MetaEntity::NodeSession(session)
                    if session.session_id == session_id
                        && session.is_live_at_unix_ms(now_ms)
            )
        })
}

pub(super) fn root_session_matches(
    state: &State,
    root_id: &str,
    root_epoch: u64,
    home_session_id: &str,
) -> bool {
    state
        .entities
        .get(&MetaKey::Root {
            root_id: root_id.to_owned(),
        })
        .is_some_and(|entity| {
            if let MetaEntity::Root(root) = &entity.entity {
                root.root_epoch == root_epoch
                    && root.home_session_id == home_session_id
                    && current_session_matches(state, &root.home_node_id, home_session_id)
            } else {
                false
            }
        })
}

pub(super) fn has_pending_root_command(
    state: &State,
    root_id: &str,
    root_epoch: u64,
    access_generation: u64,
) -> bool {
    state.entities.iter().any(|(key, versioned)| {
        matches!(key, MetaKey::RootCommand { .. })
            && matches!(
                &versioned.entity,
                MetaEntity::RootCommand(command)
                    if command.root_id == root_id
                        && command.root_epoch == root_epoch
                        && command.old_access_generation == access_generation
            )
    })
}
