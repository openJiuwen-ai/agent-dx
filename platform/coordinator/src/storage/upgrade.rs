//! One durable rolling-upgrade owner, fenced by the normal Coordinator header CAS.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct UpgradeNode {
    pub node_id: String,
    pub pod_uid: String,
    pub original_pod_uid: String,
    pub original_session_id: String,
    pub node_session_id: String,
    pub original_paused: bool,
    pub phase: String,
    pub control_revision: u64,
    pub permit_id: Option<String>,
    pub delete_resource_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeOperation {
    pub environment_id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    #[serde(default)]
    pub automatic_target: bool,
    pub source_generation: u64,
    pub source_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_stopped_revision: Option<u64>,
    pub source_session_id: String,
    pub target_session_id: String,
    pub was_running: bool,
    pub phase: String,
}
impl UpgradeOperation {
    pub(crate) fn settled(&self) -> bool {
        matches!(self.phase.as_str(), "Completed" | "Cancelled")
    }

    pub(crate) fn owns_transfer(&self, stored: &StoredEnvironment) -> bool {
        !stored.invalidated
            && stored.assignment.node_id == self.target_node_id
            && stored.assignment.generation > self.source_generation
            && stored.recovery.as_ref().is_some_and(|recovery| {
                recovery.source.node_id == self.source_node_id
                    && recovery.source.generation == self.source_generation
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(try_from = "UpgradeTaskWire")]
pub struct UpgradeTask {
    pub protocol_version: u32,
    pub task_id: String,
    pub epoch: u64,
    pub namespace: String,
    pub daemonset_uid: String,
    pub target_image: String,
    pub revision: u64,
    pub phase: String,
    pub nodes: BTreeMap<String, UpgradeNode>,
    pub operations: BTreeMap<String, UpgradeOperation>,
}

// Only fully settled legacy terminal records may be replaced by a new task.
// Their probe history is read as evidence, never carried into the v3 workflow.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct UpgradeTaskWire {
    protocol_version: u32,
    task_id: String,
    epoch: u64,
    namespace: String,
    daemonset_uid: String,
    target_image: String,
    revision: u64,
    phase: String,
    nodes: BTreeMap<String, UpgradeNode>,
    operations: BTreeMap<String, UpgradeOperation>,
    canaries: Option<BTreeMap<String, String>>,
    revoked_canaries: Option<BTreeSet<String>>,
    cleaned_canaries: Option<BTreeSet<String>>,
}

impl TryFrom<UpgradeTaskWire> for UpgradeTask {
    type Error = String;
    fn try_from(value: UpgradeTaskWire) -> std::result::Result<Self, Self::Error> {
        let legacy = (
            &value.canaries,
            &value.revoked_canaries,
            &value.cleaned_canaries,
        );
        match (value.protocol_version, legacy) {
            (3, (None, None, None)) => (),
            (2, (Some(canaries), Some(revoked), Some(cleaned)))
                if matches!(value.phase.as_str(), "Completed" | "Aborted")
                    && value.operations.values().all(UpgradeOperation::settled)
                    && canaries
                        .keys()
                        .all(|id| revoked.contains(id) && cleaned.contains(id)) => {}
            _ => {
                return Err(
                    "finish legacy upgrade tasks with their original version before starting v3"
                        .into(),
                )
            }
        }
        Ok(Self {
            protocol_version: value.protocol_version,
            task_id: value.task_id,
            epoch: value.epoch,
            namespace: value.namespace,
            daemonset_uid: value.daemonset_uid,
            target_image: value.target_image,
            revision: value.revision,
            phase: value.phase,
            nodes: value.nodes,
            operations: value.operations,
        })
    }
}

impl UpgradeTask {
    pub(crate) fn require_protocol(&self) -> Result<()> {
        if self.protocol_version != 3 {
            return Err(Error::Invalid("upgrade task protocol version 3 required; finish legacy tasks with their original version".into()));
        }
        Ok(())
    }
    pub fn active(&self) -> bool {
        !matches!(self.phase.as_str(), "Completed" | "Aborted")
    }
    pub(crate) fn migration_operation(
        &self,
        source: &Assignment,
        target: &Assignment,
    ) -> Option<&UpgradeOperation> {
        (self.phase == "Active").then_some(())?;
        self.operations.values().find(|operation| {
            operation.environment_id == source.environment_id
                && operation.source_node_id == source.node_id
                && operation.source_generation == source.generation
                && operation.target_node_id == target.node_id
                && matches!(operation.phase.as_str(), "Prepared" | "Assigned")
                && self.nodes.get(&source.node_id).is_some_and(|node| {
                    matches!(node.phase.as_str(), "evacuating" | "draining")
                        && node.node_session_id == operation.source_session_id
                        && (node.phase != "draining"
                            || self.drain_target_eligible(
                                &operation.target_node_id,
                                &operation.target_session_id,
                            ))
                })
        })
    }
    pub(crate) fn drain_target_eligible(&self, node_id: &str, session_id: &str) -> bool {
        self.nodes
            .get(node_id)
            .is_some_and(|entry| entry.phase == "pending" && entry.node_session_id == session_id)
    }
    /// An upgrade-owned pause can still drive accepted resumes on the same session.
    pub(crate) fn allows_drain_resume(&self, node: &StoredNode) -> bool {
        matches!(self.phase.as_str(), "Active" | "Aborting")
            && node.upgrade_pause.as_ref().is_some_and(|owner| {
                owner.task_id == self.task_id && owner.epoch == self.epoch && !owner.original_paused
            })
            && self.nodes.get(&node.node.id).is_some_and(|entry| {
                entry.phase == "draining"
                    && node
                        .session
                        .as_ref()
                        .is_some_and(|session| session.id == entry.node_session_id)
            })
    }

    pub(crate) fn blocks_resume(&self, stored: &StoredEnvironment) -> bool {
        self.active()
            && (self.operations.values().any(|operation| {
                operation.environment_id == stored.spec.id && !operation.settled()
            }) || self
                .nodes
                .get(&stored.assignment.node_id)
                .is_some_and(|node| {
                    matches!(node.phase.as_str(), "evacuating" | "quiescing")
                        // Freeze new demand while the existing intents settle before evacuation.
                        || (node.phase == "draining"
                            && self.phase == "Aborting"
                            && stored.resume.is_none())
                }))
    }
    pub fn validate(&self) -> Result<()> {
        if self.task_id.trim().is_empty()
            || self.task_id.len() > 128
            || self.epoch == 0
            || self.namespace.is_empty()
            || self.daemonset_uid.is_empty()
            || !self
                .target_image
                .rsplit_once("@sha256:")
                .is_some_and(|(name, digest)| {
                    !name.is_empty()
                        && digest.len() == 64
                        && digest.bytes().all(|b| b.is_ascii_hexdigit())
                })
            || self.nodes.is_empty()
            || self.nodes.iter().any(|(id, node)| {
                id != &node.node_id || node.pod_uid.is_empty() || node.node_session_id.is_empty()
            })
        {
            return Err(Error::Invalid(
                "upgrade identity, immutable image and node sessions required".into(),
            ));
        }
        Ok(())
    }
    pub fn fence(&self, task_id: &str, epoch: u64) -> Result<()> {
        self.require_protocol()?;
        if self.task_id != task_id || self.epoch != epoch || !self.active() {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
impl Session {
    pub(crate) async fn confirmed_deletion(
        &self,
        id: &str,
        minimum_generation: u64,
    ) -> Result<bool> {
        let [raw] = self.store.fields([HEADER.into()]).await?;
        self.header(&raw)?;
        Ok(self
            .store
            .deleted_receipt(id)
            .await?
            .is_some_and(|receipt| {
                receipt.environment_id == id && receipt.generation >= minimum_generation
            }))
    }

    pub async fn upgrade_task(&self) -> Result<Option<UpgradeTask>> {
        let [raw, value] = self.store.fields([HEADER.into(), UPGRADE.into()]).await?;
        self.header(&raw)?;
        value.as_deref().map(decode::<UpgradeTask>).transpose()
    }
    pub async fn save_upgrade(
        &self,
        mut task: UpgradeTask,
        expected_revision: Option<u64>,
    ) -> Result<UpgradeTask> {
        task.require_protocol()?;
        task.validate()?;
        for _ in 0..ATTEMPTS {
            // Registration can add isolation after the last release RPC. Read
            // every node with the same header that fences the terminal write.
            let terminal = if task.active() {
                None
            } else {
                Some(self.store.raw().await?)
            };
            let [raw, value] = match &terminal {
                Some(values) => [values.get(HEADER).cloned(), values.get(UPGRADE).cloned()],
                None => self.store.fields([HEADER.into(), UPGRADE.into()]).await?,
            };
            let header = self.header(&raw)?;
            let previous = value.as_deref().map(decode::<UpgradeTask>).transpose()?;
            if let Some(old) = previous.as_ref().filter(|old| {
                old.active() || expected_revision.is_some() || old.task_id == task.task_id
            }) {
                old.require_protocol()?;
            }
            match (&previous, expected_revision) {
                (Some(old), Some(revision))
                    if old.task_id == task.task_id
                        && old.epoch == task.epoch
                        && old.revision == revision => {}
                (Some(old), None) if old.active() => return Err(Error::Conflict),
                (None, None) | (Some(_), None) => {}
                _ => return Err(Error::Conflict),
            }
            let mut checks = vec![(HEADER.into(), raw.clone())];
            let old_active = previous.as_ref().filter(|old| old.active());
            if let Some(old) = old_active {
                for (id, node) in &task.nodes {
                    let previous = old.nodes.get(id);
                    let old_rv = previous.and_then(|node| node.delete_resource_version.as_ref());
                    let confirms_delete = node.delete_resource_version.is_some()
                        && old_rv != node.delete_resource_version.as_ref();
                    let restores_original = node.phase == "RestoredOriginal"
                        && previous.is_some_and(|old| {
                            old.node_session_id != node.node_session_id
                                || old.control_revision != node.control_revision
                        });
                    if !confirms_delete && !restores_original {
                        continue;
                    }
                    // Registration does not update this task. Fence both DELETE
                    // confirmation and original-Pod release against that race.
                    let field = format!("node-session:{id}");
                    let [token] = self.store.fields([field.clone()]).await?;
                    if token.as_deref() != Some(node.node_session_id.as_str()) {
                        return Err(Error::Conflict);
                    }
                    checks.push((field, token));
                }
            }
            let new_fence = task.active()
                && (task.nodes.iter().any(|(id, node)| {
                    matches!(node.phase.as_str(), "evacuating" | "quiescing")
                        && old_active
                            .and_then(|old| old.nodes.get(id))
                            .is_none_or(|old| {
                                !matches!(old.phase.as_str(), "evacuating" | "quiescing")
                            })
                }) || task.operations.iter().any(|(id, operation)| {
                    !operation.settled()
                        && old_active
                            .and_then(|old| old.operations.get(id))
                            .is_none_or(|old| {
                                old.settled() || old.environment_id != operation.environment_id
                            })
                }));
            if new_fence {
                // Resume intent and the upgrade barrier share this header CAS.
                // Never fence an accepted wake: migration cannot consume its
                // checkpoint until the wake settles, so doing so would deadlock.
                let values = self.store.raw().await?;
                if values.get(HEADER) != raw.as_ref() {
                    continue;
                }
                checks.extend(
                    values
                        .iter()
                        .filter(|(field, _)| field.starts_with("environment:"))
                        .map(|(field, value)| (field.clone(), Some(value.clone()))),
                );
                // The node and target checks in the RPC can wait while a local
                // Resume publishes. Revalidate the source from this CAS snapshot
                // when inserting a new operation into the active task.
                if let Some(old) = old_active {
                    for (id, operation) in &task.operations {
                        if operation.phase != "Prepared" || old.operations.contains_key(id) {
                            continue;
                        }
                        let key = format!("environment:{}", operation.environment_id);
                        let source: StoredEnvironment =
                            decode(values.get(&key).ok_or(Error::Conflict)?)?;
                        let expected_state = if operation.was_running {
                            EnvironmentState::Running
                        } else {
                            EnvironmentState::Paused
                        };
                        if source.assignment.node_id != operation.source_node_id
                            || source.assignment.generation != operation.source_generation
                            || source.invalidated
                            || source.resume.is_some()
                            || source.recovery.as_ref().is_some_and(|r| r.pending)
                            || source.result.as_ref().is_none_or(|record| {
                                record.state != expected_state
                                    || record.revision != operation.source_revision
                                    || record.restart_pending
                            })
                        {
                            return Err(Error::Conflict);
                        }
                        for (id, expected) in [
                            (&operation.source_node_id, &operation.source_session_id),
                            (&operation.target_node_id, &operation.target_session_id),
                        ] {
                            let key = format!("node-session:{id}");
                            if values.get(&key) != Some(expected) {
                                return Err(Error::Conflict);
                            }
                            checks.push((key, Some(expected.clone())));
                        }
                    }
                }
                for (field, value) in &values {
                    if field.starts_with("environment:") {
                        let stored: StoredEnvironment = decode(value)?;
                        if stored.resume.is_some()
                            && task.blocks_resume(&stored)
                            && !old_active.is_some_and(|old| old.blocks_resume(&stored))
                        {
                            return Err(Error::Conflict);
                        }
                    }
                }
            }
            if let Some(values) = terminal {
                checks.extend(
                    values
                        .iter()
                        .filter(|(field, _)| field.starts_with("node:"))
                        .map(|(field, value)| (field.clone(), Some(value.clone()))),
                );
                let nodes = values
                    .iter()
                    .filter_map(|(field, value)| {
                        field
                            .strip_prefix("node:")
                            .map(|id| decode::<StoredNode>(value).map(|node| (id, node)))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                if task.nodes.iter().any(|(id, expected)| {
                    nodes.get(id.as_str()).is_none_or(|node| {
                        node.session
                            .as_ref()
                            .is_none_or(|session| session.id != expected.node_session_id)
                    })
                }) || nodes.values().any(|node| {
                    node.upgrade_pause.as_ref().is_some_and(|owner| {
                        owner.task_id == task.task_id && owner.epoch == task.epoch
                    })
                }) {
                    return Err(Error::Conflict);
                }
            }
            task.revision = expected_revision
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(Error::Conflict)?;
            raw.as_deref().ok_or(Error::Conflict)?;
            if Box::pin(self.store.object_cas_checked(
                &header,
                Some((UPGRADE, value.as_deref(), encode(&task)?)),
                None,
                &checks,
            ))
            .await?
            {
                return Ok(task);
            }
        }
        Err(Error::Unavailable(
            "concurrent upgrade update; query before retry".into(),
        ))
    }
}

#[cfg(test)]
mod drain_tests {
    use super::*;

    #[test]
    fn drain_migration_fences_target_phase_and_session_at_transfer() {
        let source = Assignment {
            environment_id: "env".into(),
            node_id: "a".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        let target = Assignment {
            node_id: "b".into(),
            generation: 2,
            ..source.clone()
        };
        let mut task = UpgradeTask {
            phase: "Active".into(),
            nodes: [
                (
                    "a".into(),
                    UpgradeNode {
                        node_session_id: "source".into(),
                        phase: "draining".into(),
                        ..Default::default()
                    },
                ),
                (
                    "b".into(),
                    UpgradeNode {
                        node_session_id: "target".into(),
                        phase: "pending".into(),
                        ..Default::default()
                    },
                ),
            ]
            .into(),
            operations: [(
                "move".into(),
                UpgradeOperation {
                    environment_id: "env".into(),
                    source_node_id: "a".into(),
                    target_node_id: "b".into(),
                    automatic_target: true,
                    source_generation: 1,
                    source_revision: 1,
                    source_stopped_revision: Some(2),
                    source_session_id: "source".into(),
                    target_session_id: "target".into(),
                    was_running: true,
                    phase: "Prepared".into(),
                },
            )]
            .into(),
            ..Default::default()
        };
        assert!(task.migration_operation(&source, &target).is_some());
        let node = task.nodes.get_mut("b").unwrap();
        node.node_session_id = "replacement".into();
        assert!(task.migration_operation(&source, &target).is_none());
        let node = task.nodes.get_mut("b").unwrap();
        node.node_session_id = "target".into();
        node.phase = "verified".into();
        assert!(task.migration_operation(&source, &target).is_none());

        // Ordinary migration may use verified nodes as destinations.
        task.nodes.get_mut("a").unwrap().phase = "evacuating".into();
        assert!(task.migration_operation(&source, &target).is_some());
    }
}
