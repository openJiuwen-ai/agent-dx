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
    /// Whether the target should restore the execution after adopting the checkpoint.
    pub was_running: bool,
    pub phase: String,
}
impl UpgradeOperation {
    pub(crate) fn settled(&self) -> bool {
        matches!(self.phase.as_str(), "Completed" | "Cancelled" | "Deferred")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeTaskKind {
    #[default]
    Pod,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(try_from = "UpgradeTaskWire")]
pub struct UpgradeTask {
    pub kind: UpgradeTaskKind,
    pub protocol_version: u32,
    pub task_id: String,
    pub epoch: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub daemonset_uid: String,
    #[serde(skip_serializing_if = "String::is_empty")]
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
    kind: UpgradeTaskKind,
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
            kind: value.kind,
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
    pub(crate) fn has_deferral_rebind(&self, id: &str) -> bool {
        self.nodes.get(id).is_some_and(|node| {
            node.pod_uid == node.original_pod_uid
                && node.node_session_id != node.original_session_id
                && matches!(node.phase.as_str(), "draining" | "evacuating")
                && self.operations.values().any(|op| {
                    op.source_node_id == id
                        && op.source_session_id != node.node_session_id
                        && matches!(op.phase.as_str(), "Deferring" | "Deferred")
                })
        })
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

    pub(crate) fn allows_deferred_route(&self, stored: &StoredEnvironment) -> bool {
        self.active()
            && self
                .nodes
                .get(&stored.assignment.node_id)
                .is_some_and(|node| node.phase == "evacuating")
            && !self.has_unsettled_migration(&stored.spec.id)
            && self.operations.values().any(|op| {
                op.phase == "Deferred"
                    && op.environment_id == stored.spec.id
                    && op.source_node_id == stored.assignment.node_id
                    && op.source_generation == stored.assignment.generation
            })
    }

    fn has_unsettled_migration(&self, id: &str) -> bool {
        self.operations
            .values()
            .any(|op| op.environment_id == id && !op.settled())
    }

    pub(crate) fn blocks_resume(&self, stored: &StoredEnvironment) -> bool {
        self.active()
            && (self.has_unsettled_migration(&stored.spec.id)
                || self
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
    /// Check the record and migration in one authoritative read. A finished
    /// transfer can still explain a rejection sent to the old generation.
    pub(crate) async fn network_migration(
        &self,
        id: &str,
        generation: u64,
    ) -> Result<(StoredEnvironment, Option<&'static str>)> {
        let [header, environment, upgrade] = self
            .store
            .fields([HEADER.into(), format!("environment:{id}"), UPGRADE.into()])
            .await?;
        self.header(&header)?;
        let stored: StoredEnvironment = decode(environment.as_deref().ok_or(Error::NotFound)?)?;
        stored.validate()?;
        let task: Option<UpgradeTask> = upgrade.as_deref().map(decode).transpose()?;
        let hint = task.as_ref().and_then(|task| {
            task.operations.values().find_map(|op| {
                if op.environment_id != id || !op.was_running || stored.invalidated {
                    return None;
                }
                let source = op.source_generation == generation
                    && stored.assignment.generation == generation
                    && stored.assignment.node_id == op.source_node_id;
                let transfer = op.owns_transfer(&stored)
                    && (op.source_generation == generation
                        || stored.assignment.generation == generation);
                if !(source || transfer) {
                    return None;
                }
                match op.phase.as_str() {
                    "Prepared" | "Assigned" | "Deferring" if task.active() => Some("waiting"),
                    "Completed" if transfer => Some("ready"),
                    "Deferred" if source => Some("ready"),
                    _ => None,
                }
            })
        });
        Ok((stored, hint))
    }

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
        task: UpgradeTask,
        expected_revision: Option<u64>,
    ) -> Result<UpgradeTask> {
        self.save_upgrade_with_result(task, expected_revision, None)
            .await
    }

    pub(crate) async fn save_upgrade_with_result(
        &self,
        mut task: UpgradeTask,
        expected_revision: Option<u64>,
        source_result: Option<&EnvironmentRecord>,
    ) -> Result<UpgradeTask> {
        task.require_protocol()?;
        task.validate()?;
        for _ in 0..ATTEMPTS {
            // Creation checks every process maintenance marker. Terminal writes
            // check every node for isolation added after the last release RPC.
            let full_inventory = expected_revision.is_none() || !task.active();
            let inventory = if full_inventory {
                self.store.raw().await?
            } else {
                // Process maintenance cannot start while the Pod task is active.
                let fields: Vec<_> = [HEADER.into(), UPGRADE.into()]
                    .into_iter()
                    .chain(task.nodes.keys().map(|id| format!("node:{id}")))
                    .collect();
                let values = self.store.fields_vec(&fields).await?;
                fields
                    .into_iter()
                    .zip(values)
                    .filter_map(|(field, value)| value.map(|value| (field, value)))
                    .collect()
            };
            let raw = inventory.get(HEADER).cloned();
            let value = inventory.get(UPGRADE).cloned();
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
                        && old.revision == revision
                        && (old.active() || !task.active()) => {}
                (Some(old), None) if old.active() => return Err(Error::Conflict),
                (None, None) | (Some(_), None) => {}
                _ => return Err(Error::Conflict),
            }
            let mut checks = vec![(HEADER.into(), raw.clone())];
            if task.active() {
                if full_inventory
                    && inventory
                        .keys()
                        .any(|field| field.starts_with("process-upgrade:"))
                {
                    return Err(Error::Conflict);
                }
                // Begin and source additions share the same Header fence as
                // per-node maintenance preparation, including absent markers.
                for id in task.nodes.keys() {
                    let marker = format!("process-upgrade:{id}");
                    let node_field = format!("node:{id}");
                    if let Some(value) = inventory.get(&node_field) {
                        let node: StoredNode = decode(value)?;
                        if node.upgrade_pause.as_ref().is_some_and(|pause| {
                            pause.task_id != task.task_id || pause.epoch != task.epoch
                        }) {
                            return Err(Error::Conflict);
                        }
                    }
                    checks.push((marker, None));
                    checks.push((node_field.clone(), inventory.get(&node_field).cloned()));
                }
            }
            let old_active = previous.as_ref().filter(|old| old.active());
            if let Some(old) = old_active {
                for (id, operation) in &task.operations {
                    let starts_deferral = operation.phase == "Deferring"
                        && old
                            .operations
                            .get(id)
                            .is_some_and(|op| op.phase == "Prepared");
                    let finishes_deferral = operation.phase == "Deferred"
                        && old
                            .operations
                            .get(id)
                            .is_some_and(|op| op.phase == "Deferring");
                    if !starts_deferral && !finishes_deferral {
                        continue;
                    }
                    let field = format!("environment:{}", operation.environment_id);
                    let token_field = format!("node-session:{}", operation.source_node_id);
                    let [value, token] = self
                        .store
                        .fields([field.clone(), token_field.clone()])
                        .await?;
                    let source: StoredEnvironment =
                        decode(value.as_deref().ok_or(Error::Conflict)?)?;
                    let source_session = if finishes_deferral {
                        &task
                            .nodes
                            .get(&operation.source_node_id)
                            .ok_or(Error::Conflict)?
                            .node_session_id
                    } else {
                        &operation.source_session_id
                    };
                    if source.assignment.node_id != operation.source_node_id
                        || source.assignment.generation != operation.source_generation
                        || source.invalidated
                        || source.resume.is_some()
                        || source.recovery.as_ref().is_some_and(|r| r.pending)
                        || token.as_deref() != Some(source_session.as_str())
                        || starts_deferral
                            && source.result.as_ref().is_none_or(|r| {
                                r.state != EnvironmentState::Paused
                                    || r.resources_held
                                    || Some(r.revision) != operation.source_stopped_revision
                            })
                        || finishes_deferral
                            && (source_result.is_none() || source.result.as_ref() != source_result)
                    {
                        return Err(Error::Conflict);
                    }
                    // Ownership transfer also compares the upgrade record. Only
                    // one of the transfer and deferral CAS operations can win.
                    checks.extend([(field, value), (token_field, token)]);
                }
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
                    let rebinds_deferral = previous
                        .is_some_and(|old| old.node_session_id != node.node_session_id)
                        && task.has_deferral_rebind(id);
                    if !confirms_delete && !restores_original && !rebinds_deferral {
                        continue;
                    }
                    // Registration does not update this task. Fence DELETE,
                    // original-Pod release and deferral rebind against that race.
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
                let snapshot;
                let values = if full_inventory {
                    &inventory
                } else {
                    snapshot = self.store.raw().await?;
                    &snapshot
                };
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
                                // An inventory revision may predate the RPC's
                                // source snapshot. Fence the snapshot actually
                                // validated by the RPC when it provided one.
                                let source_changed = match source_result {
                                    Some(expected) if expected.spec.id == record.spec.id => {
                                        record != expected
                                    }
                                    _ => record.revision != operation.source_revision,
                                };
                                record.state != expected_state
                                    || source_changed
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
                for (field, value) in values {
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
            if !task.active() {
                let values = &inventory;
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

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn active_upgrade_progress_does_not_read_the_full_inventory() {
        let redis = transport_test_redis::Redis::new().await;
        let store = redis.store().await;
        let session = store.begin(1).await.unwrap();
        let task = UpgradeTask {
            protocol_version: 3,
            task_id: "upgrade".into(),
            epoch: 1,
            namespace: "test".into(),
            daemonset_uid: "daemonset".into(),
            target_image: format!("node@sha256:{}", "a".repeat(64)),
            phase: "Active".into(),
            nodes: [(
                "node".into(),
                UpgradeNode {
                    node_id: "node".into(),
                    pod_uid: "pod".into(),
                    node_session_id: "session".into(),
                    phase: "pending".into(),
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };
        let task = session.save_upgrade(task, None).await.unwrap();
        let command_stats = || async {
            let mut command = redis::cmd("INFO");
            command.arg("commandstats");
            let info: String = store.query(command).await.unwrap();
            info.lines()
                .find_map(|line| line.strip_prefix("cmdstat_hgetall:calls="))
                .unwrap()
                .split(',')
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        let initial = command_stats().await;
        let revision = task.revision;
        let task = session.save_upgrade(task, Some(revision)).await.unwrap();
        assert_eq!(task.revision, revision + 1);
        assert_eq!(command_stats().await, initial);
    }

    #[test]
    fn drain_resume_requires_the_current_task_owned_source_session() {
        let node: StoredNode = serde_json::from_value(serde_json::json!({
            "node": {"id": "a", "capacity": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0},
                "available": false, "labels": {}, "devices": [], "runtime_classes": ["runsc"]},
            "shard_id": 0, "address": "a:1", "proxy_address": "a:2",
            "session": {"id": "source", "sequence": 1, "routable": true},
            "scheduling_paused": true,
            "upgrade_pause": {"task_id": "upgrade", "epoch": 1, "original_paused": false}
        })).unwrap();
        for (phase, session, original_paused, allowed) in [
            ("draining", "source", false, true),
            ("evacuating", "source", false, false),
            ("draining", "predecessor", false, false),
            ("draining", "source", true, false),
        ] {
            let task = UpgradeTask {
                task_id: "upgrade".into(),
                epoch: 1,
                phase: "Active".into(),
                nodes: [(
                    "a".into(),
                    UpgradeNode {
                        phase: phase.into(),
                        node_session_id: session.into(),
                        ..Default::default()
                    },
                )]
                .into(),
                ..Default::default()
            };
            let mut node = node.clone();
            node.upgrade_pause.as_mut().unwrap().original_paused = original_paused;
            assert_eq!(
                task.allows_drain_resume(&node),
                allowed,
                "{phase} {session} {original_paused}"
            );
        }
    }

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
