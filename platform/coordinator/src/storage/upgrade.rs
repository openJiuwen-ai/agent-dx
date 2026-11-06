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
    pub source_generation: u64,
    pub source_revision: u64,
    pub source_session_id: String,
    pub target_session_id: String,
    pub was_running: bool,
    pub phase: String,
}
impl UpgradeOperation {
    pub(crate) fn settled(&self) -> bool {
        matches!(self.phase.as_str(), "Completed" | "Cancelled")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
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
    pub canaries: BTreeMap<String, String>,
    pub revoked_canaries: std::collections::BTreeSet<String>,
    pub cleaned_canaries: std::collections::BTreeSet<String>,
}
impl UpgradeTask {
    pub(crate) fn require_protocol(&self) -> Result<()> {
        if self.protocol_version != 2 {
            return Err(Error::Invalid("upgrade task protocol version 2 required; finish legacy tasks with their original version".into()));
        }
        Ok(())
    }
    pub fn active(&self) -> bool {
        !matches!(self.phase.as_str(), "Completed" | "Aborted")
    }
    pub(crate) fn blocks_resume(&self, stored: &StoredEnvironment) -> bool {
        self.active()
            && (self.operations.values().any(|operation| {
                operation.environment_id == stored.spec.id && !operation.settled()
            }) || self
                .nodes
                .get(&stored.assignment.node_id)
                .is_some_and(|node| matches!(node.phase.as_str(), "evacuating" | "quiescing")))
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
    /// Reserved probe names never become ordinary user creations after a task ends.
    pub(crate) async fn check_canary_creation(&self, id: &str) -> Result<()> {
        if !id.starts_with(UPGRADE_CANARY_PREFIX) {
            return Ok(());
        }
        let [header, task] = self.store.fields([HEADER.into(), UPGRADE.into()]).await?;
        self.header(&header)?;
        canary_creation(id, task.as_deref())
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
