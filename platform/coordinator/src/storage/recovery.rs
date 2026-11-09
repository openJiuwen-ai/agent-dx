//! Atomic ownership transfer after the source execution has been invalidated.
use super::*;

/// Facts captured with the checkpoint, rather than the source node's current
/// software after a restart or upgrade, constrain every cross-node recovery.
pub(crate) fn recovery_profile_requirements(
    runtime_class: &str,
    profile: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let keys = [
        "ADX_RECOVERY_HOST".to_string(),
        format!("ADX_RECOVERY_RUNTIME_{runtime_class}"),
        "ADX_RECOVERY_ROOTFS_SHA256".to_string(),
        "ADX_RECOVERY_ROOTFS_PATH".to_string(),
    ];
    let mut required = BTreeMap::new();
    for key in keys {
        let value = profile
            .get(&key)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::Invalid("checkpoint recovery compatibility evidence is missing".into())
            })?;
        if key != "ADX_RECOVERY_ROOTFS_PATH"
            && (value.len() != 64
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        {
            return Err(Error::Invalid(
                "invalid checkpoint recovery fingerprint".into(),
            ));
        }
        required.insert(key, value.clone());
    }
    if !std::path::Path::new(&required["ADX_RECOVERY_ROOTFS_PATH"]).is_absolute() {
        return Err(Error::Invalid(
            "invalid checkpoint recovery rootfs path".into(),
        ));
    }
    Ok(required)
}

pub(crate) fn checkpoint_requirements(
    spec: &EnvironmentSpec,
    profile: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let required = recovery_profile_requirements(&spec.runtime_class, profile)?;
    let path = &required["ADX_RECOVERY_ROOTFS_PATH"];
    if !spec.image.is_empty()
        || spec.execution.rootfs.is_some()
        || spec.runtime_profile.as_ref().is_some_and(|runtime| {
            runtime.rootfs.runtime_class != spec.runtime_class
                || runtime.rootfs.r#type != "local"
                || runtime.rootfs.path != *path
                || runtime.bootstrap.r#type != "erofs"
                || runtime.bootstrap.root != *path
        })
    {
        return Err(Error::Invalid(
            "checkpoint recovery requires a verified execution rootfs".into(),
        ));
    }
    Ok(required)
}

pub(crate) fn validate_checkpoint_target(
    spec: &EnvironmentSpec,
    checkpoint: &adx_core::RestorePoint,
    target: &StoredNode,
) -> Result<()> {
    let required = checkpoint_requirements(spec, &checkpoint.recovery_profile)?;
    if checkpoint.artifact.storage == "local"
        || checkpoint.artifact.scope.is_empty()
        || target.checkpoint_gc_fenced
        || !target
            .checkpoint_scopes
            .contains(&checkpoint.artifact.scope)
        || target.node.labels.get("ADX_CHECKPOINT_STORAGE") != Some(&checkpoint.artifact.storage)
        || target.node.labels.get("ADX_CHECKPOINT_SCOPE") != Some(&checkpoint.artifact.scope)
        || !target.node.runtime_classes.contains(&spec.runtime_class)
        || required
            .iter()
            .any(|(key, value)| target.node.labels.get(key) != Some(value))
    {
        return Err(Error::Invalid(
            "checkpoint is incompatible with the recovery target".into(),
        ));
    }
    Ok(())
}

impl StoredEnvironment {
    pub fn resume_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        let record = self.result.as_ref()?;
        if record.state != EnvironmentState::Paused
            || record.resources_held
            || self.resume.is_none()
            || (!self.invalidated && !self.resume.as_ref()?.local_rejected)
        {
            return None;
        }
        record
            .checkpoint
            .as_ref()
            .or(record.snapshot_source.as_ref())
            .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now))
    }
    pub fn recovery_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        if !self.invalidated {
            return None;
        }
        let record = self.result.as_ref()?;
        if record.state != EnvironmentState::Failed || record.resources_held {
            return None;
        }
        record
            .checkpoint
            .as_ref()
            .or(record.snapshot_source.as_ref())
            .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now))
    }
}
impl Session {
    pub async fn reserve_recovery(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
    ) -> Result<StoredEnvironment> {
        Box::pin(self.transfer_checkpoint(previous, replacement, now, None, None)).await
    }

    /// Planned transfer requires a published, stopped source; it never simulates node failure.
    pub async fn reserve_migration(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
        resume: bool,
        expected_source_session: &str,
        expected_target_session: &str,
    ) -> Result<StoredEnvironment> {
        Box::pin(self.transfer_checkpoint(
            previous,
            replacement,
            now,
            Some(resume),
            Some((expected_source_session, expected_target_session)),
        ))
        .await
    }

    async fn transfer_checkpoint(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
        planned_resume: Option<bool>,
        expected_sessions: Option<(&str, &str)>,
    ) -> Result<StoredEnvironment> {
        let (planned, resume) = (planned_resume.is_some(), planned_resume.unwrap_or(true));
        if previous.environment_id != replacement.environment_id
            || previous.node_id == replacement.node_id
            || previous.generation >= replacement.generation
        {
            return Err(Error::Conflict);
        }
        if replacement.generation >> 32 == self.epoch {
            self.generations
                .fetch_max(replacement.generation, Ordering::AcqRel);
        }
        let field = format!("environment:{}", previous.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value, source_node_value, upgrade_value] =
                self.store
                    .fields([
                        HEADER.into(),
                        field.clone(),
                        format!("node:{}", replacement.node_id),
                        format!("node:{}", previous.node_id),
                        UPGRADE.into(),
                    ])
                    .await?;
            let header = self.header(&header_value)?;
            let upgrade = upgrade_value
                .as_deref()
                .map(decode::<UpgradeTask>)
                .transpose()?;
            let mut stored_environment: StoredEnvironment =
                decode(environment_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_environment.validate()?;
            if planned {
                let task = upgrade.as_ref().ok_or(Error::Conflict)?;
                task.require_protocol()?;
                let operation = task
                    .migration_operation(previous, &replacement)
                    .ok_or(Error::Conflict)?;
                if expected_sessions
                    != Some((
                        operation.source_session_id.as_str(),
                        operation.target_session_id.as_str(),
                    ))
                {
                    return Err(Error::Conflict);
                }
            }
            if planned && volumes::has_volumes(&stored_environment.spec) {
                return Err(Error::Invalid(
                    "planned migration of managed Volumes is not supported".into(),
                ));
            }
            if let Some((source_session, target_session)) = expected_sessions {
                let source: StoredNode =
                    decode(source_node_value.as_deref().ok_or(Error::NotFound)?)?;
                let target: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
                if source_session.is_empty()
                    || target_session.is_empty()
                    || source
                        .session
                        .as_ref()
                        .is_none_or(|s| s.id != source_session)
                    || target
                        .session
                        .as_ref()
                        .is_none_or(|s| s.id != target_session)
                {
                    return Err(Error::Conflict);
                }
            }
            if stored_environment.assignment == replacement
                && stored_environment
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| &recovery.source == previous)
            {
                return Ok(stored_environment);
            }
            if &stored_environment.assignment != previous {
                return Err(Error::Conflict);
            }
            let planned_point = stored_environment
                .result
                .as_ref()
                .filter(|r| {
                    planned
                        && !stored_environment.invalidated
                        && stored_environment.resume.is_none()
                        && r.state == EnvironmentState::Paused
                        && !r.resources_held
                        && !r.restart_pending
                })
                .and_then(|r| r.checkpoint.as_ref().or(r.snapshot_source.as_ref()))
                .filter(|cp| cp.artifact.storage != "local" && !cp.expired(now));
            let mut checkpoint = if planned {
                planned_point
            } else {
                stored_environment
                    .recovery_point(now)
                    .or_else(|| stored_environment.resume_point(now))
            }
            .ok_or(Error::Conflict)?
            .clone();
            node_value.as_deref().ok_or(Error::NotFound)?;
            let target = self.get_node(&replacement.node_id).await?;
            validate_checkpoint_target(&stored_environment.spec, &checkpoint, &target)?;
            if planned {
                let source: StoredNode =
                    decode(source_node_value.as_deref().ok_or(Error::NotFound)?)?;
                if !source
                    .checkpoint_scopes
                    .contains(&checkpoint.artifact.scope)
                    || !target
                        .checkpoint_scopes
                        .contains(&checkpoint.artifact.scope)
                {
                    return Err(Error::Invalid(
                        "checkpoint backend identity is unavailable on migration nodes".into(),
                    ));
                }
            }
            if !target.node.available
                || target.shard_id != replacement.shard_id
                || target.session.as_ref().is_some_and(|s| !s.routable)
            {
                return Err(Error::Conflict);
            }
            if checkpoint.origin.is_none() {
                checkpoint.origin = Some(adx_core::runtime::RuntimeIdentity {
                    environment_id: stored_environment.spec.id.clone(),
                    ownership_generation: previous.generation,
                    runtime_id: checkpoint.source_runtime_id.clone(),
                });
            }
            let source = stored_environment
                .result
                .as_ref()
                .ok_or(Error::Conflict)?
                .clone();
            stored_environment.result = Some(EnvironmentRecord {
                observation: None,
                execution_artifacts: vec![],
                snapshot_source: source.snapshot_source.clone(),
                failure: None,
                spec: stored_environment.spec.clone(),
                assignment: replacement.clone(),
                state: EnvironmentState::Paused,
                revision: 1,
                runtime: adx_core::Runtime {
                    id: format!("{}-{}", stored_environment.spec.id, replacement.generation),
                    ip: None,
                },
                resources_held: false,
                checkpoint: if source.snapshot_source.is_some() && source.checkpoint.is_none() {
                    None
                } else {
                    Some(checkpoint.clone())
                },
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            });
            stored_environment.assignment = replacement.clone();
            stored_environment.volume_execution = None;
            stored_environment.invalidated = false;
            if let Some(intent) = &mut stored_environment.resume {
                intent.local_rejected = false;
            }
            stored_environment.failed_since_unix_seconds = None;
            stored_environment.recovery = Some(Recovery {
                source: previous.clone(),
                pending: resume,
            });
            if let Some(last) = stored_environment
                .last_interruption
                .as_mut()
                .filter(|last| last.resolved_at_unix_millis.is_none())
            {
                last.recovery_state = RecoveryState::Recovering;
            }
            stored_environment.validate()?;
            let catalog = if super::volumes::has_volumes(&stored_environment.spec) {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                catalog.reserve(&stored_environment, &target)?;
                Some(catalog)
            } else {
                None
            };

            if Box::pin(self.assignment_cas_checked(
                &header,
                environment_value.as_deref(),
                &stored_environment,
                catalog.as_ref(),
                None,
                &[
                    (UPGRADE.into(), upgrade_value.clone()),
                    (
                        format!("node:{}", previous.node_id),
                        source_node_value.clone(),
                    ),
                    (format!("node:{}", replacement.node_id), node_value.clone()),
                ],
            ))
            .await?
            {
                return Ok(stored_environment);
            }
        }
        Err(Error::Unavailable(
            "concurrent recovery assignment; retry".into(),
        ))
    }
}
