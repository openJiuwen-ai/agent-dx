//! Atomic ownership transfer after the source execution has been invalidated.
use super::*;

impl StoredCapsule {
    pub fn recovery_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        if !self.invalidated {
            return None;
        }
        let record = self.result.as_ref()?;
        if record.state != CapsuleState::Failed || record.resources_held {
            return None;
        }
        record
            .checkpoint
            .as_ref()
            .filter(|cp| cp.artifact.storage != "local" && cp.expires_at_unix_seconds > now)
    }
}
impl Session {
    pub async fn reserve_recovery(
        &self,
        previous: &Assignment,
        replacement: Assignment,
        now: u64,
    ) -> Result<StoredCapsule> {
        if previous.capsule_id != replacement.capsule_id
            || previous.node_id == replacement.node_id
            || previous.generation >= replacement.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("capsule:{}", previous.capsule_id);
        for _ in 0..ATTEMPTS {
            let [header_value, capsule_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let mut stored_capsule: StoredCapsule =
                decode(capsule_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_capsule.validate()?;
            if stored_capsule.assignment == replacement
                && stored_capsule
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| &recovery.source == previous)
            {
                return Ok(stored_capsule);
            }
            if &stored_capsule.assignment != previous || replacement.generation <= header.generation
            {
                return Err(Error::Conflict);
            }
            let mut checkpoint = stored_capsule
                .recovery_point(now)
                .ok_or(Error::Conflict)?
                .clone();
            let target: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if !target.node.available
                || target.shard_id != replacement.shard_id
                || target.session.as_ref().is_some_and(|s| !s.routable)
            {
                return Err(Error::Conflict);
            }
            if checkpoint.origin.is_none() {
                checkpoint.origin = Some(adx_core::runtime::RuntimeIdentity {
                    capsule_id: stored_capsule.spec.id.clone(),
                    ownership_generation: previous.generation,
                    runtime_id: checkpoint.source_runtime_id.clone(),
                });
            }
            stored_capsule.result = Some(CapsuleRecord {
                spec: stored_capsule.spec.clone(),
                assignment: replacement.clone(),
                state: CapsuleState::Paused,
                revision: 1,
                runtime: adx_core::Runtime {
                    id: format!("{}-{}", stored_capsule.spec.id, replacement.generation),
                    ip: None,
                },
                resources_held: false,
                checkpoint: Some(checkpoint),
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            });
            stored_capsule.assignment = replacement.clone();
            stored_capsule.invalidated = false;
            stored_capsule.recovery = Some(Recovery {
                source: previous.clone(),
                pending: true,
            });
            stored_capsule.validate()?;
            header.generation = replacement.generation;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&stored_capsule)?)),
                )
                .await?
            {
                return Ok(stored_capsule);
            }
        }
        Err(Error::Unavailable(
            "concurrent recovery assignment; retry".into(),
        ))
    }
}
