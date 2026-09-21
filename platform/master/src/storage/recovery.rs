//! Atomic ownership transfer after the source execution has been invalidated.
use super::*;

impl StoredInstance {
    pub fn recovery_point(&self, now: u64) -> Option<&adx_core::RestorePoint> {
        if !self.invalidated {
            return None;
        }
        let record = self.result.as_ref()?;
        if record.state != InstanceState::Failed || record.resources_held {
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
    ) -> Result<StoredInstance> {
        if previous.instance_id != replacement.instance_id
            || previous.node_id == replacement.node_id
            || previous.generation >= replacement.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("instance:{}", previous.instance_id);
        for _ in 0..ATTEMPTS {
            let [header_value, instance_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let mut stored_instance: StoredInstance =
                decode(instance_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_instance.validate()?;
            if stored_instance.assignment == replacement
                && stored_instance
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| &recovery.source == previous)
            {
                return Ok(stored_instance);
            }
            if &stored_instance.assignment != previous
                || replacement.generation <= header.generation
            {
                return Err(Error::Conflict);
            }
            let mut checkpoint = stored_instance
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
                    instance_id: stored_instance.spec.id.clone(),
                    ownership_generation: previous.generation,
                    runtime_id: checkpoint.source_runtime_id.clone(),
                });
            }
            stored_instance.result = Some(InstanceRecord {
                spec: stored_instance.spec.clone(),
                assignment: replacement.clone(),
                state: InstanceState::Paused,
                revision: 1,
                runtime_id: format!("{}-{}", stored_instance.spec.id, replacement.generation),
                resources_held: false,
                runtime_ip: None,
                checkpoint: Some(checkpoint),
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            });
            stored_instance.assignment = replacement.clone();
            stored_instance.invalidated = false;
            stored_instance.recovery = Some(Recovery {
                source: previous.clone(),
                pending: true,
            });
            stored_instance.validate()?;
            header.generation = replacement.generation;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&stored_instance)?)),
                )
                .await?
            {
                return Ok(stored_instance);
            }
        }
        Err(Error::Unavailable(
            "concurrent recovery assignment; retry".into(),
        ))
    }
}
