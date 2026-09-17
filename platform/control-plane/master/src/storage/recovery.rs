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
            let values = self
                .store
                .fields(&[
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let mut h = self.header(&values[0])?;
            let mut old: StoredInstance = decode(values[1].as_deref().ok_or(Error::NotFound)?)?;
            old.validate()?;
            if old.assignment == replacement
                && old.recovery.as_ref().is_some_and(|r| &r.source == previous)
            {
                return Ok(old);
            }
            if &old.assignment != previous || replacement.generation <= h.generation {
                return Err(Error::Conflict);
            }
            let mut cp = old.recovery_point(now).ok_or(Error::Conflict)?.clone();
            let target: StoredNode = decode(values[2].as_deref().ok_or(Error::NotFound)?)?;
            if !target.node.available
                || target.shard_id != replacement.shard_id
                || target.session.as_ref().is_some_and(|s| !s.routable)
            {
                return Err(Error::Conflict);
            }
            if cp.origin.is_none() {
                cp.origin = Some(adx_core::runtime::RuntimeIdentity {
                    instance_id: old.spec.id.clone(),
                    ownership_generation: previous.generation,
                    runtime_id: cp.source_runtime_id.clone(),
                });
            }
            old.result = Some(InstanceRecord {
                spec: old.spec.clone(),
                assignment: replacement.clone(),
                state: InstanceState::Paused,
                revision: 1,
                runtime_id: format!("{}-{}", old.spec.id, replacement.generation),
                resources_held: false,
                runtime_ip: None,
                checkpoint: Some(cp),
                last_operation: None,
                restart_attempts: 0,
                restart_pending: false,
            });
            old.assignment = replacement.clone();
            old.invalidated = false;
            old.recovery = Some(Recovery {
                source: previous.clone(),
                pending: true,
            });
            old.validate()?;
            h.generation = replacement.generation;
            h.advance()?;
            if self
                .store
                .cas(
                    values[0].as_deref().unwrap(),
                    &h,
                    Some((&field, encode(&old)?)),
                )
                .await?
            {
                return Ok(old);
            }
        }
        Err(Error::Unavailable(
            "concurrent recovery assignment; retry".into(),
        ))
    }
}
