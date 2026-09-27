//! Atomic ownership registration for a locally admitted candidate. This is a
//! storage primitive, not a scheduling/admission RPC: the coordinator must fence
//! heartbeat expiry, apply placement rules and update its scheduling ledger.
use super::*;
use adx_core::{
    scheduling::DeviceAllocation,
    snapshots::{Reference, Snapshot},
};
use std::collections::{BTreeMap, BTreeSet};

const CLAIM_BATCH: &str = r#"
if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
local cursor = 4
for _ = 1, tonumber(ARGV[3]) do
  if redis.call('HGET', KEYS[2], ARGV[cursor]) ~= ARGV[cursor + 1] then return 0 end
  cursor = cursor + 2
end
local writes = tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'header', ARGV[cursor])
cursor = cursor + 1
for _ = 1, writes do
  redis.call('HSET', KEYS[1], ARGV[cursor], ARGV[cursor + 1])
  cursor = cursor + 2
end
return 1
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalClaim {
    pub node_id: String,
    pub node_session_id: String,
    /// Cards already tentatively held by the node's shared Admission ledger.
    pub devices: Vec<DeviceAllocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This node owns a pending initial creation. Includes replay after a lost
    /// acknowledgement; start through the existing per-Environment controller.
    Owned(StoredEnvironment),
    /// Another node owns it, or a result/retirement/recovery already exists.
    /// Do not start a new runtime. Reuse/query this record and release only the
    /// losing attempt's tentative resources, never an existing execution's hold.
    Existing(StoredEnvironment),
}
impl ClaimOutcome {
    pub fn record(&self) -> &StoredEnvironment {
        match self {
            Self::Owned(record) | Self::Existing(record) => record,
        }
    }
}

impl Session {
    /// Fence CAS commands from completed-but-unacknowledged operations. Call only
    /// after their producer has returned; a plain NotFound read is not a barrier.
    pub async fn claim_barrier(&self) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let [header_value] = self.store.fields([HEADER.into()]).await?;
            let mut header = self.header(&header_value)?;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    None,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable("claim barrier contended".into()))
    }

    /// Register one globally unique owner for a normalized EnvironmentSpec.
    ///
    /// The spec (including tenant) must match on every retry. The Environment ID is
    /// the idempotency key; retries must not generate a new ID. Generation and
    /// ownership are committed in the same Redis CAS. On an unavailable result,
    /// retry this operation or query `get`; a timeout does not prove no write.
    /// This never transfers ownership or reactivates a completed Environment.
    pub async fn claim(
        &self,
        spec: EnvironmentSpec,
        candidate: &LocalClaim,
    ) -> Result<ClaimOutcome> {
        spec.validate()?;
        if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
            return Err(Error::Invalid(
                "claim requires node and process session".into(),
            ));
        }
        validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        let field = format!("environment:{}", spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", candidate.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if node.node.id != candidate.node_id
                || node.shard_id >= header.shards
                || node
                    .session
                    .as_ref()
                    .is_none_or(|s| s.id != candidate.node_session_id)
            {
                return Err(Error::Conflict);
            }
            if let Some(environment_value) = &environment_value {
                let existing: StoredEnvironment = decode(environment_value)?;
                existing.validate()?;
                if existing.spec != spec {
                    return Err(Error::Conflict);
                }
                if existing.assignment.node_id != candidate.node_id
                    || existing.result.is_some()
                    || existing.invalidated
                    || existing.recovery.is_some()
                {
                    return Ok(ClaimOutcome::Existing(existing));
                }
                if existing.assignment.devices != candidate.devices {
                    return Err(Error::Conflict);
                }
                claim_admission(&node)?;
                return Ok(ClaimOutcome::Owned(existing));
            }
            claim_admission(&node)?;
            // Snapshot references are kept in a separate hash and do not bump
            // the control header. Compare their exact bytes in the same CAS.
            let snapshot = if let Some(id) = &spec.snapshot_id {
                let raw = self.snapshot_raw(id).await?.ok_or(Error::NotFound)?;
                let snapshot: Snapshot = decode(&raw)?;
                snapshot.validate()?;
                if snapshot.template.tenant_id != spec.tenant_id
                    || !snapshot.references.contains(&Reference::Restore {
                        environment_id: spec.id.clone(),
                    })
                {
                    return Err(Error::Conflict);
                }
                Some(raw)
            } else {
                None
            };
            let generation = header.generation.checked_add(1).ok_or(Error::Conflict)?;
            let record = StoredEnvironment {
                recovery: None,
                invalidated: false,
                spec: spec.clone(),
                result: None,
                assignment: Assignment {
                    environment_id: spec.id.clone(),
                    node_id: candidate.node_id.clone(),
                    shard_id: node.shard_id,
                    generation,
                    devices: candidate.devices.clone(),
                },
            };
            record.validate()?;
            header.generation = generation;
            header.advance()?;
            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
                if ARGV[5] ~= '' and redis.call('HGET', KEYS[2], ARGV[5]) ~= ARGV[6] then return 0 end
                redis.call('HSET', KEYS[1], 'header', ARGV[2], ARGV[3], ARGV[4]); return 1
            "#)
                .arg(2).arg(&self.store.key).arg(self.snapshots_key())
                .arg(header_value.as_deref().ok_or(Error::Conflict)?)
                .arg(encode(&header)?).arg(&field).arg(encode(&record)?)
                .arg(spec.snapshot_id.as_deref().unwrap_or(""))
                .arg(snapshot.as_deref().unwrap_or(""));
            if self.store.query::<u8>(command).await? == 1 {
                self.store
                    .notify_committed_change(header.revision, vec![field]);
                return Ok(ClaimOutcome::Owned(record));
            }
        }
        Err(Error::Unavailable(
            "concurrent ownership registration; retry same Environment ID".into(),
        ))
    }

    /// Commit independent local ownership requests with one header CAS. The
    /// returned vector preserves request order; a conflict in one entry does not
    /// discard other valid claims in the same batch.
    pub async fn claim_batch(
        &self,
        claims: Vec<(EnvironmentSpec, LocalClaim)>,
    ) -> Result<Vec<Result<ClaimOutcome>>> {
        if claims.is_empty() {
            return Ok(Vec::new());
        }
        for (spec, candidate) in &claims {
            spec.validate()?;
            if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
                return Err(Error::Invalid(
                    "claim requires node and process session".into(),
                ));
            }
            validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        }
        let environment_fields: BTreeSet<_> = claims
            .iter()
            .map(|(spec, _)| format!("environment:{}", spec.id))
            .collect();
        let node_fields: BTreeSet<_> = claims
            .iter()
            .map(|(_, candidate)| format!("node:{}", candidate.node_id))
            .collect();
        for _ in 0..ATTEMPTS {
            let fields: Vec<_> = std::iter::once(HEADER.to_owned())
                .chain(environment_fields.iter().cloned())
                .chain(node_fields.iter().cloned())
                .collect();
            let values = self.store.fields_vec(&fields).await?;
            let mut by_field: BTreeMap<_, _> = fields.into_iter().zip(values).collect();
            let header_value = by_field.remove(HEADER).flatten().ok_or(Error::Conflict)?;
            let mut header = self.header(&Some(header_value.clone()))?;
            let mut environments = BTreeMap::new();
            for field in &environment_fields {
                if let Some(value) = by_field.remove(field).flatten() {
                    let record: StoredEnvironment = decode(&value)?;
                    record.validate()?;
                    environments.insert(record.spec.id.clone(), record);
                }
            }
            let mut nodes = BTreeMap::new();
            for field in &node_fields {
                let value = by_field.remove(field).flatten().ok_or(Error::NotFound)?;
                let node: StoredNode = decode(&value)?;
                nodes.insert(node.node.id.clone(), node);
            }
            let snapshot_ids: BTreeSet<_> = claims
                .iter()
                .filter(|(spec, _)| !environments.contains_key(&spec.id))
                .filter_map(|(spec, _)| spec.snapshot_id.clone())
                .collect();
            let mut snapshots = BTreeMap::new();
            for id in &snapshot_ids {
                let raw = self.snapshot_raw(id).await?.ok_or(Error::NotFound)?;
                let snapshot: Snapshot = decode(&raw)?;
                snapshot.validate()?;
                snapshots.insert(id.clone(), (raw, snapshot));
            }

            let mut results = Vec::with_capacity(claims.len());
            let mut writes = Vec::new();
            for (spec, candidate) in &claims {
                let result = (|| {
                    let node = nodes.get(&candidate.node_id).ok_or(Error::NotFound)?;
                    if node.node.id != candidate.node_id
                        || node.shard_id >= header.shards
                        || node
                            .session
                            .as_ref()
                            .is_none_or(|session| session.id != candidate.node_session_id)
                    {
                        return Err(Error::Conflict);
                    }
                    claim_admission(node)?;
                    if let Some(existing) = environments.get(&spec.id) {
                        if existing.spec != *spec {
                            return Err(Error::Conflict);
                        }
                        if existing.assignment.node_id != candidate.node_id
                            || existing.result.is_some()
                            || existing.invalidated
                            || existing.recovery.is_some()
                        {
                            return Ok(ClaimOutcome::Existing(existing.clone()));
                        }
                        if existing.assignment.devices != candidate.devices {
                            return Err(Error::Conflict);
                        }
                        return Ok(ClaimOutcome::Owned(existing.clone()));
                    }
                    if let Some(id) = &spec.snapshot_id {
                        let (_, snapshot) = snapshots.get(id).ok_or(Error::NotFound)?;
                        if snapshot.template.tenant_id != spec.tenant_id
                            || !snapshot.references.contains(&Reference::Restore {
                                environment_id: spec.id.clone(),
                            })
                        {
                            return Err(Error::Conflict);
                        }
                    }
                    let generation = header.generation.checked_add(1).ok_or(Error::Conflict)?;
                    header.generation = generation;
                    header.advance()?;
                    let record = StoredEnvironment {
                        recovery: None,
                        invalidated: false,
                        spec: spec.clone(),
                        result: None,
                        assignment: Assignment {
                            environment_id: spec.id.clone(),
                            node_id: candidate.node_id.clone(),
                            shard_id: node.shard_id,
                            generation,
                            devices: candidate.devices.clone(),
                        },
                    };
                    record.validate()?;
                    environments.insert(spec.id.clone(), record.clone());
                    writes.push((
                        format!("environment:{}", spec.id),
                        encode(&record)?,
                        header.revision,
                    ));
                    Ok(ClaimOutcome::Owned(record))
                })();
                results.push(result);
            }
            if writes.is_empty() {
                return Ok(results);
            }
            let mut command = redis::cmd("EVAL");
            command
                .arg(CLAIM_BATCH)
                .arg(2)
                .arg(&self.store.key)
                .arg(self.snapshots_key())
                .arg(&header_value)
                .arg(writes.len())
                .arg(snapshot_ids.len());
            for id in &snapshot_ids {
                command.arg(id).arg(&snapshots[id].0);
            }
            command.arg(encode(&header)?);
            for (field, value, _) in &writes {
                command.arg(field).arg(value);
            }
            if self.store.query::<u8>(command).await? == 1 {
                for (field, _, revision) in writes {
                    self.store.notify_committed_change(revision, vec![field]);
                }
                return Ok(results);
            }
        }
        Err(Error::Unavailable(
            "concurrent ownership batch registration; retry same Environment IDs".into(),
        ))
    }
}

fn claim_admission(node: &StoredNode) -> Result<()> {
    if !node.node.available || node.session.as_ref().is_none_or(|s| !s.routable) {
        return Err(Error::Unavailable(
            "node is not accepting ownership claims".into(),
        ));
    }
    Ok(())
}
