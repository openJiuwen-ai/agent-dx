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
if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
local cursor=4
for _=1,tonumber(ARGV[3]) do
  if redis.call('HGET',KEYS[2],ARGV[cursor]) ~= ARGV[cursor+1] then return 0 end
  cursor=cursor+2
end
if (redis.call('HGET',KEYS[1],'claim-fence') or '0') ~= ARGV[cursor] then return 0 end
cursor=cursor+1
local first=cursor
for _=1,tonumber(ARGV[2]) do
  if redis.call('HEXISTS',KEYS[1],ARGV[cursor]) == 1 then return 0 end
  cursor=cursor+2
end
local nodes=tonumber(ARGV[cursor]); cursor=cursor+1
local node_start=cursor
for _=1,nodes do
  if redis.call('HGET',KEYS[1],ARGV[cursor]) ~= ARGV[cursor+1] then return 0 end
  cursor=cursor+2
end
if ARGV[cursor] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[cursor] then return 0 end
local next_header=ARGV[cursor+1]; cursor=cursor+2
for i=first,first+tonumber(ARGV[2])*2-1,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
for i=node_start,node_start+nodes*2-1,2 do redis.call('HINCRBY',KEYS[1],'node-ownership:' .. string.sub(ARGV[i],14),1) end
local count=tonumber(ARGV[cursor]); cursor=cursor+1
for _=1,count do
  if ARGV[cursor+1] == '' then redis.call('HDEL',KEYS[3],ARGV[cursor])
  else redis.call('HSET',KEYS[3],ARGV[cursor],ARGV[cursor+1]) end
  cursor=cursor+2
end
if next_header ~= '' then redis.call('HSET',KEYS[1],'header',next_header) end
return 1
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalClaim {
    pub request_id: String,
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
    /// Fence uncertain optional Claim commands before rebuilding their catalog.
    /// Ordinary lifecycle mutations never compare or advance this token.
    pub async fn claim_barrier(&self) -> Result<()> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            redis.call('HINCRBY',KEYS[1],'claim-fence',1); return 1
        "#,
            )
            .arg(1)
            .arg(&self.store.key)
            .arg(self.epoch.to_string());
        match self.store.query::<u8>(command).await? {
            1 => Ok(()),
            _ => Err(Error::Conflict),
        }
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
        if volumes::has_volumes(&spec) {
            return self
                .claim_batch(vec![(spec, candidate.clone())])
                .await?
                .into_iter()
                .next()
                .ok_or(Error::Conflict)?;
        }
        spec.validate()?;
        if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
            return Err(Error::Invalid(
                "claim requires node and process session".into(),
            ));
        }
        validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        let field = format!("environment:{}", spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value, claim_fence] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", candidate.node_id),
                    "claim-fence".into(),
                ])
                .await?;
            let header = self.header(&header_value)?;
            let node = self.get_node(&candidate.node_id).await?;
            let _ = node_value;
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
            let generation = self.next_generation()?;
            let record = StoredEnvironment {
                creation_generation: generation,
                creation: Some(CreationSummary::new(&candidate.request_id)?),
                last_interruption: None,
                volume_execution: None,
                resume: None,
                failed_since_unix_seconds: None,
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

            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
                if ARGV[5] ~= '' and redis.call('HGET', KEYS[2], ARGV[5]) ~= ARGV[6] then return 0 end
                if (redis.call('HGET',KEYS[1],'claim-fence') or '0') ~= ARGV[7] then return 0 end
                if redis.call('HGET',KEYS[1],ARGV[8]) ~= ARGV[9] then return 0 end
                if redis.call('HEXISTS',KEYS[1],ARGV[3]) == 1 then return 0 end
                redis.call('HSET', KEYS[1], ARGV[3], ARGV[4])
                redis.call('HINCRBY',KEYS[1],'node-ownership:' .. string.sub(ARGV[8],14),1); return 1
            "#)
                .arg(2).arg(&self.store.key).arg(self.snapshots_key())
                .arg(self.epoch.to_string())
                .arg(encode(&header)?).arg(&field).arg(encode(&record)?)
                .arg(spec.snapshot_id.as_deref().unwrap_or(""))
                .arg(snapshot.as_deref().unwrap_or(""))
                .arg(claim_fence.as_deref().unwrap_or("0"))
                .arg(format!("node-session:{}", candidate.node_id))
                .arg(&candidate.node_session_id);
            let values = vec![PublicationValue::Environment(
                record.spec.id.clone(),
                Some(Arc::new(record.clone())),
            )];
            let write = self
                .store
                .publication_write(self.epoch, vec![field.clone()])
                .await;
            let applied = self.store.query::<u8>(command).await? == 1;
            write.finish(applied, values);
            if applied {
                return Ok(ClaimOutcome::Owned(record));
            }
        }
        Err(Error::Unavailable(
            "concurrent ownership registration; retry same Environment ID".into(),
        ))
    }

    /// Commit independent optional local ownership requests in one epoch-fenced batch. The
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
            if candidate.request_id.len() > 256 {
                return Err(Error::Invalid("request ID too long".into()));
            }
            if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
                return Err(Error::Invalid(
                    "claim requires node and process session".into(),
                ));
            }
            validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        }
        let mut pinned = Vec::with_capacity(claims.len());
        for (spec, _) in &claims {
            pinned.push(self.pin_volumes(spec).await.map(|_| ()));
        }
        let uses_volumes = claims.iter().any(|(spec, _)| volumes::has_volumes(spec));
        let environment_fields: BTreeSet<_> = claims
            .iter()
            .map(|(spec, _)| format!("environment:{}", spec.id))
            .collect();
        let node_fields: BTreeSet<_> = claims
            .iter()
            .map(|(_, candidate)| format!("node:{}", candidate.node_id))
            .collect();
        for _ in 0..ATTEMPTS {
            let fields: Vec<_> = [HEADER.to_owned(), "claim-fence".to_owned()]
                .into_iter()
                .chain(environment_fields.iter().cloned())
                .chain(node_fields.iter().cloned())
                .collect();
            let values = self.store.fields_vec(&fields).await?;
            let mut by_field: BTreeMap<_, _> = fields.into_iter().zip(values).collect();
            let header_value = by_field.remove(HEADER).flatten().ok_or(Error::Conflict)?;
            let header = self.header(&Some(header_value.clone()))?;
            let claim_fence = by_field
                .remove("claim-fence")
                .flatten()
                .unwrap_or_else(|| "0".into());
            let mut catalog = if uses_volumes {
                let catalog = self.volume_catalog().await?;
                if catalog.header_value != header_value {
                    continue;
                }
                Some(catalog)
            } else {
                None
            };
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
                let Some(value) = by_field.remove(field).flatten() else {
                    continue;
                };
                let node: StoredNode = decode(&value)?;
                let node = self.get_node(&node.node.id).await?;
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
            let mut writing_nodes = BTreeMap::new();
            for ((spec, candidate), pinned) in claims.iter().zip(&pinned) {
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
                        claim_admission(node)?;
                        return Ok(ClaimOutcome::Owned(existing.clone()));
                    }
                    claim_admission(node)?;
                    pinned.clone()?;
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
                    let generation = self.next_generation()?;

                    let record = StoredEnvironment {
                        creation_generation: generation,
                        creation: Some(CreationSummary::new(&candidate.request_id)?),
                        last_interruption: None,
                        volume_execution: None,
                        resume: None,
                        failed_since_unix_seconds: None,
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
                    if volumes::has_volumes(spec) {
                        catalog
                            .as_mut()
                            .ok_or(Error::Conflict)?
                            .reserve(&record, node)?;
                    }
                    environments.insert(spec.id.clone(), record.clone());
                    writing_nodes
                        .insert(candidate.node_id.clone(), candidate.node_session_id.clone());
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
                .arg(3)
                .arg(&self.store.key)
                .arg(self.snapshots_key())
                .arg(self.volumes_key())
                .arg(self.epoch.to_string())
                .arg(writes.len())
                .arg(snapshot_ids.len());
            for id in &snapshot_ids {
                command.arg(id).arg(&snapshots[id].0);
            }
            command.arg(&claim_fence);
            for (field, value, _) in &writes {
                command.arg(field).arg(value);
            }
            command.arg(writing_nodes.len());
            for (node, session) in &writing_nodes {
                command.arg(format!("node-session:{node}")).arg(session);
            }
            if let Some(catalog) = &catalog {
                command
                    .arg(&catalog.header_value)
                    .arg(encode(&catalog.advanced_header()?)?);
                catalog.append_writes(&mut command)?;
            } else {
                command.arg("").arg("").arg(0);
            }
            let values = RedisStore::publication_values(
                writes
                    .iter()
                    .map(|(f, v, _)| (f.as_str(), Some(v.as_str()))),
            )?;
            let write = self
                .store
                .publication_write(
                    self.epoch,
                    writes.iter().map(|(f, _, _)| f.clone()).collect(),
                )
                .await;
            let applied = self.store.query::<u8>(command).await? == 1;
            write.finish_with_fields(
                applied,
                values,
                catalog
                    .iter()
                    .flat_map(volumes::Catalog::change_fields)
                    .collect(),
            );
            if applied {
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
