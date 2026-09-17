//! Atomic ownership registration for a locally admitted candidate. This is a
//! storage primitive, not a scheduling/admission RPC: the coordinator must fence
//! heartbeat expiry, apply placement rules and update its scheduling ledger.
use super::*;
use adx_core::{
    scheduling::DeviceAllocation,
    snapshots::{Reference, Snapshot},
};

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
    /// acknowledgement; start through the existing per-Instance controller.
    Owned(StoredInstance),
    /// Another node owns it, or a result/retirement/recovery already exists.
    /// Do not start a new runtime. Reuse/query this record and release only the
    /// losing attempt's tentative resources, never an existing execution's hold.
    Existing(StoredInstance),
}
impl ClaimOutcome {
    pub fn record(&self) -> &StoredInstance {
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
            let values = self.store.fields(&[HEADER.into()]).await?;
            let mut header = self.header(&values[0])?;
            header.advance()?;
            if self
                .store
                .cas(values[0].as_deref().ok_or(Error::Conflict)?, &header, None)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable("claim barrier contended".into()))
    }

    /// Register one globally unique owner for a normalized InstanceSpec.
    ///
    /// The spec (including tenant) must match on every retry. The Instance ID is
    /// the idempotency key; retries must not generate a new ID. Generation and
    /// ownership are committed in the same Redis CAS. On an unavailable result,
    /// retry this operation or query `get`; a timeout does not prove no write.
    /// This never transfers ownership or reactivates a completed Instance.
    pub async fn claim(&self, spec: InstanceSpec, candidate: &LocalClaim) -> Result<ClaimOutcome> {
        spec.validate()?;
        if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
            return Err(Error::Invalid(
                "claim requires node and process session".into(),
            ));
        }
        validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        let field = format!("instance:{}", spec.id);
        for _ in 0..ATTEMPTS {
            let values = self
                .store
                .fields(&[
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", candidate.node_id),
                ])
                .await?;
            let mut h = self.header(&values[0])?;
            let node: StoredNode = decode(values[2].as_deref().ok_or(Error::NotFound)?)?;
            if node.node.id != candidate.node_id
                || node.shard_id >= h.shards
                || node
                    .session
                    .as_ref()
                    .is_none_or(|s| s.id != candidate.node_session_id)
            {
                return Err(Error::Conflict);
            }
            if let Some(raw) = &values[1] {
                let old: StoredInstance = decode(raw)?;
                old.validate()?;
                if old.spec != spec {
                    return Err(Error::Conflict);
                }
                if old.assignment.node_id != candidate.node_id
                    || old.result.is_some()
                    || old.invalidated
                    || old.recovery.is_some()
                {
                    return Ok(ClaimOutcome::Existing(old));
                }
                if old.assignment.devices != candidate.devices {
                    return Err(Error::Conflict);
                }
                claim_admission(&node)?;
                return Ok(ClaimOutcome::Owned(old));
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
                        instance_id: spec.id.clone(),
                    })
                {
                    return Err(Error::Conflict);
                }
                Some(raw)
            } else {
                None
            };
            let generation = h.generation.checked_add(1).ok_or(Error::Conflict)?;
            let record = StoredInstance {
                recovery: None,
                invalidated: false,
                spec: spec.clone(),
                result: None,
                assignment: Assignment {
                    instance_id: spec.id.clone(),
                    node_id: candidate.node_id.clone(),
                    shard_id: node.shard_id,
                    generation,
                    devices: candidate.devices.clone(),
                },
            };
            record.validate()?;
            h.generation = generation;
            h.advance()?;
            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
                if ARGV[5] ~= '' and redis.call('HGET', KEYS[2], ARGV[5]) ~= ARGV[6] then return 0 end
                redis.call('HSET', KEYS[1], 'header', ARGV[2], ARGV[3], ARGV[4]); return 1
            "#)
                .arg(2).arg(&self.store.key).arg(self.snapshots_key())
                .arg(values[0].as_deref().ok_or(Error::Conflict)?)
                .arg(encode(&h)?).arg(&field).arg(encode(&record)?)
                .arg(spec.snapshot_id.as_deref().unwrap_or(""))
                .arg(snapshot.as_deref().unwrap_or(""));
            if self.store.query::<u8>(command).await? == 1 {
                return Ok(ClaimOutcome::Owned(record));
            }
        }
        Err(Error::Unavailable(
            "concurrent ownership registration; retry same Instance ID".into(),
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
