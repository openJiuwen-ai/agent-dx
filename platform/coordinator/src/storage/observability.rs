//! Live facts commit with their owning object; retirement folds into fixed CAS buckets.
use super::*;
use adx_core::observation::{
    now_millis, EvidenceSource, InterruptionEvidence, InterruptionReason, OperationIntent,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessCounters {
    #[serde(default)]
    pub started_at_unix_millis: u64,
    pub creations: [u64; 3],
    pub interruptions: [[u64; 7]; 4],
    pub worker_incidents: u64,
    pub recoveries: [u64; 3],
}
fn increment(counter: &mut u64) -> Result<()> {
    *counter = counter.checked_add(1).ok_or(Error::Conflict)?;
    Ok(())
}
pub(crate) fn runtime_class(value: &str) -> usize {
    match value {
        "runsc" => 0,
        "firecracker" => 1,
        "runc" => 2,
        _ => 3,
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreationResult {
    #[default]
    Unsettled,
    Success,
    Failure,
    Timeout,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreationSummary {
    pub request_id: String,
    pub result: CreationResult,
    pub reason: Option<String>,
    pub settled_at_unix_millis: Option<u64>,
    pub started_at_unix_millis: u64,
}
impl CreationSummary {
    pub(crate) fn new(request_id: &str) -> Result<Self> {
        if request_id.len() > 256 {
            return Err(Error::Invalid("request ID too long".into()));
        }
        Ok(Self {
            request_id: request_id.into(),
            result: CreationResult::Unsettled,
            reason: None,
            settled_at_unix_millis: None,
            started_at_unix_millis: now_millis()?,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryState {
    Waiting,
    Recovering,
    Recovered,
    Unrecoverable,
    ClosedByUser,
}
impl RecoveryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Recovering => "recovering",
            Self::Recovered => "recovered",
            Self::Unrecoverable => "unrecoverable",
            Self::ClosedByUser => "closed_by_user",
        }
    }
    pub fn unresolved(self) -> bool {
        matches!(self, Self::Waiting | Self::Recovering | Self::Unrecoverable)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterruptionSummary {
    #[serde(flatten)]
    pub evidence: InterruptionEvidence,
    pub incident_id: Option<String>,
    pub occurred_at_unix_millis: Option<u64>,
    pub recovery_state: RecoveryState,
    pub recovery_kind: String,
    pub recovery_worker_id: Option<String>,
    pub recovery_runtime_id: Option<String>,
    pub resolved_at_unix_millis: Option<u64>,
}

impl StoredEnvironment {
    pub(super) fn observe_result(
        &mut self,
        next: &EnvironmentRecord,
        counters: &mut BusinessCounters,
    ) -> Result<()> {
        let now = now_millis()?;
        if let Some(creation) = self
            .creation
            .as_mut()
            .filter(|c| c.result == CreationResult::Unsettled)
        {
            let outcome = if next.state == EnvironmentState::Running {
                Some((CreationResult::Success, 0))
            } else if next.state == EnvironmentState::Failed && !next.restart_pending {
                let timeout = next
                    .failure
                    .as_ref()
                    .is_some_and(|f| f.reason == adx_core::RuntimeFailureReason::DeadlineExceeded);
                Some(if timeout {
                    (CreationResult::Timeout, 2)
                } else {
                    (CreationResult::Failure, 1)
                })
            } else {
                None
            };
            if let Some((result, index)) = outcome {
                creation.result = result;
                creation.reason = (index != 0).then(|| {
                    if index == 2 {
                        "timeout"
                    } else {
                        "runtime_error"
                    }
                    .into()
                });
                creation.settled_at_unix_millis = Some(now);
                increment(&mut counters.creations[index])?;
            }
        }
        if let Some(evidence) = next
            .observation
            .as_ref()
            .and_then(|o| o.interruption.as_ref())
        {
            evidence.validate()?;
            if self
                .last_interruption
                .as_ref()
                .is_none_or(|last| last.evidence.event_id != evidence.event_id)
                && self
                    .result
                    .as_ref()
                    .and_then(|r| r.observation.as_ref())
                    .and_then(|o| o.interruption.as_ref())
                    != Some(evidence)
            {
                if self.assignment.generation != evidence.assignment_generation
                    || self.assignment.node_id != evidence.worker_id
                {
                    return Err(Error::Conflict);
                }
                // An execution may exit before its first Running publication reaches
                // either Coordinator or the journal. Settle that lifecycle result,
                // retaining evidence without counting a pre-availability interruption.
                // Repeated evidence is recognized from the previous record above.
                if let Some(previous) = self
                    .result
                    .as_ref()
                    .filter(|r| r.state == EnvironmentState::Running)
                {
                    if previous.runtime.id == evidence.runtime_id {
                        self.record_interruption(evidence.clone(), None, counters)?;
                    } else if next.runtime.id != evidence.runtime_id {
                        return Err(Error::Conflict);
                    }
                    // A replacement may fail before publishing Running while the
                    // previous execution is still authoritative. Its own evidence
                    // must not block the already-fenced lifecycle result, nor count
                    // as an interruption of the previously published execution.
                }
            }
        }
        self.observe_recovery(next, counters, now)
    }
    pub(super) fn record_interruption(
        &mut self,
        evidence: InterruptionEvidence,
        incident_id: Option<String>,
        counters: &mut BusinessCounters,
    ) -> Result<()> {
        if self.last_interruption.as_ref().is_some_and(|last| {
            last.evidence.runtime_id == evidence.runtime_id
                && last.evidence.assignment_generation == evidence.assignment_generation
        }) {
            return Ok(());
        }
        increment(
            &mut counters.interruptions[runtime_class(&self.spec.runtime_class)]
                [evidence.reason as usize],
        )?;
        self.last_interruption = Some(InterruptionSummary {
            evidence,
            incident_id,
            occurred_at_unix_millis: None,
            recovery_state: RecoveryState::Waiting,
            recovery_kind: "none".into(),
            recovery_worker_id: None,
            recovery_runtime_id: None,
            resolved_at_unix_millis: None,
        });
        Ok(())
    }
    fn observe_recovery(
        &mut self,
        next: &EnvironmentRecord,
        counters: &mut BusinessCounters,
        now: u64,
    ) -> Result<()> {
        let Some(last) = self
            .last_interruption
            .as_mut()
            .filter(|last| last.resolved_at_unix_millis.is_none())
        else {
            return Ok(());
        };
        let checkpoint = next.checkpoint.as_ref().or(next.snapshot_source.as_ref());
        let restore_possible = checkpoint.is_some_and(|cp| {
            !cp.expired(now / 1000) && (!self.invalidated || cp.artifact.storage != "local")
        });
        let cold_restart = !self.invalidated
            && next
                .spec
                .lifecycle
                .restart
                .as_ref()
                .is_some_and(|p| next.restart_attempts < p.max_attempts);
        let result = if next.state == EnvironmentState::Running
            && (next.runtime.id != last.evidence.runtime_id
                || next.assignment.generation != last.evidence.assignment_generation)
        {
            last.recovery_worker_id = Some(next.assignment.node_id.clone());
            last.recovery_runtime_id = Some(next.runtime.id.clone());
            last.recovery_kind = if next.spec.execution.failover || self.recovery.is_some() {
                "checkpoint_restore"
            } else {
                "restart"
            }
            .into();
            Some((RecoveryState::Recovered, 0))
        } else if next.state == EnvironmentState::Deleted {
            if next.observation.as_ref().and_then(|o| o.intent) == Some(OperationIntent::User) {
                Some((RecoveryState::ClosedByUser, 2))
            } else {
                Some((RecoveryState::Unrecoverable, 1))
            }
        } else if next.state == EnvironmentState::Failed
            && !next.restart_pending
            && !restore_possible
            && !cold_restart
        {
            Some((RecoveryState::Unrecoverable, 1))
        } else {
            None
        };
        if let Some((state, index)) = result {
            last.recovery_state = state;
            last.resolved_at_unix_millis = Some(now);
            increment(&mut counters.recoveries[index])?;
        } else if next.restart_pending || self.recovery.as_ref().is_some_and(|r| r.pending) {
            last.recovery_state = RecoveryState::Recovering;
        }
        Ok(())
    }
    pub(super) fn log_observation_change(&self, previous: Option<&InterruptionSummary>) {
        let Some(last) = &self.last_interruption else {
            return;
        };
        if previous.is_none_or(|old| old.evidence.event_id != last.evidence.event_id) {
            let evidence = &last.evidence;
            adx_observability::warn!(event="environment_interrupted", event_id=%evidence.event_id,
                incident_id=?last.incident_id, environment_id=%self.spec.id, runtime_id=%evidence.runtime_id,
                generation=evidence.assignment_generation, worker_id=%evidence.worker_id,
                reason=evidence.reason.as_str(), exit_code=?evidence.exit_code, "Environment interrupted");
        }
        if last.resolved_at_unix_millis.is_some()
            && previous.is_none_or(|old| {
                old.evidence.event_id != last.evidence.event_id
                    || old.resolved_at_unix_millis.is_none()
            })
        {
            adx_observability::info!(event="environment_recovery_resolved", event_id=%last.evidence.event_id,
                environment_id=%self.spec.id, runtime_id=?last.recovery_runtime_id, worker_id=?last.recovery_worker_id,
                result=last.recovery_state.as_str(), "Environment recovery resolved");
        }
    }
    pub(super) fn observe_node_loss(
        &mut self,
        incident: &str,
        planned: bool,
        counters: &mut BusinessCounters,
    ) -> Result<()> {
        let previous = self.result.clone();
        if let Some(record) = &previous {
            if record.state == EnvironmentState::Running
                && record.observation.as_ref().and_then(|o| o.intent).is_none()
            {
                self.record_interruption(
                    InterruptionEvidence {
                        event_id: format!(
                            "execution:{}:{}",
                            record.assignment.generation, record.runtime.id
                        ),
                        detected_at_unix_millis: now_millis()?,
                        assignment_generation: record.assignment.generation,
                        runtime_id: record.runtime.id.clone(),
                        worker_id: record.assignment.node_id.clone(),
                        reason: if planned {
                            InterruptionReason::Unknown
                        } else {
                            InterruptionReason::NodeUnreachable
                        },
                        evidence_source: EvidenceSource::Coordinator,
                        exit_code: None,
                        detail: if planned {
                            "Node explicitly unregistered"
                        } else {
                            "Coordinator retired unreachable node ownership"
                        }
                        .into(),
                    },
                    Some(incident.into()),
                    counters,
                )?;
            }
        }
        Ok(())
    }
}
impl Session {
    /// Fold historical orphan fields once at startup, fenced by the new epoch.
    /// Runtime retirement performs the same fold in its lifecycle transaction.
    pub(super) async fn compact_retired_counters(
        &self,
        active: &BTreeMap<String, String>,
    ) -> Result<()> {
        let mut command = redis::cmd("HKEYS");
        command.arg(format!("{}:business", self.store.key));
        let fields = match self.store.query::<Vec<String>>(command).await {
            Ok(fields) => fields,
            Err(error) => {
                adx_observability::warn!(%error, "business history scan deferred");
                return Ok(());
            }
        };
        for field in fields {
            if active.contains_key(&field)
                || !(field.starts_with("environment:") || field.starts_with("node:"))
            {
                continue;
            }
            match self.compact_retired_counter(&field).await {
                Ok(true) => (),
                Ok(false) => return Err(Error::Conflict),
                Err(error) => {
                    adx_observability::warn!(%error, %field, "business history field compaction deferred")
                }
            }
        }
        Ok(())
    }

    /// False means a newer control epoch fenced this session. Observability
    /// failures stay recoverable without discarding the original history field.
    async fn compact_retired_counter(&self, field: &str) -> Result<bool> {
        for attempt in 0..ATTEMPTS {
            let mut counters = CounterUpdate::load(&self.store, field.to_owned()).await?;
            if counters.old.is_none() {
                return Ok(true);
            }
            counters.prepare_retirement(&self.store).await?;
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                    if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 3 end
                    local facts = cjson.decode(ARGV[2])
                    if redis.call('HEXISTS',KEYS[1],facts[1]) == 1 then return 2 end
                    if (redis.call('HGET',KEYS[2],facts[1]) or '') ~= facts[2] then return 0 end
                    if (redis.call('HGET',KEYS[2],facts[4]) or '') ~= facts[5] then return 0 end
                    redis.call('HSET',KEYS[2],facts[4],facts[6])
                    redis.call('HDEL',KEYS[2],facts[1]); return 1
                    "#,
                )
                .arg(2)
                .arg(&self.store.key)
                .arg(format!("{}:business", self.store.key))
                .arg(self.epoch.to_string())
                .arg(counters.argument()?);
            match self.store.query::<u8>(command).await? {
                1 | 2 => return Ok(true),
                3 => return Ok(false),
                _ if attempt + 1 == ATTEMPTS => {
                    return Err(Error::Unavailable(
                        "concurrent business history compaction; retry later".into(),
                    ));
                }
                _ => (),
            }
        }
        Ok(true)
    }

    pub async fn business_counters(&self) -> Result<BusinessCounters> {
        let [encoded] = self.store.fields([HEADER.into()]).await?;
        let mut header = self.header(&encoded)?;
        let mut total = header.observability.clone();
        let mut command = redis::cmd("HVALS");
        command.arg(format!("{}:business", self.store.key));
        for value in self.store.query::<Vec<String>>(command).await? {
            total.add(&decode::<BusinessCounters>(&value)?)?;
        }
        if total.started_at_unix_millis == 0 {
            // Coverage begins now, after a complete valid read, without backfilling history.
            // Preserve the header's base counts; the aggregated total must not be persisted.
            header.observability.started_at_unix_millis = now_millis()?;
            header.advance().map_err(|_| {
                Error::Unavailable("business history coverage revision exhausted".into())
            })?;
            if !self
                .store
                .cas(encoded.as_deref().ok_or(Error::Conflict)?, &header, None)
                .await?
            {
                return Err(Error::Unavailable(
                    "concurrent business history coverage initialization; retry later".into(),
                ));
            }
            total.started_at_unix_millis = header.observability.started_at_unix_millis;
        }
        Ok(total)
    }
}

impl BusinessCounters {
    fn add(&mut self, other: &Self) -> Result<()> {
        for (a, b) in self.creations.iter_mut().zip(other.creations) {
            *a = a.checked_add(b).ok_or(Error::Conflict)?;
        }
        for (row, other_row) in self.interruptions.iter_mut().zip(other.interruptions) {
            for (a, b) in row.iter_mut().zip(other_row) {
                *a = a.checked_add(b).ok_or(Error::Conflict)?;
            }
        }
        self.worker_incidents = self
            .worker_incidents
            .checked_add(other.worker_incidents)
            .ok_or(Error::Conflict)?;
        for (a, b) in self.recoveries.iter_mut().zip(other.recoveries) {
            *a = a.checked_add(b).ok_or(Error::Conflict)?;
        }
        Ok(())
    }
}

/// Live object facts are folded into 64 stable buckets on retirement.
/// Rust checks u64 arithmetic; Lua compares opaque bytes with the lifecycle write.
pub(super) struct CounterUpdate {
    pub field: String,
    pub old: Option<String>,
    pub value: BusinessCounters,
    retired: Option<RetiredCounters>,
}
struct RetiredCounters {
    field: String,
    old: Option<String>,
    value: BusinessCounters,
}
impl CounterUpdate {
    pub async fn load(store: &RedisStore, field: String) -> Result<Self> {
        let mut command = redis::cmd("HGET");
        command.arg(format!("{}:business", store.key)).arg(&field);
        let old = store.query(command).await?;
        Self::new(field, old)
    }
    pub fn new(field: String, old: Option<String>) -> Result<Self> {
        let value = old.as_deref().map(decode).transpose()?.unwrap_or_default();
        Ok(Self {
            field,
            old,
            value,
            retired: None,
        })
    }
    pub async fn prepare_retirement(&mut self, store: &RedisStore) -> Result<()> {
        // Independent retirement buckets avoid a global control-header CAS.
        let field = format!(
            "retired:{:02x}",
            Sha256::digest(self.field.as_bytes())[0] % 64
        );
        let mut total = Self::load(store, field.clone()).await?;
        total.value.add(&self.value)?;
        self.retired = Some(RetiredCounters {
            field,
            old: total.old,
            value: total.value,
        });
        Ok(())
    }
    pub fn argument(&self) -> Result<String> {
        let (retired_field, retired_old, retired_value) = match &self.retired {
            Some(retired) => (
                retired.field.as_str(),
                retired.old.as_deref().unwrap_or(""),
                encode(&retired.value)?,
            ),
            None => ("", "", String::new()),
        };
        encode(&(
            &self.field,
            self.old.as_deref().unwrap_or(""),
            encode(&self.value)?,
            retired_field,
            retired_old,
            retired_value,
        ))
    }
}
