//! Volume metadata shares the Coordinator epoch and CAS with execution ownership.
use super::*;
use adx_core::{
    execution::MountSource,
    volumes::{
        AccessMode, Occupancy, OccupancyPhase, StorageDomain, Volume, VolumeRef, VolumeSource,
        VolumeSpec,
    },
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredVolume {
    pub volume: Volume,
    pub backend_id: String,
    pub references: BTreeSet<String>,
    pub owner_ids: BTreeSet<String>,
    pub snapshot_ids: BTreeSet<String>,
    pub delete_when_unused: bool,
    pub occupancy: Option<Occupancy>,
    pub initialization: Initialization,
}

impl StoredVolume {
    fn retained(self) -> Option<Self> {
        if self.delete_when_unused
            && self.owner_ids.is_empty()
            && self.references.is_empty()
            && self.snapshot_ids.is_empty()
            && self.occupancy.is_none()
        {
            None
        } else {
            Some(self)
        }
    }
}

/// Preparation decision for this RPC; no additional state is persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitializationAction {
    Ready,
    Prepare,
    Verify,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializationOwner {
    pub assignment: Assignment,
    pub runtime_id: String,
    pub node_session_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Initialization {
    NotRequested,
    Uninitialized,
    Pending { owner: InitializationOwner },
    Ready { owner: InitializationOwner },
    NeedsRepair { error: String },
}

fn initialization_owner(record: &EnvironmentRecord, session: &str) -> InitializationOwner {
    InitializationOwner {
        assignment: record.assignment.clone(),
        runtime_id: record.runtime.id.clone(),
        node_session_id: session.into(),
    }
}

/// The current authorized node attempt, independent of the last completed result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizedExecution {
    pub runtime_id: String,
    pub node_session_id: String,
    pub revision: u64,
    pub released: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixedReferences {
    tenant_id: String,
    request_digest: String,
    volumes: Vec<VolumeRef>,
    active: bool,
}

pub(super) struct Catalog {
    pub(super) header_value: String,
    header: Header,
    pub(super) control: BTreeMap<String, String>,
    volumes: BTreeMap<String, StoredVolume>,
    references: BTreeMap<String, FixedReferences>,
    changed: BTreeSet<String>,
}

impl Catalog {
    fn update(&mut self, key: String, record: StoredVolume) {
        self.changed.insert(key.clone());
        match record.retained() {
            Some(record) => {
                self.volumes.insert(key, record);
            }
            None => {
                self.volumes.remove(&key);
            }
        }
    }

    pub(super) fn advanced_header(&self) -> Result<Header> {
        let mut header = self.header.clone();
        header.advance()?;
        Ok(header)
    }

    pub(super) fn protect_snapshot(
        &mut self,
        snapshot: &mut adx_core::snapshots::Snapshot,
    ) -> Result<()> {
        if !has_volumes(&snapshot.template) {
            return Ok(());
        }
        let records = self.resolve(&snapshot.template)?;
        let fixed: Vec<_> = records
            .iter()
            .map(|record| VolumeRef {
                id: record.volume.spec.id.clone(),
                uid: Some(record.volume.uid.clone()),
            })
            .collect();
        snapshot.template.fix_volume_references(&fixed)?;
        for mut record in records {
            let key = volume_key(&snapshot.template.tenant_id, &record.volume.spec.id)?;
            record.snapshot_ids.insert(snapshot.id.clone());
            self.update(key, record);
        }
        Ok(())
    }

    pub(super) fn release_snapshot(
        &mut self,
        snapshot: &adx_core::snapshots::Snapshot,
    ) -> Result<()> {
        for mount in &snapshot.template.execution.mounts {
            let MountSource::Volume(reference) = &mount.source else {
                continue;
            };
            let key = volume_key(&snapshot.template.tenant_id, &reference.id)?;
            // A volume can be mounted at several targets; release each UID once.
            if self.changed.contains(&key) {
                continue;
            }
            let mut record = self.volumes.get(&key).ok_or(Error::Conflict)?.clone();
            if reference.uid.as_ref() != Some(&record.volume.uid)
                || !record.snapshot_ids.remove(&snapshot.id)
            {
                return Err(Error::Conflict);
            }
            self.update(key, record);
        }
        Ok(())
    }
    pub(super) fn change_fields(&self) -> impl Iterator<Item = String> + '_ {
        self.changed.iter().map(|key| format!("volume:{key}"))
    }

    pub(super) fn reference_value(&self, id: &str) -> Result<String> {
        encode(self.references.get(id).ok_or(Error::Conflict)?)
    }

    /// A verified process replacement retains its assignments under the new session.
    pub(super) fn rebind_execution_session(
        &mut self,
        record: &mut StoredEnvironment,
        previous: &str,
        next: &str,
    ) -> Result<()> {
        for mut volume in self.resolve(&record.spec)? {
            if let Some(slot) = &mut volume.occupancy {
                if slot.assignment == record.assignment && slot.node_session_id == previous {
                    slot.node_session_id = next.into();
                }
            }
            if matches!(&volume.initialization, Initialization::Pending { owner }
                if owner.assignment == record.assignment && owner.node_session_id == previous)
            {
                volume.initialization = Initialization::NeedsRepair {
                    error: "initialization owner process replaced before publishing completion"
                        .into(),
                };
            }
            let key = volume_key(&record.spec.tenant_id, &volume.volume.spec.id)?;
            self.update(key, volume);
        }
        if let Some(execution) = &mut record.volume_execution {
            if !execution.released && execution.node_session_id == previous {
                execution.node_session_id = next.into();
            }
        }
        Ok(())
    }

    pub(super) fn apply_result(
        &mut self,
        result: &EnvironmentRecord,
        session: &str,
        stopped: &[String],
    ) -> Result<()> {
        let mut records = self.resolve(&result.spec)?;
        for volume in &mut records {
            if let Some(current) = &volume.occupancy {
                let same =
                    current.assignment == result.assignment && current.node_session_id == session;
                if !result.resources_held && same && stopped.contains(&current.runtime_id) {
                    volume.occupancy = None;
                } else if result.resources_held && result.state == EnvironmentState::Running {
                    if current.assignment != result.assignment
                        || current.runtime_id != result.runtime.id
                        || current.phase != OccupancyPhase::Held
                    {
                        return Err(Error::Conflict);
                    }
                } else if result.state == EnvironmentState::Deleted
                    && current.assignment.environment_id == result.spec.id
                {
                    return Err(Error::Conflict);
                }
            } else if volume.volume.spec.access_mode == AccessMode::Exclusive
                && result.resources_held
            {
                return Err(Error::Conflict);
            }
            if !result.resources_held
                && matches!(&volume.initialization, Initialization::Pending {owner}
                    if owner.assignment == result.assignment
                        && owner.node_session_id == session
                        && stopped.contains(&owner.runtime_id))
            {
                volume.initialization = Initialization::NeedsRepair {
                    error: "initialization owner stopped before publishing completion".into(),
                };
            }
            if result.state == EnvironmentState::Deleted {
                volume.references.remove(&result.spec.id);
            }
        }
        for record in records {
            let key = volume_key(&result.spec.tenant_id, &record.volume.spec.id)?;
            self.update(key, record);
        }
        if result.state == EnvironmentState::Deleted {
            self.references
                .get_mut(&result.spec.id)
                .ok_or(Error::Conflict)?
                .active = false;
        }
        Ok(())
    }

    /// Node invalidation retires only this assignment's matching process session.
    pub(super) fn invalidate_execution(
        &mut self,
        record: &StoredEnvironment,
        session: &str,
    ) -> Result<()> {
        let runtime = record.effective_record().runtime.id;
        for mut volume in self.resolve(&record.spec)? {
            let mut changed = false;
            if volume.occupancy.as_ref().is_some_and(|current| {
                current.assignment == record.assignment
                    && current.runtime_id == runtime
                    && current.node_session_id == session
            }) {
                volume.occupancy = None;
                changed = true;
            }
            if matches!(&volume.initialization, Initialization::Pending {owner}
                if owner.assignment == record.assignment && owner.node_session_id == session)
            {
                volume.initialization = Initialization::NeedsRepair {
                    error: "initialization owner lost before publishing completion".into(),
                };
                changed = true;
            }
            if changed {
                let key = volume_key(&record.spec.tenant_id, &volume.volume.spec.id)?;
                self.changed.insert(key.clone());
                self.volumes.insert(key, volume);
            }
        }
        Ok(())
    }

    /// Only the confirmed unstarted reservation can be moved by replace_rejected.
    pub(super) fn retire_unstarted(
        &mut self,
        record: &StoredEnvironment,
        node: &StoredNode,
    ) -> Result<()> {
        for mut volume in self.resolve(&record.spec)? {
            if let Some(current) = &volume.occupancy {
                let expected = Occupancy {
                    volume_uid: volume.volume.uid.clone(),
                    assignment: record.assignment.clone(),
                    runtime_id: record.result.as_ref().map_or_else(
                        || format!("{}-{}", record.spec.id, record.assignment.generation),
                        |result| result.runtime.id.clone(),
                    ),
                    node_session_id: node.session.as_ref().ok_or(Error::Conflict)?.id.clone(),
                    phase: OccupancyPhase::Reserved,
                };
                if current != &expected {
                    return Err(Error::Conflict);
                }
                let key = volume_key(&record.spec.tenant_id, &volume.volume.spec.id)?;
                volume.occupancy = None;
                self.changed.insert(key.clone());
                self.volumes.insert(key, volume);
            }
        }
        Ok(())
    }

    fn resolve(&self, spec: &EnvironmentSpec) -> Result<Vec<StoredVolume>> {
        let fixed = self
            .references
            .get(&spec.id)
            .filter(|fixed| fixed.active)
            .ok_or(Error::Conflict)?;
        if fixed.tenant_id != spec.tenant_id {
            return Err(Error::Conflict);
        }
        let mut records = BTreeMap::new();
        for mount in &spec.execution.mounts {
            let MountSource::Volume(reference) = &mount.source else {
                continue;
            };
            let saved = fixed
                .volumes
                .iter()
                .find(|saved| saved.id == reference.id)
                .ok_or(Error::Conflict)?;
            let key = volume_key(&spec.tenant_id, &reference.id)?;
            let volume = self.volumes.get(&key).ok_or(Error::Conflict)?;
            if saved.uid.as_ref() != Some(&volume.volume.uid)
                || reference
                    .uid
                    .as_ref()
                    .is_some_and(|uid| uid != &volume.volume.uid)
                || !volume.references.contains(&spec.id)
            {
                return Err(Error::Conflict);
            }
            records.insert(key, volume.clone());
        }
        if records.len() != fixed.volumes.len() {
            return Err(Error::Conflict);
        }
        Ok(records.into_values().collect())
    }

    /// Validate fixed references, then reserve the one distinct exclusive Volume.
    /// Mutate the working catalog only after all validation succeeds.
    pub(super) fn reserve(&mut self, record: &StoredEnvironment, node: &StoredNode) -> Result<()> {
        let spec = &record.spec;
        let session = node.session.as_ref().ok_or(Error::Conflict)?;
        if self.control.get(&format!("node-session:{}", node.node.id)) != Some(&session.id) {
            return Err(Error::Conflict);
        }
        let mut writes = Vec::new();
        for mut volume in self.resolve(spec)? {
            let key = volume_key(&spec.tenant_id, &volume.volume.spec.id)?;
            if volume.volume.spec.access_mode == AccessMode::Exclusive {
                let occupancy = Occupancy {
                    volume_uid: volume.volume.uid.clone(),
                    assignment: record.assignment.clone(),
                    runtime_id: format!("{}-{}", spec.id, record.assignment.generation),
                    node_session_id: node.session.as_ref().ok_or(Error::Conflict)?.id.clone(),
                    phase: OccupancyPhase::Reserved,
                };
                occupancy.validate()?;
                if let Some(current) = &volume.occupancy {
                    if current != &occupancy {
                        return Err(Error::VolumeBusy(volume.volume.uid.clone()));
                    }
                } else {
                    volume.occupancy = Some(occupancy);
                    writes.push((key, volume));
                }
            }
        }
        for (key, record) in writes {
            self.changed.insert(key.clone());
            self.volumes.insert(key, record);
        }
        Ok(())
    }

    pub(super) fn append_writes(&self, command: &mut redis::Cmd) -> Result<()> {
        command.arg(self.changed.len());
        for key in &self.changed {
            command.arg(key).arg(
                self.volumes
                    .get(key)
                    .map(encode)
                    .transpose()?
                    .unwrap_or_default(),
            );
        }
        Ok(())
    }
}

pub(crate) fn has_volumes(spec: &EnvironmentSpec) -> bool {
    spec.execution
        .mounts
        .iter()
        .any(|mount| matches!(mount.source, MountSource::Volume(_)))
}

fn volume_key(tenant: &str, id: &str) -> Result<String> {
    adx_core::volumes::validate_volume_id(id)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(encode(&(tenant, id))?.as_bytes())
    ))
}

fn validate_owner(owner: &str, allow_empty: bool) -> Result<()> {
    if (owner.is_empty() && !allow_empty)
        || owner.len() > 512
        || owner.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid Volume owner ID".into()));
    }
    Ok(())
}

fn request_digest(spec: &EnvironmentSpec) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(encode(spec)?.as_bytes())))
}

impl Session {
    async fn validate_initialization_owner(
        &self,
        request: &EnvironmentRecord,
        session: &str,
    ) -> Result<()> {
        let stored = self.get(&request.spec.id).await?;
        let node = self.get_node(&request.assignment.node_id).await?;
        if stored.invalidated
            || stored.assignment != request.assignment
            || stored.spec != request.spec
            || stored.volume_execution.as_ref().is_none_or(|execution| {
                execution.released
                    || execution.runtime_id != request.runtime.id
                    || execution.node_session_id != session
            })
            || node.session.as_ref().is_none_or(|node| node.id != session)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Acquire one durable initialization intent; another owner waits, failed work requires repair.
    pub async fn begin_volume_initialization(
        &self,
        request: &EnvironmentRecord,
        session: &str,
        uid: &str,
    ) -> Result<InitializationAction> {
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            self.validate_initialization_owner(request, session).await?;
            let mut record = catalog
                .resolve(&request.spec)?
                .into_iter()
                .find(|record| record.volume.uid == uid)
                .ok_or(Error::NotFound)?;
            let owner = initialization_owner(request, session);
            match &record.initialization {
                Initialization::Ready { .. } | Initialization::NotRequested => {
                    return Ok(InitializationAction::Ready)
                }
                Initialization::Pending { owner: current } if current == &owner => {
                    return Ok(InitializationAction::Verify)
                }
                Initialization::Pending { .. } => return Err(Error::VolumeBusy(uid.into())),
                Initialization::NeedsRepair { .. } => {
                    return Err(Error::Invalid(
                        "Volume initialization requires repair".into(),
                    ))
                }
                Initialization::Uninitialized => (),
            }
            record.initialization = Initialization::Pending { owner };
            let key = volume_key(&request.spec.tenant_id, &record.volume.spec.id)?;
            if self
                .volume_cas(&catalog, vec![(key, Some(record))], None)
                .await?
            {
                return Ok(InitializationAction::Prepare);
            }
        }
        Err(Error::Unavailable(
            "volume initialization admission contended".into(),
        ))
    }

    /// Report the exact owner's completed preparation; an attribute failure never becomes Ready.
    pub async fn complete_volume_initialization(
        &self,
        request: &EnvironmentRecord,
        session: &str,
        uid: &str,
        error: Option<&str>,
    ) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            self.validate_initialization_owner(request, session).await?;
            let mut record = catalog
                .resolve(&request.spec)?
                .into_iter()
                .find(|record| record.volume.uid == uid)
                .ok_or(Error::NotFound)?;
            let owner = initialization_owner(request, session);
            let completed = match error {
                Some(error) => Initialization::NeedsRepair {
                    error: error.chars().take(4096).collect(),
                },
                None => Initialization::Ready {
                    owner: owner.clone(),
                },
            };
            if record.initialization == completed {
                return Ok(());
            }
            if !matches!(&record.initialization, Initialization::Pending { owner: current } if current == &owner)
            {
                return Err(Error::Conflict);
            }
            record.initialization = completed;
            let key = volume_key(&request.spec.tenant_id, &record.volume.spec.id)?;
            if self
                .volume_cas(&catalog, vec![(key, Some(record))], None)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "volume initialization publication contended".into(),
        ))
    }
    /// Authorize one concrete attempt before any managed-directory or runtime effect.
    /// Stale sessions, retired attempts and competing exclusive owners are rejected.
    pub async fn authorize_volumes(
        &self,
        request: &EnvironmentRecord,
        previous: &str,
        session: &str,
    ) -> Result<Vec<StoredVolume>> {
        if !has_volumes(&request.spec) {
            return Ok(Vec::new());
        }
        if !matches!(
            request.state,
            EnvironmentState::Starting | EnvironmentState::Resuming
        ) || !request.resources_held
            || request.revision == 0
            || !adx_core::valid_runtime_id(
                &request.spec.id,
                request.assignment.generation,
                &request.runtime.id,
            )
        {
            return Err(Error::Conflict);
        }
        for _ in 0..ATTEMPTS {
            let mut catalog = self.volume_catalog().await?;
            let field = format!("environment:{}", request.spec.id);
            let previous_value = catalog.control.get(&field).ok_or(Error::NotFound)?.clone();
            let mut stored: StoredEnvironment = decode(&previous_value)?;
            stored.validate()?;
            let node = self.get_node(&request.assignment.node_id).await?;
            if stored.spec != request.spec
                || stored.assignment != request.assignment
                || stored.invalidated
                || !node.node.available
                || catalog
                    .control
                    .get(&format!("node-session:{}", request.assignment.node_id))
                    .map(String::as_str)
                    != Some(session)
                || node
                    .session
                    .as_ref()
                    .is_none_or(|node| node.id != session || !node.routable)
                || stored
                    .result
                    .as_ref()
                    .is_some_and(|result| result.state == EnvironmentState::Deleted)
            {
                return Err(Error::Conflict);
            }
            let initial = format!("{}-{}", request.spec.id, request.assignment.generation);
            let old_runtime = stored
                .volume_execution
                .as_ref()
                .map(|execution| execution.runtime_id.as_str())
                .or_else(|| {
                    stored
                        .result
                        .as_ref()
                        .map(|result| result.runtime.id.as_str())
                })
                .unwrap_or(&initial);
            let old_revision = stored.volume_execution.as_ref().map_or_else(
                || stored.result.as_ref().map_or(0, |result| result.revision),
                |execution| execution.revision,
            );
            if request.runtime.id == old_runtime {
                let observed = stored
                    .result
                    .as_ref()
                    .map_or(initial.as_str(), |result| result.runtime.id.as_str());
                if (previous != old_runtime && previous != observed)
                    || stored.volume_execution.as_ref().is_some_and(|execution| {
                        execution.released
                            || execution.node_session_id != session
                            || execution.revision != request.revision
                    })
                    || stored.result.as_ref().is_some_and(|result| {
                        result.runtime.id == request.runtime.id
                            && result.revision >= request.revision
                    })
                {
                    return Err(Error::Conflict);
                }
            } else if previous != old_runtime
                || request.revision <= old_revision
                || request.runtime.id != format!("{}-r{}", initial, request.revision)
            {
                return Err(Error::Conflict);
            }
            let mut volumes = catalog.resolve(&request.spec)?;
            for volume in &mut volumes {
                if volume.volume.spec.access_mode != AccessMode::Exclusive {
                    continue;
                }
                if let Some(current) = &volume.occupancy {
                    if current.assignment != request.assignment {
                        return Err(Error::VolumeBusy(volume.volume.uid.clone()));
                    }
                    if current.node_session_id != session
                        || (current.runtime_id != request.runtime.id
                            && current.runtime_id != previous)
                    {
                        return Err(Error::Conflict);
                    }
                }
                volume.occupancy = Some(Occupancy {
                    volume_uid: volume.volume.uid.clone(),
                    assignment: request.assignment.clone(),
                    runtime_id: request.runtime.id.clone(),
                    node_session_id: session.into(),
                    phase: OccupancyPhase::Held,
                });
                let key = volume_key(&request.spec.tenant_id, &volume.volume.spec.id)?;
                catalog.changed.insert(key.clone());
                catalog.volumes.insert(key, volume.clone());
            }
            stored.volume_execution = Some(AuthorizedExecution {
                runtime_id: request.runtime.id.clone(),
                node_session_id: session.into(),
                revision: request.revision,
                released: false,
            });
            let mut header = catalog.header.clone();
            header.advance()?;
            if self
                .assignment_cas(
                    &header,
                    Some(&previous_value),
                    &stored,
                    Some(&catalog),
                    None,
                )
                .await?
            {
                return Ok(volumes);
            }
        }
        Err(Error::Unavailable(
            "execution authorization contended; retry same attempt".into(),
        ))
    }

    /// Resolve the previously fixed UIDs without changing the original request receipt.
    pub async fn resolve_volumes(&self, spec: &EnvironmentSpec) -> Result<Vec<StoredVolume>> {
        if !has_volumes(spec) {
            return Ok(Vec::new());
        }
        self.volume_catalog().await?.resolve(spec)
    }

    pub(crate) async fn volume_owners(&self) -> Result<BTreeMap<String, Option<Occupancy>>> {
        Ok(self
            .volume_catalog()
            .await?
            .volumes
            .into_values()
            .map(|record| (record.volume.uid, record.occupancy))
            .collect())
    }

    pub(super) async fn assignment_cas(
        &self,
        header: &Header,
        previous: Option<&str>,
        record: &StoredEnvironment,
        catalog: Option<&Catalog>,
        counters: Option<&CounterUpdate>,
    ) -> Result<bool> {
        let field = format!("environment:{}", record.spec.id);
        let Some(catalog) = catalog else {
            return self
                .store
                .object_cas_with_counters(
                    header,
                    Some((&field, previous, encode(record)?)),
                    counters,
                )
                .await;
        };
        if catalog.control.get(&field).map(String::as_str) != previous {
            return Ok(false);
        }
        self.control_volume_cas(
            &catalog.header_value,
            &catalog.advanced_header()?,
            &[(field, encode(record)?)],
            catalog,
            counters,
        )
        .await
    }

    /// Volume mutations retain their shared header CAS; touched lifecycle records
    /// additionally compare exact bytes because ordinary writes use object CAS.
    pub(super) async fn control_volume_cas(
        &self,
        expected: &str,
        header: &Header,
        fields: &[(String, String)],
        catalog: &Catalog,
        counters: Option<&CounterUpdate>,
    ) -> Result<bool> {
        let mut checks = BTreeMap::new();
        let mut artifacts = Vec::new();
        let mut memberships = Vec::new();
        let mut sessions = Vec::new();
        for (field, value) in fields {
            checks.insert(
                field.clone(),
                catalog.control.get(field).cloned().unwrap_or_default(),
            );
            if field.starts_with("environment:") {
                let record: StoredEnvironment = decode(value)?;
                for key in [
                    format!("node:{}", record.assignment.node_id),
                    format!("node-session:{}", record.assignment.node_id),
                ] {
                    checks.insert(
                        key.clone(),
                        catalog.control.get(&key).cloned().unwrap_or_default(),
                    );
                }
                memberships.push(format!("node-ownership:{}", record.assignment.node_id));
                if let Some(result) = &record.result {
                    artifacts.extend(artifacts::record_artifacts(result));
                }
            } else if field.starts_with("node:") {
                let node: StoredNode = decode(value)?;
                let key = format!("node-ownership:{}", node.node.id);
                checks.insert(
                    key.clone(),
                    catalog.control.get(&key).cloned().unwrap_or_default(),
                );
                if let Some(session) = node.session {
                    sessions.push((format!("node-session:{}", node.node.id), session.id));
                }
            }
        }
        let _access = self.store.lock_artifacts(&artifacts)?;
        let mut command = redis::cmd("EVAL");
        command.arg(r#"
            if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
            local expected=cjson.decode(ARGV[3])
            for field,value in pairs(expected) do if (redis.call('HGET',KEYS[1],field) or '') ~= value then return 0 end end
            local facts=cjson.decode(ARGV[4])
            if facts[1] ~= '' and (redis.call('HGET',KEYS[4],facts[1]) or '') ~= facts[2] then return 0 end
            local cursor=6
            for _=1,tonumber(ARGV[5]) do redis.call('HSET',KEYS[1],ARGV[cursor],ARGV[cursor+1]); cursor=cursor+2 end
            local count=tonumber(ARGV[cursor]); cursor=cursor+1
            for _=1,count do
                if ARGV[cursor+1] == '' then redis.call('HDEL',KEYS[2],ARGV[cursor])
                else redis.call('HSET',KEYS[2],ARGV[cursor],ARGV[cursor+1]) end
                cursor=cursor+2
            end
            redis.call('HSET',KEYS[1],'header',ARGV[2])
            if facts[1] ~= '' then redis.call('HSET',KEYS[4],facts[1],facts[3]) end
            for _,key in ipairs(cjson.decode(ARGV[cursor])) do redis.call('HINCRBY',KEYS[1],key,1) end
            cursor=cursor+1
            for field,value in pairs(cjson.decode(ARGV[cursor])) do redis.call('HSET',KEYS[1],field,value) end
            for i=cursor+1,#ARGV,2 do redis.call('HSET',KEYS[3],ARGV[i],ARGV[i+1]) end
            return 1
        "#).arg(4).arg(&self.store.key).arg(self.volumes_key())
            .arg(format!("{}:artifacts",self.store.key)).arg(format!("{}:business",self.store.key))
            .arg(expected).arg(encode(header)?).arg(encode(&checks)?)
            .arg(counters.map(CounterUpdate::argument).transpose()?.unwrap_or_else(|| r#"["","",""]"#.into()))
            .arg(fields.len());
        for (field, value) in fields {
            command.arg(field).arg(value);
        }
        catalog.append_writes(&mut command)?;
        command
            .arg(encode(&memberships)?)
            .arg(encode(&sessions.into_iter().collect::<BTreeMap<_, _>>())?);
        for artifact in artifacts {
            command
                .arg(artifacts::artifact_key(&artifact))
                .arg(encode(&artifact)?);
        }
        let values = RedisStore::publication_values(
            fields.iter().map(|(f, v)| (f.as_str(), Some(v.as_str()))),
        )?;
        let write = self
            .store
            .publication_write(self.epoch, fields.iter().map(|(f, _)| f.clone()).collect())
            .await;
        let applied = self.store.query::<u8>(command).await? == 1;
        write.finish_with_fields(applied, values, catalog.change_fields().collect());
        Ok(applied)
    }

    pub(super) fn volumes_key(&self) -> String {
        format!("{}:volumes", self.store.key)
    }
    fn volume_references_key(&self) -> String {
        format!("{}:volume-references", self.store.key)
    }

    pub(super) async fn volume_catalog(&self) -> Result<Catalog> {
        let mut command = redis::cmd("EVAL");
        command.arg("return {redis.call('HGET', KEYS[1], 'header'), redis.call('HGETALL', KEYS[2]), redis.call('HGETALL', KEYS[3]), redis.call('HGETALL', KEYS[1])}")
            .arg(3).arg(&self.store.key).arg(self.volumes_key()).arg(self.volume_references_key());
        type RawFields = BTreeMap<String, String>;
        let (header_value, values, references, control): (
            Option<String>,
            RawFields,
            RawFields,
            RawFields,
        ) = self.store.query(command).await?;
        let header = self.header(&header_value)?;
        let mut volumes = BTreeMap::new();
        for (key, value) in values {
            let record: StoredVolume = decode(&value)?;
            record.volume.validate()?;
            if key != volume_key(&record.volume.spec.tenant_id, &record.volume.spec.id)?
                || record.backend_id.is_empty()
            {
                return Err(Error::Unavailable("corrupt volume identity".into()));
            }
            if let Some(occupancy) = &record.occupancy {
                occupancy.validate()?;
                if record.volume.spec.access_mode != AccessMode::Exclusive
                    || occupancy.volume_uid != record.volume.uid
                {
                    return Err(Error::Unavailable("corrupt volume occupancy".into()));
                }
            }
            volumes.insert(key, record);
        }
        let references = references
            .into_iter()
            .map(|(id, value)| Ok((id, decode(&value)?)))
            .collect::<Result<_>>()?;
        Ok(Catalog {
            header_value: header_value.ok_or(Error::Conflict)?,
            header,
            control,
            volumes,
            references,
            changed: BTreeSet::new(),
        })
    }

    async fn volume_cas(
        &self,
        catalog: &Catalog,
        writes: Vec<(String, Option<StoredVolume>)>,
        reference: Option<(&str, FixedReferences)>,
    ) -> Result<bool> {
        let mut header = catalog.header.clone();
        header.advance()?;
        let mut command = redis::cmd("EVAL");
        command.arg(r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            local cursor = 4
            for _ = 1, tonumber(ARGV[3]) do
                if ARGV[cursor + 1] == '' then redis.call('HDEL', KEYS[2], ARGV[cursor])
                else redis.call('HSET', KEYS[2], ARGV[cursor], ARGV[cursor + 1]) end
                cursor = cursor + 2
            end
            if ARGV[cursor] ~= '' then redis.call('HSET', KEYS[3], ARGV[cursor], ARGV[cursor + 1]) end
            redis.call('HSET', KEYS[1], 'header', ARGV[2])
            return 1
        "#).arg(3).arg(&self.store.key).arg(self.volumes_key()).arg(self.volume_references_key())
            .arg(&catalog.header_value).arg(encode(&header)?).arg(writes.len());
        let changed = writes
            .iter()
            .map(|(key, _)| format!("volume:{key}"))
            .collect();
        for (key, record) in writes {
            command
                .arg(key)
                .arg(record.as_ref().map(encode).transpose()?.unwrap_or_default());
        }
        match reference {
            Some((id, record)) => {
                command.arg(id).arg(encode(&record)?);
            }
            None => {
                command.arg("").arg("");
            }
        }
        let applied = self.store.query::<u8>(command).await? == 1;
        if applied {
            self.store.notify_committed_change(header.revision, changed);
        }
        Ok(applied)
    }

    /// Return an existing tenant/id unchanged, or atomically create a directory definition.
    /// New unauthorized/overlapping sources are rejected. A lost response is retried with the same ID.
    pub async fn ensure_volume(
        &self,
        spec: VolumeSpec,
        domains: &BTreeMap<String, StorageDomain>,
    ) -> Result<Volume> {
        self.ensure_volume_for_owner(spec, domains, "").await
    }

    /// Ensure the definition and its opaque lifecycle owner association atomically.
    /// An empty owner creates a standalone definition without an association.
    pub async fn ensure_volume_for_owner(
        &self,
        spec: VolumeSpec,
        domains: &BTreeMap<String, StorageDomain>,
        owner: &str,
    ) -> Result<Volume> {
        validate_owner(owner, true)?;
        let key = volume_key(&spec.tenant_id, &spec.id)?;
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            if let Some(existing) = catalog.volumes.get(&key) {
                if owner.is_empty() || existing.owner_ids.contains(owner) {
                    return Ok(existing.volume.clone());
                }
                let mut record = existing.clone();
                record.owner_ids.insert(owner.into());
                record.delete_when_unused = false;
                if self
                    .volume_cas(&catalog, vec![(key.clone(), Some(record))], None)
                    .await?
                {
                    return Ok(existing.volume.clone());
                }
                continue;
            }
            spec.validate()?;
            let VolumeSource::Directory {
                storage_domain,
                path,
            } = &spec.source;
            let domain = domains
                .get(storage_domain)
                .ok_or_else(|| Error::Invalid("unknown volume storage domain".into()))?;
            domain.authorize(&spec)?;
            for existing in catalog
                .volumes
                .values()
                .filter(|record| record.backend_id == domain.backend_id)
            {
                let VolumeSource::Directory { path: other, .. } = &existing.volume.spec.source;
                if path == other
                    || path
                        .strip_prefix(other)
                        .is_some_and(|tail| tail.starts_with('/'))
                    || other
                        .strip_prefix(path)
                        .is_some_and(|tail| tail.starts_with('/'))
                {
                    return Err(Error::Conflict);
                }
            }
            let volume = Volume {
                uid: uuid::Uuid::new_v4().to_string(),
                spec: spec.clone(),
                revision: 1,
            };
            let record = StoredVolume {
                initialization: if volume.spec.create_if_missing {
                    Initialization::Uninitialized
                } else {
                    Initialization::NotRequested
                },
                volume: volume.clone(),
                backend_id: domain.backend_id.clone(),
                references: BTreeSet::new(),
                snapshot_ids: BTreeSet::new(),
                delete_when_unused: false,
                owner_ids: if owner.is_empty() {
                    BTreeSet::new()
                } else {
                    [owner.into()].into()
                },
                occupancy: None,
            };
            if self
                .volume_cas(&catalog, vec![(key.clone(), Some(record))], None)
                .await?
            {
                return Ok(volume);
            }
        }
        Err(Error::Unavailable(
            "concurrent volume creation; retry same Volume ID".into(),
        ))
    }

    pub async fn get_volume(&self, tenant: &str, id: &str) -> Result<StoredVolume> {
        self.volume_catalog()
            .await?
            .volumes
            .remove(&volume_key(tenant, id)?)
            .ok_or(Error::NotFound)
    }

    pub async fn list_volumes(&self, tenant: &str) -> Result<Vec<Volume>> {
        Ok(self
            .list_volume_records(tenant)
            .await?
            .into_iter()
            .map(|record| record.volume)
            .collect())
    }

    pub async fn list_volume_records(&self, tenant: &str) -> Result<Vec<StoredVolume>> {
        let mut volumes: Vec<_> = self
            .volume_catalog()
            .await?
            .volumes
            .into_values()
            .filter(|record| record.volume.spec.tenant_id == tenant)
            .collect();
        volumes.sort_by(|a, b| a.volume.spec.id.cmp(&b.volume.spec.id));
        Ok(volumes)
    }

    /// Fix UID references before placement, without persisting an executable scheduling request.
    /// A repeated Environment ID must retain its request and previously fixed UIDs.
    pub async fn pin_volumes(&self, spec: &EnvironmentSpec) -> Result<Vec<Volume>> {
        spec.validate()?;
        if !has_volumes(spec) {
            return Ok(Vec::new());
        }
        let digest = request_digest(spec)?;
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            let old = catalog.references.get(&spec.id);
            if old
                .is_some_and(|old| old.tenant_id != spec.tenant_id || old.request_digest != digest)
            {
                return Err(Error::Conflict);
            }
            let references: Vec<_> = match old {
                Some(old) => old.volumes.clone(),
                None => spec
                    .execution
                    .mounts
                    .iter()
                    .filter_map(|mount| match &mount.source {
                        MountSource::Volume(reference) => Some(reference.clone()),
                        _ => None,
                    })
                    .collect(),
            };
            let mut records = BTreeMap::new();
            for reference in references {
                reference.validate()?;
                let key = volume_key(&spec.tenant_id, &reference.id)?;
                let record = catalog.volumes.get(&key).ok_or(Error::NotFound)?.clone();
                if reference
                    .uid
                    .as_ref()
                    .is_some_and(|uid| uid != &record.volume.uid)
                {
                    return Err(Error::Conflict);
                }
                records.insert(key, record);
            }
            if records
                .values()
                .filter(|record| record.volume.spec.access_mode == AccessMode::Exclusive)
                .count()
                > 1
            {
                return Err(Error::Invalid(
                    "an Environment may reference at most one distinct exclusive Volume".into(),
                ));
            }
            let volumes = records
                .values()
                .map(|record| record.volume.clone())
                .collect();
            if records.is_empty() || old.is_some_and(|old| old.active) {
                return Ok(volumes);
            }
            let fixed = FixedReferences {
                tenant_id: spec.tenant_id.clone(),
                request_digest: digest.clone(),
                active: true,
                volumes: records
                    .values()
                    .map(|record| VolumeRef {
                        id: record.volume.spec.id.clone(),
                        uid: Some(record.volume.uid.clone()),
                    })
                    .collect(),
            };
            for record in records.values_mut() {
                record.references.insert(spec.id.clone());
            }
            let writes = records
                .into_iter()
                .map(|(key, record)| (key, Some(record)))
                .collect();
            if self
                .volume_cas(&catalog, writes, Some((&spec.id, fixed)))
                .await?
            {
                return Ok(volumes);
            }
        }
        Err(Error::Unavailable(
            "concurrent volume reference update; retry same Environment ID".into(),
        ))
    }

    /// Release references after definitive cancellation or completed deletion.
    /// Callers must fence pending claims before cancelling an unassigned request.
    /// The fixed UID receipt remains, so retries cannot bind a recreated same-name Volume.
    pub async fn unpin_volumes(&self, spec: &EnvironmentSpec) -> Result<()> {
        let digest = request_digest(spec)?;
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            let Some(old) = catalog.references.get(&spec.id) else {
                return Ok(());
            };
            if old.tenant_id != spec.tenant_id || old.request_digest != digest {
                return Err(Error::Conflict);
            }
            if !old.active {
                return Ok(());
            }
            match self.get(&spec.id).await {
                Ok(environment)
                    if environment.result.as_ref().is_some_and(|result| {
                        result.state == EnvironmentState::Deleted
                            && !result.resources_held
                            && !result.restart_pending
                    }) && !environment.recovery.as_ref().is_some_and(|r| r.pending) => {}
                Ok(_) => return Err(Error::Conflict),
                Err(Error::NotFound) => {}
                Err(error) => return Err(error),
            }
            let mut writes = Vec::new();
            for reference in &old.volumes {
                let key = volume_key(&spec.tenant_id, &reference.id)?;
                let mut record = catalog.volumes.get(&key).ok_or(Error::Conflict)?.clone();
                if reference.uid.as_ref() != Some(&record.volume.uid)
                    || record
                        .occupancy
                        .as_ref()
                        .is_some_and(|occupancy| occupancy.assignment.environment_id == spec.id)
                {
                    return Err(Error::Conflict);
                }
                record.references.remove(&spec.id);
                writes.push((key, record.retained()));
            }
            let mut fixed = old.clone();
            fixed.active = false;
            if self
                .volume_cas(&catalog, writes, Some((&spec.id, fixed)))
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent volume reference release; retry request".into(),
        ))
    }

    /// Release an owner's associations after its Environment is absent. Remove
    /// its unassigned references and delete only the last owner's unused definitions.
    /// No directory operation or permanent owner-deletion marker is performed.
    pub async fn release_volume_owner(&self, tenant: &str, owner: &str) -> Result<()> {
        validate_owner(owner, false)?;
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            match self.get(owner).await {
                Ok(_) => return Err(Error::Conflict),
                Err(Error::NotFound) => (),
                Err(error) => return Err(error),
            }
            let fixed = catalog.references.get(owner);
            if fixed.is_some_and(|fixed| fixed.tenant_id != tenant) {
                return Err(Error::Conflict);
            }
            let mut writes = Vec::new();
            for (key, current) in &catalog.volumes {
                if current.volume.spec.tenant_id != tenant {
                    continue;
                }
                let mut record = current.clone();
                let owned = record.owner_ids.remove(owner);
                let referenced = record.references.remove(owner);
                if !(owned || referenced) {
                    continue;
                }
                if record
                    .occupancy
                    .as_ref()
                    .is_some_and(|slot| slot.assignment.environment_id == owner)
                {
                    return Err(Error::Conflict);
                }
                if owned && record.owner_ids.is_empty() {
                    record.delete_when_unused = true;
                }
                writes.push((key.clone(), record.retained()));
            }
            let reference = fixed.filter(|fixed| fixed.active).map(|fixed| {
                let mut fixed = fixed.clone();
                fixed.active = false;
                (owner, fixed)
            });
            if writes.is_empty() && reference.is_none() {
                return Ok(());
            }
            if self.volume_cas(&catalog, writes, reference).await? {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "volume owner release contended; retry cleanup".into(),
        ))
    }

    /// Remove a definition only. Directory data is never changed by this operation.
    pub async fn delete_volume(&self, tenant: &str, id: &str) -> Result<()> {
        let key = volume_key(tenant, id)?;
        for _ in 0..ATTEMPTS {
            let catalog = self.volume_catalog().await?;
            let Some(record) = catalog.volumes.get(&key) else {
                return Ok(());
            };
            if !record.references.is_empty()
                || !record.owner_ids.is_empty()
                || !record.snapshot_ids.is_empty()
                || record.occupancy.is_some()
            {
                return Err(Error::Conflict);
            }
            if self
                .volume_cas(&catalog, vec![(key.clone(), None)], None)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent volume deletion; retry request".into(),
        ))
    }
}
