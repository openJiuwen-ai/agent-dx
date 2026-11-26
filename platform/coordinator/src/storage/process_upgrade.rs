//! Per-node maintenance ownership and management-session handoff.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MaintenanceOwner {
    pub node_id: String,
    pub rollout_id: String,
    pub local_task_id: String,
    pub epoch: u64,
}

impl MaintenanceOwner {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.epoch == 0
            || [&self.node_id, &self.rollout_id, &self.local_task_id]
                .iter()
                .any(|id| id.is_empty() || id.len() > 128)
        {
            return Err(Error::Invalid("maintenance owner required".into()));
        }
        Ok(())
    }
    fn release_key(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "process-release:{:x}",
            Sha256::digest(encode(self)?.as_bytes())
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionReplacement {
    pub expected_session: String,
    pub next_session: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NodeMaintenance {
    pub owner: MaintenanceOwner,
    pub origin_session: String,
    pub current_session: String,
    pub yielded: bool,
    pub replacement: Option<SessionReplacement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MaintenanceRelease {
    pub owner: MaintenanceOwner,
    pub node_session: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum MaintenanceStatus {
    Absent {
        owner: MaintenanceOwner,
    },
    Active {
        owner: MaintenanceOwner,
        origin_session: String,
        current_session: String,
        yielded: bool,
    },
    Released {
        owner: MaintenanceOwner,
        node_session: String,
    },
}

impl MaintenanceRelease {
    pub(super) fn validate_metadata_key(&self, key: &str) -> Result<()> {
        if self.owner.release_key()? != key || self.node_session.is_empty() {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

impl NodeMaintenance {
    pub(crate) fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        if [&self.origin_session, &self.current_session]
            .iter()
            .any(|id| id.is_empty() || id.len() > 128)
            || self.replacement.as_ref().is_some_and(|receipt| {
                !self.yielded
                    || receipt.next_session != self.current_session
                    || receipt.expected_session.is_empty()
                    || receipt.expected_session == receipt.next_session
            })
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) fn validate_metadata_key(&self, key: &str) -> Result<()> {
        self.validate()?;
        if key != format!("process-upgrade:{}", self.owner.node_id) {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub(super) fn registration_session(
        &self,
        node: &str,
        session: &str,
        token: Option<&str>,
    ) -> Result<()> {
        self.validate()?;
        if self.owner.node_id != node
            || self.current_session != session
            || (self.yielded && self.replacement.is_none())
            || token.is_some_and(|token| token != session)
            || (token.is_none() && self.replacement.is_none())
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    fn require_pause(&self, node: &StoredNode) -> Result<()> {
        if !node.scheduling_paused
            || node.retirement.is_some()
            || node.upgrade_pause.as_ref().is_none_or(|pause| {
                pause.task_id != self.owner.local_task_id || pause.epoch != self.owner.epoch
            })
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    fn replace(&mut self, request: &SessionReplacement) -> Result<bool> {
        if self.replacement.as_ref() == Some(request)
            && self.current_session == request.next_session
        {
            return Ok(false);
        }
        if !self.yielded
            || self.current_session != request.expected_session
            || request.next_session.is_empty()
            || request.next_session.len() > 128
            || request.next_session == request.expected_session
            || request.next_session == self.origin_session
        {
            return Err(Error::Conflict);
        }
        self.current_session = request.next_session.clone();
        self.replacement = Some(request.clone());
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeldRoute {
    pub generation: u64,
    pub runtime_id: String,
    pub ip: std::net::IpAddr,
}
impl HeldRoute {
    pub(super) fn matches(
        &self,
        generation: u64,
        runtime: &str,
        ip: Option<std::net::IpAddr>,
    ) -> bool {
        self.generation == generation && self.runtime_id == runtime && Some(self.ip) == ip
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteHold {
    pub relay_address: String,
    pub routes: BTreeMap<String, HeldRoute>,
}

impl Session {
    pub(crate) async fn process_maintenance_status(
        &self,
        owner: MaintenanceOwner,
    ) -> Result<MaintenanceStatus> {
        let field = format!("process-upgrade:{}", owner.node_id);
        let (marker, receipt) = self.maintenance_snapshot(&owner).await?;
        // The exact task's receipt survives a subsequent task taking maintenance.
        if let Some(receipt) = receipt {
            return Ok(MaintenanceStatus::Released {
                owner,
                node_session: receipt.node_session,
            });
        }
        let Some(marker) = marker else {
            return Ok(MaintenanceStatus::Absent { owner });
        };
        let marker: NodeMaintenance = decode(&marker)?;
        marker.validate_metadata_key(&field)?;
        if marker.owner != owner {
            return Err(Error::Conflict);
        }
        Ok(MaintenanceStatus::Active {
            owner,
            origin_session: marker.origin_session,
            current_session: marker.current_session,
            yielded: marker.yielded,
        })
    }
    pub(crate) async fn process_upgrade(&self, node: &str) -> Result<Option<NodeMaintenance>> {
        let [header, value] = self
            .store
            .fields([HEADER.into(), format!("process-upgrade:{node}")])
            .await?;
        self.header(&header)?;
        value.as_deref().map(decode).transpose()
    }
    pub(crate) async fn process_node_protected(&self, node: &str) -> Result<bool> {
        Ok(self.process_upgrade(node).await?.is_some())
    }
    pub(crate) async fn guard_process_registration(
        &self,
        node: &str,
        session: &str,
    ) -> Result<bool> {
        let [header, marker, token] = self
            .store
            .fields([
                HEADER.into(),
                format!("process-upgrade:{node}"),
                format!("node-session:{node}"),
            ])
            .await?;
        self.header(&header)?;
        let Some(marker) = marker else {
            return Ok(false);
        };
        decode::<NodeMaintenance>(&marker)?.registration_session(
            node,
            session,
            token.as_deref(),
        )?;
        Ok(true)
    }
    pub(crate) async fn maintenance_release(
        &self,
        owner: &MaintenanceOwner,
    ) -> Result<Option<MaintenanceRelease>> {
        Ok(self.maintenance_snapshot(owner).await?.1)
    }
    fn process_releases_key(&self) -> String {
        format!("{}:process-releases", self.store.key)
    }
    async fn maintenance_snapshot(
        &self,
        owner: &MaintenanceOwner,
    ) -> Result<(Option<String>, Option<MaintenanceRelease>)> {
        let key = owner.release_key()?;
        for _ in 0..ATTEMPTS {
            let mut read = redis::cmd("EVAL");
            read.arg(
                r#"
                return {redis.call('HGET',KEYS[1],'header'),
                    redis.call('HGET',KEYS[1],ARGV[1]),
                    redis.call('HGET',KEYS[1],ARGV[2]),
                    redis.call('HGET',KEYS[2],ARGV[2])}
            "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.process_releases_key())
            .arg(format!("process-upgrade:{}", owner.node_id))
            .arg(&key);
            let [header, marker, hot, cold]: [Option<String>; 4] = self.store.query(read).await?;
            self.header(&header)?;
            let mut receipt = None;
            for value in [hot.as_deref(), cold.as_deref()].into_iter().flatten() {
                let saved: MaintenanceRelease = decode(value)?;
                saved.validate_metadata_key(&key)?;
                if saved.owner != *owner || receipt.as_ref().is_some_and(|old| old != &saved) {
                    return Err(Error::Conflict);
                }
                receipt = Some(saved);
            }
            let Some(hot) = hot else {
                return Ok((marker, receipt));
            };
            // Move only a validated exact owner's existing receipt. Both copies
            // and the Coordinator epoch are compared before either Hash changes.
            let mut transfer = redis::cmd("EVAL");
            transfer
                .arg(
                    r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1]
                    or redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3]
                    or (redis.call('HGET',KEYS[2],ARGV[2]) or '') ~= ARGV[4] then return 0 end
                redis.call('HSET',KEYS[2],ARGV[2],ARGV[3])
                redis.call('HDEL',KEYS[1],ARGV[2]); return 1
            "#,
                )
                .arg(2)
                .arg(&self.store.key)
                .arg(self.process_releases_key())
                .arg(self.epoch.to_string())
                .arg(&key)
                .arg(hot)
                .arg(cold.unwrap_or_default());
            if self.store.query::<u8>(transfer).await? == 1 {
                return Ok((marker, receipt));
            }
        }
        Err(Error::Unavailable(
            "concurrent release receipt transfer; retry exact owner".into(),
        ))
    }
    pub(crate) async fn prepare_process_maintenance(
        &self,
        owner: MaintenanceOwner,
        session: String,
    ) -> Result<NodeMaintenance> {
        owner.validate()?;
        let marker = NodeMaintenance {
            owner: owner.clone(),
            origin_session: session.clone(),
            current_session: session,
            yielded: false,
            replacement: None,
        };
        marker.validate()?;
        let field = format!("process-upgrade:{}", owner.node_id);
        for _ in 0..ATTEMPTS {
            if self.maintenance_release(&owner).await?.is_some() {
                return Err(Error::Conflict);
            }
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if raw.contains_key(&owner.release_key()?) {
                continue;
            }
            if let Some(saved) = raw.get(&field) {
                let saved: NodeMaintenance = decode(saved)?;
                return if saved.owner == owner && saved.origin_session == marker.origin_session {
                    Ok(saved)
                } else {
                    Err(Error::Conflict)
                };
            }
            if raw
                .get(UPGRADE)
                .map(|value| decode::<UpgradeTask>(value))
                .transpose()?
                .is_some_and(|task| task.active())
            {
                return Err(Error::Conflict);
            }
            let node_field = format!("node:{}", owner.node_id);
            let token = format!("node-session:{}", owner.node_id);
            let mut node: StoredNode = decode(raw.get(&node_field).ok_or(Error::NotFound)?)?;
            // Ordinary heartbeats publish readiness in the live overlay. Merge
            // only that same-session fact, preserving durable pause metadata
            // and the raw session/token checks below. Routes remain open until Yield.
            let observed = self.get_node(&owner.node_id).await?;
            if node.upgrade_pause.is_some()
                || node.retirement.is_some()
                || node
                    .session
                    .as_ref()
                    .is_none_or(|saved| saved.id != marker.origin_session)
                || observed
                    .session
                    .as_ref()
                    .is_none_or(|saved| saved.id != marker.origin_session || !saved.routable)
                || raw.get(&token) != Some(&marker.origin_session)
            {
                return Err(Error::Conflict);
            }
            node.session.as_mut().ok_or(Error::Conflict)?.routable = true;
            node.upgrade_pause = Some(UpgradePause {
                task_id: owner.local_task_id.clone(),
                epoch: owner.epoch,
                original_paused: node.scheduling_paused,
                route_hold: None,
            });
            node.scheduling_paused = true;
            node.node.available = false;
            header.advance()?;
            let checks = [
                HEADER.to_owned(),
                UPGRADE.to_owned(),
                field.clone(),
                node_field.clone(),
                token,
                owner.release_key()?,
            ]
            .into_iter()
            .map(|key| {
                let value = raw.get(&key).cloned();
                (key, value)
            })
            .collect::<Vec<_>>();
            if self
                .maintenance_cas(
                    &header,
                    &checks,
                    &[
                        (field.clone(), Some(encode(&marker)?)),
                        (node_field, Some(encode(&node)?)),
                    ],
                )
                .await?
            {
                return Ok(marker);
            }
        }
        Err(Error::Unavailable(
            "concurrent maintenance preparation; retry same owner".into(),
        ))
    }
    pub(crate) async fn yield_node_session(
        &self,
        owner: MaintenanceOwner,
        expected: String,
    ) -> Result<NodeMaintenance> {
        let field = format!("process-upgrade:{}", owner.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let mut marker: NodeMaintenance = decode(raw.get(&field).ok_or(Error::Conflict)?)?;
            if marker.owner != owner
                || marker.origin_session != expected
                || marker.current_session != expected
            {
                return Err(Error::Conflict);
            }
            let node_field = format!("node:{}", owner.node_id);
            let token = format!("node-session:{}", owner.node_id);
            let mut node: StoredNode = decode(raw.get(&node_field).ok_or(Error::Conflict)?)?;
            marker.require_pause(&node)?;
            if node
                .session
                .as_ref()
                .is_none_or(|session| session.id != marker.origin_session)
            {
                return Err(Error::Conflict);
            }
            if marker.yielded {
                return if raw.contains_key(&token) {
                    Err(Error::Conflict)
                } else {
                    Ok(marker)
                };
            }
            if raw.get(&token) != Some(&expected) {
                return Err(Error::Conflict);
            }
            let ownership = format!("node-ownership:{}", owner.node_id);
            let mut checks = [
                HEADER.to_owned(),
                field.clone(),
                node_field.clone(),
                token.clone(),
                ownership,
            ]
            .into_iter()
            .map(|key| {
                let value = raw.get(&key).cloned();
                (key, value)
            })
            .collect::<Vec<_>>();
            let mut routes = BTreeMap::new();
            for (key, value) in &raw {
                if !key.starts_with("environment:") {
                    continue;
                }
                let stored: StoredEnvironment = decode(value)?;
                if stored.assignment.node_id != owner.node_id {
                    continue;
                }
                // Existing ownership must not move/disappear after this read;
                // the membership counter also fences newly assigned objects.
                checks.push((key.clone(), Some(value.clone())));
                if stored.invalidated {
                    continue;
                }
                if let Some(record) = &stored.result {
                    if record.assignment == stored.assignment
                        && record.state == EnvironmentState::Running
                        && !record.restart_pending
                        && record.resources_held
                        && (!volumes::has_volumes(&stored.spec)
                            || stored.volume_execution.as_ref().is_some_and(|execution| {
                                !execution.released
                                    && execution.runtime_id == record.runtime.id
                                    && execution.node_session_id == expected
                            }))
                    {
                        if let Some(ip) = record.runtime.ip {
                            routes.insert(
                                record.spec.id.clone(),
                                HeldRoute {
                                    generation: record.assignment.generation,
                                    runtime_id: record.runtime.id.clone(),
                                    ip,
                                },
                            );
                        }
                    }
                }
            }
            node.upgrade_pause
                .as_mut()
                .ok_or(Error::Conflict)?
                .route_hold = Some(RouteHold {
                relay_address: node.proxy_address.clone(),
                routes,
            });
            node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
            node.node.available = false;
            marker.yielded = true;
            header.advance()?;
            if self
                .maintenance_cas(
                    &header,
                    &checks,
                    &[
                        (field.clone(), Some(encode(&marker)?)),
                        (node_field, Some(encode(&node)?)),
                        (token, None),
                    ],
                )
                .await?
            {
                return Ok(marker);
            }
        }
        Err(Error::Unavailable(
            "concurrent Yield; retry exact session".into(),
        ))
    }
    pub(crate) async fn replace_node_session(
        &self,
        owner: MaintenanceOwner,
        replacement: SessionReplacement,
    ) -> Result<NodeMaintenance> {
        let field = format!("process-upgrade:{}", owner.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let mut marker: NodeMaintenance = decode(raw.get(&field).ok_or(Error::Conflict)?)?;
            if marker.owner != owner {
                return Err(Error::Conflict);
            }
            let node_field = format!("node:{}", owner.node_id);
            let token = format!("node-session:{}", owner.node_id);
            let mut node: StoredNode = decode(raw.get(&node_field).ok_or(Error::Conflict)?)?;
            marker.require_pause(&node)?;
            if !marker.replace(&replacement)? {
                return Ok(marker);
            }
            if raw
                .get(&token)
                .is_some_and(|saved| saved != &replacement.expected_session)
            {
                return Err(Error::Conflict);
            }
            let next_departure =
                node_retirement::departure_field(&owner.node_id, &replacement.next_session);
            if raw.contains_key(&next_departure) {
                return Err(Error::Conflict);
            }
            let departure =
                node_retirement::departure_field(&owner.node_id, &replacement.expected_session);
            // Reserve only the marker. Ordinary registration must see the last
            // actual session so its existing volume CAS rebinds that execution.
            node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
            node.node.available = false;
            node.checkpoint_gc_fenced = true;
            header.advance()?;
            let checks = [
                HEADER.to_owned(),
                field.clone(),
                node_field.clone(),
                token.clone(),
                departure.clone(),
                next_departure,
            ]
            .into_iter()
            .map(|key| {
                let value = raw.get(&key).cloned();
                (key, value)
            })
            .collect::<Vec<_>>();
            if self
                .maintenance_cas(
                    &header,
                    &checks,
                    &[
                        (field.clone(), Some(encode(&marker)?)),
                        (node_field, Some(encode(&node)?)),
                        (token, None),
                        (
                            departure,
                            Some(encode(&(&owner.node_id, &replacement.expected_session))?),
                        ),
                    ],
                )
                .await?
            {
                return Ok(marker);
            }
        }
        Err(Error::Unavailable(
            "concurrent replacement; retry exact sessions".into(),
        ))
    }
    pub(crate) async fn release_process_maintenance(
        &self,
        request: MaintenanceRelease,
    ) -> Result<MaintenanceRelease> {
        let receipt_field = request.owner.release_key()?;
        let field = format!("process-upgrade:{}", request.owner.node_id);
        for _ in 0..ATTEMPTS {
            if let Some(receipt) = self.maintenance_release(&request.owner).await? {
                return if receipt == request {
                    Ok(request)
                } else {
                    Err(Error::Conflict)
                };
            }
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if raw.contains_key(&receipt_field) {
                continue;
            }
            let marker: NodeMaintenance = decode(raw.get(&field).ok_or(Error::Conflict)?)?;
            if marker.owner != request.owner || marker.current_session != request.node_session {
                return Err(Error::Conflict);
            }
            let node_field = format!("node:{}", request.owner.node_id);
            let token = format!("node-session:{}", request.owner.node_id);
            let mut node: StoredNode = decode(raw.get(&node_field).ok_or(Error::Conflict)?)?;
            marker.require_pause(&node)?;
            if raw.get(&token) != Some(&request.node_session)
                || node
                    .session
                    .as_ref()
                    .is_none_or(|saved| saved.id != request.node_session || !saved.routable)
            {
                return Err(Error::Conflict);
            }
            node.scheduling_paused = node
                .upgrade_pause
                .as_ref()
                .ok_or(Error::Conflict)?
                .original_paused;
            node.upgrade_pause = None;
            node.node.available = false;
            header.advance()?;
            let checks = [
                HEADER.to_owned(),
                field.clone(),
                node_field.clone(),
                token,
                receipt_field.clone(),
            ]
            .into_iter()
            .map(|key| {
                let value = raw.get(&key).cloned();
                (key, value)
            })
            .collect::<Vec<_>>();
            if self
                .maintenance_cas(
                    &header,
                    &checks,
                    &[
                        (field.clone(), None),
                        (node_field, Some(encode(&node)?)),
                        (receipt_field.clone(), Some(encode(&request)?)),
                    ],
                )
                .await?
            {
                return Ok(request);
            }
        }
        Err(Error::Unavailable(
            "concurrent release; retry exact owner/session".into(),
        ))
    }
    async fn maintenance_cas(
        &self,
        header: &Header,
        checks: &[(String, Option<String>)],
        writes: &[(String, Option<String>)],
    ) -> Result<bool> {
        if checks
            .iter()
            .any(|(key, expected)| key.starts_with("process-release:") && expected.is_some())
        {
            return Err(Error::Invalid(
                "release receipt CAS only supports absence checks".into(),
            ));
        }
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            for _,c in ipairs(cjson.decode(ARGV[1])) do
                local v=redis.call('HGET',KEYS[1],c[1])
                if c[2]==cjson.null then if v then return 0 end elseif v~=c[2] then return 0 end
                if string.sub(c[1],1,16)=='process-release:'
                    and redis.call('HEXISTS',KEYS[2],c[1]) ~= 0 then return 0 end
            end
            redis.call('HSET',KEYS[1],'header',ARGV[2])
            for _,w in ipairs(cjson.decode(ARGV[3])) do
                local target=string.sub(w[1],1,16)=='process-release:' and KEYS[2] or KEYS[1]
                if w[2]==cjson.null then redis.call('HDEL',target,w[1])
                else redis.call('HSET',target,w[1],w[2]) end
            end
            return 1
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.process_releases_key())
            .arg(encode(&checks)?)
            .arg(encode(header)?)
            .arg(encode(&writes)?);
        let values = RedisStore::publication_values(
            writes
                .iter()
                .map(|(field, value)| (field.as_str(), value.as_deref())),
        )?;
        let publication = self
            .store
            .publication_write(
                self.epoch,
                writes.iter().map(|(field, _)| field.clone()).collect(),
            )
            .await;
        let applied = self.store.query::<u8>(command).await? == 1;
        publication.finish_with_fields(applied, values, Vec::new());
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(epoch: u64) -> MaintenanceOwner {
        MaintenanceOwner {
            node_id: "node".into(),
            rollout_id: "rollout".into(),
            local_task_id: format!("task-{epoch}"),
            epoch,
        }
    }

    fn release_receipt(owner: MaintenanceOwner) -> MaintenanceRelease {
        MaintenanceRelease {
            owner,
            node_session: "a".into(),
        }
    }

    async fn hash_set(store: &RedisStore, key: &str, field: &str, value: &impl Serialize) {
        let mut command = redis::cmd("HSET");
        command.arg(key).arg(field).arg(encode(value).unwrap());
        store.query::<u8>(command).await.unwrap();
    }

    async fn hash_get(store: &RedisStore, key: &str, field: &str) -> Option<String> {
        let mut command = redis::cmd("HGET");
        command.arg(key).arg(field);
        store.query(command).await.unwrap()
    }

    async fn active_owner(session: &Session) -> MaintenanceOwner {
        session
            .process_upgrade("node")
            .await
            .unwrap()
            .unwrap()
            .owner
    }

    async fn register_process_node(session: &Session, id: &str) {
        session
            .register_session(
                Node {
                    id: id.into(),
                    runtime_classes: vec!["runsc".into()],
                    capacity: Default::default(),
                    available: true,
                    labels: Default::default(),
                    devices: vec![],
                },
                "node:1".into(),
                "node:2".into(),
                Some(NodeSession {
                    id: "a".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
    }

    async fn running_process_environment(
        session: &Session,
        id: &str,
        node_id: &str,
    ) -> EnvironmentRecord {
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":id, "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
            "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        }))
        .unwrap();
        let assignment = Assignment {
            environment_id: id.into(),
            node_id: node_id.into(),
            shard_id: session.get_node(node_id).await.unwrap().shard_id,
            generation: 1,
            devices: vec![],
        };
        let mut record = session
            .reserve(spec, assignment)
            .await
            .unwrap()
            .effective_record();
        record.state = EnvironmentState::Running;
        record.revision = 2;
        record.resources_held = true;
        record.runtime.id = format!("{id}-1");
        record.runtime.ip = Some("10.0.0.1".parse().unwrap());
        session.commit(record.clone()).await.unwrap();
        record
    }

    async fn eval_calls(store: &RedisStore) -> u64 {
        let mut command = redis::cmd("INFO");
        command.arg("commandstats");
        let info: String = store.query(command).await.unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("cmdstat_eval:calls="))
            .unwrap()
            .split(',')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    async fn hold_node_write<F: std::future::Future>(
        session: &Session,
        operation: F,
    ) -> (catalog::PublicationWrite, std::pin::Pin<Box<F>>) {
        let held = session
            .store
            .publication_prepare(session.epoch, vec!["node:node".into()])
            .await;
        let lock = session
            .store
            .connection
            .publication_locks
            .lock()
            .unwrap()
            .get("node:node")
            .unwrap()
            .upgrade()
            .unwrap();
        let owners = Arc::strong_count(&lock);
        let mut operation = Box::pin(operation);
        // The operation queues on this held lock after its Redis snapshot.
        // Its queued Arc proves that snapshot was captured without a sleep/hook.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    _ = operation.as_mut() => panic!("operation completed before the held Node write"),
                    _ = tokio::task::yield_now() => {
                        if Arc::strong_count(&lock) > owners {
                            break;
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        (held, operation)
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn yield_cas_ignores_unrelated_environment_and_fences_changed_or_new_owned_environment() {
        for change in ["unrelated", "owned", "new-owned"] {
            let rig = transport_test_redis::Redis::new().await;
            let store = rig.store().await;
            let session = store.begin(1).await.unwrap();
            register_process_node(&session, "node").await;
            register_process_node(&session, "other").await;
            let mut owned = running_process_environment(&session, "owned", "node").await;
            let mut unrelated = running_process_environment(&session, "unrelated", "other").await;
            session
                .prepare_process_maintenance(owner(1), "a".into())
                .await
                .unwrap();
            let (held, yielding) =
                hold_node_write(&session, session.yield_node_session(owner(1), "a".into())).await;
            match change {
                "unrelated" => {
                    unrelated.revision += 1;
                    session.commit(unrelated).await.unwrap();
                }
                "owned" => {
                    owned.revision += 1;
                    owned.runtime.ip = Some("10.0.0.2".parse().unwrap());
                    session.commit(owned).await.unwrap();
                }
                "new-owned" => {
                    running_process_environment(&session, "new", "node").await;
                }
                _ => unreachable!(),
            }
            let before = eval_calls(&store).await;
            held.finish(false, vec![]);
            assert!(yielding.await.unwrap().yielded);
            assert_eq!(
                eval_calls(&store).await - before,
                if change == "unrelated" { 1 } else { 2 },
                "Yield checked an unrelated field or failed to fence relevant changes: {change}"
            );
            let node = session.get_node("node").await.unwrap();
            let routes = &node
                .upgrade_pause
                .as_ref()
                .unwrap()
                .route_hold
                .as_ref()
                .unwrap()
                .routes;
            assert_eq!(
                routes["owned"].ip,
                if change == "owned" {
                    "10.0.0.2"
                } else {
                    "10.0.0.1"
                }
                .parse::<std::net::IpAddr>()
                .unwrap()
            );
            assert_eq!(routes.contains_key("new"), change == "new-owned");
            assert!(!routes.contains_key("unrelated"));
        }
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn process_release_receipts_stay_cold_persistent_and_replay_across_tasks_and_restart() {
        let rig = transport_test_redis::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        register_process_node(&session, "node").await;
        for epoch in [1, 2] {
            session
                .prepare_process_maintenance(owner(epoch), "a".into())
                .await
                .unwrap();
            session
                .release_process_maintenance(release_receipt(owner(epoch)))
                .await
                .unwrap();
        }
        let hot = store.raw().await.unwrap();
        let header = session.header(&hot.get(HEADER).cloned()).unwrap();
        assert!(matches!(
            session
                .maintenance_cas(
                    &header,
                    &[(owner(1).release_key().unwrap(), Some(String::new()))],
                    &[],
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(store.raw().await.unwrap(), hot);
        assert!(
            hot.keys().all(|key| !key.starts_with("process-release:")),
            "historical receipts must not grow the hot control Hash"
        );
        let mut ttl = redis::cmd("PTTL");
        ttl.arg(format!("{}:process-releases", store.key));
        assert_eq!(store.query::<i64>(ttl).await.unwrap(), -1);
        let current = store.begin(1).await.unwrap();
        current
            .prepare_process_maintenance(owner(3), "a".into())
            .await
            .unwrap();
        for epoch in [1, 2] {
            let receipt = release_receipt(owner(epoch));
            assert_eq!(
                current.maintenance_release(&owner(epoch)).await.unwrap(),
                Some(receipt.clone())
            );
            assert!(matches!(
                current
                    .process_maintenance_status(owner(epoch))
                    .await
                    .unwrap(),
                MaintenanceStatus::Released { .. }
            ));
            assert_eq!(
                current
                    .release_process_maintenance(receipt)
                    .await
                    .unwrap()
                    .owner,
                owner(epoch)
            );
            assert!(matches!(
                current
                    .prepare_process_maintenance(owner(epoch), "a".into())
                    .await,
                Err(Error::Conflict)
            ));
        }
        assert_eq!(active_owner(&current).await, owner(3));
        assert!(matches!(
            session.maintenance_release(&owner(1)).await,
            Err(Error::Conflict)
        ));
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn existing_hot_release_moves_atomically_and_rejects_conflicting_cold_receipt() {
        let rig = transport_test_redis::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        register_process_node(&session, "node").await;
        let receipt = release_receipt(owner(1));
        let receipt_field = owner(1).release_key().unwrap();
        let cold = format!("{}:process-releases", store.key);
        hash_set(&store, &store.key, &receipt_field, &receipt).await;
        session
            .prepare_process_maintenance(owner(2), "a".into())
            .await
            .unwrap();
        assert_eq!(
            session.maintenance_release(&owner(1)).await.unwrap(),
            Some(receipt.clone())
        );
        assert!(hash_get(&store, &store.key, &receipt_field).await.is_none());
        assert_eq!(
            hash_get(&store, &cold, &receipt_field).await,
            Some(encode(&receipt).unwrap())
        );
        assert_eq!(
            session
                .release_process_maintenance(receipt.clone())
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(active_owner(&session).await, owner(2));
        session
            .release_process_maintenance(release_receipt(owner(2)))
            .await
            .unwrap();
        assert!(matches!(
            session
                .prepare_process_maintenance(owner(1), "a".into())
                .await,
            Err(Error::Conflict)
        ));
        let conflict = MaintenanceRelease {
            node_session: "other".into(),
            ..receipt
        };
        hash_set(&store, &store.key, &receipt_field, &conflict).await;
        assert!(matches!(
            session.maintenance_release(&owner(1)).await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            session.process_maintenance_status(owner(1)).await,
            Err(Error::Conflict)
        ));
        assert!(
            hash_get(&store, &store.key, &receipt_field).await.is_some(),
            "conflicting data must not be discarded"
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn release_epoch_change_leaves_pause_and_receipt_unmodified() {
        let rig = transport_test_redis::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        register_process_node(&session, "node").await;
        session
            .prepare_process_maintenance(owner(1), "a".into())
            .await
            .unwrap();
        let receipt = release_receipt(owner(1));
        let (held, release) = hold_node_write(
            &session,
            session.release_process_maintenance(receipt.clone()),
        )
        .await;
        let current = store.begin(1).await.unwrap();
        held.finish(false, vec![]);
        assert!(matches!(release.await, Err(Error::Conflict)));
        assert_eq!(current.maintenance_release(&owner(1)).await.unwrap(), None);
        assert_eq!(active_owner(&current).await, owner(1));
        assert!(current
            .get_node("node")
            .await
            .unwrap()
            .upgrade_pause
            .is_some());
        assert_eq!(
            current
                .release_process_maintenance(receipt.clone())
                .await
                .unwrap(),
            receipt
        );
        assert!(current.process_upgrade("node").await.unwrap().is_none());
        assert!(current
            .get_node("node")
            .await
            .unwrap()
            .upgrade_pause
            .is_none());
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn unspawned_reservation_rebinds_actual_volume_session_and_fences_late_commit() {
        use adx_core::{
            execution::{Mount, MountSource},
            volumes::{AccessMode, StorageDomain, VolumeRef, VolumeSource, VolumeSpec},
        };
        let rig = transport_test_redis::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"env", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
            "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        }))
        .unwrap();
        let node = Node {
            id: "node".into(),
            runtime_classes: vec!["runsc".into()],
            capacity: spec.resources,
            available: true,
            labels: Default::default(),
            devices: vec![],
        };
        let register = |id: &str| {
            session.register_session(
                node.clone(),
                "node:1".into(),
                "node:2".into(),
                Some(NodeSession {
                    id: id.into(),
                    sequence: 1,
                    routable: false,
                }),
            )
        };
        session
            .register_session(
                node.clone(),
                "node:1".into(),
                "node:2".into(),
                Some(NodeSession {
                    id: "a".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let domains = BTreeMap::from([(
            "sfs".into(),
            StorageDomain {
                dynamic_nodes: false,
                backend_id: "server:/export".into(),
                node_roots: BTreeMap::from([("node".into(), "/mnt/sfs".into())]),
                tenant_paths: BTreeMap::from([("tenant".into(), "workspaces".into())]),
            },
        )]);
        session
            .ensure_volume(
                VolumeSpec {
                    tenant_id: "tenant".into(),
                    id: "workspace".into(),
                    source: VolumeSource::Directory {
                        storage_domain: "sfs".into(),
                        path: "workspaces/user".into(),
                    },
                    access_mode: AccessMode::Exclusive,
                    create_if_missing: false,
                    create_options: None,
                },
                &domains,
            )
            .await
            .unwrap();
        spec.execution.mounts.push(Mount {
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["rw".into()],
            source: MountSource::Volume(VolumeRef {
                id: "workspace".into(),
                uid: None,
            }),
            create_if_missing: false,
            create_options: None,
        });
        let mut record = session
            .claim(
                spec.clone(),
                &LocalClaim {
                    request_id: String::new(),
                    node_id: "node".into(),
                    node_session_id: "a".into(),
                    devices: vec![],
                },
            )
            .await
            .unwrap()
            .record()
            .effective_record();
        record.state = EnvironmentState::Starting;
        // Initial execution of the claimed ownership: `{environment}-{generation}`.
        record.runtime.id = format!("{}-{}", record.spec.id, record.assignment.generation);
        record.revision = 1;
        session
            .authorize_volumes(&record, &record.runtime.id, "a")
            .await
            .unwrap();
        record.state = EnvironmentState::Running;
        record.revision = 2;
        record.runtime.ip = Some("10.0.0.1".parse().unwrap());
        session
            .commit_releasing(record.clone(), "a", &[])
            .await
            .unwrap();
        let owner = MaintenanceOwner {
            node_id: "node".into(),
            rollout_id: "r".into(),
            local_task_id: "t".into(),
            epoch: 1,
        };
        session
            .prepare_process_maintenance(owner.clone(), "a".into())
            .await
            .unwrap();
        session
            .yield_node_session(owner.clone(), "a".into())
            .await
            .unwrap();
        for (expected, next) in [("a", "b"), ("b", "c")] {
            session
                .replace_node_session(
                    owner.clone(),
                    SessionReplacement {
                        expected_session: expected.into(),
                        next_session: next.into(),
                    },
                )
                .await
                .unwrap();
            let actual = session.get_node("node").await.unwrap();
            assert_eq!(actual.session.as_ref().unwrap().id, "a");
            assert!(actual.route_eligible(&session.get("env").await.unwrap()));
        }
        register("c").await.unwrap();
        let saved = session.get("env").await.unwrap();
        assert_eq!(
            saved.volume_execution.as_ref().unwrap().node_session_id,
            "c"
        );
        let volumes = session.resolve_volumes(&record.spec).await.unwrap();
        assert_eq!(volumes[0].occupancy.as_ref().unwrap().node_session_id, "c");
        assert!(matches!(
            session.commit_releasing(record.clone(), "a", &[]).await,
            Err(Error::Conflict)
        ));
        assert_eq!(
            store.fields(["node-session:node".into()]).await.unwrap()[0].as_deref(),
            Some("c")
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated real Redis"]
    async fn process_release_keeps_latest_admin_pause_and_only_removes_its_own() {
        let rig = transport_test_redis::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        register_process_node(&session, "node").await;
        let owner = MaintenanceOwner {
            node_id: "node".into(),
            rollout_id: "r".into(),
            local_task_id: "t".into(),
            epoch: 1,
        };
        session
            .prepare_process_maintenance(owner.clone(), "a".into())
            .await
            .unwrap();
        // Administrator intent changes during process maintenance are accepted
        // without a PodUpgrade task and never lift this task's pause.
        for admin_paused in [true, false, true] {
            let saved = session
                .set_node_scheduling("node", admin_paused, !admin_paused)
                .await
                .unwrap();
            assert!(saved.scheduling_paused);
            assert!(!saved.node.available);
            let pause = saved.upgrade_pause.as_ref().unwrap();
            assert_eq!((pause.task_id.as_str(), pause.epoch), ("t", 1));
            assert_eq!(pause.original_paused, admin_paused);
        }
        session
            .release_process_maintenance(release_receipt(owner.clone()))
            .await
            .unwrap();
        let released = session.get_node("node").await.unwrap();
        assert!(released.upgrade_pause.is_none());
        assert!(released.scheduling_paused, "latest admin pause survives");
        // Unpausing afterwards is an ordinary administrator update again.
        let resumed = session
            .set_node_scheduling("node", false, true)
            .await
            .unwrap();
        assert!(!resumed.scheduling_paused);
        // A marker naming another node never authorizes this node's pause, even
        // with the same task id and epoch.
        let next = MaintenanceOwner {
            epoch: 2,
            ..owner.clone()
        };
        session
            .prepare_process_maintenance(next.clone(), "a".into())
            .await
            .unwrap();
        let foreign = NodeMaintenance {
            owner: MaintenanceOwner {
                node_id: "other".into(),
                ..next
            },
            origin_session: "a".into(),
            current_session: "a".into(),
            yielded: false,
            replacement: None,
        };
        hash_set(&store, &store.key, "process-upgrade:node", &foreign).await;
        assert!(matches!(
            session.set_node_scheduling("node", true, false).await,
            Err(Error::Conflict)
        ));
    }

    #[test]
    fn replacement_is_one_cas_chain_not_two_execution_legs() {
        let mut marker = NodeMaintenance {
            owner: MaintenanceOwner {
                node_id: "n".into(),
                rollout_id: "r".into(),
                local_task_id: "t".into(),
                epoch: 1,
            },
            origin_session: "a".into(),
            current_session: "a".into(),
            yielded: false,
            replacement: None,
        };
        let first = SessionReplacement {
            expected_session: "a".into(),
            next_session: "b".into(),
        };
        assert!(marker.replace(&first).is_err());
        marker.yielded = true;
        assert!(marker.replace(&first).unwrap());
        assert!(!marker.replace(&first).unwrap());
        assert!(marker
            .replace(&SessionReplacement {
                expected_session: "b".into(),
                next_session: "c".into()
            })
            .unwrap());
        assert!(marker.replace(&first).is_err());
        assert_eq!(marker.current_session, "c");
        assert!(marker.registration_session("n", "a", Some("a")).is_err());
        assert!(marker.registration_session("n", "c", None).is_ok());
        assert!(marker.registration_session("n", "c", Some("b")).is_err());
        assert_eq!(
            decode::<NodeMaintenance>(&encode(&marker).unwrap()).unwrap(),
            marker
        );
    }
}
