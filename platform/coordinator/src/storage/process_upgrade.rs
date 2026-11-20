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
        let key = owner.release_key()?;
        let field = format!("process-upgrade:{}", owner.node_id);
        let [header, marker, receipt] = self
            .store
            .fields([HEADER.into(), field.clone(), key.clone()])
            .await?;
        self.header(&header)?;
        // The exact task's receipt survives a subsequent task taking maintenance.
        if let Some(receipt) = receipt {
            let receipt: MaintenanceRelease = decode(&receipt)?;
            receipt.validate_metadata_key(&key)?;
            if receipt.owner != owner {
                return Err(Error::Conflict);
            }
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
        let [header, value] = self
            .store
            .fields([HEADER.into(), owner.release_key()?])
            .await?;
        self.header(&header)?;
        value.as_deref().map(decode).transpose()
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
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if raw.contains_key(&owner.release_key()?) {
                return Err(Error::Conflict);
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
            let mut routes = BTreeMap::new();
            for (key, value) in &raw {
                if !key.starts_with("environment:") {
                    continue;
                }
                let stored: StoredEnvironment = decode(value)?;
                if stored.assignment.node_id != owner.node_id || stored.invalidated {
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
            let mut checks = raw
                .iter()
                .map(|(key, value)| (key.clone(), Some(value.clone())))
                .collect::<Vec<_>>();
            let ownership = format!("node-ownership:{}", owner.node_id);
            checks.retain(|(key, _)| key != &ownership);
            checks.push((ownership.clone(), raw.get(&ownership).cloned()));
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
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if let Some(value) = raw.get(&receipt_field) {
                return if decode::<MaintenanceRelease>(value)? == request {
                    Ok(request)
                } else {
                    Err(Error::Conflict)
                };
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
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            for _,c in ipairs(cjson.decode(ARGV[1])) do
                local v=redis.call('HGET',KEYS[1],c[1])
                if c[2]==cjson.null then if v then return 0 end elseif v~=c[2] then return 0 end
            end
            redis.call('HSET',KEYS[1],'header',ARGV[2])
            for _,w in ipairs(cjson.decode(ARGV[3])) do
                if w[2]==cjson.null then redis.call('HDEL',KEYS[1],w[1])
                else redis.call('HSET',KEYS[1],w[1],w[2]) end
            end
            return 1
        "#,
            )
            .arg(1)
            .arg(&self.store.key)
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
        let node = Node {
            id: "node".into(),
            runtime_classes: vec!["runsc".into()],
            capacity: Default::default(),
            available: true,
            labels: Default::default(),
            devices: vec![],
        };
        session
            .register_session(
                node,
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
            .release_process_maintenance(MaintenanceRelease {
                owner: owner.clone(),
                node_session: "a".into(),
            })
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
        let client = redis::Client::open(rig.url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: usize = redis::cmd("HSET")
            .arg("adx:{test}:control:v1")
            .arg("process-upgrade:node")
            .arg(encode(&foreign).unwrap())
            .query_async(&mut conn)
            .await
            .unwrap();
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
