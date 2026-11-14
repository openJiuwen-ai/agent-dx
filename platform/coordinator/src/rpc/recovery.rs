//! Recover invalidated executions through normal ShardScheduler placement and a durable
//! Paused record at a new generation. No image-start fallback is permitted.
use super::*;
use adx_core::scheduling::{LabelRequirement, LabelSelector, SelectorOp};
use futures_util::{stream, StreamExt};

pub(super) fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))
}

pub(super) fn checkpoint_spec(
    stored: &StoredEnvironment,
    checkpoint: Option<&adx_core::RestorePoint>,
    source: Option<&StoredNode>,
    excluded: &BTreeSet<String>,
    target: Option<&str>,
) -> Result<EnvironmentSpec> {
    let (alias, scope, profile) = if let Some(point) = checkpoint {
        (
            &point.artifact.storage,
            &point.artifact.scope,
            &point.recovery_profile,
        )
    } else {
        let source = source.ok_or(Error::Conflict)?;
        let labels = &source.node.labels;
        let alias = labels
            .get("ADX_CHECKPOINT_STORAGE")
            .ok_or(Error::Conflict)?;
        let scope = labels.get("ADX_CHECKPOINT_SCOPE").ok_or(Error::Conflict)?;
        if !source.checkpoint_scopes.contains(scope) {
            return Err(Error::Invalid(
                "shared checkpoint backend evidence is missing".into(),
            ));
        }
        (alias, scope, labels)
    };
    if alias == "local" || alias.is_empty() || scope.is_empty() {
        return Err(Error::Invalid(
            "cross-node recovery requires a verified shared checkpoint backend".into(),
        ));
    }
    let mut required = crate::storage::checkpoint_requirements(&stored.spec, profile)?;
    required.insert("ADX_CHECKPOINT_STORAGE".into(), alias.clone());
    required.insert("ADX_CHECKPOINT_SCOPE".into(), scope.clone());
    let mut spec = stored.spec.clone();
    if spec.scheduling.required_node.is_empty() {
        spec.scheduling.required_node.push(LabelSelector::default());
    }
    let excluded: Vec<_> = excluded
        .iter()
        .cloned()
        .chain(std::iter::once(stored.assignment.node_id.clone()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for selector in &mut spec.scheduling.required_node {
        selector
            .expressions
            .extend(required.iter().map(|(key, value)| LabelRequirement {
                key: key.clone(),
                op: SelectorOp::In,
                values: vec![value.clone()],
            }));
        selector.expressions.push(LabelRequirement {
            key: "NODE_ID".into(),
            op: SelectorOp::NotIn,
            values: excluded.clone(),
        });
        if let Some(target) = target {
            selector.expressions.push(LabelRequirement {
                key: "NODE_ID".into(),
                op: SelectorOp::In,
                values: vec![target.into()],
            });
        }
    }
    Ok(spec)
}

impl CoordinatorRpc {
    pub async fn recover_environments(&self) -> Result<usize> {
        let mut blocked = None;
        let (work, coordinator_epoch) = {
            let mut state = self.0.state.lock().await;
            state.healthy()?;
            let coordinator_epoch = state.session.epoch();
            let now = now()?;
            let upgrade = state.session.upgrade_task().await?;
            let upgrade_owned = |id: &str| {
                upgrade.as_ref().is_some_and(|task| {
                    task.active()
                        && task
                            .operations
                            .values()
                            .any(|op| op.environment_id == id && !op.settled())
                })
            };
            let candidates: Vec<_> = state
                .environments
                .values()
                .filter(|i| !upgrade_owned(&i.spec.id) && i.recovery_point(now).is_some())
                .cloned()
                .collect();
            for stored in candidates {
                let domains = self.volume_domains(&state)?;
                state.prepare_volumes(&stored.spec, &domains).await?;
                let point = stored.recovery_point(now).ok_or(Error::Conflict)?;
                let point = state.session.ordinary_recovery_point(&stored.spec, point);
                let spec =
                    match checkpoint_spec(&stored, Some(&point), None, &BTreeSet::new(), None) {
                        Ok(spec) => spec,
                        Err(error @ Error::Invalid(_)) => {
                            adx_observability::warn!(environment_id=%stored.spec.id, error=%error,
                            "checkpoint recovery admission blocked");
                            blocked.get_or_insert(error);
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                state.scheduler.submit_recovery(spec)?;
            }
            // Each ShardScheduler round already has its own attempt/time budget.
            for _ in 0..state.scheduler.shards.len() {
                if state.drive().await? {
                    self.0.changed.notify_waiters();
                }
            }
            let cursor = state.recovery_cursor.clone().unwrap_or_default();
            let work: Vec<_> = state
                .environments
                .values()
                .filter(|i| i.spec.id > cursor)
                .chain(state.environments.values().filter(|i| i.spec.id <= cursor))
                .filter(|i| {
                    !upgrade_owned(&i.spec.id)
                        && !i.invalidated
                        && i.recovery.as_ref().is_some_and(|r| r.pending)
                })
                .filter_map(|i| {
                    let node = state.nodes.get(&i.assignment.node_id)?;
                    let live = state.live.get(&i.assignment.node_id)?;
                    if !live.inspected
                        || live.report.reconciling
                        || live.expired
                        || live.last_seen.elapsed() >= self.0.heartbeat_timeout
                    {
                        return None;
                    }
                    Some((
                        node.clone(),
                        i.result.clone()?,
                        i.resume
                            .as_ref()
                            .and_then(|intent| intent.run_timeout_seconds),
                    ))
                })
                .take(16)
                .collect();
            state.recovery_cursor = work.last().map(|(_, record, _)| record.spec.id.clone());
            (work, coordinator_epoch)
        };
        let results = stream::iter(work.into_iter().map(|(node, record, timeout)| {
            self.recover_record(node, record, timeout, self.0.timeout, false, coordinator_epoch)
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
        let completed = results.iter().filter(|r| r.is_ok()).count();
        if let Some(error) = results.into_iter().find_map(Result::err).or(blocked) {
            return Err(error);
        }
        Ok(completed)
    }
    /// Both planned and failure recovery restore the same committed checkpoint;
    /// a planned transfer additionally requires a published Running acknowledgement.
    pub(super) fn recover_record(
        &self,
        node: StoredNode,
        record: EnvironmentRecord,
        run_timeout_seconds: Option<u64>,
        timeout: Duration,
        require_running: bool,
        coordinator_epoch: u64,
    ) -> RpcFuture<'_, EnvironmentRecord> {
        Box::pin(async move {
            let assignment = record.assignment.clone();
            let channel = self
                .0
                .node_tls
                .endpoint(&node.address)
                .map_err(|_| Error::Conflict)?
                .connect_timeout(self.0.timeout)
                .timeout(timeout)
                .connect()
                .await
                .map_err(|_| Error::Unavailable("recovery node unavailable".into()))?;
            let session = node.session.ok_or(Error::Conflict)?.id;
            let result =
                pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
                    .recover_environment(pb::RecoverEnvironmentRequest {
                        run_timeout_seconds,
                        record: Some(record.try_into()?),
                        node_session_id: session.clone(),
                        coordinator_epoch,
                    })
                    .await
                    .map_err(adx_protocol::dependency_status)?
                    .into_inner();
            if result.durability != pb::Durability::Published as i32 {
                return Err(Error::Unavailable(
                    "recovery result publication pending".into(),
                ));
            }
            let record: EnvironmentRecord = result.record.ok_or(Error::Conflict)?.try_into()?;
            if record.assignment != assignment
                || (require_running
                    && (record.state != EnvironmentState::Running
                        || !record.resources_held
                        || record.runtime.ip.is_none()))
            {
                return Err(Error::Unavailable(
                    "target restore is not published Running; retain source checkpoint".into(),
                ));
            }
            self.commit(record, session).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (StoredEnvironment, adx_core::RestorePoint, Node) {
        let mut stored: StoredEnvironment = serde_json::from_value(serde_json::json!({
            "creation_generation":1,"spec":{"id":"move","tenant_id":"tenant","image":"image",
                "runtime_class":"runsc","priority":0,"resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}},
            "assignment":{"environment_id":"move","node_id":"source","shard_id":0,"generation":1,"devices":[]},
            "result":null
        })).unwrap();
        let profile = crate::rpc::recovery_test_profile::qualify(&mut stored.spec);
        let point = adx_core::RestorePoint {
            recovery_profile: profile.clone(),
            origin: None,
            id: "checkpoint".into(),
            artifact: adx_core::CheckpointArtifact {
                storage: "shared".into(),
                scope: "backend".into(),
                location: "artifact".into(),
                size_bytes: 1,
            },
            expires_at_unix_seconds: None,
            source_runtime_id: "move-1".into(),
        };
        let mut labels = profile;
        labels.extend([
            ("ADX_CHECKPOINT_STORAGE".into(), "shared".into()),
            ("ADX_CHECKPOINT_SCOPE".into(), "backend".into()),
            ("NODE_ID".into(), "target".into()),
        ]);
        let node = Node {
            id: "target".into(),
            capacity: stored.spec.resources,
            available: true,
            labels,
            devices: vec![],
            runtime_classes: vec!["runsc".into()],
        };
        (stored, point, node)
    }

    #[test]
    fn checkpoint_filters_use_captured_backend_and_runtime_evidence() {
        let (stored, mut point, mut target) = fixture();
        let request = checkpoint_spec(&stored, Some(&point), None, &BTreeSet::new(), None).unwrap();
        assert!(request.scheduling.matches_node(&target.labels));
        let mut default = stored.clone();
        default.spec.runtime_profile = None;
        assert!(checkpoint_spec(&default, Some(&point), None, &BTreeSet::new(), None).is_ok());
        let mut other = stored.clone();
        other.spec.runtime_profile.as_mut().unwrap().rootfs.path = "/other/rootfs".into();
        assert!(checkpoint_spec(&other, Some(&point), None, &BTreeSet::new(), None).is_err());
        for key in [
            "ADX_CHECKPOINT_SCOPE",
            "ADX_RECOVERY_HOST",
            "ADX_RECOVERY_RUNTIME_runsc",
            "ADX_RECOVERY_ROOTFS_SHA256",
        ] {
            let previous = target
                .labels
                .insert(key.into(), "different".into())
                .unwrap();
            assert!(!request.scheduling.matches_node(&target.labels));
            target.labels.insert(key.into(), previous);
        }
        target.labels.insert("NODE_ID".into(), "source".into());
        assert!(!request.scheduling.matches_node(&target.labels));
        point.recovery_profile.clear();
        assert!(checkpoint_spec(&stored, Some(&point), None, &BTreeSet::new(), None).is_err());
        point = fixture().1;
        point.artifact.scope.clear();
        assert!(checkpoint_spec(&stored, Some(&point), None, &BTreeSet::new(), None).is_err());
    }

    async fn report_recovery_target(state: &mut State) {
        let mut target = state.nodes["target"].clone();
        target.node.available = true;
        let session = target.session.as_mut().unwrap();
        session.sequence += 1;
        session.routable = true;
        let target = state
            .session
            .register_session_with_scopes(
                target.node,
                target.address,
                target.proxy_address,
                target.session,
                target.checkpoint_scopes,
            )
            .await
            .unwrap();
        state.scheduler.register(target.node.clone()).unwrap();
        state.nodes.insert(target.node.id.clone(), target);
        state.recovering.remove("target");
    }

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn planned_transfer_uses_recovery_drive_and_rechecks_capacity_and_abort() {
        use crate::storage::{UpgradeNode, UpgradeOperation, UpgradeTask};
        for (resume, fault) in [
            (false, "none"),
            (true, "none"),
            (false, "capacity"),
            (true, "capacity"),
            (false, "abort"),
            (true, "abort"),
            (false, "failed-source"),
        ] {
            let (_rig, session, rpc, original, mut task) = {
                let rig = crate::rpc::cancellation_test_redis::Redis::new().await;
                let session = rig.store().await.begin(2).await.unwrap();
                let (stored, point, target) = fixture();
                for id in ["source", "target"] {
                    let mut node = target.clone();
                    node.id = id.into();
                    node.labels.insert("NODE_ID".into(), id.into());
                    session
                        .register_session_with_scopes(
                            node,
                            "127.0.0.1:1".into(),
                            "127.0.0.1:2".into(),
                            Some(crate::storage::NodeSession {
                                id: format!("{id}-session"),
                                sequence: 1,
                                routable: true,
                            }),
                            ["backend".into()].into(),
                        )
                        .await
                        .unwrap();
                }
                session
                    .reserve(stored.spec.clone(), stored.assignment.clone())
                    .await
                    .unwrap();
                let mut record = stored.effective_record();
                record.state = EnvironmentState::Paused;
                record.revision = 1;
                record.resources_held = false;
                record.checkpoint = Some(point);
                session.commit(record).await.unwrap();
                let task = session
                    .save_upgrade(
                        UpgradeTask {
                            protocol_version: 3,
                            task_id: "task".into(),
                            epoch: 1,
                            namespace: "test".into(),
                            daemonset_uid: "ds".into(),
                            target_image: format!("node@sha256:{}", "a".repeat(64)),
                            phase: "Active".into(),
                            nodes: [(
                                "source".into(),
                                UpgradeNode {
                                    node_id: "source".into(),
                                    pod_uid: "pod".into(),
                                    node_session_id: "source-session".into(),
                                    phase: "evacuating".into(),
                                    ..Default::default()
                                },
                            )]
                            .into(),
                            operations: [(
                                "move".into(),
                                UpgradeOperation {
                                    environment_id: "move".into(),
                                    source_node_id: "source".into(),
                                    target_node_id: "target".into(),
                                    automatic_target: false,
                                    source_generation: 1,
                                    source_revision: 1,
                                    source_stopped_revision: Some(1),
                                    source_session_id: "source-session".into(),
                                    target_session_id: "target-session".into(),
                                    was_running: resume,
                                    phase: "Prepared".into(),
                                },
                            )]
                            .into(),
                            ..Default::default()
                        },
                        None,
                    )
                    .await
                    .unwrap();
                let rpc = CoordinatorRpc::new(
                    session.clone(),
                    Placement::Pack,
                    Peers::network(),
                    RpcClient::network(Principal::Coordinator),
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
                (rig, session, rpc, stored.assignment, task)
            };
            if fault == "abort" {
                task.phase = "Aborting".into();
                session
                    .save_upgrade(task.clone(), Some(task.revision))
                    .await
                    .unwrap();
            }
            let mut state = rpc.0.state.lock().await;
            report_recovery_target(&mut state).await;
            let stored = session.get("move").await.unwrap();
            let point = stored.result.as_ref().unwrap().checkpoint.as_ref().unwrap();
            let request =
                checkpoint_spec(&stored, Some(point), None, &BTreeSet::new(), Some("target"))
                    .unwrap();
            assert_eq!(
                state
                    .scheduler
                    .recovery_candidate(&request)
                    .unwrap()
                    .as_deref(),
                Some("target")
            );
            if fault == "capacity" {
                let mut node = state.nodes["target"].node.clone();
                node.capacity.cpu_millis = 0;
                state.scheduler.register(node).unwrap();
            }
            state.scheduler.submit_recovery(request).unwrap();
            if fault == "failed-source" {
                // The queued planned assignment must not become ordinary recovery
                // if the original source publishes failure and is then fenced.
                let mut failed = stored.result.clone().unwrap();
                failed.state = EnvironmentState::Failed;
                failed.revision += 1;
                session.commit(failed).await.unwrap();
                state.invalidate_node("source").await.unwrap();
            }
            for _ in 0..state.scheduler.shards.len() {
                state.drive().await.unwrap();
            }
            let current = session.get("move").await.unwrap();
            if fault == "none" {
                assert_eq!(current.assignment.node_id, "target");
                assert!(current.assignment.generation > original.generation);
                assert_eq!(current.recovery.as_ref().unwrap().pending, resume);
                if resume {
                    for _ in 0..2 {
                        state
                            .scheduler
                            .accept_claim(&current.spec, &current.assignment)
                            .unwrap();
                    }
                    assert_eq!(
                        state.scheduler.snapshot().environments()["move"].spec,
                        current.spec
                    );
                } else {
                    assert!(!state
                        .scheduler
                        .snapshot()
                        .environments()
                        .contains_key("move"));
                }
                assert_eq!(
                    state
                        .scheduler
                        .node_resources("target")
                        .unwrap()
                        .1
                        .cpu_millis,
                    if resume {
                        0
                    } else {
                        current.spec.resources.cpu_millis
                    }
                );
            } else {
                assert_eq!(current.assignment, original);
                assert_eq!(
                    current.result.unwrap().state,
                    if fault == "failed-source" {
                        EnvironmentState::Failed
                    } else {
                        EnvironmentState::Paused
                    }
                );
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn legacy_checkpoint_recovery_after_coordinator_restart_requires_exact_proof() {
        let _rig = crate::rpc::cancellation_test_redis::Redis::new().await;
        let db = _rig.store().await;
        let session = db.begin(2).await.unwrap();
        let (template, point, mut target) = fixture();
        target.capacity.cpu_millis *= 3;
        target.capacity.memory_bytes *= 3;
        for id in ["source", "target"] {
            let mut node = target.clone();
            node.id = id.into();
            if id == "source" {
                node.labels.clear();
                node.labels
                    .insert("ADX_CHECKPOINT_STORAGE".into(), "shared".into());
            }
            node.labels.insert("NODE_ID".into(), id.into());
            session
                .register_session_with_scopes(
                    node,
                    "127.0.0.1:1".into(),
                    "127.0.0.1:2".into(),
                    Some(crate::storage::NodeSession {
                        id: format!("{id}-session"),
                        sequence: 1,
                        routable: true,
                    }),
                    if id == "target" {
                        ["backend".into()].into()
                    } else {
                        Default::default()
                    },
                )
                .await
                .unwrap();
        }
        for (index, id) in ["good", "attested", "bad", "resumable"]
            .into_iter()
            .enumerate()
        {
            let mut stored = template.clone();
            stored.spec.id = id.into();
            stored.assignment.environment_id = id.into();
            stored.assignment.generation = u64::try_from(index).unwrap() + 1;
            session
                .reserve(stored.spec.clone(), stored.assignment.clone())
                .await
                .unwrap();
            let mut record = stored.effective_record();
            record.state = if id == "resumable" {
                EnvironmentState::Paused
            } else {
                EnvironmentState::Failed
            };
            record.revision = 1;
            record.resources_held = false;
            let mut captured = point.clone();
            captured.source_runtime_id = record.runtime.id.clone();
            captured.artifact.location = id.into();
            if id != "good" {
                captured.recovery_profile.clear();
                captured.artifact.scope.clear();
            }
            record.checkpoint = Some(captured);
            session.commit(record).await.unwrap();
        }
        let proofs = [("attested", "attested-2"), ("resumable", "resumable-4")]
            .into_iter()
            .map(|(id, runtime)| crate::storage::LegacyCheckpointProof {
                artifact: adx_core::CheckpointArtifact {
                    location: id.into(),
                    ..point.artifact.clone()
                },
                runtime_class: template.spec.runtime_class.clone(),
                source_runtime_id: runtime.into(),
                recovery_profile: point.recovery_profile.clone(),
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let session = db
            .begin(2)
            .await
            .unwrap()
            .with_legacy_checkpoint_proofs(proofs);
        let legacy = session
            .get("attested")
            .await
            .unwrap()
            .result
            .unwrap()
            .checkpoint
            .unwrap();
        let mut wrong_size = legacy.clone();
        wrong_size.artifact.size_bytes += 1;
        let mut wrong_scope = legacy.clone();
        wrong_scope.artifact.scope = "another-backend".into();
        let mut wrong_runtime = legacy;
        wrong_runtime.source_runtime_id = "attested-1".into();
        for mismatch in [wrong_size, wrong_scope, wrong_runtime] {
            let candidate = session.ordinary_recovery_point(&template.spec, &mismatch);
            assert_eq!(candidate.as_ref(), &mismatch);
            assert!(
                checkpoint_spec(&template, Some(&candidate), None, &BTreeSet::new(), None).is_err()
            );
        }
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        {
            let mut state = rpc.0.state.lock().await;
            report_recovery_target(&mut state).await;
            // The old source disappears before it can register in the new epoch.
            state.invalidate_node("source").await.unwrap();
            // Exercise durable placement without dispatching a runtime RPC.
            state.live.insert(
                "target".into(),
                LiveNode {
                    expired: false,
                    session: "target-session".into(),
                    sequence: 1,
                    last_seen: tokio::time::Instant::now(),
                    inspected: false,
                    report: pb::RegisterNodeRequest::default(),
                },
            );
        }
        assert!(matches!(
            rpc.recover_environments().await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            session.get("good").await.unwrap().assignment.node_id,
            "target"
        );
        let bad = session.get("bad").await.unwrap();
        assert_eq!(bad.assignment.node_id, "source");
        assert!(bad.invalidated);
        let attested = session.get("attested").await.unwrap();
        assert_eq!(attested.assignment.node_id, "target");
        let checkpoint = attested.result.unwrap().checkpoint.unwrap();
        assert_eq!(checkpoint.artifact.scope, point.artifact.scope);
        assert_eq!(checkpoint.recovery_profile, point.recovery_profile);
        let resume = rpc
            .ensure_running(pb::EnsureRunningRequest {
                environment_id: "resumable".into(),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant".into(),
                    administrator: false,
                }),
                timeout_seconds: 1,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(resume.code(), tonic::Code::DeadlineExceeded);
        let resumed = session.get("resumable").await.unwrap();
        assert_eq!(resumed.assignment.node_id, "target");
        assert_eq!(
            resumed.result.unwrap().checkpoint.unwrap().recovery_profile,
            point.recovery_profile
        );
    }
}
