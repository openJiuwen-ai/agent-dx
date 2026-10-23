//! Snapshot creation inherits runtime geometry and defaults to source-node
//! affinity. Its reference protects the artifact while restoring the execution.
use super::*;
use adx_core::snapshots::{Reference, Snapshot, SnapshotState};

pub(super) async fn normalize(
    session: &Session,
    request: pb::EnvironmentSpec,
) -> Result<EnvironmentSpec> {
    let Some(id) = &request.snapshot_id else {
        return request.try_into();
    };
    let snapshot = session.get_snapshot(id).await?;
    let reference = Reference::Restore {
        environment_id: request.id.clone(),
    };
    if request.tenant_id != snapshot.template.tenant_id {
        return Err(Error::NotFound);
    }
    // A completed request may be retried after its source snapshot was deleted.
    let existing = match session.get(&request.id).await {
        Ok(stored) if stored.spec.snapshot_id == request.snapshot_id => Some(stored.spec),
        Ok(_) => return Err(Error::Conflict),
        Err(Error::NotFound) => None,
        Err(error) => return Err(error),
    };
    if existing.is_none()
        && snapshot.state != SnapshotState::Ready
        && !snapshot.references.contains(&reference)
    {
        return Err(Error::Conflict);
    }
    if request.id == snapshot.template.id {
        return Err(Error::Conflict);
    }
    inherit_template(request, &snapshot, existing.as_ref())
}

fn inherit_template(
    mut request: pb::EnvironmentSpec,
    snapshot: &Snapshot,
    existing: Option<&EnvironmentSpec>,
) -> Result<EnvironmentSpec> {
    snapshot.origin()?;
    let template = &snapshot.template;
    request.runtime_profile = template.runtime_profile.clone().map(Into::into);
    if request.image.is_empty() {
        request.image = template.image.clone();
    }
    if request.runtime_class.is_empty() {
        request.runtime_class = template.runtime_class.clone();
    }
    let resources = request.resources.get_or_insert_with(Default::default);
    if resources.cpu_millis == 0 {
        resources.cpu_millis = template.resources.cpu_millis;
    }
    if resources.memory_bytes == 0 {
        resources.memory_bytes = template.resources.memory_bytes;
    }
    if resources.disk_bytes == 0 {
        resources.disk_bytes = template.resources.disk_bytes;
    }
    if request.image != template.image
        || request.runtime_class != template.runtime_class
        || *resources != pb::Resources::from(template.resources)
    {
        return Err(Error::Invalid(
            "snapshot image, runtime and resource geometry must match its source".into(),
        ));
    }
    let overrides = std::mem::take(&mut request.env);
    request.env = template.env.clone().into_iter().collect();
    request.env.extend(overrides);
    let policy = request.scheduling.get_or_insert_with(Default::default);
    if policy.devices.is_empty() {
        policy.devices = template
            .scheduling
            .devices
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
    }
    if snapshot.artifact.storage == "local" {
        if policy.required_node.is_empty() {
            policy.required_node.push(Default::default());
        }
        for alternative in &mut policy.required_node {
            let required = pb::LabelRequirement {
                key: "NODE_ID".into(),
                op: pb::SelectorOp::In as i32,
                values: vec![snapshot.source_node_id.clone()],
            };
            if !alternative.expressions.contains(&required) {
                alternative.expressions.push(required);
            }
        }
    }
    if snapshot.artifact.storage != "local" {
        let shared_pause = request
            .lifecycle
            .as_ref()
            .and_then(|lifecycle| lifecycle.pause.as_ref())
            .is_some_and(|pause| pause.storage == pb::CheckpointStorage::Shared as i32);
        // Do not add a new default to a replay of an older committed create.
        // Explicit policy changes must still fail the create identity check.
        if !has_placement(policy, shared_pause)
            && existing.is_none_or(|spec| !spec.scheduling.preferred_node.is_empty())
        {
            policy.preferred_node.push(pb::WeightedSelector {
                selector: Some(pb::LabelSelector {
                    match_labels: [("NODE_ID".into(), snapshot.source_node_id.clone())].into(),
                    ..Default::default()
                }),
                weight: 1,
            });
        }
        require_shared_storage(policy, &snapshot.artifact.storage);
    }
    request.try_into()
}

fn has_placement(policy: &pb::SchedulingPolicy, shared_pause: bool) -> bool {
    // The API adds Exists + NotIn(local) for shared pause storage. Retain those
    // guards, but do not mistake them for an explicit node-placement choice.
    let implicit_storage_guard = |requirement: &pb::LabelRequirement| {
        shared_pause
            && requirement.key == "ADX_CHECKPOINT_STORAGE"
            && ((requirement.op == pb::SelectorOp::Exists as i32 && requirement.values.is_empty())
                || (requirement.op == pb::SelectorOp::NotIn as i32
                    && requirement.values == ["local"]))
    };
    policy.required_node.iter().any(|selector| {
        !selector.match_labels.is_empty()
            || selector
                .expressions
                .iter()
                .any(|r| !implicit_storage_guard(r))
    }) || !policy.preferred_node.is_empty()
        || !policy.required_affinity.is_empty()
        || !policy.preferred_affinity.is_empty()
        || !policy.required_anti_affinity.is_empty()
        || !policy.preferred_anti_affinity.is_empty()
        || !policy.topology_spread.is_empty()
        || !policy.placement_groups.is_empty()
}

fn require_shared_storage(policy: &mut pb::SchedulingPolicy, storage: &str) {
    if policy.required_node.is_empty() {
        policy.required_node.push(Default::default());
    }
    let required = pb::LabelRequirement {
        key: "ADX_CHECKPOINT_STORAGE".into(),
        op: pb::SelectorOp::In as i32,
        values: vec![storage.into()],
    };
    for alternative in &mut policy.required_node {
        if !alternative.expressions.contains(&required) {
            alternative.expressions.push(required.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(storage: &str) -> Snapshot {
        let template = serde_json::from_value(serde_json::json!({
            "id": "source", "tenant_id": "tenant", "image": "image",
            "runtime_class": "runsc", "priority": 0,
            "resources": {"cpu_millis": 100, "memory_bytes": 128, "disk_bytes": 0}
        }))
        .unwrap();
        Snapshot::new(
            "snapshot".into(),
            vec![],
            template,
            "source-node".into(),
            "source-1".into(),
            adx_core::CheckpointArtifact {
                storage: storage.into(),
                scope: String::new(),
                location: "artifact".into(),
                size_bytes: 100,
            },
        )
        .unwrap()
    }

    fn clone_request(snapshot: &Snapshot) -> pb::EnvironmentSpec {
        pb::EnvironmentSpec {
            id: "clone".into(),
            tenant_id: "tenant".into(),
            snapshot_id: Some(snapshot.id.clone()),
            ..Default::default()
        }
    }

    #[test]
    fn shared_snapshot_defaults_to_source_preference_without_pinning() {
        for storage in ["s3", "afs", "juicefs"] {
            let snapshot = source(storage);
            let normalized = inherit_template(clone_request(&snapshot), &snapshot, None).unwrap();
            assert_eq!(normalized.scheduling.preferred_node.len(), 1);
            let source_labels = [
                ("NODE_ID".into(), "source-node".into()),
                ("ADX_CHECKPOINT_STORAGE".into(), storage.into()),
            ]
            .into();
            let other_labels = [
                ("NODE_ID".into(), "other-node".into()),
                ("ADX_CHECKPOINT_STORAGE".into(), storage.into()),
            ]
            .into();
            assert!(normalized.scheduling.preferred_node[0]
                .selector
                .matches(&source_labels));
            assert!(!normalized.scheduling.preferred_node[0]
                .selector
                .matches(&other_labels));
            assert!(normalized.scheduling.matches_node(&other_labels));
            assert_eq!(
                inherit_template(normalized.clone().into(), &snapshot, None).unwrap(),
                normalized
            );
            assert_eq!(
                inherit_template(clone_request(&snapshot), &snapshot, Some(&normalized)).unwrap(),
                normalized
            );
        }
    }

    #[test]
    fn snapshot_explicit_placement_is_preserved() {
        for field in [
            "required_node",
            "preferred_node",
            "required_affinity",
            "preferred_affinity",
            "required_anti_affinity",
            "preferred_anti_affinity",
            "topology_spread",
            "placement_groups",
        ] {
            let snapshot = source("juicefs");
            let mut request = clone_request(&snapshot);
            let mut policy = pb::SchedulingPolicy::default();
            let selector = pb::LabelSelector {
                match_labels: [("NODE_ID".into(), "other-node".into())].into(),
                ..Default::default()
            };
            let peer = pb::PeerTerm {
                selector: Some(selector.clone()),
                topology_key: "zone".into(),
                tenants: vec![],
            };
            match field {
                "required_node" => policy.required_node.push(selector),
                "preferred_node" => policy.preferred_node.push(pb::WeightedSelector {
                    weight: 1,
                    selector: Some(selector),
                }),
                "required_affinity" => policy.required_affinity.push(peer),
                "preferred_affinity" => policy.preferred_affinity.push(pb::WeightedPeer {
                    term: Some(peer),
                    weight: 1,
                }),
                "required_anti_affinity" => policy.required_anti_affinity.push(peer),
                "preferred_anti_affinity" => {
                    policy.preferred_anti_affinity.push(pb::WeightedPeer {
                        term: Some(peer),
                        weight: 1,
                    })
                }
                "topology_spread" => policy.topology_spread.push(pb::TopologySpread {
                    topology_key: "zone".into(),
                    selector: Some(selector),
                    max_skew: 1,
                    min_domains: 1,
                    when_unsatisfiable: pb::SpreadMode::ScheduleAnyway as i32,
                }),
                "placement_groups" => policy.placement_groups.push(pb::PlacementGroup {
                    target: pb::PlacementTarget::Node as i32,
                    terms: vec![pb::WeightedSelector {
                        selector: Some(selector),
                        weight: 1,
                    }],
                    ..Default::default()
                }),
                _ => continue,
            }
            request.scheduling = Some(policy.clone());
            require_shared_storage(&mut policy, "juicefs");
            let normalized = inherit_template(request, &snapshot, None).unwrap();
            let actual: pb::SchedulingPolicy = normalized.scheduling.into();
            assert_eq!(actual, policy, "{field}");
        }
    }

    #[test]
    fn snapshot_labels_and_device_requirements_keep_source_preference() {
        let mut snapshot = source("juicefs");
        snapshot
            .template
            .scheduling
            .devices
            .push(adx_core::scheduling::DeviceRequest {
                kind: adx_core::scheduling::DeviceKind::Gpu,
                model: None,
                count: 1,
            });
        let mut request = clone_request(&snapshot);
        request.scheduling = Some(pb::SchedulingPolicy {
            labels: [("app".into(), "worker".into())].into(),
            ..Default::default()
        });
        let normalized = inherit_template(request, &snapshot, None).unwrap();
        assert_eq!(normalized.scheduling.preferred_node.len(), 1);
        assert_eq!(normalized.scheduling.labels["app"], "worker");
        assert_eq!(
            normalized.scheduling.devices,
            snapshot.template.scheduling.devices
        );
    }

    #[test]
    fn shared_pause_storage_guard_does_not_override_snapshot_source_preference() {
        let snapshot = source("juicefs");
        let mut request = clone_request(&snapshot);
        request.lifecycle = Some(
            adx_core::lifecycle::LifecyclePolicy {
                pause: Some(adx_core::lifecycle::PausePolicy {
                    storage: adx_core::lifecycle::CheckpointStorage::Shared,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .into(),
        );
        // The public API adds these compatibility guards for pause.storage=shared.
        // They are not a caller-supplied node-placement policy.
        request.scheduling = Some(pb::SchedulingPolicy {
            required_node: vec![pb::LabelSelector {
                expressions: vec![
                    pb::LabelRequirement {
                        key: "ADX_CHECKPOINT_STORAGE".into(),
                        op: pb::SelectorOp::Exists as i32,
                        values: vec![],
                    },
                    pb::LabelRequirement {
                        key: "ADX_CHECKPOINT_STORAGE".into(),
                        op: pb::SelectorOp::NotIn as i32,
                        values: vec!["local".into()],
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        });
        let normalized = inherit_template(request.clone(), &snapshot, None).unwrap();
        assert_eq!(normalized.scheduling.preferred_node.len(), 1);
        assert_eq!(normalized.scheduling.required_node[0].expressions.len(), 3);
        request.scheduling.as_mut().unwrap().required_node[0]
            .match_labels
            .insert("NODE_ID".into(), "other-node".into());
        let explicit = inherit_template(request, &snapshot, None).unwrap();
        assert!(explicit.scheduling.preferred_node.is_empty());
    }

    #[test]
    fn local_snapshot_keeps_required_source_and_old_requests_keep_their_policy() {
        let snapshot = source("local");
        let normalized = inherit_template(clone_request(&snapshot), &snapshot, None).unwrap();
        assert!(normalized.scheduling.preferred_node.is_empty());
        assert!(normalized
            .scheduling
            .matches_node(&[("NODE_ID".into(), "source-node".into())].into()));
        assert!(!normalized
            .scheduling
            .matches_node(&[("NODE_ID".into(), "other-node".into())].into()));
        let snapshot = source("juicefs");
        let mut old = inherit_template(clone_request(&snapshot), &snapshot, None).unwrap();
        old.scheduling.preferred_node.clear();
        assert_eq!(
            inherit_template(clone_request(&snapshot), &snapshot, Some(&old)).unwrap(),
            old
        );
    }

    #[test]
    fn removing_explicit_placement_does_not_replay_a_different_request() {
        let snapshot = source("juicefs");
        let mut request = clone_request(&snapshot);
        request.scheduling = Some(pb::SchedulingPolicy {
            preferred_node: vec![pb::WeightedSelector {
                selector: Some(pb::LabelSelector {
                    match_labels: [("NODE_ID".into(), "explicit-target".into())].into(),
                    ..Default::default()
                }),
                weight: 1,
            }],
            ..Default::default()
        });
        let stored = inherit_template(request, &snapshot, None).unwrap();
        let changed = inherit_template(clone_request(&snapshot), &snapshot, Some(&stored)).unwrap();
        assert_ne!(
            changed, stored,
            "the create identity check must reject the changed policy"
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn snapshot_shard_transfer_keeps_rpc_driving_and_replays_after_restart() {
        let mut redis = crate::rpc::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(2).await.unwrap();
        let snapshot = source("juicefs");
        session.publish_snapshot(snapshot.clone()).await.unwrap();
        let mut nodes = Vec::new();
        for id in ["source-node", "other-node"] {
            let node = Node {
                id: id.into(),
                capacity: snapshot.template.resources,
                available: true,
                runtime_classes: vec!["runsc".into()],
                devices: vec![],
                labels: [
                    ("NODE_ID".into(), id.into()),
                    ("ADX_CHECKPOINT_STORAGE".into(), "juicefs".into()),
                ]
                .into(),
            };
            session
                .register(node.clone(), "127.0.0.1:1".into(), "127.0.0.1:2".into())
                .await
                .unwrap();
            nodes.push(node);
        }
        let rpc = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();
        let spec = normalize(&session, clone_request(&snapshot)).await.unwrap();
        session
            .acquire_snapshot(
                &snapshot.id,
                "tenant",
                Reference::Restore {
                    environment_id: spec.id.clone(),
                },
            )
            .await
            .unwrap();
        let assignment = {
            let mut state = rpc.0.state.lock().await;
            for node in &nodes {
                state.scheduler.register(node.clone()).unwrap();
            }
            assert_eq!(state.scheduler.submit(spec.clone()).unwrap(), 0);
            state.specs.insert(spec.id.clone(), spec.clone());
            nodes[0].available = false;
            state.scheduler.register(nodes[0].clone()).unwrap();
            assert!(
                state.drive().await.unwrap(),
                "a transfer must wake the next shard without waiting for a heartbeat"
            );
            assert!(state.environments.is_empty());
            assert!(state.drive().await.unwrap());
            let assignment = state.environments[&spec.id].assignment.clone();
            assert_eq!(assignment.node_id, "other-node");
            assert!(!state.drive().await.unwrap());
            assignment
        };
        drop(rpc);
        redis.crash();
        redis.start().await;
        let recovered = redis.store().await.begin(2).await.unwrap();
        assert_eq!(
            normalize(&recovered, clone_request(&snapshot))
                .await
                .unwrap(),
            spec
        );
        assert_eq!(
            recovered.get(&spec.id).await.unwrap().assignment,
            assignment
        );
        let saved = recovered.snapshot().await.unwrap();
        let mut scheduler = Coordinator::restore(&saved, Placement::Pack).unwrap();
        assert_eq!(scheduler.submit(spec).unwrap(), assignment.shard_id);
        assert!(scheduler.pending_requests().is_empty());
        assert_eq!(
            scheduler.node_resources("other-node").unwrap().1.cpu_millis,
            0
        );
    }

    #[test]
    fn shared_snapshot_normalization_is_idempotent_for_every_placement_alternative() {
        let mut policy = pb::SchedulingPolicy::default();
        // Cover both unrestricted scheduling and an explicit cross-node target.
        for target in [None, Some("target-node")] {
            if let Some(node) = target {
                policy.required_node.push(pb::LabelSelector {
                    expressions: vec![pb::LabelRequirement {
                        key: "NODE_ID".into(),
                        op: pb::SelectorOp::In as i32,
                        values: vec![node.into()],
                    }],
                    ..Default::default()
                });
            }
            require_shared_storage(&mut policy, "shared-fs");
            let normalized = policy.clone();
            require_shared_storage(&mut policy, "shared-fs");
            assert_eq!(policy, normalized);
            assert!(policy.required_node.iter().all(|alternative| {
                alternative
                    .expressions
                    .iter()
                    .filter(|requirement| requirement.key == "ADX_CHECKPOINT_STORAGE")
                    .count()
                    == 1
            }));
        }
    }
}
