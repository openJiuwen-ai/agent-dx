//! Seal accepted work and adopt an inspected inventory without backend cleanup.
use crate::Adxlet;
use adx_core::{Error, Result};
use adx_protocol::relay as pb;

fn poll_seal_authority(tasks: &mut crate::AuthorityTasks) -> Result<bool> {
    let mut completed = Vec::new();
    let mut blocked = false;
    for (key, receiver) in tasks.iter_mut() {
        match receiver.try_recv() {
            Ok(Ok(crate::controller::AuthorityResult::Stale)) => blocked = true,
            Ok(Ok(_)) => completed.push(key.clone()),
            Ok(Err(_)) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => blocked = true,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }
    }
    for key in completed {
        tasks.remove(&key);
    }
    if blocked {
        return Err(Error::Unavailable(
            "authority outcome unresolved; seal blocked".into(),
        ));
    }
    Ok(tasks.is_empty())
}

impl Adxlet {
    pub async fn restore_successor_records(
        &self,
        records: &[adx_core::EnvironmentRecord],
    ) -> Result<()> {
        self.validate_successor_inventory(records).await?;
        let control = self
            .services
            .runtime_control
            .as_ref()
            .ok_or(Error::Conflict)?;
        if !control.uses_stream() {
            return Err(Error::Conflict);
        }
        for record in records {
            if record.state == adx_core::EnvironmentState::Running {
                control.expect_runtime(record)?;
                let status = control.status(record).await?;
                if status.identity != crate::runtime_control::RuntimeControlClient::identity(record)
                    || status.phase != adx_core::runtime::RuntimePhase::Running
                    || status.failure.is_some()
                    || status.requested_checkpoint.is_some()
                    || status.requested_checkpoint_deadline_unix_millis.is_some()
                    || status.checkpoint.as_ref().is_some_and(|checkpoint| {
                        checkpoint.error.is_some()
                            || !matches!(
                                checkpoint.phase,
                                adx_core::runtime::CheckpointPhase::Resumed
                                    | adx_core::runtime::CheckpointPhase::Restored
                                    | adx_core::runtime::CheckpointPhase::Aborted
                            )
                    })
                    || status.revision == 0
                {
                    return Err(Error::Conflict);
                }
                self.services.readiness.expect_runtime(record)?;
                self.services.readiness.wait_ready(record).await?;
            } else if record.checkpoint.is_none() {
                return Err(Error::Conflict);
            }
            self.adopt_successor_record(record)?
                .ok_or(Error::Conflict)?
                .prepare_successor()
                .await?;
        }
        Ok(())
    }
    /// Called only after the exact normal registration ACK for this session.
    pub async fn publish_successor_ready(&self, identity: &pb::AdxletProcessRequest) -> Result<()> {
        self.registration_acknowledged
            .store(false, std::sync::atomic::Ordering::Release);
        let handles: Vec<_> = self
            .environments
            .lock()
            .map_err(|_| Error::Conflict)?
            .values()
            .map(|(_, _, handle)| handle.clone())
            .collect();
        for handle in handles {
            handle
                .release_successor(self.services.upgrade.session())
                .await?;
        }
        let mut ready = self.lifecycle_ready.write().await;
        self.services.upgrade.finish_successor(identity)?;
        self.authority_initialized
            .store(true, std::sync::atomic::Ordering::Release);
        self.authority_reconciling
            .store(false, std::sync::atomic::Ordering::Release);
        *ready = true;
        Ok(())
    }
    pub async fn complete_successor_maintenance(
        &self,
        identity: &pb::AdxletProcessRequest,
        receipt: &adx_protocol::control::MaintenanceReleaseReceipt,
    ) -> Result<()> {
        let _exclusive = self.process_transition.lock().await;
        let owner = receipt.owner.as_ref().ok_or(Error::Conflict)?;
        if owner.node_id != self.node_id
            || owner.rollout_id != identity.rollout_id
            || owner.local_task_id != identity.local_task_id
            || owner.epoch != identity.epoch
            || receipt.node_session != self.services.upgrade.session()
            || !self.services.upgrade.completed_process(identity)?
        {
            return Err(Error::Conflict);
        }
        self.services
            .upgrade
            .release_process_maintenance(identity)?;
        self.set_maintenance(false);
        Ok(())
    }
    /// Read-only inventory comparison, never generic reconciliation or removal.
    pub async fn validate_successor_inventory(
        &self,
        records: &[adx_core::EnvironmentRecord],
    ) -> Result<()> {
        let actual = tokio::time::timeout(
            self.services.operation_timeout,
            self.services.runtime.inventory(),
        )
        .await
        .map_err(|_| Error::Unavailable("successor inventory timed out".into()))??;
        let mut expected = std::collections::BTreeMap::new();
        for record in records {
            if record.assignment.node_id != self.node_id
                || record.restart_pending
                || !matches!(
                    record.state,
                    adx_core::EnvironmentState::Running | adx_core::EnvironmentState::Paused
                )
            {
                return Err(Error::Conflict);
            }
            if record.state == adx_core::EnvironmentState::Running {
                if !record.resources_held || record.runtime.ip.is_none() {
                    return Err(Error::Conflict);
                }
                if expected
                    .insert(
                        record.runtime.id.clone(),
                        (
                            record.spec.id.clone(),
                            record.assignment.generation,
                            record.spec.tenant_id.clone(),
                        ),
                    )
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
            } else if record.resources_held {
                return Err(Error::Conflict);
            }
        }
        if actual.len() != expected.len() {
            return Err(Error::Conflict);
        }
        for runtime in actual {
            if !runtime.running
                || expected.remove(&runtime.runtime_id)
                    != Some((
                        runtime.environment_id,
                        runtime.generation,
                        runtime.tenant_id,
                    ))
            {
                return Err(Error::Conflict);
            }
        }
        if !expected.is_empty() {
            return Err(Error::Conflict);
        }
        let controllers = self.environments.lock().map_err(|_| Error::Conflict)?;
        for (id, (_, _, handle)) in controllers.iter() {
            if records.iter().find(|record| &record.spec.id == id) != Some(&handle.view().0) {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    pub fn process_sealing(&self) -> bool {
        self.services
            .upgrade
            .process_identity()
            .map_or(true, |identity| {
                identity.is_some_and(|identity| {
                    identity.old_node_session == self.services.upgrade.session()
                })
            })
    }
    pub(crate) async fn seal_process(
        &self,
        request: pb::AdxletProcessRequest,
    ) -> Result<pb::AdxletProcessState> {
        let _exclusive = self.process_transition.lock().await;
        if self.is_draining() {
            return Err(Error::Conflict);
        }
        if self.services.upgrade.process_identity()?.as_ref() != Some(&request) {
            return Err(Error::Conflict);
        }
        let state = self.services.upgrade.process_state()?;
        if state.phase == "sealed" {
            return Ok(state);
        }
        self.set_maintenance(true);
        let deadline = tokio::time::Instant::now() + self.services.upgrade.process_remaining()?;
        let result = tokio::time::timeout_at(deadline, async {
            let _recovery = self.recovery.lock().await;
            self.services.upgrade.wait_process_leases().await?;
            self.pause_lifecycle().await;
            if !self
                .local_holds
                .lock()
                .map_err(|_| Error::Conflict)?
                .is_empty()
            {
                return Err(Error::Conflict);
            }
            loop {
                let notified = self.authority_progress.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let mut tasks = self.authority_tasks.lock().map_err(|_| Error::Conflict)?;
                    if poll_seal_authority(&mut tasks)? {
                        break;
                    }
                }
                notified.await;
            }
            let handles: Vec<_> = self
                .environments
                .lock()
                .map_err(|_| Error::Conflict)?
                .values()
                .map(|(_, _, handle)| handle.clone())
                .collect();
            for handle in handles {
                handle.seal().await?;
            }
            Ok::<(), Error>(())
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            return Err(Error::Unavailable(
                "process seal unresolved; remains fenced".into(),
            ));
        }
        self.services.upgrade.mark_process_sealed()?;
        self.services.upgrade.process_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn successor_record() -> adx_core::EnvironmentRecord {
        serde_json::from_value(serde_json::json!({
            "spec":{"id":"successor-test","tenant_id":"tenant","image":"image",
                "runtime_class":"runc","priority":0,
                "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}},
            "assignment":{"environment_id":"successor-test","node_id":"node",
                "shard_id":0,"generation":1,"devices":[]},
            "state":"Running","revision":1,"runtime":{"id":"successor-test-1","ip":"10.0.0.2"},
            "resources_held":true,"checkpoint":null,"last_operation":null,
            "restart_attempts":0,"restart_pending":false,"execution_artifacts":[],
            "snapshot_source":null,"failure":null
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn rebase_compat_process_seal_waits_for_existing_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let node = crate::admin::tests::manager()
            .with_upgrade_control(directory.path().join("authority.json"), "origin".into())
            .unwrap();
        let identity = pb::AdxletProcessRequest {
            rollout_id: "rollout".into(),
            local_task_id: "local".into(),
            epoch: 1,
            old_node_session: "origin".into(),
            old_relay_session: "relay".into(),
            deadline_unix_millis: adx_core::observation::now_millis().unwrap() + 60_000,
        };
        node.services
            .upgrade
            .fence_process(
                identity.clone(),
                std::time::Instant::now() + std::time::Duration::from_secs(60),
            )
            .unwrap();
        let recovering = node.recovery.lock().await;
        let sealing = node.seal_process(identity);
        tokio::pin!(sealing);
        assert!(futures_util::poll!(&mut sealing).is_pending());
        drop(recovering);
        assert_eq!(sealing.await.unwrap().phase, "sealed");
    }

    #[tokio::test]
    async fn rebase_compat_successor_adoption_keeps_closed_admission_and_restores_capacity_once() {
        let directory = tempfile::tempdir().unwrap();
        let node = crate::admin::tests::manager()
            .with_upgrade_control(directory.path().join("authority.json"), "successor".into())
            .unwrap();
        node.isolate_upgrade_bootstrap().unwrap();
        let record = successor_record();
        assert!(!node.accepting_allocations());
        assert!(node
            .services
            .upgrade
            .record_action(&record.spec.id, 1, "adopt")
            .is_err());
        let handle = node.adopt_successor_record(&record).unwrap().unwrap();
        assert_eq!(node.used(), record.spec.resources);
        handle.prepare_successor().await.unwrap();
        let replay = node.adopt_successor_record(&record).unwrap().unwrap();
        replay.prepare_successor().await.unwrap();
        assert_eq!(node.used(), record.spec.resources);
        assert!(!node.accepting_allocations());
    }

    #[tokio::test]
    async fn rejected_delete_while_successor_pending_does_not_block_released_controller() {
        let directory = tempfile::tempdir().unwrap();
        let node = crate::admin::tests::manager()
            .with_upgrade_control(directory.path().join("authority.json"), "successor".into())
            .unwrap();
        let record = successor_record();
        let handle = crate::controller::spawn_comparison(record.clone(), node.services.clone());
        handle.prepare_successor().await.unwrap();
        assert!(matches!(handle.delete().await, Err(Error::Conflict)));
        handle.release_successor("successor").await.unwrap();
        let result = handle.ensure_running("after-release".into()).await.unwrap();
        assert_eq!(result.record, record);
        assert_eq!(result.durability, crate::Durability::Published);
    }

    #[tokio::test]
    async fn persisted_sealed_predecessor_accepts_bootstrap_owner_and_release() {
        for released_pod_control in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("authority.json");
            let predecessor =
                crate::upgrade::UpgradeGate::open(path.clone(), "origin".into()).unwrap();
            if released_pod_control {
                predecessor
                    .apply(crate::upgrade::NodeControl {
                        task_id: "previous-pod-upgrade".into(),
                        epoch: 1,
                        revision: 1,
                        node_session_id: "origin".into(),
                        phase: "released".into(),
                        grants: vec![],
                    })
                    .await
                    .unwrap();
            }
            let sealed = pb::AdxletProcessRequest {
                rollout_id: "rollout".into(),
                local_task_id: "local".into(),
                epoch: 2,
                old_node_session: "origin".into(),
                old_relay_session: "relay".into(),
                deadline_unix_millis: adx_core::observation::now_millis().unwrap() + 60_000,
            };
            predecessor
                .fence_process(
                    sealed.clone(),
                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                )
                .unwrap();
            predecessor.mark_process_sealed().unwrap();
            drop(predecessor);
            // This is exactly the request constructed by the startup loop and
            // parent ready query, not the old process's full Begin request.
            let identity = pb::AdxletProcessRequest {
                rollout_id: sealed.rollout_id,
                local_task_id: sealed.local_task_id,
                epoch: sealed.epoch,
                ..Default::default()
            };
            for session in ["target", "rollback"] {
                let node = crate::admin::tests::manager()
                    .with_upgrade_control(path.clone(), session.into())
                    .unwrap();
                node.isolate_upgrade_bootstrap().unwrap();
                node.set_maintenance(true);
                let receipt = adx_protocol::control::MaintenanceReleaseReceipt {
                    owner: Some(adx_protocol::control::MaintenanceOwner {
                        node_id: "node".into(),
                        rollout_id: identity.rollout_id.clone(),
                        local_task_id: identity.local_task_id.clone(),
                        epoch: identity.epoch,
                    }),
                    node_session: session.into(),
                };
                assert!(
                    node.complete_successor_maintenance(&identity, &receipt)
                        .await
                        .is_err(),
                    "release never bypasses local recovery"
                );
                node.publish_successor_ready(&identity).await.unwrap();
                assert!(node.services.upgrade.completed_process(&identity).unwrap());
                node.publish_successor_ready(&identity).await.unwrap();
                node.complete_successor_maintenance(&identity, &receipt)
                    .await
                    .unwrap();
                assert_eq!(
                    node.services.upgrade.control().unwrap().is_some(),
                    released_pod_control,
                    "a prior Pod release must remain durable across another restart"
                );
            }
        }
    }

    #[test]
    fn unresolved_authority_receiver_is_retained() {
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut tasks = crate::AuthorityTasks::new();
        tasks.insert(("env".into(), None), receive);
        assert!(!poll_seal_authority(&mut tasks).unwrap());
        assert!(send.send(Err(Error::Conflict)).is_ok());
        assert!(poll_seal_authority(&mut tasks).is_err());
        assert_eq!(tasks.len(), 1);
    }
}
