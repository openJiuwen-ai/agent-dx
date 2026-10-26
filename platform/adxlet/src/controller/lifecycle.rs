use super::*;
use crate::checkpoint::{now, validate_operation};
use adx_core::{CompletedOperation, LifecycleKind, RestorePoint};
use std::{path::Path, time::Duration};

impl Controller {
    pub(super) async fn auto_pause(
        &mut self,
        request: PauseRequest,
        stamp: (String, u64, u64),
        demand: u64,
    ) -> Result<()> {
        if !self
            .services
            .routes
            .quiesce(&self.record, &stamp.0, stamp.1)
            .await?
        {
            return Ok(());
        }
        let result = async {
            let activity = self.services.readiness.activity(&self.record).await?;
            if activity != (stamp.2, 0)
                || self.demand.load(std::sync::atomic::Ordering::Acquire) != demand
            {
                return Ok(());
            }
            self.auto_pause_guard = Some((stamp, demand));
            let trace = adx_observability::trace::Trace::child("environment.auto_pause");
            trace.attribute("environment.id", self.record.spec.id.clone());
            trace.run_result(self.pause(request)).await.map(|_| ())
        }
        .await;
        self.auto_pause_guard = None;
        if self.record.state == EnvironmentState::Running {
            self.services.routes.reopen(&self.record).await?;
        }
        result
    }
    pub(super) async fn reload(
        &mut self,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<OperationResult> {
        let services = self.services.clone();
        let mut attempt = services
            .metrics
            .operation_attempt("reload", adx_core::observation::OperationIntent::User);
        let result = self.reload_inner(operation_id, expected_revision).await;
        let cleared = self.clear_operation_intent().await;
        let result = match (result, cleared) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        attempt.finish(&result);
        result
    }
    async fn reload_inner(
        &mut self,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<OperationResult> {
        if self.replay_operation(&operation_id, expected_revision, LifecycleKind::Reload)? {
            return self.replay_or_sync().await;
        }
        if self.record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        let checkpoint = self
            .record
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("reload recovery point is unavailable".into()))?;
        if checkpoint.expired(now()?) {
            return Err(Error::Invalid("reload recovery point expired".into()));
        }
        self.services
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Unavailable("checkpoint storage is unavailable".into()))?
            .store
            .validate_artifact(&checkpoint.artifact)
            .await?;
        self.intent_retry = Some(CompletedOperation {
            id: operation_id.clone(),
            expected_revision,
            kind: LifecycleKind::Reload,
        });
        self.operation_intent(adx_core::observation::OperationIntent::User);
        self.publish_operation_intent().await?;
        self.cleanup().await?;
        self.transition(Event::Fail)?;
        self.start_attempt(
            true,
            true,
            Some((operation_id, expected_revision, LifecycleKind::Reload)),
        )
        .await
    }

    pub(super) async fn update_network_policy(
        &mut self,
        policy: Option<adx_core::execution::NetworkPolicy>,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<OperationResult> {
        if self.replay_operation(&operation_id, expected_revision, LifecycleKind::Network)? {
            return self.replay_or_sync().await;
        }
        if self.record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        if let Some(policy) = &policy {
            policy.validate()?;
        }
        self.record
            .spec
            .execution
            .validate_tunnel_network(policy.as_ref())?;
        self.services
            .runtime
            .set_network_policy(
                &self.record.runtime.id,
                policy.as_ref(),
                &self.record.spec.execution.ports,
                self.record.spec.execution.tunnel_http_port,
            )
            .await?;
        self.record.spec.execution.network = policy;
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.completed(operation_id, expected_revision, LifecycleKind::Network);
        self.durability = None;
        self.sync().await
    }

    pub(super) async fn expire_checkpoint(&mut self, now: u64) -> Result<Option<OperationResult>> {
        let Some(cp) = self.record.checkpoint.clone().filter(|cp| cp.expired(now)) else {
            return Ok(None);
        };
        if !matches!(
            self.record.state,
            EnvironmentState::Paused | EnvironmentState::Running | EnvironmentState::Failed
        ) {
            return Ok(None);
        }
        let trace = adx_observability::trace::Trace::child("environment.checkpoint_expired");
        trace.attribute("environment.id", self.record.spec.id.clone());
        trace.run_result(self.expire_checkpoint_inner(cp)).await
    }

    async fn expire_checkpoint_inner(
        &mut self,
        cp: RestorePoint,
    ) -> Result<Option<OperationResult>> {
        if self.record.state == EnvironmentState::Paused {
            self.operation_intent(adx_core::observation::OperationIntent::Policy);
            return self.delete().await.map(Some);
        }
        if self.record.state != EnvironmentState::Running
            && self.record.state != EnvironmentState::Failed
        {
            return Ok(None);
        }
        // An expired restore point is no longer usable, but the live execution
        // can still read its state/memory files. Persist that reference until stop.
        self.obsolete_checkpoints.push(cp.artifact);
        self.record.checkpoint = None;
        self.record.last_operation = None;
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        self.durability = None;
        self.sync().await?;
        Ok(None)
    }

    pub(super) fn replay_operation(
        &self,
        id: &str,
        revision: u64,
        kind: LifecycleKind,
    ) -> Result<bool> {
        validate_operation(id, revision)?;
        if let Some((op, _)) = &self.capture_preparation {
            return if op.id == id && op.expected_revision == revision && op.kind == kind {
                Ok(false)
            } else {
                Err(Error::Conflict)
            };
        }
        if self
            .record
            .last_operation
            .as_ref()
            .is_some_and(|op| op.id == id && op.kind == kind && op.expected_revision == revision)
        {
            return Ok(true);
        }
        if self
            .intent_retry
            .as_ref()
            .is_some_and(|op| op.id == id && op.expected_revision == revision && op.kind == kind)
        {
            return Ok(false);
        }
        if self.record.revision != revision {
            return Err(Error::Conflict);
        }
        Ok(false)
    }
    pub(super) async fn capture_directory(
        &mut self,
        store: &dyn crate::checkpoint::CheckpointAccess,
        id: &str,
        revision: u64,
        kind: LifecycleKind,
    ) -> Result<std::path::PathBuf> {
        if let Some((op, path)) = &self.capture_preparation {
            if op.id == id && op.expected_revision == revision && op.kind == kind {
                return Ok(path.clone());
            }
            return Err(Error::Conflict);
        }
        let path = store.begin_capture().await?;
        self.capture_preparation = Some((
            CompletedOperation {
                id: id.into(),
                expected_revision: revision,
                kind,
            },
            path.clone(),
        ));
        Ok(path)
    }
    pub(super) fn completed(&mut self, id: String, expected_revision: u64, kind: LifecycleKind) {
        self.intent_retry = None;
        self.record.last_operation = Some(CompletedOperation {
            id,
            expected_revision,
            kind,
        });
    }
    pub(super) async fn pause(&mut self, request: PauseRequest) -> Result<OperationResult> {
        let intent = if self.auto_pause_guard.is_some() {
            adx_core::observation::OperationIntent::Idle
        } else {
            adx_core::observation::OperationIntent::User
        };
        let services = self.services.clone();
        let mut attempt = services.metrics.operation_attempt("pause", intent);
        let result = self.pause_inner(request).await;
        let cleared = self.clear_operation_intent().await;
        let result = match (result, cleared) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        attempt.finish(&result);
        result
    }
    async fn pause_inner(&mut self, request: PauseRequest) -> Result<OperationResult> {
        if self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Pause,
        )? {
            return self.replay_or_sync().await;
        }
        if self.record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        request.retention.validate()?;
        if request.timeout_seconds == 0 || request.timeout_seconds > 3600 {
            return Err(Error::Invalid(
                "positive checkpoint TTL and timeout (at most 3600 seconds) required".into(),
            ));
        }
        let expires_at = request.retention.expires_at(now()?)?;
        let services = self
            .services
            .checkpoint
            .clone()
            .ok_or_else(|| Error::Invalid("checkpoint storage is not configured".into()))?;
        let storage = self
            .record
            .spec
            .lifecycle
            .pause
            .clone()
            .unwrap_or_default()
            .storage;
        services.store.validate_policy(storage)?;
        self.services
            .runtime
            .checkpoint_supported(&self.record.spec.runtime_class)
            .await?;
        let staged = self
            .capture_directory(
                services.store.as_ref(),
                &request.operation_id,
                request.expected_revision,
                LifecycleKind::Pause,
            )
            .await?;
        // Preparation errors can be ambiguous: abort is valid only before the backend call.
        let preparation = match &self.auto_pause_guard {
            Some((stamp, _)) => {
                services
                    .cooperation
                    .prepare_idle(&self.record, &request.operation_id, stamp.2)
                    .await
            }
            None => {
                services
                    .cooperation
                    .prepare(&self.record, &request.operation_id)
                    .await
            }
        };
        if let Err(error) = preparation {
            if self.auto_pause_guard.is_some() && error == Error::Conflict {
                // A definitive idle-barrier rejection has not changed runtime
                // state. In particular, do not abort another workload checkpoint.
                services.store.discard_staged(&staged).await?;
                self.capture_preparation = None;
                return self.sync().await;
            }
            let abort = services
                .cooperation
                .abort_unstarted(&self.record, &request.operation_id)
                .await;
            services.store.discard_staged(&staged).await?;
            self.capture_preparation = None;
            if let Err(abort) = abort {
                self.transition(Event::Fail)?;
                let retire = self.services.routes.retire(&self.record).await;
                let commit = self.sync().await;
                let commit_status = if commit.is_ok() {
                    "accepted"
                } else {
                    "unavailable"
                };
                return Err(Error::Unavailable(format!(
                    "prepare failed: {error}; abort failed: {abort}; route retirement: \
                     {retire:?}; state commit: {commit_status}"
                )));
            }
            return Err(error);
        }
        if let Some((stamp, demand)) = &self.auto_pause_guard {
            let proxy = self.services.routes.activity(&self.record).await;
            if self.demand.load(std::sync::atomic::Ordering::Acquire) != *demand
                || !matches!(proxy, Ok((ref session, revision, 0)) if session == &stamp.0 && revision == stamp.1)
            {
                let abort = services
                    .cooperation
                    .abort_unstarted(&self.record, &request.operation_id)
                    .await;
                services.store.discard_staged(&staged).await?;
                self.capture_preparation = None;
                if let Err(error) = abort {
                    self.transition(Event::Fail)?;
                    self.services.routes.retire(&self.record).await?;
                    self.sync().await?;
                    return Err(error);
                }
                return self.sync().await;
            }
        }
        for reference in services
            .store
            .capture_references(&staged)?
            .into_iter()
            .filter(|a| {
                storage != adx_core::lifecycle::CheckpointStorage::Local || a.storage == "local"
            })
        {
            if !self.record.execution_artifacts.contains(&reference) {
                self.record.execution_artifacts.push(reference);
            }
        }
        self.operation_intent(if self.auto_pause_guard.is_some() {
            adx_core::observation::OperationIntent::Idle
        } else {
            adx_core::observation::OperationIntent::User
        });
        self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        match self.sync().await {
            Ok(result) if result.durability == Durability::Published => {}
            _ => {
                services
                    .cooperation
                    .abort_unstarted(&self.record, &request.operation_id)
                    .await?;
                return Err(Error::Unavailable(
                    "capture input reservation publication pending".into(),
                ));
            }
        }
        self.capture_preparation = None;
        self.transition(Event::Pause)?;
        if let Err(error) = self.services.routes.retire(&self.record).await {
            let abort = services
                .cooperation
                .abort_unstarted(&self.record, &request.operation_id)
                .await;
            services.store.discard_staged(&staged).await?;
            self.capture_preparation = None;
            if let Err(abort) = abort {
                self.transition(Event::Fail)?;
                self.sync().await?;
                return Err(Error::Unavailable(format!(
                    "route retirement failed: {error}; abort failed: {abort}"
                )));
            }
            self.transition(Event::Rollback)?;
            if let Err(activate) = self.services.routes.activate(&self.record).await {
                self.transition(Event::Fail)?;
                self.sync().await?;
                return Err(activate);
            }
            self.sync().await?;
            return Err(error);
        }
        // This accepted call runs to settlement even if the HTTP caller disconnects.
        if let Err(error) = self
            .services
            .runtime
            .checkpoint(
                &self.record.runtime.id,
                &staged,
                Duration::from_secs(request.timeout_seconds),
            )
            .await
        {
            // The backend may have started checkpointing. Never tell EXECD to cancel an
            // "unstarted" checkpoint here, and never advertise the execution as ready.
            let cleanup = self.cleanup().await;
            self.transition(Event::Fail)?;
            self.sync().await?;
            cleanup?;
            services.store.discard_staged(&staged).await?;
            self.capture_preparation = None;
            return Err(error);
        }
        // Checkpoint success confirms the artifact, not completion of the backend's
        // asynchronous exit notification. Idempotent remove is the stop barrier.
        // Source deletion must be confirmed before releasing local resources.
        if let Err(error) = self.services.runtime.remove(&self.record.runtime.id).await {
            self.transition(Event::Fail)?;
            self.sync().await?;
            return Err(error);
        }
        if let Some(control) = &self.services.runtime_control {
            control.retire(&self.record);
        }
        self.record_stopped_runtime();
        // The stop barrier has completed; pre-pause usage is no longer current.
        self.services.metrics.remove(&self.metrics_registration);
        self.recovery_files = None;
        self.execution_files.clear();
        self.record.execution_artifacts.clear();
        self.release_capacity()?;
        self.record.runtime.ip = None;
        let artifact = match services.store.publish_with_policy(&staged, storage).await {
            Ok(artifact) => artifact,
            Err(error) => {
                // Staging contains the complete local recovery point, even if upload
                // failed. Restore it with a fresh execution identity before reporting failure.
                let artifact = match services.store.retain_staged(&staged).await {
                    Ok(artifact) => artifact,
                    Err(local_error) => {
                        self.transition(Event::Fail)?;
                        self.sync().await?;
                        return Err(local_error);
                    }
                };
                if let Some(old) = self.record.checkpoint.take() {
                    self.obsolete_checkpoints.push(old.artifact);
                }
                self.record.snapshot_source = None;
                self.record.checkpoint = Some(RestorePoint {
                    origin: None,
                    id: request.operation_id.clone(),
                    artifact,
                    expires_at_unix_seconds: expires_at,
                    source_runtime_id: self.record.runtime.id.clone(),
                });
                self.transition(Event::Checkpointed)?;
                let artifact = self
                    .record
                    .checkpoint
                    .as_ref()
                    .ok_or(Error::Conflict)?
                    .artifact
                    .clone();
                let path = services.store.prepare_restore(&artifact).await?;
                let rollback = self.restore_execution(&path).await;
                if self.held
                    && (rollback.is_err()
                        || self
                            .services
                            .runtime
                            .retains_checkpoint_files(&self.record.spec.runtime_class))
                {
                    self.hold_artifact(artifact.clone(), path);
                }
                if rollback.is_err() && self.record.state != EnvironmentState::Failed {
                    self.transition(Event::Fail)?;
                }
                self.sync().await?;
                rollback?;
                return Err(error);
            }
        };
        if let Some(previous) = self.record.checkpoint.take() {
            self.obsolete_checkpoints.push(previous.artifact);
        }
        self.record.snapshot_source = None;
        self.record.checkpoint = Some(RestorePoint {
            origin: None,
            id: request.operation_id.clone(),
            artifact,
            // Retention starts after the storage publication, including remote upload.
            expires_at_unix_seconds: request.retention.expires_at(now()?)?,
            source_runtime_id: self.record.runtime.id.clone(),
        });
        self.transition(Event::Checkpointed)?;
        self.completed(
            request.operation_id,
            request.expected_revision,
            LifecycleKind::Pause,
        );
        self.sync().await
    }
    async fn restore_execution(&mut self, path: &Path) -> Result<()> {
        let previous_runtime_id = self.record.runtime.id.clone();
        let next_revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        let runtime_id = format!(
            "{}-{}-r{}",
            self.record.spec.id, self.record.assignment.generation, next_revision
        );
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .reserve(&runtime_id, &self.record.spec, &self.record.assignment)?;
        self.record.runtime.id = runtime_id;
        self.held = true;
        self.record.resources_held = true;
        self.transition(Event::Resume)?;
        self.record.failure = None;
        let initialization_timeout = if self.record.spec.execution.init_timeout_seconds > 0 {
            Duration::from_secs(self.record.spec.execution.init_timeout_seconds)
        } else {
            self.services.operation_timeout
        };
        let restore_started = Instant::now();
        let mut execution_admitted = false;
        let attempt = tokio::time::timeout(initialization_timeout, async {
            self.services.readiness.expect_runtime(&self.record)?;
            let execution = self
                .services
                .sink
                .prepare_execution(&self.record, &previous_runtime_id)
                .await?;
            execution_admitted = true;
            let runtime_started = Instant::now();
            self.record.runtime.ip = Some(
                self.services
                    .runtime
                    .restore_from(
                        &execution,
                        &self.record.runtime.id,
                        self.record.assignment.generation,
                        &self.record.assignment.devices,
                        path,
                        self.record
                            .checkpoint
                            .as_ref()
                            .or(self.record.snapshot_source.as_ref())
                            .and_then(|cp| cp.origin.as_ref()),
                    )
                    .await?,
            );
            let runtime_ms = runtime_started.elapsed().as_secs_f64() * 1_000.0;
            let ready_started = Instant::now();
            self.services.readiness.wait_ready(&self.record).await?;
            let readiness_ms = ready_started.elapsed().as_secs_f64() * 1_000.0;
            let route_started = Instant::now();
            self.services.routes.activate(&self.record).await?;
            adx_observability::info!(
                event = "environment_restore_execution_stages",
                environment_id = %self.record.spec.id, runtime_id = %self.record.runtime.id,
                runtime_ms, readiness_ms,
                route_activate_ms = route_started.elapsed().as_secs_f64() * 1_000.0,
                total_ms = restore_started.elapsed().as_secs_f64() * 1_000.0,
                "environment restore execution stages completed"
            );
            Ok(())
        })
        .await
        .unwrap_or_else(|_| {
            Err(Error::Unavailable(
                "restore initialization timed out".into(),
            ))
        });
        if let Err(error) = attempt {
            if let Error::ProbeFailed(failure) = &error {
                self.record.failure = Some(failure.clone().into());
            }
            match self.cleanup().await {
                Ok(()) => {
                    self.record.runtime.ip = None;
                    if !execution_admitted
                        && matches!(error, Error::VolumeBusy(_) | Error::NoCapacity)
                    {
                        // Admission refused before granting this attempt. Keep
                        // the previous identity for the next Paused retry.
                        self.record.runtime.id = previous_runtime_id;
                    }
                    self.transition(Event::Rollback)?;
                }
                Err(cleanup) => {
                    self.transition(Event::Fail)?;
                    return Err(Error::Unavailable(format!(
                        "restore failed: {error}; cleanup failed: {cleanup}"
                    )));
                }
            }
            return Err(error);
        }
        self.transition(Event::Ready)?;
        Ok(())
    }
    pub(super) async fn recover(&mut self, mut request: ResumeRequest) -> Result<OperationResult> {
        if matches!(
            self.record.state,
            EnvironmentState::Failed | EnvironmentState::Deleted
        ) {
            return self.replay_or_sync().await;
        }
        // A Coordinator retry can carry a newer published Paused progress
        // revision. This generation's admitted operation keeps its original
        // identity, including while its input hold or Running result awaits ack.
        // Explicit resume still validates the caller's revision unchanged.
        if let Some(operation) = self
            .capture_preparation
            .as_ref()
            .map(|(operation, _)| operation)
            .or(self.record.last_operation.as_ref())
            .filter(|operation| {
                operation.id == request.operation_id && operation.kind == LifecycleKind::Resume
            })
        {
            if request.expected_revision != operation.expected_revision {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "checkpoint recovery operation reused",
                        "environment_id": self.record.spec.id,
                        "operation_id": request.operation_id,
                        "progress_revision": request.expected_revision,
                        "operation_expected_revision": operation.expected_revision,
                    })
                );
            }
            request.expected_revision = operation.expected_revision;
        }
        let result = self.resume(request).await;
        if self.capture_preparation.is_some() || matches!(result, Err(Error::NoCapacity)) {
            return result;
        }
        if let Err(error) = &result {
            // Paused plus no local reservation proves this attempt did not leave
            // an execution. A Running result whose publication failed is retried
            // through the operation record, never by starting another runtime.
            if self.record.state == EnvironmentState::Paused
                && !self.held
                && !self.record.resources_held
            {
                eprintln!("checkpoint recovery failed: {error}");
                self.transition(Event::Fail)?;
                return self.sync().await;
            }
        }
        result
    }
    pub(super) async fn resume(&mut self, request: ResumeRequest) -> Result<OperationResult> {
        if self.replay_operation(
            &request.operation_id,
            request.expected_revision,
            LifecycleKind::Resume,
        )? {
            return self.replay_or_sync().await;
        }
        if self.record.state != EnvironmentState::Paused {
            return Err(Error::Conflict);
        }
        let cp = self
            .record
            .checkpoint
            .clone()
            .or_else(|| self.record.snapshot_source.clone())
            .ok_or(Error::Conflict)?;
        if cp.expired(now()?) {
            return Err(Error::Invalid("checkpoint expired".into()));
        }
        let services = self.services.checkpoint.clone().ok_or(Error::Conflict)?;
        let resume_started = Instant::now();
        let prepare_started = Instant::now();
        let path = services.store.prepare_restore(&cp.artifact).await?;
        let prepare_restore_ms = prepare_started.elapsed().as_secs_f64() * 1_000.0;
        let input_started = Instant::now();
        let already_held = self.record.execution_artifacts.contains(&cp.artifact);
        if !already_held {
            self.capture_preparation = Some((
                CompletedOperation {
                    id: request.operation_id.clone(),
                    expected_revision: request.expected_revision,
                    kind: LifecycleKind::Resume,
                },
                path.to_path_buf(),
            ));
            self.record.execution_artifacts.push(cp.artifact.clone());
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        }
        if self.capture_preparation.is_some()
            && self.sync().await?.durability != Durability::Published
        {
            return Err(Error::Unavailable(
                "restore input publication pending".into(),
            ));
        }
        self.capture_preparation = None;
        let input_commit_ms = input_started.elapsed().as_secs_f64() * 1_000.0;
        let execution_started = Instant::now();
        let restored = self.restore_execution(&path).await;
        let restore_execution_ms = execution_started.elapsed().as_secs_f64() * 1_000.0;
        if restored == Err(Error::NoCapacity) && !already_held {
            self.record
                .execution_artifacts
                .retain(|a| a != &cp.artifact);
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
        }
        if self.held
            && (restored.is_err()
                || self
                    .services
                    .runtime
                    .retains_checkpoint_files(&self.record.spec.runtime_class))
        {
            self.hold_artifact(cp.artifact.clone(), path);
        }
        if let Err(error) = restored {
            if let Err(commit) = self.sync().await {
                return Err(Error::Unavailable(format!(
                    "{error}; result commit failed: {commit}"
                )));
            }
            return Err(error);
        }
        if !already_held
            && !self
                .services
                .runtime
                .retains_checkpoint_files(&self.record.spec.runtime_class)
        {
            self.record
                .execution_artifacts
                .retain(|a| a != &cp.artifact);
        }
        // Keep the artifact through commit: a lost result must never leave Redis
        // pointing at a deleted recovery point. Expiry/delete performs later cleanup.
        self.completed(
            request.operation_id,
            request.expected_revision,
            LifecycleKind::Resume,
        );
        let commit_started = Instant::now();
        let result = self.sync().await;
        adx_observability::info!(
            event = "environment_resume_stages",
            environment_id = %self.record.spec.id, runtime_id = %self.record.runtime.id,
            prepare_restore_ms, input_commit_ms, restore_execution_ms,
            result_commit_ms = commit_started.elapsed().as_secs_f64() * 1_000.0,
            total_ms = resume_started.elapsed().as_secs_f64() * 1_000.0,
            published = result.as_ref().is_ok_and(|result| result.durability == Durability::Published),
            "environment resume stages completed"
        );
        result
    }
}
