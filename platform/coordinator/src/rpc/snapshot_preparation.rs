//! Fixed image-to-snapshot preparation using existing Environment lifecycle operations.
use super::*;
use adx_core::{
    snapshot_preparation::{PreparationState, SnapshotPreparation, SnapshotTemplate},
    snapshots::{snapshot_id, Snapshot, SnapshotKind, SnapshotState},
};
use adx_observability::trace;
use adx_protocol::control::snapshot_preparation_service_server::SnapshotPreparationService;
use futures_util::{stream, StreamExt};

fn preparation_phase(state: PreparationState) -> pb::SnapshotPreparationPhase {
    match state {
        PreparationState::Registered => pb::SnapshotPreparationPhase::Registered,
        PreparationState::Creating => pb::SnapshotPreparationPhase::Creating,
        PreparationState::Snapshotting => pb::SnapshotPreparationPhase::Snapshotting,
        PreparationState::Cleaning => pb::SnapshotPreparationPhase::Cleaning,
        PreparationState::Ready => pb::SnapshotPreparationPhase::Ready,
        PreparationState::Failed => pb::SnapshotPreparationPhase::Failed,
    }
}

fn view(
    template: &SnapshotTemplate,
    preparation: &SnapshotPreparation,
    snapshot: Option<&Snapshot>,
) -> pb::SnapshotPreparationView {
    let spec = preparation
        .spec
        .as_ref()
        .or_else(|| snapshot.map(|snapshot| &snapshot.template));
    pb::SnapshotPreparationView {
        template_id: template.id.clone(),
        name: template.name.clone(),
        preparation_id: preparation.id.clone(),
        resources: Some(
            snapshot
                .map_or(preparation.resources, |snapshot| {
                    snapshot.template.resources
                })
                .into(),
        ),
        phase: preparation_phase(preparation.state) as i32,
        snapshot_available: snapshot.is_some_and(|snapshot| snapshot.state == SnapshotState::Ready),
        snapshot_id: preparation.snapshot_id.clone().unwrap_or_default(),
        error: preparation.error.clone().unwrap_or_default(),
        created_at_unix_ms: template.created_at_unix_ms,
        template_updated_at_unix_ms: template.updated_at_unix_ms,
        preparation_created_at_unix_ms: preparation.created_at_unix_ms,
        preparation_updated_at_unix_ms: preparation.updated_at_unix_ms,
        last_spawned_at_unix_ms: template
            .usage
            .as_ref()
            .and_then(|usage| usage.last_spawned_at_unix_ms),
        spawn_count: template.usage.as_ref().map(|usage| usage.spawn_count),
        image: spec.map(|spec| spec.image.clone()).unwrap_or_default(),
        envd_version: spec
            .and_then(|spec| spec.envd.as_ref())
            .map(|envd| envd.version.clone())
            .unwrap_or_default(),
    }
}

async fn preparation_snapshot(
    session: &Session,
    preparation: &SnapshotPreparation,
) -> Result<Option<Snapshot>> {
    let Some(id) = preparation.snapshot_id.as_deref() else {
        return Ok(None);
    };
    match session.get_snapshot(id).await {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(Error::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

fn preparation<'a>(template: &'a SnapshotTemplate, id: &str) -> Result<&'a SnapshotPreparation> {
    if template.preparation.id != id {
        return Err(Error::NotFound);
    }
    Ok(&template.preparation)
}

fn preparation_mut<'a>(
    template: &'a mut SnapshotTemplate,
    id: &str,
) -> Result<&'a mut SnapshotPreparation> {
    if template.preparation.id != id {
        return Err(Error::NotFound);
    }
    Ok(&mut template.preparation)
}

async fn successful_view(
    session: &Session,
    template: &SnapshotTemplate,
) -> Result<pb::SnapshotPreparationView> {
    let completed = template
        .latest_successful
        .as_ref()
        .ok_or_else(|| Error::Invalid("snapshot template has no ready preparation".into()))?;
    let snapshot =
        session
            .get_snapshot(&completed.snapshot_id)
            .await
            .map_err(|error| match error {
                Error::NotFound => {
                    Error::Invalid("registered template snapshot is not available".into())
                }
                error => error,
            })?;
    if snapshot.state != SnapshotState::Ready
        || snapshot.kind != SnapshotKind::Golden
        || snapshot.template.tenant_id != template.tenant_id
    {
        return Err(Error::Invalid(
            "registered template snapshot is not available".into(),
        ));
    }
    let build = SnapshotPreparation {
        id: completed.preparation_id.clone(),
        created_at_unix_ms: completed.created_at_unix_ms,
        updated_at_unix_ms: completed.updated_at_unix_ms,
        resources: snapshot.template.resources,
        state: PreparationState::Ready,
        spec: None,
        snapshot_id: Some(snapshot.id.clone()),
        expected_revision: None,
        error: None,
        cleanup_pending: false,
    };
    Ok(view(template, &build, Some(&snapshot)))
}

fn validate_preparation_snapshot(
    snapshot: &Snapshot,
    spec: &EnvironmentSpec,
    id: &str,
    name: &str,
) -> Result<()> {
    snapshot.validate()?;
    snapshot
        .origin()
        .map_err(|_| Error::Invalid("invalid snapshot preparation capture origin".into()))?;
    if snapshot.id != id
        || snapshot.kind != SnapshotKind::Golden
        || snapshot.state != SnapshotState::Ready
        || snapshot.names.len() != 1
        || snapshot.names.first().map(String::as_str) != Some(name)
        || !snapshot.matches_template(spec)
    {
        return Err(Error::Invalid(
            "snapshot preparation capture does not match its source".into(),
        ));
    }
    Ok(())
}

fn validate_preparation_capture(
    result: &pb::CreateSnapshotResponse,
    spec: &EnvironmentSpec,
    id: &str,
    name: &str,
    assignment: &adx_core::Assignment,
) -> Result<()> {
    let uncertain =
        || Error::Unavailable("snapshot preparation acknowledgement is incomplete".into());
    let snapshot: Snapshot = result
        .snapshot
        .clone()
        .ok_or_else(uncertain)?
        .try_into()
        .map_err(|_| uncertain())?;
    validate_preparation_snapshot(&snapshot, spec, id, name).map_err(|_| uncertain())?;
    let environment = result.environment.as_ref().ok_or_else(uncertain)?;
    let record: EnvironmentRecord = environment
        .record
        .clone()
        .ok_or_else(uncertain)?
        .try_into()
        .map_err(|_| uncertain())?;
    if environment.durability != pb::Durability::Published as i32
        || record.state != EnvironmentState::Running
        || record.assignment != *assignment
        || record.runtime.id != snapshot.source_runtime_id
        || record.assignment.node_id != snapshot.source_node_id
        || !snapshot.matches_template(&record.spec)
    {
        return Err(uncertain());
    }
    Ok(())
}

// Pending seed records may still carry the original, unresolved input. Captures
// use Snapshot::matches_template and always require fixed Volume identities.
fn preparation_seed_matches(expected: &EnvironmentSpec, actual: &EnvironmentSpec) -> bool {
    expected == actual || expected.matches_fixed_spec(actual)
}

fn retry_preparation_error(error: &Error) -> bool {
    match error {
        Error::Invalid(_) | Error::UnsupportedCapability(_) | Error::ProbeFailed(_) => false,
        Error::BackendFailure { code, .. } => matches!(
            code,
            adx_core::BackendFailureCode::Cancelled
                | adx_core::BackendFailureCode::Unknown
                | adx_core::BackendFailureCode::DeadlineExceeded
                | adx_core::BackendFailureCode::Unavailable
                | adx_core::BackendFailureCode::ResourceExhausted
                | adx_core::BackendFailureCode::Aborted
        ),
        _ => true,
    }
}

impl CoordinatorRpc {
    async fn preparation_session(&self) -> Result<Session> {
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await?;
        state.healthy()?;
        Ok(state.session.clone())
    }

    #[allow(clippy::result_large_err)] // Preserve tonic Status at the RPC authentication boundary.
    fn preparation_caller<T>(&self, request: &Request<T>) -> std::result::Result<(), Status> {
        if self.0.peers.authenticate(request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        Ok(())
    }

    async fn preparation_node(
        &self,
        environment: &str,
    ) -> Result<(
        StoredEnvironment,
        pb::node_service_client::NodeServiceClient<adx_transport::rpc::RpcChannel>,
        String,
    )> {
        let session = self.preparation_session().await?;
        let stored = session.get(environment).await?;
        let (address, session_id) = {
            let state = self.0.state.lock().await;
            state.healthy()?;
            let node = state
                .nodes
                .get(&stored.assignment.node_id)
                .ok_or_else(|| Error::Unavailable("preparation owner unavailable".into()))?;
            let live = state
                .live
                .get(&stored.assignment.node_id)
                .ok_or_else(|| Error::Unavailable("preparation owner unavailable".into()))?;
            state
                .live_claimant(
                    &stored.assignment.node_id,
                    &live.session,
                    self.0.heartbeat_timeout,
                )
                .map_err(|_| Error::Unavailable("preparation owner is reconciling".into()))?;
            if live.report.reconciling || stored.invalidated {
                return Err(Error::Unavailable(
                    "preparation owner is reconciling".into(),
                ));
            }
            (node.address.clone(), live.session.clone())
        };
        let endpoint = self
            .0
            .node_tls
            .endpoint(&address)
            .map_err(|_| Error::Unavailable("preparation owner endpoint unavailable".into()))?
            .connect_timeout(self.0.timeout.min(Duration::from_secs(10)))
            .timeout(Duration::from_secs(600));
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| Error::Unavailable("preparation owner connection unavailable".into()))?;
        Ok((
            stored,
            pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel)),
            session_id,
        ))
    }

    async fn preparation_delete_seed(&self, spec: &EnvironmentSpec) -> Result<()> {
        let (stored, mut node, node_session_id) = match self.preparation_node(&spec.id).await {
            Ok(value) => value,
            Err(Error::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if !preparation_seed_matches(spec, &stored.spec) {
            return Err(Error::Conflict);
        }
        let record = stored
            .result
            .as_ref()
            .ok_or_else(|| Error::Unavailable("preparation seed creation is unconfirmed".into()))?;
        if record.state == EnvironmentState::Deleted {
            return Ok(());
        }
        let result = node
            .delete_environment(trace::inject(pb::DeleteEnvironmentRequest {
                assignment: Some(stored.assignment.clone().try_into()?),
                caller: Some(pb::CallerContext {
                    tenant_id: spec.tenant_id.clone(),
                    administrator: false,
                }),
                node_session_id: node_session_id.clone(),
                expected_revision: Some(record.revision),
                ..Default::default()
            }))
            .await
            .map_err(adx_protocol::dependency_status)?
            .into_inner();
        let deleted: EnvironmentRecord = result
            .record
            .ok_or_else(|| {
                Error::Unavailable("preparation cleanup acknowledgement is incomplete".into())
            })?
            .try_into()
            .map_err(|_| {
                Error::Unavailable("preparation cleanup acknowledgement is invalid".into())
            })?;
        if deleted.state != EnvironmentState::Deleted
            || deleted.resources_held
            || deleted.assignment != stored.assignment
            || !preparation_seed_matches(spec, &deleted.spec)
            || result.durability != pb::Durability::Published as i32
        {
            return Err(Error::Unavailable(
                "preparation cleanup has not been published".into(),
            ));
        }
        self.commit(deleted, node_session_id).await?;
        Ok(())
    }

    async fn preparation_capture(
        &self,
        session: &Session,
        template: &SnapshotTemplate,
        build: &SnapshotPreparation,
        spec: &EnvironmentSpec,
    ) -> Result<String> {
        let operation_id = format!("snapshot-{}", build.id);
        let snapshot_id = snapshot_id(&template.tenant_id, &spec.id, &operation_id)?;
        match session.get_snapshot(&snapshot_id).await {
            Ok(snapshot) => {
                validate_preparation_snapshot(&snapshot, spec, &snapshot_id, &template.name)?;
                return Ok(snapshot_id);
            }
            Err(Error::NotFound) => (),
            Err(error) => return Err(error),
        }
        let expected_revision = build.expected_revision.ok_or_else(|| {
            Error::Invalid("snapshot preparation source revision is required".into())
        })?;
        let (stored, mut node, node_session_id) = self.preparation_node(&spec.id).await?;
        if !preparation_seed_matches(spec, &stored.spec) {
            return Err(Error::Conflict);
        }
        let captured = node
            .create_snapshot(trace::inject(pb::CreateSnapshotRequest {
                assignment: Some(stored.assignment.clone().try_into()?),
                caller: Some(pb::CallerContext {
                    tenant_id: template.tenant_id.clone(),
                    administrator: false,
                }),
                node_session_id,
                operation_id,
                expected_revision,
                names: vec![template.name.clone()],
                timeout_seconds: 300,
                kind: pb::SnapshotKind::Golden as i32,
            }))
            .await;
        match captured {
            Ok(result) => validate_preparation_capture(
                result.get_ref(),
                spec,
                &snapshot_id,
                &template.name,
                &stored.assignment,
            )?,
            Err(error) => {
                // A returned error does not erase an already published catalog result.
                // In particular, a transport wait expiring must never delete its seed.
                match session.get_snapshot(&snapshot_id).await {
                    Ok(snapshot) => {
                        validate_preparation_snapshot(
                            &snapshot,
                            spec,
                            &snapshot_id,
                            &template.name,
                        )?;
                        return Ok(snapshot_id);
                    }
                    Err(Error::NotFound) => return Err(adx_protocol::dependency_status(error)),
                    Err(error) => return Err(error),
                }
            }
        }
        let snapshot = session
            .get_snapshot(&snapshot_id)
            .await
            .map_err(|error| match error {
                Error::NotFound => {
                    Error::Unavailable("snapshot catalog publication is unconfirmed".into())
                }
                error => error,
            })?;
        validate_preparation_snapshot(&snapshot, spec, &snapshot_id, &template.name)?;
        Ok(snapshot_id)
    }

    async fn drive_preparation(
        &self,
        template_id: String,
        build_id: String,
        tenant_id: String,
    ) -> Result<()> {
        let Ok(_operation) = self
            .create_lock(format!("snapshot-preparation:{build_id}"))
            .try_lock_owned()
        else {
            return Ok(());
        };
        let session = self.preparation_session().await?;
        let template = session
            .get_snapshot_template(&tenant_id, &template_id)
            .await?;
        let build = preparation(&template, &build_id)?.clone();
        if build.state == PreparationState::Registered
            || (build.state == PreparationState::Ready && !template.deleted)
        {
            return Ok(());
        }
        let Some(spec) = build.spec.clone() else {
            if matches!(
                build.state,
                PreparationState::Creating
                    | PreparationState::Snapshotting
                    | PreparationState::Cleaning
            ) {
                return self
                    .fail_preparation(
                        &session,
                        &template_id,
                        &build_id,
                        "snapshot preparation source spec is required",
                    )
                    .await;
            }
            return Ok(());
        };
        // A tombstone must settle a possibly accepted capture before deleting its
        // input. The Golden object remains independently usable through native APIs.
        if (template.deleted || build.state == PreparationState::Failed)
            && build.state != PreparationState::Snapshotting
        {
            if build.cleanup_pending {
                self.preparation_delete_seed(&spec).await?;
                session
                    .update_snapshot_template(&template_id, |record| {
                        let current = preparation_mut(record, &build_id)?;
                        current.cleanup_pending = false;
                        if current.state != PreparationState::Ready {
                            current.state = PreparationState::Failed;
                        }
                        current.spec = None;
                        Ok(())
                    })
                    .await?;
            }
            return Ok(());
        }
        // Registration time is not the start time. Individual lifecycle RPCs are
        // bounded, while uncertain accepted work keeps its durable fixed identity.
        let outcome: Result<()> = async {
            if build.state == PreparationState::Creating {
                let existing = match session.get(&spec.id).await {
                    Ok(stored) => Some(stored),
                    Err(Error::NotFound) => None,
                    Err(error) => return Err(error),
                };
                let confirmed = existing.filter(|stored| stored.result.is_some());
                let created = if let Some(stored) = confirmed {
                    if stored.invalidated || stored.recovery.as_ref().is_some_and(|r| r.pending) {
                        return Err(Error::Unavailable(
                            "preparation seed reconciliation pending".into(),
                        ));
                    }
                    if !preparation_seed_matches(&spec, &stored.spec) {
                        return Err(Error::Invalid(
                            "preparation seed specification changed".into(),
                        ));
                    }
                    stored.result.ok_or(Error::Conflict)?
                } else {
                    let created =
                        Box::pin(self.create_from_request(pb::CreateEnvironmentRequest {
                            spec: Some(spec.clone().into()),
                            caller: Some(pb::CallerContext {
                                tenant_id: tenant_id.clone(),
                                administrator: false,
                            }),
                            request_id: format!("preparation-create-{build_id}"),
                            schedule_timeout_seconds: 300,
                            create_timeout_seconds: 600,
                            ..Default::default()
                        }))
                        .await;
                    let response = match created {
                        Ok(response) => response.into_inner(),
                        Err(error) if confirmed_create_failure(&error) => {
                            // The native create boundary has already confirmed failure.
                            // Preserve that decision before generic status conversion
                            // can turn a missing image into a retryable NotFound.
                            self.fail_preparation(
                                &session,
                                &template_id,
                                &build_id,
                                error.message(),
                            )
                            .await?;
                            return Ok(());
                        }
                        Err(error) => return Err(adx_protocol::dependency_status(error)),
                    };
                    if response.durability != pb::Durability::Published as i32 {
                        return Err(Error::Unavailable(
                            "preparation create publication is unconfirmed".into(),
                        ));
                    }
                    response
                        .record
                        .ok_or_else(|| {
                            Error::Unavailable(
                                "preparation create acknowledgement is incomplete".into(),
                            )
                        })?
                        .try_into()
                        .map_err(|_| {
                            Error::Unavailable(
                                "preparation create acknowledgement is invalid".into(),
                            )
                        })?
                };
                if !preparation_seed_matches(&spec, &created.spec) {
                    return Err(Error::Unavailable(
                        "preparation create identity mismatch".into(),
                    ));
                }
                if matches!(
                    created.state,
                    EnvironmentState::Failed | EnvironmentState::Deleted
                ) {
                    return Err(Error::Invalid(
                        "preparation seed did not become Running".into(),
                    ));
                }
                if created.state != EnvironmentState::Running {
                    return Err(Error::Unavailable("preparation seed is not Running".into()));
                }
                session
                    .update_snapshot_template(&template_id, |record| {
                        let current = preparation_mut(record, &build_id)?;
                        if current.state != PreparationState::Creating {
                            return Err(Error::Conflict);
                        }
                        current.state = PreparationState::Snapshotting;
                        current.expected_revision = Some(created.revision);
                        Ok(())
                    })
                    .await?;
            }
            let current_template = session
                .get_snapshot_template(&tenant_id, &template_id)
                .await?;
            let current = preparation(&current_template, &build_id)?.clone();
            if current.state == PreparationState::Snapshotting {
                let snapshot_id = self
                    .preparation_capture(&session, &current_template, &current, &spec)
                    .await?;
                session
                    .update_snapshot_template(&template_id, |record| {
                        let current = preparation_mut(record, &build_id)?;
                        if current.state != PreparationState::Snapshotting {
                            return Err(Error::Conflict);
                        }
                        current.snapshot_id = Some(snapshot_id.clone());
                        current.state = PreparationState::Cleaning;
                        Ok(())
                    })
                    .await?;
            } else if current.state != PreparationState::Cleaning {
                return Err(Error::Conflict);
            }
            // Re-read catalog authority on Cleaning retries, before releasing the seed.
            let saved = session
                .get_snapshot_template(&tenant_id, &template_id)
                .await?;
            let current = preparation(&saved, &build_id)?;
            let snapshot_id = current.snapshot_id.as_deref().ok_or_else(|| {
                Error::Invalid("snapshot preparation cleanup requires a capture identity".into())
            })?;
            let snapshot = session.get_snapshot(snapshot_id).await?;
            validate_preparation_snapshot(&snapshot, &spec, snapshot_id, &saved.name)?;
            self.preparation_delete_seed(&spec).await?;
            session
                .update_snapshot_template(&template_id, |record| {
                    let current = preparation_mut(record, &build_id)?;
                    if current.state != PreparationState::Cleaning {
                        return Err(Error::Conflict);
                    }
                    current.cleanup_pending = false;
                    current.state = PreparationState::Ready;
                    current.error = None;
                    current.spec = None;
                    Ok(())
                })
                .await?;
            Ok(())
        }
        .await;
        if let Err(error) = outcome {
            if retry_preparation_error(&error) {
                return Err(error);
            }
            self.fail_preparation(&session, &template_id, &build_id, &error.to_string())
                .await?;
        }
        Ok(())
    }

    async fn fail_preparation(
        &self,
        session: &Session,
        template_id: &str,
        build_id: &str,
        error: &str,
    ) -> Result<()> {
        let error: String = error.chars().take(512).collect();
        session
            .update_snapshot_template(template_id, |record| {
                let current = preparation_mut(record, build_id)?;
                if current.state == PreparationState::Ready {
                    return Err(Error::Conflict);
                }
                current.state = PreparationState::Failed;
                current.error = Some(error.clone());
                current.cleanup_pending = current.spec.is_some();
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Re-adopt known preparation identities, or clean failed seeds. Never invent replacement IDs.
    pub async fn reconcile_snapshot_preparations(&self) -> Result<usize> {
        let session = self.preparation_session().await?;
        let templates = session.retained_snapshot_templates().await?;
        let mut work = Vec::new();
        let mut first_error = None;
        for template in templates {
            if template.deleted {
                if let Err(error) = session
                    .remove_finished_snapshot_template(&template.id)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
            let build = template.preparation;
            if matches!(
                build.state,
                PreparationState::Creating
                    | PreparationState::Snapshotting
                    | PreparationState::Cleaning
            ) || build.cleanup_pending
            {
                work.push((
                    template.id.clone(),
                    build.id,
                    template.tenant_id.clone(),
                    template.deleted,
                ));
            }
        }
        // Match native Environment recovery's bounded concurrency; busy builds
        // are skipped by the shared lock rather than queuing this scan behind them.
        let results = stream::iter(work.into_iter().map(|(id, build, tenant, deleted)| {
            let session = session.clone();
            Box::pin(async move {
                Box::pin(self.drive_preparation(id.clone(), build, tenant)).await?;
                if deleted {
                    session.remove_finished_snapshot_template(&id).await?;
                }
                Ok::<_, Error>(())
            })
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
        let count = results.iter().filter(|result| result.is_ok()).count();
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(count),
        }
    }
}

#[tonic::async_trait]
impl SnapshotPreparationService for CoordinatorRpc {
    async fn register_snapshot_preparation(
        &self,
        request: Request<pb::RegisterSnapshotPreparationRequest>,
    ) -> std::result::Result<Response<pb::SnapshotPreparationView>, Status> {
        self.preparation_caller(&request)?;
        let request = request.into_inner();
        let caller = request
            .caller
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        tenant(Some(caller), &caller.tenant_id)?;
        let input = request
            .resources
            .ok_or_else(|| Status::invalid_argument("resources required"))?;
        let resources = adx_core::Resources {
            cpu_millis: input.cpu_millis,
            memory_bytes: input.memory_bytes,
            disk_bytes: input.disk_bytes,
        };
        let session = self.preparation_session().await.map_err(status)?;
        let (template, build) = session
            .register_snapshot_preparation(&caller.tenant_id, &request.name, resources)
            .await
            .map_err(status)?;
        Ok(Response::new(view(&template, &build, None)))
    }

    async fn start_snapshot_preparation(
        &self,
        request: Request<pb::StartSnapshotPreparationRequest>,
    ) -> std::result::Result<Response<pb::SnapshotPreparationView>, Status> {
        self.preparation_caller(&request)?;
        let request = request.into_inner();
        let caller = request
            .caller
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        let session = self.preparation_session().await.map_err(status)?;
        let template = session
            .get_snapshot_template(&caller.tenant_id, &request.template_id)
            .await
            .map_err(status)?;
        if template.deleted {
            return Err(Status::not_found("snapshot template not found"));
        }
        let build = preparation(&template, &request.preparation_id).map_err(status)?;
        let mut spec: EnvironmentSpec = request
            .spec
            .ok_or_else(|| Status::invalid_argument("preparation spec required"))?
            .try_into()
            .map_err(status)?;
        spec.id = format!("golden-{}", build.id);
        spec.tenant_id = caller.tenant_id.clone();
        spec.run_policy = None;
        spec.snapshot_id = None;
        spec.resources = build.resources;
        spec.validate().map_err(status)?;
        if build.state != PreparationState::Registered {
            return Err(Status::invalid_argument(
                "snapshot preparation has already started",
            ));
        }
        let updated = session
            .update_snapshot_template(&template.id, |record| {
                if record.deleted {
                    return Err(Error::Conflict);
                }
                let current = preparation_mut(record, &build.id)?;
                if current.state != PreparationState::Registered {
                    return Err(Error::Conflict);
                }
                current.state = PreparationState::Creating;
                current.spec = Some(spec.clone());
                current.cleanup_pending = true;
                Ok(())
            })
            .await
            .map_err(status)?;
        let response = view(
            &updated,
            preparation(&updated, &build.id).map_err(status)?,
            None,
        );
        let service = self.clone();
        let template_id = template.id.clone();
        let build_id = build.id.clone();
        tokio::spawn(async move {
            if let Err(error) =
                Box::pin(service.drive_preparation(template_id, build_id, caller.tenant_id)).await
            {
                adx_observability::warn!("snapshot preparation reconciliation incomplete: {error}");
            }
        });
        Ok(Response::new(response))
    }

    async fn get_snapshot_preparation(
        &self,
        request: Request<pb::GetSnapshotPreparationRequest>,
    ) -> std::result::Result<Response<pb::SnapshotPreparationView>, Status> {
        self.preparation_caller(&request)?;
        let request = request.into_inner();
        let caller = request
            .caller
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        let session = self.preparation_session().await.map_err(status)?;
        let template = session
            .get_snapshot_template(&caller.tenant_id, &request.template_id)
            .await
            .map_err(status)?;
        if template.deleted {
            return Err(Status::not_found("snapshot template not found"));
        }
        if template.preparation.id == request.preparation_id {
            let build = &template.preparation;
            let snapshot = preparation_snapshot(&session, build)
                .await
                .map_err(status)?;
            return Ok(Response::new(view(&template, build, snapshot.as_ref())));
        }
        if template
            .latest_successful
            .as_ref()
            .is_some_and(|build| build.preparation_id == request.preparation_id)
        {
            return Ok(Response::new(
                successful_view(&session, &template).await.map_err(status)?,
            ));
        }
        Err(Status::not_found("snapshot preparation not found"))
    }

    async fn resolve_snapshot_template(
        &self,
        request: Request<pb::ResolveSnapshotTemplateRequest>,
    ) -> std::result::Result<Response<pb::SnapshotPreparationView>, Status> {
        self.preparation_caller(&request)?;
        let request = request.into_inner();
        let caller = request
            .caller
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        let session = self.preparation_session().await.map_err(status)?;
        let template = session
            .get_snapshot_template(&caller.tenant_id, &request.reference)
            .await
            .map_err(status)?;
        if template.deleted {
            return Err(Status::not_found("snapshot template not found"));
        }
        if request.registration_only {
            let build = &template.preparation;
            let snapshot = preparation_snapshot(&session, build)
                .await
                .map_err(status)?;
            return Ok(Response::new(view(&template, build, snapshot.as_ref())));
        }
        if template.latest_successful.is_none() {
            return Err(Status::failed_precondition(
                "snapshot template has no ready preparation",
            ));
        }
        let result = successful_view(&session, &template)
            .await
            .map_err(|error| match error {
                Error::Invalid(message) => Status::failed_precondition(message),
                error => status(error),
            })?;
        Ok(Response::new(result))
    }

    async fn delete_snapshot_template(
        &self,
        request: Request<pb::DeleteSnapshotTemplateRequest>,
    ) -> std::result::Result<Response<pb::DeleteSnapshotTemplateResponse>, Status> {
        self.preparation_caller(&request)?;
        let request = request.into_inner();
        let caller = request
            .caller
            .ok_or_else(|| Status::unauthenticated("caller required"))?;
        let session = self.preparation_session().await.map_err(status)?;
        let template = session
            .get_snapshot_template(&caller.tenant_id, &request.template_id)
            .await
            .map_err(status)?;
        if template.deleted {
            return Err(Status::not_found("snapshot template not found"));
        }
        if request.require_unused {
            let ids: BTreeSet<&str> = template
                .preparation
                .snapshot_id
                .as_deref()
                .into_iter()
                .chain(
                    template
                        .latest_successful
                        .as_ref()
                        .map(|build| build.snapshot_id.as_str()),
                )
                .collect();
            {
                let state = self.0.state.lock().await;
                if state.environments.values().any(|stored| {
                    stored
                        .spec
                        .snapshot_id
                        .as_deref()
                        .is_some_and(|id| ids.contains(id))
                        && stored.result.as_ref().is_none_or(|record| {
                            !matches!(
                                record.state,
                                EnvironmentState::Deleted | EnvironmentState::Failed
                            )
                        })
                }) {
                    return Err(Status::failed_precondition(
                        "snapshot template is used by running or paused environments",
                    ));
                }
            }
            for id in &ids {
                match session.get_snapshot(id).await {
                    Ok(snapshot) if !snapshot.references.is_empty() => {
                        return Err(Status::failed_precondition("snapshot template is in use"));
                    }
                    Ok(_) | Err(Error::NotFound) => (),
                    Err(error) => return Err(status(error)),
                }
            }
        }
        session
            .update_snapshot_template(&template.id, |record| {
                record.deleted = true;
                Ok(())
            })
            .await
            .map_err(status)?;
        session
            .remove_finished_snapshot_template(&template.id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::DeleteSnapshotTemplateResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::{
        snapshots::{snapshot_id, Snapshot, SnapshotKind, SnapshotState},
        CheckpointArtifact,
    };

    fn source() -> EnvironmentSpec {
        serde_json::from_value(serde_json::json!({
            "id":"golden-seed", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
            "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        }))
        .unwrap()
    }
    fn capture(spec: &EnvironmentSpec, id: String) -> Snapshot {
        Snapshot::new_with_kind(
            SnapshotKind::Golden,
            id,
            vec!["base".into()],
            spec.clone(),
            "node".into(),
            format!("{}-1", spec.id),
            CheckpointArtifact {
                storage: "shared".into(),
                scope: String::new(),
                location: "artifact".into(),
                size_bytes: 1,
            },
        )
        .unwrap()
    }
    #[test]
    fn persisted_capture_must_be_ready_golden_and_match_its_fixed_source() {
        let spec = source();
        let saved = capture(&spec, "snapshot".into());
        validate_preparation_snapshot(&saved, &spec, "snapshot", "base").unwrap();
        let mut invalid = saved.clone();
        invalid.state = SnapshotState::Deleting;
        assert!(validate_preparation_snapshot(&invalid, &spec, "snapshot", "base").is_err());
        invalid = saved.clone();
        invalid.kind = SnapshotKind::Regular;
        assert!(validate_preparation_snapshot(&invalid, &spec, "snapshot", "base").is_err());
        invalid = saved.clone();
        invalid
            .template
            .env
            .insert("DIFFERENT".into(), "value".into());
        assert!(validate_preparation_snapshot(&invalid, &spec, "snapshot", "base").is_err());
        invalid = saved.clone();
        invalid.source_runtime_id = "different-1".into();
        assert!(validate_preparation_snapshot(&invalid, &spec, "snapshot", "base").is_err());
        assert!(validate_preparation_snapshot(&saved, &spec, "different", "base").is_err());
        assert!(validate_preparation_snapshot(&saved, &spec, "snapshot", "other").is_err());
    }
    #[test]
    fn capture_acknowledgement_requires_published_running_source_and_exact_assignment() {
        let spec = source();
        let saved = capture(&spec, "snapshot".into());
        let assignment = adx_core::Assignment {
            environment_id: spec.id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        let record = EnvironmentRecord {
            observation: None,
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
            resume_pending: false,
            spec: spec.clone(),
            assignment: assignment.clone(),
            state: EnvironmentState::Running,
            revision: 8,
            runtime: adx_core::Runtime {
                id: format!("{}-1", spec.id),
                ip: None,
            },
            resources_held: true,
            checkpoint: None,
            execution_artifacts: vec![saved.artifact.clone()],
            snapshot_source: None,
            last_operation: None,
        };
        let response = pb::CreateSnapshotResponse {
            snapshot: Some(saved.try_into().unwrap()),
            environment: Some(pb::EnvironmentResult {
                newly_created: false,
                record: Some(record.try_into().unwrap()),
                durability: pb::Durability::Published as i32,
                ..Default::default()
            }),
        };
        validate_preparation_capture(&response, &spec, "snapshot", "base", &assignment).unwrap();
        let mut uncertain = response.clone();
        uncertain.environment.as_mut().unwrap().durability = pb::Durability::Journaled as i32;
        let error =
            validate_preparation_capture(&uncertain, &spec, "snapshot", "base", &assignment)
                .unwrap_err();
        assert!(retry_preparation_error(&error));
        uncertain = response.clone();
        uncertain
            .environment
            .as_mut()
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .state = pb::EnvironmentState::Paused as i32;
        assert!(
            validate_preparation_capture(&uncertain, &spec, "snapshot", "base", &assignment,)
                .is_err()
        );
        let mut replaced = assignment;
        replaced.generation += 1;
        assert!(
            validate_preparation_capture(&response, &spec, "snapshot", "base", &replaced,).is_err()
        );
    }
    #[test]
    fn uncertain_results_remain_retryable_but_definitive_input_errors_fail() {
        for error in [
            Error::Unavailable("capture result unknown".into()),
            Error::Conflict,
            Error::NotFound,
            Error::DeadlineExceeded("RPC wait expired".into()),
            Error::ResponseTimeout("waiting".into()),
            Error::NoCapacity,
            Error::BackendFailure {
                code: adx_core::BackendFailureCode::Unknown,
                message: "capture result unknown".into(),
            },
        ] {
            assert!(retry_preparation_error(&error));
        }
        assert!(!retry_preparation_error(&Error::Invalid(
            "bad image".into()
        )));
        assert!(!retry_preparation_error(&Error::UnsupportedCapability(
            adx_core::UnsupportedCapability::CheckpointRestore,
        )));
    }
    #[test]
    fn family_view_preserves_real_phase_identity_and_resources() {
        let spec = source();
        let build = SnapshotPreparation {
            id: "00000000-0000-4000-8000-000000000001".into(),
            created_at_unix_ms: 20,
            updated_at_unix_ms: 30,
            resources: spec.resources,
            state: PreparationState::Failed,
            spec: Some(spec),
            snapshot_id: None,
            expected_revision: None,
            error: Some("image preparation failed".into()),
            cleanup_pending: false,
        };
        let template = SnapshotTemplate {
            id: "00000000-0000-4000-8000-000000000003".into(),
            tenant_id: "tenant".into(),
            name: "base".into(),
            created_at_unix_ms: 10,
            updated_at_unix_ms: 40,
            usage: Some(adx_core::snapshot_preparation::SnapshotTemplateUsage {
                spawn_count: 2,
                last_spawned_at_unix_ms: Some(40),
            }),
            revision: 1,
            deleted: false,
            preparation: build.clone(),
            latest_successful: None,
        };
        let result = view(&template, &build, None);
        assert_eq!(result.preparation_id, build.id);
        assert_eq!(result.phase, pb::SnapshotPreparationPhase::Failed as i32);
        assert_eq!(
            result.resources.unwrap().cpu_millis,
            build.resources.cpu_millis
        );
        assert!(!result.snapshot_available);
        assert_eq!(result.error, "image preparation failed");
        assert_eq!(result.created_at_unix_ms, 10);
        assert_eq!(result.template_updated_at_unix_ms, 40);
        assert_eq!(result.preparation_created_at_unix_ms, 20);
        assert_eq!(result.preparation_updated_at_unix_ms, 30);
        assert_eq!(result.spawn_count, Some(2));
        assert_eq!(result.last_spawned_at_unix_ms, Some(40));
    }

    async fn registered(session: &Session, name: &str) -> (SnapshotTemplate, SnapshotPreparation) {
        let spec = source();
        let (template, build) = session
            .register_snapshot_preparation("tenant", name, spec.resources)
            .await
            .unwrap();
        session
            .update_snapshot_template(&template.id, |template| {
                let current = &mut template.preparation;
                current.state = PreparationState::Snapshotting;
                current.spec = Some(spec.clone());
                current.expected_revision = Some(7);
                current.cleanup_pending = true;
                Ok(())
            })
            .await
            .unwrap();
        (template, build)
    }
    async fn rpc(session: Session) -> CoordinatorRpc {
        CoordinatorRpc::new(
            session,
            Placement::Pack,
            Peers::network(),
            RpcClient::network(Principal::Coordinator),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
    }
    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn preparation_passes_image_and_environment_to_native_creation_unchanged() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let mut spec = source();
        // The native backend owns registry access. This loopback reference must
        // reach scheduling without Coordinator connecting to a registry first.
        spec.image = format!("127.0.0.1:1/base@sha256:{}", "a".repeat(64));
        spec.env.insert("PATH".into(), "/requested/bin".into());
        spec.env.insert("CUSTOM".into(), "value=unchanged".into());
        let (template, build) = session
            .register_snapshot_preparation("tenant", "native-image", spec.resources)
            .await
            .unwrap();
        session
            .update_snapshot_template(&template.id, |template| {
                let current = &mut template.preparation;
                current.state = PreparationState::Creating;
                current.spec = Some(spec.clone());
                current.cleanup_pending = true;
                Ok(())
            })
            .await
            .unwrap();
        let service = rpc(session.clone()).await;
        let driver = service.clone();
        let template_id = template.id.clone();
        let task = tokio::spawn(async move {
            driver
                .drive_preparation(template_id, build.id, "tenant".into())
                .await
        });
        let queued = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(queued) = service.0.state.lock().await.specs.get(&spec.id).cloned() {
                    break queued;
                }
                assert!(
                    !task.is_finished(),
                    "preparation failed before native scheduling"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        task.abort();
        let _ = task.await;
        assert_eq!(queued.unwrap(), spec);
        let saved = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert_eq!(saved.preparation.spec.as_ref(), Some(&spec));
        assert_eq!(saved.preparation.state, PreparationState::Creating);
    }
    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn unknown_capture_stays_durable_then_adopts_late_catalog_publication() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let (template, build) = registered(&session, "base").await;
        let service = rpc(session.clone()).await;
        assert!(service
            .drive_preparation(template.id.clone(), build.id.clone(), "tenant".into(),)
            .await
            .is_err());
        let waiting = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert_eq!(waiting.preparation.state, PreparationState::Snapshotting);
        assert_eq!(waiting.preparation.expected_revision, Some(7));
        assert!(waiting.preparation.cleanup_pending);
        let operation = format!("snapshot-{}", build.id);
        let id = snapshot_id("tenant", &source().id, &operation).unwrap();
        session
            .publish_snapshot(capture(&source(), id.clone()))
            .await
            .unwrap();
        service
            .drive_preparation(template.id.clone(), build.id, "tenant".into())
            .await
            .unwrap();
        let ready = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert_eq!(ready.preparation.state, PreparationState::Ready);
        assert_eq!(ready.preparation.snapshot_id.as_deref(), Some(id.as_str()));
        assert!(!ready.preparation.cleanup_pending);
        assert_eq!(
            session.get_snapshot(&id).await.unwrap().state,
            SnapshotState::Ready
        );
    }
    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn tombstone_adopts_uncertain_capture_before_cleanup_and_preserves_native_golden() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let (template, build) = registered(&session, "base").await;
        session
            .update_snapshot_template(&template.id, |template| {
                template.deleted = true;
                Ok(())
            })
            .await
            .unwrap();
        let service = rpc(session.clone()).await;
        assert!(service
            .drive_preparation(template.id.clone(), build.id.clone(), "tenant".into(),)
            .await
            .is_err());
        let waiting = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert!(waiting.preparation.cleanup_pending);
        let id = snapshot_id("tenant", &source().id, &format!("snapshot-{}", build.id)).unwrap();
        session
            .publish_snapshot(capture(&source(), id.clone()))
            .await
            .unwrap();
        service
            .drive_preparation(template.id.clone(), build.id, "tenant".into())
            .await
            .unwrap();
        let deleted = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert!(deleted.deleted);
        assert!(!deleted.preparation.cleanup_pending);
        let golden = session.get_snapshot(&id).await.unwrap();
        assert_eq!(golden.kind, SnapshotKind::Golden);
        assert_eq!(golden.state, SnapshotState::Ready);
    }
    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn one_uncertain_preparation_does_not_starve_other_reconcilable_builds() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let mut builds = [
            registered(&session, "base-a").await,
            registered(&session, "base-b").await,
        ];
        builds.sort_by(|left, right| left.0.id.cmp(&right.0.id));
        let (ready_template, ready_build) = &builds[1];
        let id = snapshot_id(
            "tenant",
            &source().id,
            &format!("snapshot-{}", ready_build.id),
        )
        .unwrap();
        let mut saved = capture(&source(), id);
        saved.names = vec![ready_template.name.clone()];
        session.publish_snapshot(saved).await.unwrap();
        let service = rpc(session.clone()).await;
        assert!(service.reconcile_snapshot_preparations().await.is_err());
        let blocked = session
            .get_snapshot_template("tenant", &builds[0].0.id)
            .await
            .unwrap();
        assert_eq!(blocked.preparation.state, PreparationState::Snapshotting);
        assert!(blocked.preparation.cleanup_pending);
        let ready = session
            .get_snapshot_template("tenant", &ready_template.id)
            .await
            .unwrap();
        assert_eq!(ready.preparation.state, PreparationState::Ready);
        assert!(!ready.preparation.cleanup_pending);
    }
    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn reconciliation_skips_busy_build_and_finishes_another_capture() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let mut builds = [
            registered(&session, "busy").await,
            registered(&session, "ready").await,
        ];
        builds.sort_by(|left, right| left.0.id.cmp(&right.0.id));
        let (template, build) = &builds[1];
        let id = snapshot_id("tenant", &source().id, &format!("snapshot-{}", build.id)).unwrap();
        let mut saved = capture(&source(), id.clone());
        saved.names = vec![template.name.clone()];
        session.publish_snapshot(saved).await.unwrap();
        let service = rpc(session.clone()).await;
        let _busy = service
            .create_guard(format!("snapshot-preparation:{}", builds[0].1.id))
            .await;
        tokio::time::timeout(
            Duration::from_secs(1),
            service.reconcile_snapshot_preparations(),
        )
        .await
        .unwrap()
        .unwrap();
        let ready = session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap();
        assert_eq!(ready.preparation.state, PreparationState::Ready);
        assert!(ready.preparation.spec.is_none());
        let result = view(
            &ready,
            &ready.preparation,
            Some(&session.get_snapshot(&id).await.unwrap()),
        );
        assert_eq!(result.image, source().image);
        assert!(result.snapshot_available);
        let retained = ready.latest_successful.as_ref().unwrap();
        let successful = successful_view(&session, &ready).await.unwrap();
        assert_eq!(successful.created_at_unix_ms, ready.created_at_unix_ms);
        assert_eq!(
            successful.preparation_created_at_unix_ms,
            retained.created_at_unix_ms
        );
        assert_eq!(
            successful.preparation_updated_at_unix_ms,
            retained.updated_at_unix_ms
        );
        assert_eq!(
            successful.template_updated_at_unix_ms,
            ready.updated_at_unix_ms
        );
        let busy = session
            .get_snapshot_template("tenant", &builds[0].0.id)
            .await
            .unwrap();
        assert_eq!(busy.preparation.state, PreparationState::Snapshotting);
    }

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn deleted_active_preparations_finish_cleanup_and_release_their_records() {
        let redis = super::super::cancellation_test_redis::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        for (name, phase) in [
            ("deleted-creating", PreparationState::Creating),
            ("deleted-cleaning", PreparationState::Cleaning),
        ] {
            let (template, build) = registered(&session, name).await;
            let id =
                snapshot_id("tenant", &source().id, &format!("snapshot-{}", build.id)).unwrap();
            if phase == PreparationState::Cleaning {
                let mut saved = capture(&source(), id.clone());
                saved.names = vec![template.name.clone()];
                session.publish_snapshot(saved).await.unwrap();
            }
            session
                .update_snapshot_template(&template.id, |record| {
                    record.deleted = true;
                    let current = &mut record.preparation;
                    current.state = phase;
                    current.snapshot_id = (phase == PreparationState::Cleaning).then(|| id.clone());
                    Ok(())
                })
                .await
                .unwrap();
            let service = rpc(session.clone()).await;
            service
                .drive_preparation(template.id.clone(), build.id, "tenant".into())
                .await
                .unwrap();
            let cleaned = session
                .get_snapshot_template("tenant", &template.id)
                .await
                .unwrap();
            assert_eq!(cleaned.preparation.state, PreparationState::Failed);
            assert!(!cleaned.preparation.cleanup_pending);
            assert!(cleaned.preparation.spec.is_none());
            service.reconcile_snapshot_preparations().await.unwrap();
            assert!(matches!(
                session.get_snapshot_template("tenant", &template.id).await,
                Err(Error::NotFound)
            ));
            if phase == PreparationState::Cleaning {
                assert_eq!(
                    session.get_snapshot(&id).await.unwrap().state,
                    SnapshotState::Ready
                );
            }
        }
    }
}
