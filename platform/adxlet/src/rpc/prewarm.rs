use super::*;
use adx_core::prewarm::{ArtifactCommand, ArtifactObservation};

#[tonic::async_trait]
impl pb::preparation_node_service_server::PreparationNodeService for NodeRpc {
    async fn artifact(
        &self,
        request: Request<pb::ArtifactRequest>,
    ) -> std::result::Result<Response<pb::ArtifactResponse>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator required"));
        }
        let request = request.into_inner();
        if request.node_session_id.is_empty() || request.node_session_id != self.session_id {
            return Err(Status::failed_precondition("node session changed"));
        }
        if request.command_json.len() > 65_536 {
            return Err(Status::invalid_argument("artifact request too large"));
        }
        let command: ArtifactCommand = serde_json::from_slice(&request.command_json)
            .map_err(|_| Status::invalid_argument("invalid artifact command"))?;
        let cache = self.artifact_cache.as_ref().ok_or_else(|| {
            Status::unimplemented("immutable EROFS preparation is not configured")
        })?;
        let observation = match command {
            ArtifactCommand::Qualify { profile } => cache.qualify(&profile).await,
            ArtifactCommand::Prepare {
                id,
                profile,
                max_bytes,
                expected_recipe_digest,
            } => {
                if self.manager.is_draining() {
                    return Err(Status::unavailable("node draining"));
                }
                if cache.qualify(&profile).await.map_err(status)?.recipe_digest
                    != expected_recipe_digest
                {
                    return Err(Status::failed_precondition(
                        "artifact recipe differs from the qualified pool",
                    ));
                }
                cache.prepare(&id, &profile, max_bytes).await
            }
            ArtifactCommand::Observe { id } => cache.observe(&id).await,
            ArtifactCommand::Destroy { id } => {
                cache.destroy(&id).await.map(|()| ArtifactObservation {
                    recipe_digest: String::new(),
                    size_bytes: 0,
                    ready: false,
                    in_use: false,
                })
            }
        }
        .map_err(status)?;
        Ok(Response::new(pb::ArtifactResponse {
            observation_json: serde_json::to_vec(&observation)
                .map_err(|_| Status::internal("artifact response serialization failed"))?,
        }))
    }
}

impl NodeRpc {
    #[allow(clippy::result_large_err)] // Tonic status is the shared RPC boundary error.
    pub(super) fn environment_entry_lock(
        &self,
        id: &str,
    ) -> std::result::Result<Arc<tokio::sync::Mutex<()>>, Status> {
        let mut locks = self
            .entry_locks
            .lock()
            .map_err(|_| Status::unavailable("entry locks unavailable"))?;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(id).and_then(std::sync::Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(id.into(), Arc::downgrade(&lock));
        Ok(lock)
    }

    pub(super) async fn allow_checkpoint(
        &self,
        assignment: Option<&pb::Assignment>,
    ) -> std::result::Result<(), Status> {
        if let (Some(cache), Some(owner)) = (&self.artifact_cache, assignment) {
            if cache
                .holds_environment(&owner.environment_id)
                .await
                .map_err(|_| {
                    Status::unavailable("artifact checkpoint references are unconfirmed")
                })?
            {
                return Err(Status::unimplemented("prepared EROFS executions cannot checkpoint until artifact references are supported"));
            }
        }
        Ok(())
    }
    pub(super) async fn pin_artifact(
        &self,
        id: &str,
        spec: &adx_core::EnvironmentSpec,
        assignment: &adx_core::Assignment,
    ) -> std::result::Result<(), Status> {
        if id.is_empty() {
            return Ok(());
        }
        let cache = self
            .artifact_cache
            .as_ref()
            .ok_or_else(|| Status::unimplemented("artifact cache is not configured"))?;
        let identity = adx_core::runtime::RuntimeIdentity {
            environment_id: spec.id.clone(),
            runtime_id: format!("{}-{}", spec.id, assignment.generation),
            ownership_generation: assignment.generation,
        };
        match cache.pin(id, &spec.image, &spec.tenant_id, &identity).await {
            Ok(()) => Ok(()),
            Err(Error::NotFound) => match cache.prepared_input(&identity.runtime_id).await {
                Ok(None) => {
                    adx_observability::info!(event="prewarm_artifact_miss",environment_id=%spec.id,generation=assignment.generation,"confirmed artifact absence; cold start retains this assignment");
                    Ok(())
                }
                _ => Err(Status::unavailable(
                    "artifact lease inspection is unconfirmed; query the same Environment",
                )),
            },
            Err(error) => Err(status(error)),
        }
    }
}
