//! Thin authenticated management RPCs backed by the Platform authority.
use super::*;
use crate::storage::prewarm::{Mutation, PoolError};

#[allow(clippy::result_large_err)] // Preserve Tonic's native authentication error at the RPC boundary.
fn management_caller(caller: Option<&pb::CallerContext>) -> std::result::Result<&str, Status> {
    let caller = caller.ok_or_else(|| Status::unauthenticated("caller context required"))?;
    if caller.tenant_id.is_empty() {
        return Err(Status::unauthenticated("tenant required"));
    }
    if !caller.administrator {
        return Err(Status::permission_denied(
            "prewarm management requires administrator",
        ));
    }
    Ok(&caller.tenant_id)
}
fn pool_status(error: PoolError) -> Status {
    let (mut error, code) = match error {
        PoolError::NotFound => (
            Status::not_found("prewarm pool or operation not found"),
            "not_found",
        ),
        PoolError::PreconditionRequired => (
            Status::failed_precondition("conditional request header required"),
            "precondition_required",
        ),
        PoolError::PreconditionFailed => (
            Status::failed_precondition("prewarm revision mismatch"),
            "precondition_failed",
        ),
        PoolError::IdempotencyConflict => (
            Status::already_exists("logical request ID has different arguments"),
            "idempotency_conflict",
        ),
        PoolError::ImmutableProfile => (
            Status::already_exists("preparation profile and mode are immutable"),
            "immutable_profile",
        ),
        PoolError::Unsupported => (
            Status::unimplemented("no qualified preparation backend is installed"),
            "unsupported",
        ),
        PoolError::Control(error) => return status(error),
    };
    error.metadata_mut().insert(
        "adx-prewarm-error",
        tonic::metadata::MetadataValue::from_static(code),
    );
    error
}
#[allow(clippy::result_large_err)] // The response adapter propagates the service's native Tonic status.
fn view(
    value: &impl serde::Serialize,
    revision: u64,
) -> std::result::Result<Response<pb::PrewarmView>, Status> {
    Ok(Response::new(pb::PrewarmView {
        json: serde_json::to_vec(value)
            .map_err(|_| Status::internal("prewarm response serialization failed"))?,
        revision,
    }))
}
impl CoordinatorRpc {
    pub(super) async fn accept_preparation_context(
        &self,
        spec: &EnvironmentSpec,
        context: Option<pb::PreparationContext>,
    ) -> std::result::Result<(), Status> {
        if let Some(context) = context {
            let context: adx_core::prewarm::PreparationContext =
                context.try_into().map_err(status)?;
            context.validate(&spec.tenant_id).map_err(status)?;
            self.prewarm_session()
                .await?
                .select_preparation(
                    &spec.tenant_id,
                    &spec.id,
                    &context,
                    &crate::storage::prewarm::fingerprint(spec).map_err(status)?,
                    super::recovery::now().map_err(status)?,
                )
                .await
                .map_err(status)?;
        }
        Ok(())
    }
    pub(super) async fn prewarm_session(&self) -> std::result::Result<Session, Status> {
        let state = self.0.state.lock().await;
        state.healthy().map_err(status)?;
        Ok(state.session.clone())
    }
}
#[tonic::async_trait]
impl pb::prewarm_service_server::PrewarmService for CoordinatorRpc {
    async fn list_artifacts(
        &self,
        request: Request<pb::PrewarmArtifactsQuery>,
    ) -> std::result::Result<Response<pb::PrewarmView>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let tenant = management_caller(r.caller.as_ref())?;
        let state = if r.state.is_empty() {
            None
        } else {
            Some(
                serde_json::from_value::<adx_core::prewarm::SlotState>(serde_json::Value::String(
                    r.state,
                ))
                .map_err(|_| Status::invalid_argument("invalid artifact state"))?,
            )
        };
        let page = self
            .prewarm_session()
            .await?
            .prewarm_artifact_page(
                tenant,
                &r.pool_id,
                if r.page_size == 0 { 50 } else { r.page_size },
                state,
                &r.page_token,
            )
            .await
            .map_err(pool_status)?;
        view(&page, 0)
    }
    async fn mutate_pool(
        &self,
        request: Request<pb::PrewarmMutationRequest>,
    ) -> std::result::Result<Response<pb::PrewarmView>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let tenant = management_caller(r.caller.as_ref())?;
        if r.contract_version != 1 || r.mutation_json.len() > 65_536 {
            return Err(Status::invalid_argument(
                "invalid preparation contract version or payload size",
            ));
        }
        let mutation: Mutation = serde_json::from_slice(&r.mutation_json)
            .map_err(|_| Status::invalid_argument("invalid prewarm mutation"))?;
        let session = self.prewarm_session().await?;
        if let Some(accepted) = session
            .prewarm_mutation_replay(tenant, &mutation)
            .await
            .map_err(pool_status)?
        {
            return view(&accepted, accepted.pool.revision);
        }
        let profile = match &mutation.command {
            crate::storage::prewarm::Command::Put { profile, policy } if policy.enabled => {
                Some(profile.clone())
            }
            crate::storage::prewarm::Command::Action {
                action: crate::storage::prewarm::Action::Resume,
            } => Some(
                session
                    .get_pool(tenant, &mutation.pool_id)
                    .await
                    .map_err(pool_status)?
                    .profile
                    .into(),
            ),
            _ => None,
        };
        if mutation.condition == crate::storage::prewarm::Condition::Missing {
            return Err(pool_status(PoolError::PreconditionRequired));
        }
        let mut qualified = None;
        if let Some(profile) = &profile {
            for node in self.prewarm_nodes().await? {
                if !node.node.available {
                    continue;
                }
                if let Ok(observation) = self
                    .artifact_rpc(
                        &node,
                        &adx_core::prewarm::ArtifactCommand::Qualify {
                            profile: profile.clone(),
                        },
                    )
                    .await
                {
                    if adx_core::prewarm::valid_digest(&observation.recipe_digest) {
                        qualified = Some(observation.recipe_digest);
                        break;
                    }
                }
            }
        }
        let accepted = session
            .mutate_qualified_pool(
                tenant,
                &mutation,
                super::recovery::now().map_err(status)?,
                qualified,
            )
            .await
            .map_err(pool_status)?;
        view(&accepted, accepted.pool.revision)
    }
    async fn get_pool(
        &self,
        request: Request<pb::PrewarmQuery>,
    ) -> std::result::Result<Response<pb::PrewarmView>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let tenant = management_caller(r.caller.as_ref())?;
        let pool = self
            .prewarm_session()
            .await?
            .get_pool(tenant, &r.id)
            .await
            .map_err(pool_status)?;
        view(&pool, pool.revision)
    }
    async fn list_pools(
        &self,
        request: Request<pb::PrewarmQuery>,
    ) -> std::result::Result<Response<pb::PrewarmView>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let pools = self
            .prewarm_session()
            .await?
            .list_pools(management_caller(r.caller.as_ref())?)
            .await
            .map_err(pool_status)?;
        view(&serde_json::json!({"items":pools}), 0)
    }
    async fn get_operation(
        &self,
        request: Request<pb::PrewarmQuery>,
    ) -> std::result::Result<Response<pb::PrewarmView>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let r = request.into_inner();
        let operation = self
            .prewarm_session()
            .await?
            .get_prewarm_operation(management_caller(r.caller.as_ref())?, &r.id)
            .await
            .map_err(pool_status)?;
        view(&operation, operation.accepted_revision)
    }
}
