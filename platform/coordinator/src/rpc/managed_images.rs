//! Managed image lifecycle, serialized with Environment admission. Registry I/O
//! belongs to the operator reconciler, never the scheduler or a task sandbox.
use super::*;
use crate::storage::managed_images::IMAGE_REFERENCE_SAMPLE;
use adx_core::managed_images::{ImageMutation, ManagedImage, PublicationState, RegistryScope};
use serde_json::{json, Value};

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedImageConfig {
    pub registry: RegistryScope,
    #[serde(default = "default_active_publications")]
    pub max_active_per_tenant: u32,
    #[serde(default = "default_pending_publications")]
    pub max_pending_per_tenant: u32,
}

fn default_active_publications() -> u32 {
    100_000
}
fn default_pending_publications() -> u32 {
    128
}

fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| Error::Unavailable("image lifecycle clock unavailable".into()))
}

impl CoordinatorRpc {
    pub(super) fn managed_registry(&self) -> Option<&RegistryScope> {
        self.0.image_config.get().map(|config| &config.registry)
    }

    pub fn with_managed_image_config(self, config: ManagedImageConfig) -> Result<Self> {
        config.registry.validate()?;
        if config.max_pending_per_tenant == 0
            || config.max_pending_per_tenant > config.max_active_per_tenant
        {
            return Err(Error::Invalid("invalid managed image policy".into()));
        }
        self.0
            .image_config
            .set(config)
            .map_err(|_| Error::Conflict)?;
        Ok(self)
    }

    pub fn with_managed_image_reclamation(self, enabled: bool) -> Self {
        self.0
            .managed_image_reclamation
            .store(enabled, Ordering::Release);
        self
    }

    async fn observe_image_roots(
        &self,
        tenant: &str,
        id: &str,
        claiming_deletion: bool,
    ) -> Result<(i64, Vec<String>)> {
        let session = &self.0.image_session;
        tokio::time::timeout(Duration::from_secs(5), async {
            // Read the fence before any roots. Pending central creates bump
            // it under the scheduler lock. Durable writes bump it in Redis.
            let admissions = session.managed_image_admissions(tenant, id).await?;
            // An existing claim closed admission. Retrying that exact worklist
            // can proceed even while the inventory is busy.
            if claiming_deletion && admissions < 0 {
                return Ok((admissions, Vec::new()));
            }
            let _permit = self.0.image_observation.try_acquire().map_err(|_| {
                Error::Unavailable("another managed image observation is in progress".into())
            })?;
            let limit = if claiming_deletion {
                1
            } else {
                IMAGE_REFERENCE_SAMPLE
            };
            let pending = {
                let state = self.0.state.lock().await;
                state.healthy()?;
                state
                    .specs
                    .values()
                    .filter(|spec| {
                        spec.tenant_id == tenant
                            && spec.execution.managed_image_publications.contains(id)
                    })
                    .take(limit)
                    .map(|spec| format!("environment:{}", spec.id))
                    .collect()
            };
            let refs = session
                .managed_image_references(tenant, id, pending, limit)
                .await?;
            Ok((admissions, refs))
        })
        .await
        .map_err(|_| Error::Unavailable("managed image observation exceeded its budget".into()))?
    }
}

fn view(
    image: ManagedImage,
    now: u64,
    epoch: u64,
    enabled: bool,
    references_current: bool,
) -> Value {
    let eligible = references_current && image.reclaimable(now);
    json!({"image":image,"eligible_for_reclamation":eligible,"reclaimable":eligible && enabled,"reclamation_enabled":enabled,"inventory_epoch":epoch,"references_current":references_current})
}
fn response(value: Value) -> Result<Response<pb::ManagedImageResponse>> {
    Ok(Response::new(pb::ManagedImageResponse {
        view_json: serde_json::to_vec(&value)
            .map_err(|_| Error::Unavailable("managed image response encoding failed".into()))?,
    }))
}

#[tonic::async_trait]
impl pb::managed_image_service_server::ManagedImageService for CoordinatorRpc {
    async fn mutate_managed_image(
        &self,
        request: Request<pb::ManagedImageMutationRequest>,
    ) -> std::result::Result<Response<pb::ManagedImageResponse>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        let request = request.into_inner();
        tenant(request.caller.as_ref(), &request.tenant_id)?;
        if request.mutation_json.len() > adx_core::managed_images::MANAGED_IMAGE_MUTATION_BYTES {
            return Err(Status::invalid_argument("managed image mutation too large"));
        }
        let mutation: ImageMutation = serde_json::from_slice(&request.mutation_json)
            .map_err(|_| Status::invalid_argument("invalid managed image mutation"))?;
        let enabled = self.0.managed_image_reclamation.load(Ordering::Acquire);
        let administrator = request
            .caller
            .as_ref()
            .is_some_and(|caller| caller.administrator);
        let config = self.0.image_config.get().ok_or_else(|| {
            Status::failed_precondition("managed Registry scope is not configured")
        })?;
        if matches!(mutation, ImageMutation::Observe) && !administrator {
            return Err(Status::permission_denied(
                "image observer administrator required",
            ));
        }
        if matches!(mutation, ImageMutation::ReconcileAbandoned { .. }) && !administrator {
            return Err(Status::permission_denied(
                "publication reconciliation administrator required",
            ));
        }
        if matches!(
            mutation,
            ImageMutation::ClaimDeletion { .. } | ImageMutation::FinishDeletion
        ) {
            if !administrator {
                return Err(Status::permission_denied(
                    "image reclaimer administrator required",
                ));
            }
            if !enabled {
                return Err(Status::failed_precondition(
                    "managed image reclamation is disabled",
                ));
            }
        }
        let session = &self.0.image_session;
        let claiming_deletion = matches!(mutation, ImageMutation::ClaimDeletion { .. });
        let observing = claiming_deletion || matches!(mutation, ImageMutation::Observe);
        let (admissions, refs) = if observing {
            let (admissions, refs) = self
                .observe_image_roots(&request.tenant_id, &request.id, claiming_deletion)
                .await
                .map_err(status)?;
            (Some(admissions), refs)
        } else {
            (None, Vec::new())
        };
        let timestamp = now().map_err(status)?;
        let image = if let ImageMutation::Begin {
            request_id,
            retention,
        } = mutation
        {
            if !request.id.is_empty() {
                return Err(Status::invalid_argument(
                    "begin allocates an image identity",
                ));
            }
            session
                .begin_managed_image(
                    ManagedImage::new(
                        format!(
                            "{}{}",
                            uuid::Uuid::new_v4().simple(),
                            uuid::Uuid::new_v4().simple()
                        ),
                        &request.tenant_id,
                        &request_id,
                        retention,
                        config.registry.clone(),
                        timestamp,
                    )
                    .map_err(status)?,
                    config.max_active_per_tenant,
                    config.max_pending_per_tenant,
                )
                .await
                .map_err(|error| match error {
                    Error::NoCapacity => {
                        Status::resource_exhausted("managed image publication budget exhausted")
                    }
                    error => status(error),
                })?
        } else {
            session
                .mutate_managed_image_checked(
                    &request.tenant_id,
                    &request.id,
                    admissions,
                    |image| {
                        if matches!(
                            mutation,
                            ImageMutation::RegisterOutputs { .. }
                                | ImageMutation::Complete { .. }
                                | ImageMutation::ClaimDeletion { .. }
                                | ImageMutation::ReconcileAbandoned { .. }
                        ) && image.registry != config.registry
                        {
                            return Err(Error::Conflict);
                        }
                        match &mutation {
                            ImageMutation::RegisterOutputs {
                                outputs,
                                attempt_id,
                            } => image.register_outputs(outputs.clone(), attempt_id),
                            ImageMutation::Complete {
                                digests,
                                attempt_id,
                            } => image.complete(digests.clone(), attempt_id),
                            ImageMutation::CancelPreparation => image.cancel_preparation(),
                            ImageMutation::ReconcileAbandoned {
                                digests,
                                writer_fence,
                                expected_revision,
                            } => image.reconcile_abandoned(
                                digests.clone(),
                                writer_fence,
                                *expected_revision,
                            ),
                            ImageMutation::Retain { lease_id } => image.retain(lease_id),
                            ImageMutation::Release { lease_id } => image.release(lease_id),
                            ImageMutation::Extend { retain_until } => image.extend(*retain_until),
                            ImageMutation::Pin => image.pin(),
                            ImageMutation::SetRetention {
                                retention,
                                expected_revision,
                            } => image.set_retention(retention.clone(), *expected_revision),
                            ImageMutation::Observe => image.observe_after_admission(
                                &refs,
                                now()?,
                                admissions
                                    .and_then(|n| u64::try_from(n).ok())
                                    .unwrap_or_default(),
                            ),
                            ImageMutation::ClaimDeletion { manifests } => {
                                if !refs.is_empty() {
                                    return Err(Error::Conflict);
                                }
                                if image.state != PublicationState::Deleting
                                    && admissions.and_then(|n| u64::try_from(n).ok())
                                        != Some(image.observed_admissions)
                                {
                                    return Err(Error::Conflict);
                                }
                                image.claim_manifests(manifests, timestamp)
                            }
                            ImageMutation::FinishDeletion => image.finish_deletion(),
                            ImageMutation::Begin { .. } => {
                                Err(Error::Invalid("image already allocated".into()))
                            }
                        }
                    },
                )
                .await
                .map_err(status)?
        };
        response(view(
            image,
            now().map_err(status)?,
            session.epoch(),
            enabled,
            observing && !(claiming_deletion && admissions.is_some_and(|version| version < 0)),
        ))
        .map_err(status)
    }

    async fn get_managed_images(
        &self,
        request: Request<pb::ManagedImageQuery>,
    ) -> std::result::Result<Response<pb::ManagedImageResponse>, Status> {
        if self.0.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server identity required"));
        }
        let request = request.into_inner();
        tenant(request.caller.as_ref(), &request.tenant_id)?;
        let session = &self.0.image_session;
        let now = now().map_err(status)?;
        let enabled = self.0.managed_image_reclamation.load(Ordering::Acquire);
        if !request.id.is_empty() {
            let image = session
                .managed_image(&request.tenant_id, &request.id)
                .await
                .map_err(status)?;
            return response(view(image, now, session.epoch(), enabled, false)).map_err(status);
        }
        let limit = if request.limit == 0 {
            100
        } else {
            usize::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("invalid image page size"))?
        };
        let images = session
            .managed_images(&request.tenant_id, &request.after, limit)
            .await
            .map_err(status)?;
        let next = if images.len() == limit {
            images
                .last()
                .map(|image| image.id.clone())
                .unwrap_or_default()
        } else {
            String::new()
        };
        let items = images
            .into_iter()
            .map(|image| {
                json!({
                    "image": image, "summary": true, "references_current": false,
                    "reclamation_enabled": enabled, "inventory_epoch": session.epoch(),
                })
            })
            .collect::<Vec<_>>();
        response(json!({"items": items, "next_page_token": next})).map_err(status)
    }
}
