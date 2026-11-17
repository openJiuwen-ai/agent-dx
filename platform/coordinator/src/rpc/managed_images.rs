//! Managed image lifecycle, serialized with Environment admission. Registry I/O
//! belongs to the operator reconciler, never the scheduler or a task sandbox.
use super::*;
use adx_core::managed_images::{dependencies, ImageMutation, ManagedImage};
use serde_json::{json, Value};

fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| Error::Unavailable("image lifecycle clock unavailable".into()))
}

impl CoordinatorRpc {
    pub fn with_managed_image_reclamation(self, enabled: bool) -> Self {
        self.0
            .managed_image_reclamation
            .store(enabled, Ordering::Release);
        self
    }
}

impl State {
    async fn image_references(
        &self,
        tenant_id: &str,
    ) -> Result<BTreeMap<String, BTreeSet<String>>> {
        let mut inventory: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let environments = self.session.snapshot().await?.environments;
        let mut add = |spec: &EnvironmentSpec, reference: String| -> Result<()> {
            if spec.tenant_id == tenant_id {
                for id in dependencies(spec)? {
                    inventory.entry(id).or_default().insert(reference.clone());
                }
            }
            Ok(())
        };
        // Pending scheduler/local-claim requests protect images before assignment.
        for spec in self.specs.values() {
            add(spec, format!("environment:{}", spec.id))?;
        }
        for record in environments.values() {
            add(&record.spec, format!("environment:{}", record.spec.id))?;
        }
        for snapshot in self.session.retained_snapshots().await? {
            add(&snapshot.template, format!("snapshot:{}", snapshot.id))?;
        }
        // An orphaned execution hold has an unresolved runtime outcome. Its
        // image dependencies cannot be reconstructed from a checkpoint path.
        if self.session.execution_holds().await?.iter().any(|hold| {
            !environments.contains_key(&hold.assignment.environment_id)
                && !self.specs.contains_key(&hold.assignment.environment_id)
        }) {
            inventory
                .entry(String::new())
                .or_default()
                .insert("unknown:execution-hold".into());
        }
        Ok(inventory)
    }
}

fn references(inventory: &BTreeMap<String, BTreeSet<String>>, id: &str) -> Vec<String> {
    inventory
        .get(id)
        .into_iter()
        .flatten()
        .chain(inventory.get("").into_iter().flatten())
        .cloned()
        .collect()
}

fn view(
    mut image: ManagedImage,
    inventory: &BTreeMap<String, BTreeSet<String>>,
    now: u64,
    epoch: u64,
    enabled: bool,
    references_current: bool,
) -> Value {
    if references_current {
        image.references = references(inventory, &image.id).into_iter().collect();
    }
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
        if request.mutation_json.len() > 131_072 {
            return Err(Status::invalid_argument("managed image mutation too large"));
        }
        let mutation: ImageMutation = serde_json::from_slice(&request.mutation_json)
            .map_err(|_| Status::invalid_argument("invalid managed image mutation"))?;
        let enabled = self.0.managed_image_reclamation.load(Ordering::Acquire);
        if matches!(
            mutation,
            ImageMutation::ClaimDeletion { .. } | ImageMutation::FinishDeletion
        ) {
            if !request
                .caller
                .as_ref()
                .is_some_and(|caller| caller.administrator)
            {
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
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        let now = now().map_err(status)?;
        // Ordinary publication/hold updates are point CAS operations. Do not
        // scan every Environment/Snapshot under the admission lock per build.
        let references_current = matches!(
            mutation,
            ImageMutation::Observe | ImageMutation::ClaimDeletion { .. }
        );
        let inventory = if references_current {
            state
                .image_references(&request.tenant_id)
                .await
                .map_err(status)?
        } else {
            BTreeMap::new()
        };
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
            state
                .session
                .begin_managed_image(
                    ManagedImage::new(&request.tenant_id, &request_id, retention, now)
                        .map_err(status)?,
                )
                .await
                .map_err(status)?
        } else {
            let refs = references(&inventory, &request.id);
            state
                .session
                .mutate_managed_image(&request.tenant_id, &request.id, |image| {
                    match &mutation {
                        ImageMutation::Plan {
                            outputs,
                            execution_id,
                        } => {
                            if execution_id.is_empty()
                                || execution_id.len() > 256
                                || (!image.execution_id.is_empty()
                                    && image.execution_id != *execution_id)
                            {
                                return Err(Error::Conflict);
                            }
                            image.plan(outputs.clone(), now)?;
                            image.execution_id = execution_id.clone();
                            Ok(())
                        }
                        ImageMutation::Publish { digests } => image.publish(digests.clone(), now),
                        ImageMutation::Retain { lease_id } => image.retain(lease_id),
                        ImageMutation::Release { lease_id } => image.release(lease_id),
                        ImageMutation::Extend { retain_until } => image.extend(*retain_until),
                        ImageMutation::Pin => image.pin(),
                        ImageMutation::SetRetention {
                            retention,
                            expected_revision,
                        } => image.set_retention(retention.clone(), *expected_revision),
                        ImageMutation::Observe => image.observe(&refs, now),
                        ImageMutation::ClaimDeletion { manifests } => {
                            // Always recheck fresh roots while holding the same mutex
                            // as both scheduler admission paths. Deleting is durable.
                            if !refs.is_empty() {
                                return Err(Error::Conflict);
                            }
                            image.claim_manifests(manifests, now)
                        }
                        ImageMutation::FinishDeletion => image.finish_deletion(),
                        ImageMutation::Begin { .. } => {
                            Err(Error::Invalid("image already allocated".into()))
                        }
                    }
                })
                .await
                .map_err(status)?
        };
        response(view(
            image,
            &inventory,
            now,
            state.session.epoch(),
            enabled,
            references_current,
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
        let mut state = self.0.state.lock().await;
        state.recover_authoritative_state().await.map_err(status)?;
        state.healthy().map_err(status)?;
        let inventory = state
            .image_references(&request.tenant_id)
            .await
            .map_err(status)?;
        let now = now().map_err(status)?;
        let enabled = self.0.managed_image_reclamation.load(Ordering::Acquire);
        if !request.id.is_empty() {
            let image = state
                .session
                .managed_image(&request.tenant_id, &request.id)
                .await
                .map_err(status)?;
            return response(view(
                image,
                &inventory,
                now,
                state.session.epoch(),
                enabled,
                true,
            ))
            .map_err(status);
        }
        let limit = if request.limit == 0 {
            100
        } else {
            usize::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("invalid image page size"))?
        };
        let images = state
            .session
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
        response(
            json!({"items":images.into_iter().map(|image| view(image, &inventory, now, state.session.epoch(), enabled, true)).collect::<Vec<_>>(),"next_page_token":next}),
        ).map_err(status)
    }
}
