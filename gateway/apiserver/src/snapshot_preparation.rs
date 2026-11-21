//! Native Environment adapter for a fixed Platform preparation identity.
use crate::environment_service::EnvironmentService;
use adx_agent_core::prewarm::SeedResult;
use adx_observability::trace;
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::{Code, Status};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    environment: Value,
    volume: adx_agent_core::volumes::VolumeSpec,
}

pub(crate) fn snapshot_admission_stale(status: &Status) -> bool {
    status.code() == Code::Unavailable
        && status
            .metadata()
            .get(adx_core::snapshots::ADMISSION_STALE_HEADER)
            .is_some_and(|value| value == "true")
}

impl EnvironmentService {
    pub(crate) async fn update_snapshot_reference(
        &self,
        caller: &pb::CallerContext,
        id: &str,
        change: &adx_agent_core::environment::SnapshotChange,
    ) -> Result<(), Status> {
        use adx_agent_core::environment::{SnapshotChange, SnapshotOwner};
        use pb::{snapshot_reference::Owner, update_snapshot_reference_request::Change};
        let reference = |owner: &SnapshotOwner| pb::SnapshotReference {
            owner: Some(match owner {
                SnapshotOwner::Template { name, version } => {
                    Owner::AgentTemplate(pb::AgentTemplateReference {
                        name: name.clone(),
                        version: version.clone(),
                    })
                }
                SnapshotOwner::Binding {
                    binding_id,
                    environment_id,
                } => Owner::AgentBinding(pb::AgentBindingReference {
                    binding_id: binding_id.clone(),
                    environment_id: environment_id.clone(),
                }),
            }),
        };
        if matches!(
            change,
            SnapshotChange::Acquire {
                owner: SnapshotOwner::Binding { .. }
            } | SnapshotChange::AcquireBinding {
                owner: SnapshotOwner::Template { .. },
                ..
            } | SnapshotChange::CancelPending {
                owner: SnapshotOwner::Template { .. },
                ..
            }
        ) {
            return Err(Status::invalid_argument(
                "snapshot reference operation and owner disagree",
            ));
        }
        let operation = match change {
            SnapshotChange::CancelPending { owner, .. } => Change::CancelPending(reference(owner)),
            SnapshotChange::AcquireBinding { owner, .. } | SnapshotChange::Acquire { owner } => {
                Change::Acquire(reference(owner))
            }
            SnapshotChange::Release { owner } => Change::Release(reference(owner)),
            SnapshotChange::Retire => Change::RetireAgentRecipe(true),
        };
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            self.clients.coordinator().await?,
        );
        let result = self
            .clients
            .rpc(
                "apiserver.update_snapshot_reference",
                client.update_snapshot_reference(trace::inject(
                    pb::UpdateSnapshotReferenceRequest {
                        caller: Some(caller.clone()),
                        id: id.into(),
                        expected_revision: match change {
                            SnapshotChange::AcquireBinding { revision, .. }
                            | SnapshotChange::CancelPending { revision, .. } => Some(*revision),
                            _ => None,
                        },
                        change: Some(operation),
                    },
                )),
            )
            .await?;
        if result.id != id {
            return Err(Status::data_loss("snapshot reference identity mismatch"));
        }
        crate::operations::snapshot_value(&result, caller)?;
        if matches!(change, SnapshotChange::Retire) {
            self.clients
                .rpc(
                    "apiserver.delete_snapshot",
                    client.delete_snapshot(trace::inject(pb::DeleteSnapshotRequest {
                        caller: Some(caller.clone()),
                        id: id.into(),
                    })),
                )
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn prepare_snapshot(
        &self,
        id: &str,
        mut input: Value,
        caller: &pb::CallerContext,
    ) -> Result<SeedResult, Status> {
        if !adx_core::snapshot_preparation::valid_preparation_id(id) {
            return Err(Status::invalid_argument("fixed preparation UUID required"));
        }
        // The deadline fences registration. It is not part of the seed spec digest.
        let registration_deadline_unix_ms = input
            .as_object_mut()
            .and_then(|input| input.remove("registration_deadline_unix_ms"))
            .map(|deadline| {
                deadline
                    .as_u64()
                    .ok_or_else(|| Status::invalid_argument("invalid preparation deadline"))
            })
            .transpose()?;
        let bytes = serde_json::to_vec(&input)
            .map_err(|_| Status::invalid_argument("invalid snapshot preparation input"))?;
        let registration = format!("adx-agent-{id}-{:x}", Sha256::digest(bytes));
        let input: Input = serde_json::from_value(input)
            .map_err(|_| Status::invalid_argument("invalid snapshot preparation input"))?;
        let volume_id = format!("adx-agent-seed-{id}");
        if input.volume.id != volume_id
            || input.volume.access_mode != adx_agent_core::volumes::AccessMode::Exclusive
            || !input.volume.create_if_missing
        {
            return Err(Status::invalid_argument("dedicated seed Volume required"));
        }
        input.volume.validate().map_err(Status::invalid_argument)?;
        let mounts = input
            .environment
            .get("mounts")
            .and_then(Value::as_array)
            .filter(|mounts| mounts.len() == 1 && mounts[0]["volume_id"] == volume_id)
            .ok_or_else(|| Status::invalid_argument("seed must mount only its dedicated Volume"))?;
        if !mounts[0]["volume_uid"].is_null()
            || !input.environment["snapshotId"].is_null()
            || input.environment["inheritEntrypoint"] != true
            || input.environment["readinessProbe"].is_null()
        {
            return Err(Status::invalid_argument("invalid seed launch geometry"));
        }
        let mut volume = json!(input.volume);
        volume["tenant_id"] = json!(caller.tenant_id);
        let seed_volume: adx_core::volumes::VolumeSpec = serde_json::from_value(volume)
            .map_err(|_| Status::invalid_argument("invalid seed Volume definition"))?;
        seed_volume.validate().map_err(adx_protocol::status)?;
        let spec = crate::contract::create_spec_with_environment(
            input.environment,
            caller,
            self.clients.config.runtime_profile.as_ref(),
        )?;
        let mut client =
            pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
                self.clients.coordinator().await?,
            );
        let lookup = || pb::GetSnapshotPreparationRequest {
            template_id: id.into(),
            preparation_id: id.into(),
            caller: Some(caller.clone()),
        };
        let view = match self
            .clients
            .rpc(
                "apiserver.snapshot_preparation.get",
                client.get_snapshot_preparation(trace::inject(lookup())),
            )
            .await
        {
            Ok(view) => view,
            Err(e) if e.code() == Code::NotFound => {
                match self
                    .clients
                    .rpc(
                        "apiserver.snapshot_preparation.register",
                        client.register_snapshot_preparation(trace::inject(
                            pb::RegisterSnapshotPreparationRequest {
                                caller: Some(caller.clone()),
                                name: registration.clone(),
                                resources: spec.resources,
                                fixed_id: Some(id.into()),
                                registration_deadline_unix_ms,
                            },
                        )),
                    )
                    .await
                {
                    Ok(view) => view,
                    Err(e)
                        if e.code() == Code::NotFound
                            && registration_deadline_unix_ms.is_some() =>
                    {
                        return Ok(SeedResult::Failed)
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        if view.template_id != id || view.preparation_id != id {
            return Err(Status::data_loss("snapshot preparation identity mismatch"));
        }
        if view.name != registration {
            return Err(Status::already_exists(
                "preparation UUID already used with different input",
            ));
        }
        let phase = pb::SnapshotPreparationPhase::try_from(view.phase)
            .map_err(|_| Status::data_loss("invalid preparation phase"))?;
        match phase {
            pb::SnapshotPreparationPhase::Registered => {
                let started = self
                    .clients
                    .rpc(
                        "apiserver.snapshot_preparation.start",
                        client.start_snapshot_preparation(trace::inject(
                            pb::StartSnapshotPreparationRequest {
                                caller: Some(caller.clone()),
                                template_id: id.into(),
                                preparation_id: id.into(),
                                spec: Some(spec),
                                seed_volume: Some(seed_volume.into()),
                            },
                        )),
                    )
                    .await;
                match started {
                    Ok(_) => Ok(SeedResult::Pending),
                    Err(e)
                        if matches!(
                            e.code(),
                            Code::InvalidArgument
                                | Code::AlreadyExists
                                | Code::FailedPrecondition
                                | Code::Aborted
                        ) =>
                    {
                        let latest = self
                            .clients
                            .rpc(
                                "apiserver.snapshot_preparation.get",
                                client.get_snapshot_preparation(trace::inject(lookup())),
                            )
                            .await?;
                        if latest.phase == pb::SnapshotPreparationPhase::Registered as i32 {
                            Err(e)
                        } else {
                            Ok(SeedResult::Pending)
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            pb::SnapshotPreparationPhase::Ready
                if view.snapshot_available && !view.snapshot_id.is_empty() =>
            {
                Ok(SeedResult::Ready {
                    snapshot_id: view.snapshot_id,
                })
            }
            pb::SnapshotPreparationPhase::Failed | pb::SnapshotPreparationPhase::Ready => {
                Ok(SeedResult::Failed)
            }
            pb::SnapshotPreparationPhase::Creating
            | pb::SnapshotPreparationPhase::Snapshotting
            | pb::SnapshotPreparationPhase::Cleaning => Ok(SeedResult::Pending),
            pb::SnapshotPreparationPhase::Unspecified => {
                Err(Status::data_loss("invalid preparation phase"))
            }
        }
    }
}
