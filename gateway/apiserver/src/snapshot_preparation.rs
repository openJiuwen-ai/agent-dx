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

impl EnvironmentService {
    pub(crate) async fn agent_snapshot_protection(
        &self,
        caller: &pb::CallerContext,
        id: &str,
        token: String,
        retire: bool,
    ) -> Result<(), Status> {
        uuid::Uuid::parse_str(&token)
            .map_err(|_| Status::invalid_argument("invalid Agent snapshot proof"))?;
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            self.clients.coordinator().await?,
        );
        let value = if retire {
            self.clients
                .rpc(
                    "apiserver.retire_agent_snapshot",
                    client.delete_snapshot(trace::inject(pb::DeleteSnapshotRequest {
                        caller: Some(caller.clone()),
                        id: id.into(),
                        agent_token: Some(token),
                    })),
                )
                .await?
        } else {
            self.clients
                .rpc(
                    "apiserver.protect_agent_snapshot",
                    client.protect_snapshot(trace::inject(pb::ProtectSnapshotRequest {
                        caller: Some(caller.clone()),
                        id: id.into(),
                        agent_token: token,
                    })),
                )
                .await?
        };
        if value.id != id {
            return Err(Status::data_loss("snapshot protection identity mismatch"));
        }
        crate::operations::snapshot_value(&value, caller)?;
        Ok(())
    }

    pub(crate) async fn prepare_snapshot(
        &self,
        id: &str,
        input: Value,
        caller: &pb::CallerContext,
    ) -> Result<SeedResult, Status> {
        if !adx_core::snapshot_preparation::valid_preparation_id(id) {
            return Err(Status::invalid_argument("fixed preparation UUID required"));
        }
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
        let mut spec = crate::contract::create_spec_with_environment(
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
                self.clients
                    .rpc(
                        "apiserver.snapshot_preparation.register",
                        client.register_snapshot_preparation(trace::inject(
                            pb::RegisterSnapshotPreparationRequest {
                                caller: Some(caller.clone()),
                                name: registration.clone(),
                                resources: spec.resources,
                                fixed_id: Some(id.into()),
                            },
                        )),
                    )
                    .await?
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
                let mut volume_input = json!(input.volume);
                volume_input["owner_id"] = json!(format!("golden-{id}"));
                let volume = self
                    .volume_request("POST", None, volume_input, &Default::default(), caller)
                    .await?;
                let uid = volume["uid"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| Status::data_loss("seed Volume UID unavailable"))?;
                // Convert through the typed spec before marking the preparation started.
                let mut typed: adx_core::EnvironmentSpec =
                    spec.try_into().map_err(adx_protocol::status)?;
                for mount in &mut typed.execution.mounts {
                    if let adx_core::execution::MountSource::Volume(volume) = &mut mount.source {
                        volume.uid = Some(uid.into());
                    }
                }
                spec = typed.into();
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
