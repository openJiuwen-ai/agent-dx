//! Public Volume validation and direct Coordinator RPC calls.
use crate::environment_service::EnvironmentService;
use adx_core::volumes::{Occupancy, Volume, VolumeSpec};
use adx_observability::trace;
use adx_protocol::control as pb;
use serde_json::{json, Value};
use std::collections::HashMap;
use tonic::Status;

fn encode(value: impl serde::Serialize) -> Result<Value, Status> {
    serde_json::to_value(value).map_err(|_| Status::internal("Volume response encoding failed"))
}
fn volume(value: pb::Volume, caller: &pb::CallerContext) -> Result<Volume, Status> {
    let volume: Volume = value.try_into().map_err(adx_protocol::status)?;
    if volume.spec.tenant_id != caller.tenant_id && !caller.administrator {
        return Err(Status::permission_denied("Volume tenant mismatch"));
    }
    Ok(volume)
}
fn info(value: pb::VolumeInfo, caller: &pb::CallerContext) -> Result<Value, Status> {
    let mut result = encode(volume(
        value
            .volume
            .ok_or_else(|| Status::data_loss("Volume definition missing"))?,
        caller,
    )?)?;
    result["environment_ids"] = json!(value.environment_ids);
    result["owner_ids"] = json!(value.owner_ids);
    result["snapshot_ids"] = json!(value.snapshot_ids);
    let state = match pb::VolumeInitializationState::try_from(value.initialization_state) {
        Ok(pb::VolumeInitializationState::NotRequested) => "not_requested",
        Ok(pb::VolumeInitializationState::Uninitialized) => "uninitialized",
        Ok(pb::VolumeInitializationState::Pending) => "pending",
        Ok(pb::VolumeInitializationState::Ready) => "ready",
        Ok(pb::VolumeInitializationState::NeedsRepair) => "needs_repair",
        Err(_) => return Err(Status::data_loss("invalid Volume initialization state")),
    };
    result["initialization"] = json!({"state":state,"error":value.initialization_error});
    let occupancy: Option<Occupancy> = value
        .occupancy
        .map(TryInto::try_into)
        .transpose()
        .map_err(adx_protocol::status)?;
    result["occupancy"] = encode(occupancy)?;
    Ok(result)
}
impl EnvironmentService {
    pub async fn volume_request(
        &self,
        method: &str,
        id: Option<&str>,
        mut input: Value,
        query: &HashMap<String, String>,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        if let Some(id) = id {
            adx_core::volumes::validate_volume_id(id).map_err(adx_protocol::status)?;
        }
        let mut client =
            pb::volume_service_client::VolumeServiceClient::new(self.clients.coordinator().await?);
        let request = || pb::GetVolumeRequest {
            tenant_id: caller.tenant_id.clone(),
            id: id.unwrap_or_default().into(),
            caller: Some(caller.clone()),
        };
        match (method, id) {
            ("POST", None) => {
                let object = input
                    .as_object_mut()
                    .ok_or_else(|| Status::invalid_argument("Volume definition required"))?;
                let owner_id = match object.remove("owner_id") {
                    Some(Value::String(id)) => id,
                    None => String::new(),
                    _ => return Err(Status::invalid_argument("invalid Volume owner ID")),
                };
                object.insert("tenant_id".into(), json!(caller.tenant_id));
                let spec: VolumeSpec = serde_json::from_value(input)
                    .map_err(|_| Status::invalid_argument("invalid Volume definition"))?;
                spec.validate().map_err(adx_protocol::status)?;
                let value = self
                    .clients
                    .rpc(
                        "apiserver.ensure_volume",
                        client.ensure_volume(trace::inject(pb::EnsureVolumeRequest {
                            spec: Some(spec.into()),
                            caller: Some(caller.clone()),
                            owner_id,
                        })),
                    )
                    .await?;
                encode(volume(value, caller)?)
            }
            ("GET", Some(_)) => info(
                self.clients
                    .rpc(
                        "apiserver.get_volume",
                        client.get_volume(trace::inject(request())),
                    )
                    .await?,
                caller,
            ),
            ("DELETE", Some(_)) => {
                self.clients
                    .rpc(
                        "apiserver.delete_volume",
                        client.delete_volume(trace::inject(request())),
                    )
                    .await?;
                Ok(Value::Null)
            }
            ("GET", None) => {
                let page_size = query
                    .get("pageSize")
                    .map(|size| {
                        size.parse::<u32>()
                            .map_err(|_| Status::invalid_argument("invalid page size"))
                    })
                    .transpose()?
                    .unwrap_or(100);
                let page = self
                    .clients
                    .rpc(
                        "apiserver.list_volumes",
                        client.list_volumes(trace::inject(pb::ListVolumesRequest {
                            tenant_id: caller.tenant_id.clone(),
                            caller: Some(caller.clone()),
                            page_token: query.get("pageToken").cloned().unwrap_or_default(),
                            page_size,
                        })),
                    )
                    .await?;
                let items = page
                    .volumes
                    .into_iter()
                    .map(|value| info(value, caller))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({"items":items,"nextPageToken":page.next_page_token}))
            }
            _ => Err(Status::unimplemented("unsupported Volume method")),
        }
    }
    pub async fn release_volume_owner(
        &self,
        id: &str,
        caller: &pb::CallerContext,
    ) -> Result<(), Status> {
        let mut client =
            pb::volume_service_client::VolumeServiceClient::new(self.clients.coordinator().await?);
        self.clients
            .rpc(
                "apiserver.release_volume_owner",
                client.release_owner(trace::inject(pb::ReleaseVolumeOwnerRequest {
                    tenant_id: caller.tenant_id.clone(),
                    owner_id: id.into(),
                    caller: Some(caller.clone()),
                })),
            )
            .await?;
        Ok(())
    }
}
