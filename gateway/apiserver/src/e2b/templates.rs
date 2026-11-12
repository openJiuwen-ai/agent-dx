//! Resolve protocol references to platform-owned immutable snapshot sources.
use super::{network::configure_envd_service, CreateRequest};
use crate::http::Api;
use adx_observability::trace;
use adx_protocol::control as pb;
use tonic::{Code, Status};

/// The common directory owns stable family IDs and names. Only an unknown bare reference may be a raw snapshot ID.
pub(super) async fn resolve_reference(
    api: &Api,
    reference: &str,
    caller: &pb::CallerContext,
) -> Result<(String, String), Status> {
    if api.clients.config.e2b.is_none() {
        return Err(Status::unimplemented("E2B API is disabled"));
    }
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    match api
        .clients
        .rpc(
            "apiserver.resolve_snapshot_template",
            client.resolve_snapshot_template(trace::inject(
                super::template_api::resolution_request(reference, caller, false)?,
            )),
        )
        .await
    {
        Ok(value) => Ok((value.snapshot_id, value.template_id)),
        Err(error) if error.code() == Code::NotFound && !reference.contains(':') => {
            Ok((reference.into(), reference.into()))
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn create_spec(
    api: &Api,
    input: &CreateRequest,
    caller: &pb::CallerContext,
) -> Result<pb::EnvironmentSpec, Status> {
    let (id, template_id) = resolve_reference(api, &input.template_id, caller).await?;
    let mut client =
        pb::snapshot_service_client::SnapshotServiceClient::new(api.clients.coordinator().await?);
    let snapshot = api
        .clients
        .rpc(
            "apiserver.e2b_create_snapshot_source",
            client.get_snapshot(trace::inject(pb::GetSnapshotRequest {
                id,
                caller: Some(caller.clone()),
                node_session_id: String::new(),
            })),
        )
        .await?;
    let mut spec = snapshot
        .template
        .ok_or_else(|| Status::data_loss("snapshot has no execution specification"))?;
    if spec.tenant_id != caller.tenant_id {
        return Err(Status::not_found("snapshot not found"));
    }
    if matches!(
        pb::SnapshotState::try_from(snapshot.state),
        Ok(pb::SnapshotState::Deleting | pb::SnapshotState::Deleted)
    ) {
        return Err(Status::not_found("snapshot not found"));
    }
    if snapshot.state != pb::SnapshotState::Ready as i32 {
        return Err(Status::failed_precondition("snapshot is not ready"));
    }
    let service = spec
        .envd
        .clone()
        .ok_or_else(|| Status::failed_precondition("snapshot has no envd service"))?;
    if !service.gateway_auth {
        return Err(Status::failed_precondition(
            "guest-authenticated snapshots cannot be cloned with independent gateway credentials",
        ));
    }
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    let baseline: Option<adx_core::execution::NetworkPolicy> = spec
        .execution
        .as_ref()
        .and_then(|execution| execution.network.as_ref())
        .cloned()
        .map(TryInto::try_into)
        .transpose()
        .map_err(|_| Status::data_loss("invalid persisted snapshot network policy"))?;
    let (network, public_traffic) = input
        .network_configuration_with_baseline(baseline.as_ref())
        .map_err(super::http::validation)?;
    let previous_public = spec
        .compatibility
        .as_ref()
        .is_none_or(|value| value.allow_public_traffic);
    spec.id = format!("e2b-{}", uuid::Uuid::new_v4());
    spec.snapshot_id = Some(snapshot.id);
    spec.env.extend(input.env_vars.clone());
    spec.scheduling
        .get_or_insert_with(Default::default)
        .labels
        .extend(input.metadata.clone());
    if let Some(network) = network {
        spec.execution.get_or_insert_with(Default::default).network = Some(network.into());
    }
    spec.compatibility = Some(pb::EnvironmentCompatibility {
        template_id,
        envd_access_token: uuid::Uuid::new_v4().to_string(),
        traffic_access_token: uuid::Uuid::new_v4().to_string(),
        allow_public_traffic: public_traffic.unwrap_or(previous_public),
    });
    // Runtime configuration is independent from the SDK identity and credentials above.
    configure_envd_service(
        &mut spec,
        pb::EnvdServiceConfig {
            version: service.version,
            gateway_auth: true,
            access_token: None,
            ingress_sources: config.envd_ingress_sources.clone(),
        },
    )?;
    spec.run_policy = Some(pb::RunPolicy {
        timeout_seconds: input.timeout.unwrap_or(300),
        action: if input.auto_pause == Some(true) {
            pb::RunExpiryAction::Pause
        } else {
            pb::RunExpiryAction::Delete
        } as i32,
    });
    adx_core::EnvironmentSpec::try_from(spec)
        .map(Into::into)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}
