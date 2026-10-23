//! Native template resolution is read-only; resolved specifications belong to the instance.
use super::CreateRequest;
use crate::{contract, http::Api};
use adx_agent_core::environment::ExecutionSpec;
use adx_observability::trace;
use adx_protocol::control as pb;
use serde_json::json;
use tonic::Status;

pub(super) async fn create_spec(
    api: &Api,
    input: &CreateRequest,
    caller: &pb::CallerContext,
) -> Result<pb::EnvironmentSpec, Status> {
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    let (network, public_traffic) = input
        .network_configuration()
        .map_err(super::http::validation)?;
    let mut spec = if let Some(reference) = config.templates.get(&input.template_id) {
        // A failed native query must remain a failed query, not a snapshot fallback.
        let template = api
            .native_template(&caller.tenant_id, &reference.name, &reference.version)
            .await?;
        native_spec(api, input, caller, &template)?
    } else {
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            api.clients.coordinator().await?,
        );
        let snapshot = api
            .clients
            .rpc(
                "apiserver.e2b_create_snapshot_source",
                client.get_snapshot(trace::inject(pb::GetSnapshotRequest {
                    id: input.template_id.clone(),
                    caller: Some(caller.clone()),
                    node_session_id: String::new(),
                })),
            )
            .await?;
        let mut spec = snapshot
            .template
            .ok_or_else(|| Status::not_found("snapshot has no template"))?;
        if spec.tenant_id != caller.tenant_id || spec.compatibility.is_none() {
            return Err(Status::not_found("snapshot is not E2B-compatible"));
        }
        // Clone the persisted image/profile/resources/policy, never current aliases.
        spec.snapshot_id = Some(snapshot.id);
        spec
    };
    spec.id = format!("e2b-{}", uuid::Uuid::new_v4());
    spec.env.extend(input.env_vars.clone());
    if let Some(network) = network {
        spec.execution.get_or_insert_with(Default::default).network = Some(network.into());
    }
    let compatibility = spec
        .compatibility
        .as_mut()
        .ok_or_else(|| Status::data_loss("resolved template has no compatibility identity"))?;
    compatibility.envd_access_token = uuid::Uuid::new_v4().to_string();
    compatibility.traffic_access_token = uuid::Uuid::new_v4().to_string();
    compatibility.allow_public_traffic =
        public_traffic.unwrap_or(compatibility.allow_public_traffic);
    // E2B enables the common policy; platform lease logic has no protocol defaults.
    spec.run_policy = Some(pb::RunPolicy {
        timeout_seconds: input.timeout.unwrap_or(300),
        action: if input.auto_pause == Some(true) {
            pb::RunExpiryAction::Pause
        } else {
            pb::RunExpiryAction::Delete
        } as i32,
    });
    // Use the same canonical form as Coordinator for durable create replay equality.
    adx_core::EnvironmentSpec::try_from(spec)
        .map(Into::into)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn native_spec(
    api: &Api,
    input: &CreateRequest,
    caller: &pb::CallerContext,
    template: &adx_agent_core::TemplateVersion,
) -> Result<pb::EnvironmentSpec, Status> {
    if !immutable_image(&template.image) {
        return Err(Status::invalid_argument(
            "E2B native template image requires a full sha256 digest",
        ));
    }
    let mut execution = ExecutionSpec::from(template);
    execution.mounts = template
        .render_mounts(&Default::default())
        .map_err(Status::invalid_argument)?;
    let mut body = adx_activator::environment_request::execution_input(&execution)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    body["namespace"] = json!("e2b");
    body["name"] = json!(uuid::Uuid::new_v4().to_string());
    // Preserve the published native template's startup contract. envd is managed alongside the
    // image process, so readiness and init probes retain their template meaning.
    body["idleTimeoutSeconds"] = json!(0);
    body["idleAction"] = json!("delete");
    let mut spec = contract::create_spec_with_environment(
        body,
        caller,
        api.clients.config.runtime_profile.as_ref(),
    )?;
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    if !config.required_node_labels.is_empty() {
        let policy = spec.scheduling.get_or_insert_with(Default::default);
        if policy.required_node.is_empty() {
            policy.required_node.push(pb::LabelSelector::default());
        }
        for selector in &mut policy.required_node {
            selector
                .match_labels
                .extend(config.required_node_labels.clone());
        }
    }
    spec.template_identity = Some(pb::NativeTemplateIdentity {
        tenant_id: caller.tenant_id.clone(),
        name: template.name.clone(),
        version: template.version.clone(),
    });
    spec.compatibility = Some(pb::EnvironmentCompatibility {
        template_id: input.template_id.clone(),
        envd_version: config.envd_version.clone(),
        // Filled with fresh instance credentials by create_spec before validation.
        allow_public_traffic: true,
        ..Default::default()
    });
    Ok(spec)
}

fn immutable_image(image: &str) -> bool {
    image.rsplit_once("@sha256:").is_some_and(|(name, digest)| {
        !name.is_empty()
            && digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn template_images_pin_content_rather_than_mutable_tags() {
        assert!(immutable_image(&format!(
            "registry/app@sha256:{}",
            "a".repeat(64)
        )));
        for image in ["app:latest", "app@sha256:abc", "@sha256:"] {
            assert!(!immutable_image(image));
        }
    }
}
