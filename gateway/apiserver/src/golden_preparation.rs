//! Administrator entry for preparing a reusable snapshot from a native seed spec.
//! The Coordinator owns preparation state, retries, capture and seed cleanup.

use crate::{clients::Clients, contract};
use adx_observability::trace;
use adx_protocol::control as pb;
use hyper::{body::Incoming, Request};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::Status;

const PATH: &str = "/api/environment/v1/snapshot-preparations";

pub(crate) fn matches(path: &str) -> bool {
    path == PATH
        || path
            .strip_prefix(PATH)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Registration {
    name: String,
    cpu_millis: u64,
    memory_mib: u64,
    #[serde(default)]
    disk_mib: u64,
}

fn resources(input: &Registration) -> Result<pb::Resources, Status> {
    let bytes = |mib: u64| {
        mib.checked_mul(1_048_576)
            .ok_or_else(|| Status::invalid_argument("snapshot resources overflow"))
    };
    if input.cpu_millis == 0 || input.memory_mib == 0 {
        return Err(Status::invalid_argument("positive CPU and memory required"));
    }
    Ok(pb::Resources {
        cpu_millis: input.cpu_millis,
        memory_bytes: bytes(input.memory_mib)?,
        disk_bytes: bytes(input.disk_mib)?,
    })
}

fn seed_spec(
    input: Value,
    caller: &pb::CallerContext,
    profile: Option<&adx_core::runtime_profile::RuntimeProfile>,
) -> Result<pb::EnvironmentSpec, Status> {
    let spec = contract::create_spec_with_environment(input, caller, profile)?;
    if spec.snapshot_id.is_some() {
        return Err(Status::invalid_argument(
            "a golden seed must be a fresh create",
        ));
    }
    if !spec
        .execution
        .as_ref()
        .is_some_and(|options| options.inherit_entrypoint && options.readiness_probe.is_some())
    {
        return Err(Status::invalid_argument(
            "a managed entrypoint and readiness probe are required for the golden seed",
        ));
    }
    Ok(spec)
}

fn view(value: pb::SnapshotPreparationView) -> Result<Value, Status> {
    use pb::SnapshotPreparationPhase as Phase;
    let phase = match Phase::try_from(value.phase) {
        Ok(Phase::Registered) => "registered",
        Ok(Phase::Creating) => "creating",
        Ok(Phase::Snapshotting) => "snapshotting",
        Ok(Phase::Cleaning) => "cleaning",
        Ok(Phase::Ready) => "ready",
        Ok(Phase::Failed) => "failed",
        _ => return Err(Status::data_loss("unknown snapshot preparation phase")),
    };
    let mut response = json!({
        "name": value.name, "templateId": value.template_id, "preparationId": value.preparation_id,
        "phase": phase
    });
    if phase == "ready" && value.snapshot_available {
        response["snapshotId"] = json!(value.snapshot_id);
    }
    if phase == "failed" {
        response["message"] = json!(value.error);
    }
    Ok(response)
}

/// Consume a registration, start, or observation for the authenticated tenant.
pub(crate) async fn handle(
    clients: &Clients,
    request: Request<Incoming>,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    if !caller.administrator {
        return Err(Status::permission_denied("administrator required"));
    }
    let method = request.method().as_str().to_owned();
    let path = request.uri().path().to_owned();
    let input = if method == "POST" {
        crate::http::body(request.into_body(), 1_048_576).await?
    } else {
        Value::Null
    };
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        clients.coordinator().await?,
    );
    if method == "POST" && path == PATH {
        let input: Registration = serde_json::from_value(input)
            .map_err(|_| Status::invalid_argument("invalid snapshot preparation registration"))?;
        let resources = resources(&input)?;
        return view(
            clients
                .rpc(
                    "apiserver.register_snapshot_preparation",
                    client.register_snapshot_preparation(trace::inject(
                        pb::RegisterSnapshotPreparationRequest {
                            caller: Some(caller.clone()),
                            name: input.name,
                            resources: Some(resources),
                        },
                    )),
                )
                .await?,
        );
    }
    let (template_id, preparation_id) = path
        .strip_prefix(PATH)
        .and_then(|path| path.strip_prefix('/'))
        .and_then(|path| path.split_once('/'))
        .filter(|(template, preparation)| {
            !template.is_empty()
                && !preparation.is_empty()
                && !preparation.contains('/')
                && template.len() <= 256
                && preparation.len() <= 256
        })
        .ok_or_else(|| Status::invalid_argument("template and preparation IDs required"))?;
    let lookup = pb::GetSnapshotPreparationRequest {
        caller: Some(caller.clone()),
        template_id: template_id.into(),
        preparation_id: preparation_id.into(),
    };
    match method.as_str() {
        "GET" => view(
            clients
                .rpc(
                    "apiserver.get_snapshot_preparation",
                    client.get_snapshot_preparation(trace::inject(lookup)),
                )
                .await?,
        ),
        "POST" => {
            let spec = seed_spec(input, caller, clients.config.runtime_profile.as_ref())?;
            let registered = clients
                .rpc(
                    "apiserver.get_snapshot_preparation",
                    client.get_snapshot_preparation(trace::inject(lookup)),
                )
                .await?;
            if spec.resources != registered.resources {
                return Err(Status::invalid_argument(
                    "seed must match registered resources",
                ));
            }
            view(
                clients
                    .rpc(
                        "apiserver.start_snapshot_preparation",
                        client.start_snapshot_preparation(trace::inject(
                            pb::StartSnapshotPreparationRequest {
                                caller: Some(caller.clone()),
                                template_id: template_id.into(),
                                preparation_id: preparation_id.into(),
                                spec: Some(spec),
                            },
                        )),
                    )
                    .await?,
            )
        }
        _ => Err(Status::unimplemented(
            "unsupported snapshot preparation method",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn preparation() -> pb::SnapshotPreparationView {
        pb::SnapshotPreparationView {
            template_id: "template".into(),
            name: "digitalmate".into(),
            preparation_id: "preparation".into(),
            resources: Some(pb::Resources::default()),
            snapshot_id: String::new(),
            error: String::new(),
            created_at_unix_ms: 0,
            image: "registry.test/image:v1".into(),
            envd_version: String::new(),
            phase: pb::SnapshotPreparationPhase::Registered as i32,
            snapshot_available: false,
            template_updated_at_unix_ms: 0,
            preparation_created_at_unix_ms: 0,
            preparation_updated_at_unix_ms: 0,
            last_spawned_at_unix_ms: None,
            spawn_count: None,
        }
    }

    #[test]
    fn route_matches_only_preparation_collection_and_members() {
        assert!(matches(PATH));
        assert!(matches(&format!("{PATH}/template/preparation")));
        assert!(!matches(&format!("{PATH}-other")));
        assert!(!matches("/api/environment/v1/snapshots/snapshot-1"));
    }

    #[test]
    fn native_seed_requires_a_fresh_managed_workload_and_probe() {
        let caller = pb::CallerContext {
            tenant_id: "test".into(),
            administrator: true,
        };
        let mut input = json!({
            "name":"seed", "image":"registry.test/image:v1", "runtime":"runsc",
            "inheritEntrypoint":true,
            "readinessProbe":{"exec":{"command":["/usr/bin/probe"]}}
        });
        let spec = seed_spec(input.clone(), &caller, None).unwrap();
        assert!(spec.execution.unwrap().readiness_probe.is_some());
        input.as_object_mut().unwrap().remove("readinessProbe");
        assert!(seed_spec(input, &caller, None).is_err());
        assert!(seed_spec(
            json!({"name":"seed","snapshotId":"existing"}),
            &caller,
            None
        )
        .is_err());
    }

    #[test]
    fn snapshot_is_returned_only_after_seed_cleanup() {
        let mut value = pb::SnapshotPreparationView {
            phase: pb::SnapshotPreparationPhase::Cleaning as i32,
            snapshot_id: "snapshot-1".into(),
            snapshot_available: true,
            ..preparation()
        };
        assert!(view(value.clone()).unwrap().get("snapshotId").is_none());
        value.phase = pb::SnapshotPreparationPhase::Ready as i32;
        assert_eq!(view(value).unwrap()["snapshotId"], "snapshot-1");
    }
}
