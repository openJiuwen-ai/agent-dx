use super::{dto::NetworkUpdateRequest, network};
use super::{
    ConnectRequest, CreateRequest, ErrorBody, PauseRequest, SnapshotRequest, TimeoutRequest,
    ValidationError,
};
use crate::{
    http::{body, Api, Body},
    operations::Kind,
};
use adx_observability::trace;
use adx_protocol::control as pb;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, Request, Response};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tonic::{Code, Status};

const BODY_LIMIT: usize = 64 * 1024;
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;
struct ListQuery {
    states: Vec<String>,
    metadata: HashMap<String, String>,
    offset: usize,
    limit: usize,
}
fn list_query(query: &str) -> Result<ListQuery, Status> {
    let values: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    if values
        .keys()
        .any(|key| !matches!(key.as_str(), "limit" | "nextToken" | "state" | "metadata"))
    {
        return Err(validation(ValidationError::unsupported(
            "list filter is not supported",
        )));
    }
    let limit = values.get("limit").map_or(Ok(100), |value| {
        value
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid limit"))
    })?;
    if !(1..=100).contains(&limit) {
        return Err(Status::invalid_argument("invalid limit"));
    }
    let offset = values.get("nextToken").map_or(Ok(0), |value| {
        value
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid nextToken"))
    })?;
    let states = values.get("state").map_or_else(
        || Ok(vec!["running".into()]),
        |value| {
            let states = value.split(',').map(str::to_owned).collect::<Vec<_>>();
            if states.is_empty()
                || states
                    .iter()
                    .any(|state| !matches!(state.as_str(), "running" | "paused"))
            {
                Err(Status::invalid_argument("invalid state"))
            } else {
                Ok(states)
            }
        },
    )?;
    let metadata = values.get("metadata").map_or_else(
        || Ok(HashMap::new()),
        |value| {
            if !value.is_empty() && value.split('&').any(|entry| !entry.contains('=')) {
                return Err(Status::invalid_argument("invalid metadata"));
            }
            // SDK encodes each key/value before encoding the nested query.
            url::form_urlencoded::parse(value.as_bytes())
                .map(|(key, value)| {
                    let decode = |value: &str| {
                        percent_encoding::percent_decode_str(value)
                            .decode_utf8()
                            .map(|v| v.into_owned())
                            .map_err(|_| Status::invalid_argument("invalid metadata encoding"))
                    };
                    Ok((decode(&key)?, decode(&value)?))
                })
                .collect()
        },
    )?;
    Ok(ListQuery {
        states,
        metadata,
        offset,
        limit,
    })
}

pub fn route(path: &str) -> bool {
    super::keys::route(path)
        || matches!(path, "/v2/sandboxes" | "/snapshots")
        || path.starts_with("/sandboxes/")
        || path.starts_with("/v2/sandboxes/")
        || path.starts_with("/templates/")
}
pub async fn handle(
    api: Arc<Api>,
    request: Request<Incoming>,
    caller: pb::CallerContext,
) -> Response<Body> {
    if api.clients.config.e2b.is_none() {
        return error(Status::unimplemented("E2B API is disabled"));
    }
    let path = request.uri().path().trim_end_matches('/').to_owned();
    let query = request.uri().query().unwrap_or("").to_owned();
    let result = if super::keys::route(&path) {
        super::keys::handle(&api, request, &path, &caller).await
    } else {
        match (request.method().as_str(), path.as_str()) {
            // Creating a sandbox builds a large lifecycle/RPC future. Keep that
            // polling chain off the default HTTP worker frame.
            ("POST", "/v2/sandboxes") => Box::pin(create(&api, request, &caller))
                .await
                .map(|v| (201, v)),
            ("GET", "/v2/sandboxes") => list(&api, &query, &caller).await.map(|v| (200, v)),
            ("GET", "/snapshots") => snapshots(&api, &query, &caller).await.map(|v| (200, v)),
            _ => action(&api, request, &path, &caller).await,
        }
    };
    let mut response = match result {
        Ok((status, value)) => raw(status, value),
        Err(status) => error(status),
    };
    if super::keys::route(&path) {
        response.headers_mut().insert(
            "cache-control",
            hyper::header::HeaderValue::from_static("no-store"),
        );
    }
    response
}
async fn create(
    api: &Api,
    request: Request<Incoming>,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let input: CreateRequest = decode(request).await?;
    input.validate().map_err(validation)?;
    let (network, public_traffic) = input.network_configuration().map_err(validation)?;
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    let resolved = config.templates.resolve(&input.template_id);
    let snapshot = if resolved.is_err() {
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            api.clients.coordinator().await?,
        );
        Some(
            Box::pin(api.clients.rpc(
                "apiserver.e2b_create_snapshot_source",
                client.get_snapshot(trace::inject(pb::GetSnapshotRequest {
                    id: input.template_id.clone(),
                    caller: Some(caller.clone()),
                    node_session_id: String::new(),
                })),
            ))
            .await?,
        )
    } else {
        None
    };
    let snapshot_template_id = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.template.as_ref())
        .and_then(|spec| spec.compatibility.as_ref())
        .map(|compatibility| compatibility.template_id.as_str());
    let (template, template_id) = match snapshot_template_id {
        Some(template_id) => config.templates.resolve(template_id).map_err(validation)?,
        None => resolved.map_err(|error| {
            if snapshot.is_some() {
                Status::not_found("snapshot is not E2B-compatible")
            } else {
                validation(error)
            }
        })?,
    };
    let mut profile = config.runtime_profile(template).map_err(validation)?;
    template.require_capability("envd").map_err(validation)?;
    if snapshot.is_some() {
        template
            .require_capability("reusableSnapshot")
            .map_err(validation)?;
    }
    profile.env.insert(
        "ADX_ENVD_DEFAULT_USER".into(),
        template.default_user.clone(),
    );
    profile.env.insert(
        "ADX_ENVD_DEFAULT_WORKDIR".into(),
        template.default_workdir.clone(),
    );
    let id = format!("e2b-{}", uuid::Uuid::new_v4());
    let timeout = input.timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    let allow_public_traffic = public_traffic.unwrap_or(template.allow_public_traffic);
    let timeout_action = if input.auto_pause == Some(true) {
        "pause".into()
    } else {
        template.timeout_action.clone()
    };
    if timeout_action == "pause" {
        template
            .require_capability("memoryCheckpoint")
            .map_err(validation)?;
    }
    let snapshot_spec = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.template.as_ref());
    let resources = snapshot_spec
        .and_then(|spec| spec.resources)
        .unwrap_or(pb::Resources {
            cpu_millis: template.cpu_millicores,
            memory_bytes: template
                .memory_mib
                .checked_mul(1_048_576)
                .ok_or_else(|| Status::invalid_argument("template memory overflows"))?,
            disk_bytes: 0,
        });
    let runtime_class = snapshot_spec
        .map(|spec| spec.runtime_class.clone())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| profile.rootfs.runtime_class.clone());
    let mut runtime_profile = snapshot_spec
        .and_then(|spec| spec.runtime_profile.clone())
        .unwrap_or_else(|| profile.clone().into());
    runtime_profile.env.insert(
        "ADX_ENVD_DEFAULT_USER".into(),
        template.default_user.clone(),
    );
    runtime_profile.env.insert(
        "ADX_ENVD_DEFAULT_WORKDIR".into(),
        template.default_workdir.clone(),
    );
    let spec = pb::EnvironmentSpec {
        id: id.clone(),
        tenant_id: caller.tenant_id.clone(),
        runtime_class,
        runtime_profile: Some(runtime_profile),
        resources: Some(resources),
        lifecycle: Some(pb::LifecyclePolicy {
            idle_timeout_seconds: 0,
            restart: None,
            ..Default::default()
        }),
        env: input.env_vars.into_iter().collect(),
        compatibility: Some(pb::EnvironmentCompatibility {
            template_id,
            envd_version: config.envd_version.clone(),
            envd_access_token: uuid::Uuid::new_v4().to_string(),
            traffic_access_token: uuid::Uuid::new_v4().to_string(),
            allow_public_traffic,
            timeout_seconds: timeout,
            timeout_action,
            pause_retention_seconds: template.pause_retention_seconds,
            metadata: input.metadata.into_iter().collect(),
        }),
        snapshot_id: snapshot.as_ref().map(|value| value.id.clone()),
        execution: Some(pb::ExecutionOptions {
            network: network.map(Into::into),
            ..Default::default()
        }),
        ..Default::default()
    };
    // Coordinator canonicalizes Environment specs through adx-core before it
    // publishes them. Apply that same validated round trip before recording
    // this create identity, so replay comparison sees scheduling and sandbox
    // defaults in their durable form.
    let spec = adx_core::EnvironmentSpec::try_from(spec)
        .map(pb::EnvironmentSpec::from)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let request_id = operation_id("create");
    Box::pin(
        api.environment_service
            .create(spec, json!({"e2b":true}), &request_id, caller),
    )
    .await?;
    let control = Box::pin(api.clients.environment_control(&id, caller)).await?;
    Box::pin(sandbox_detail(api, &control, caller)).await
}
async fn list(api: &Api, query: &str, caller: &pb::CallerContext) -> Result<Value, Status> {
    let query = list_query(query)?;
    let mut records = api.clients.list_environment_controls(caller).await?;
    records.sort_by(|left, right| left.environment_id.cmp(&right.environment_id));
    let mut matches = Vec::new();
    for record in records {
        let mut value = match sandbox_detail(api, &record, caller).await {
            Ok(value) => value,
            Err(status) if matches!(status.code(), Code::FailedPrecondition | Code::NotFound) => {
                continue
            }
            Err(status) => return Err(status),
        };
        let state = value["state"].as_str().unwrap_or_default();
        let metadata = value["metadata"].as_object();
        if !query.states.iter().any(|expected| expected == state)
            || query.metadata.iter().any(|(key, expected)| {
                metadata
                    .and_then(|value| value.get(key))
                    .and_then(Value::as_str)
                    != Some(expected.as_str())
            })
        {
            continue;
        }
        if let Some(value) = value.as_object_mut() {
            value.remove("envdAccessToken");
            value.remove("trafficAccessToken");
        }
        matches.push(value);
    }
    let next_offset = query.offset.saturating_add(query.limit);
    let next = if next_offset < matches.len() {
        next_offset.to_string()
    } else {
        String::new()
    };
    Ok(
        json!({"items":matches.into_iter().skip(query.offset).take(query.limit).collect::<Vec<_>>(),"nextToken":next}),
    )
}
async fn action(
    api: &Api,
    request: Request<Incoming>,
    path: &str,
    caller: &pb::CallerContext,
) -> Result<(u16, Value), Status> {
    let (prefix, rest) = if let Some(v) = path.strip_prefix("/v2/sandboxes/") {
        ("v2", v)
    } else if let Some(v) = path.strip_prefix("/sandboxes/") {
        ("sandbox", v)
    } else if let Some(v) = path.strip_prefix("/templates/") {
        ("template", v)
    } else {
        return Err(Status::not_found("route not found"));
    };
    let (id, operation) = rest.split_once('/').unwrap_or((rest, ""));
    if id.is_empty() {
        return Err(Status::not_found("route not found"));
    }
    match (request.method().as_str(), prefix, operation) {
        ("PUT", "sandbox", "network") => update_network(api, request, id, caller).await,
        ("GET", "sandbox", "") => Ok((
            200,
            sandbox_detail(
                api,
                &api.clients.environment_control(id, caller).await?,
                caller,
            )
            .await?,
        )),
        ("DELETE", "sandbox", "") => {
            api.environment_service
                .execute(Kind::Delete, id, &operation_id("delete"), json!({}), caller)
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "timeout") => {
            let input: TimeoutRequest = decode(request).await?;
            input.validate().map_err(validation)?;
            api.clients
                .update_environment_control(id, caller, input.timeout, false)
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "pause") => {
            let input = pause_request(body(request.into_body(), BODY_LIMIT).await?)?;
            input.validate().map_err(validation)?;
            let template = sandbox_template(api, id, caller).await?;
            template
                .require_capability("memoryCheckpoint")
                .map_err(validation)?;
            api.environment_service
                .execute(
                    Kind::Pause,
                    id,
                    &operation_id("pause"),
                    json!({"retention":{"ttlSeconds":template.pause_retention_seconds}}),
                    caller,
                )
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "snapshots") => {
            sandbox_template(api, id, caller)
                .await?
                .require_capability("reusableSnapshot")
                .map_err(validation)?;
            let value = body(request.into_body(), BODY_LIMIT).await?;
            if !value.is_object() {
                return Err(Status::invalid_argument(
                    "snapshot request must be an object",
                ));
            }
            let input: SnapshotRequest = serde_json::from_value(value).map_err(|_| {
                validation(ValidationError::unsupported(
                    "snapshot option is not supported",
                ))
            })?;
            input.validate().map_err(validation)?;
            let value = api
                .environment_service
                .execute(
                    Kind::Snapshot,
                    id,
                    &operation_id("snapshot"),
                    snapshot_payload(input),
                    caller,
                )
                .await?;
            Ok((
                201,
                json!({"snapshotID": value["snapshotId"], "names": value["names"]}),
            ))
        }
        ("POST", "v2", "connect") => connect(api, request, id, caller).await,
        ("DELETE", "template", "") => {
            delete_snapshot(api, id, caller).await?;
            Ok((204, Value::Null))
        }
        _ => Err(Status::not_found("route not found")),
    }
}
async fn update_network(
    api: &Api,
    request: Request<Incoming>,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<(u16, Value), Status> {
    let input: NetworkUpdateRequest = decode(request).await?;
    // Reject unsupported semantics before performing an ownership lookup.
    input.policy(None).map_err(validation)?;
    let owner = api.clients.owner(id, caller, true).await?;
    let record = owner
        .record
        .ok_or_else(|| Status::data_loss("missing environment record"))?;
    let spec = record
        .spec
        .as_ref()
        .filter(|spec| spec.compatibility.is_some())
        .ok_or_else(|| Status::not_found("E2B sandbox not found"))?;
    if record.state != pb::EnvironmentState::Running as i32 {
        return Err(Status::failed_precondition(
            "network updates require a running sandbox",
        ));
    }
    let baseline = spec
        .execution
        .as_ref()
        .and_then(|execution| execution.network.clone())
        .map(adx_core::execution::NetworkPolicy::try_from)
        .transpose()
        .map_err(|_| Status::data_loss("invalid stored network policy"))?;
    let policy = input.policy(baseline.as_ref()).map_err(validation)?;
    api.environment_service
        .execute_at_revision(
            Kind::Network,
            id,
            &operation_id("network"),
            network::native_payload(Some(&policy)),
            caller,
            record.revision,
        )
        .await?;
    Ok((204, Value::Null))
}
async fn connect(
    api: &Api,
    request: Request<Incoming>,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<(u16, Value), Status> {
    let input: ConnectRequest =
        serde_json::from_value(body(request.into_body(), BODY_LIMIT).await?)
            .map_err(|_| Status::invalid_argument("invalid connect request"))?;
    input.validate().map_err(validation)?;
    let timeout = input.timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    api.clients
        .update_environment_control(id, caller, timeout, true)
        .await?;
    let owner = api
        .environment_service
        .inspect(&caller.tenant_id, id)
        .await?
        .ok_or_else(|| Status::not_found("sandbox not found"))?;
    let paused = owner
        .record
        .as_ref()
        .is_some_and(|r| r.state == pb::EnvironmentState::Paused as i32);
    if paused {
        api.environment_service
            .execute(Kind::Resume, id, &operation_id("resume"), json!({}), caller)
            .await?;
    }
    let control = api.clients.environment_control(id, caller).await?;
    Ok((
        if paused { 201 } else { 200 },
        sandbox_detail(api, &control, caller).await?,
    ))
}
fn snapshot_payload(input: SnapshotRequest) -> Value {
    input
        .name
        .map_or_else(|| json!({}), |name| json!({"name": name}))
}
fn pause_request(value: Value) -> Result<PauseRequest, Status> {
    serde_json::from_value(value).map_err(|_| Status::invalid_argument("invalid E2B request"))
}
async fn snapshots(api: &Api, query: &str, caller: &pb::CallerContext) -> Result<Value, Status> {
    let query: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    if query
        .keys()
        .any(|key| !matches!(key.as_str(), "limit" | "nextToken" | "name" | "sandboxID"))
    {
        return Err(validation(ValidationError::unsupported(
            "snapshot filter is not supported",
        )));
    }
    let size = query.get("limit").map_or(Ok(100_u32), |v| {
        v.parse::<u32>()
            .map_err(|_| Status::invalid_argument("invalid snapshot limit"))
    })?;
    if !(1..=100).contains(&size) {
        return Err(Status::invalid_argument("invalid snapshot limit"));
    }
    let mut client =
        pb::snapshot_service_client::SnapshotServiceClient::new(api.clients.coordinator().await?);
    let page = api
        .clients
        .rpc(
            "apiserver.e2b_list_snapshots",
            client.list_snapshots(trace::inject(pb::ListSnapshotsRequest {
                caller: Some(caller.clone()),
                name: query.get("name").cloned().unwrap_or_default(),
                page_token: query.get("nextToken").cloned().unwrap_or_default(),
                page_size: size,
            })),
        )
        .await?;
    let snapshots = page
        .snapshots
        .into_iter()
        .filter(|snapshot| {
            snapshot.state == pb::SnapshotState::Ready as i32
                && snapshot.template.as_ref().is_some_and(|spec| {
                    spec.compatibility.is_some()
                        && query.get("sandboxID").is_none_or(|id| id == &spec.id)
                })
        })
        .map(|snapshot| json!({"snapshotID":snapshot.id,"names":snapshot.names}))
        .collect::<Vec<_>>();
    Ok(json!({"snapshots": snapshots, "nextToken":page.next_page_token}))
}
async fn sandbox_template(
    api: &Api,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<super::Template, Status> {
    let control = api.clients.environment_control(id, caller).await?;
    let compatibility = control
        .compatibility
        .ok_or_else(|| Status::not_found("sandbox is not E2B-compatible"))?;
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    Ok(config
        .templates
        .resolve(&compatibility.template_id)
        .map_err(validation)?
        .0
        .clone())
}
async fn delete_snapshot(api: &Api, id: &str, caller: &pb::CallerContext) -> Result<(), Status> {
    let mut client =
        pb::snapshot_service_client::SnapshotServiceClient::new(api.clients.coordinator().await?);
    let value = api
        .clients
        .rpc(
            "apiserver.e2b_delete_snapshot",
            client.delete_snapshot(trace::inject(pb::DeleteSnapshotRequest {
                id: id.into(),
                caller: Some(caller.clone()),
            })),
        )
        .await?;
    if value.id != id
        || !matches!(
            pb::SnapshotState::try_from(value.state),
            Ok(pb::SnapshotState::Deleting | pb::SnapshotState::Deleted)
        )
    {
        return Err(Status::data_loss("snapshot deletion not confirmed"));
    }
    Ok(())
}
pub(super) async fn decode<T: serde::de::DeserializeOwned>(
    request: Request<Incoming>,
) -> Result<T, Status> {
    serde_json::from_value(body(request.into_body(), BODY_LIMIT).await?)
        .map_err(|_| Status::invalid_argument("invalid E2B request"))
}
fn operation_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}
async fn sandbox_detail(
    api: &Api,
    control: &pb::EnvironmentControlRecord,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let owner = api
        .clients
        .owner(&control.environment_id, caller, false)
        .await?;
    let record = owner
        .record
        .ok_or_else(|| Status::data_loss("missing environment record"))?;
    let resources = record
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .ok_or_else(|| Status::data_loss("missing environment resources"))?;
    let state = if record.state == pb::EnvironmentState::Paused as i32 {
        "paused"
    } else if record.state == pb::EnvironmentState::Running as i32 {
        "running"
    } else {
        return Err(Status::failed_precondition("sandbox is not available"));
    };
    let compatibility = control
        .compatibility
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing sandbox compatibility"))?;
    let domain = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?
        .domain
        .clone();
    Ok(
        json!({"templateID":compatibility.template_id,"sandboxID":control.environment_id,"clientID":"adx","envdVersion":compatibility.envd_version,"state":state,"startedAt":rfc3339(control.started_at_unix_ms),"endAt":rfc3339(control.expires_at_unix_ms),"cpuCount":resources.cpu_millis / 1000,"memoryMB":resources.memory_bytes / 1_048_576,"diskSizeMB":resources.disk_bytes / 1_048_576,"metadata":compatibility.metadata,"envdAccessToken":compatibility.envd_access_token,"trafficAccessToken":compatibility.traffic_access_token,"domain":domain}),
    )
}
fn rfc3339(milliseconds: u64) -> String {
    DateTime::<Utc>::from_timestamp_millis(milliseconds.min(i64::MAX as u64) as i64)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".into())
}
pub(super) fn validation(error: ValidationError) -> Status {
    let mut status = Status::invalid_argument(error.message);
    if let Ok(value) = error.error_code.parse() {
        status.metadata_mut().insert("e2b-error-code", value);
    }
    status
}
fn raw(status: u16, mut value: Value) -> Response<Body> {
    let next = value
        .get("nextToken")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let items = if value.get("snapshots").is_some() {
        "snapshots"
    } else {
        "items"
    };
    if value.get(items).is_some() {
        value = value
            .get_mut(items)
            .map(std::mem::take)
            .unwrap_or(Value::Array(vec![]));
    }
    let bytes = if status == 204 {
        Vec::new()
    } else {
        serde_json::to_vec(&value).unwrap_or_default()
    };
    let mut response = Response::new(
        Full::new(Bytes::from(bytes))
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() =
        hyper::StatusCode::from_u16(status).unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR);
    response.headers_mut().insert(
        "content-type",
        hyper::header::HeaderValue::from_static("application/json"),
    );
    if let Some(next) = next.filter(|value| !value.is_empty()) {
        match next.parse() {
            Ok(next) => {
                response.headers_mut().insert("x-next-token", next);
            }
            Err(_) => {
                return raw(
                    500,
                    json!({"code":500,"message":"invalid pagination result"}),
                )
            }
        }
    }
    response
}
fn error(status: Status) -> Response<Body> {
    let code = match status.code() {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => 409,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::DeadlineExceeded => 504,
        _ => 500,
    };
    let error_code = status
        .metadata()
        .get("e2b-error-code")
        .and_then(|value| value.to_str().ok());
    raw(
        code,
        serde_json::to_value(ErrorBody {
            code,
            message: status.message(),
            error_code,
        })
        .unwrap_or_else(|_| json!({"code":code,"message":"error"})),
    )
}
pub fn authentication_error(status: Status) -> Response<Body> {
    error(
        if matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded) {
            Status::unavailable("authentication service unavailable")
        } else {
            Status::unauthenticated("authentication failed")
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recognizes_only_e2b_management_routes() {
        assert!(route("/v2/sandboxes"));
        assert!(route("/sandboxes/sandbox-1/timeout"));
        assert!(route("/templates/snapshot-1"));
        assert!(!route("/api/sandbox/v1/sandboxes/sandbox-1"));
    }

    #[test]
    fn operation_ids_match_the_lifecycle_adapter_prefix_contract() {
        for prefix in ["create", "delete", "pause", "resume", "snapshot"] {
            let value = operation_id(prefix);
            assert!(value.starts_with(&format!("{prefix}-")));
            assert!(value.len() <= 128);
        }
    }

    #[test]
    fn timestamps_use_rfc3339_utc_milliseconds() {
        assert_eq!(rfc3339(1_000), "1970-01-01T00:00:01.000Z");
    }

    #[test]
    fn snapshot_memory_selector_preserves_the_existing_snapshot_payload() {
        assert_eq!(
            snapshot_payload(SnapshotRequest {
                name: None,
                memory: Some(true),
            }),
            json!({})
        );
        assert_eq!(
            snapshot_payload(SnapshotRequest {
                name: Some("checkpoint".into()),
                memory: None,
            }),
            json!({"name": "checkpoint"})
        );
    }

    #[test]
    fn pause_request_accepts_the_empty_body_normalized_to_an_empty_object() {
        for value in [json!({}), json!({"memory": null}), json!({"memory": true})] {
            assert!(pause_request(value).unwrap().validate().is_ok());
        }
    }

    #[test]
    fn list_query_defaults_and_rejects_malformed_cursors() {
        let query =
            list_query("state=running,paused&metadata=team%3Dcore&limit=2&nextToken=3").unwrap();
        assert_eq!(query.states, ["running", "paused"]);
        assert_eq!(query.metadata["team"], "core");
        assert_eq!((query.limit, query.offset), (2, 3));
        for query in [
            "limit=0",
            "limit=101",
            "nextToken=bad",
            "state=deleted",
            "metadata=bad",
        ] {
            assert!(list_query(query).is_err());
        }
    }
    #[test]
    fn list_metadata_matches_sdk_nested_url_encoding() {
        let query = list_query("metadata=team%3Dcore%26purpose%3Dhello%252520world").unwrap();
        assert_eq!(query.metadata["team"], "core");
        assert_eq!(query.metadata["purpose"], "hello world");
        assert!(list_query("order=desc").is_err());
    }
    #[tokio::test]
    async fn list_response_is_raw_array_with_pagination_header() {
        let response = raw(200, json!({"items":[{"sandboxID":"a"}],"nextToken":"1"}));
        assert_eq!(response.headers()["x-next-token"], "1");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!([{"sandboxID":"a"}])
        );
    }
}
