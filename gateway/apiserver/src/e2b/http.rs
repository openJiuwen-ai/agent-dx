use super::dto::{NetworkUpdateRequest, ResumeRequest, SandboxSummary};
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
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, num::NonZeroUsize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tonic::{Code, Status};

const BODY_LIMIT: usize = 64 * 1024;
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;
const TEMPLATE_ALIAS_PREFIX: &str = "/templates/aliases/";
const MAX_TEMPLATE_ALIAS_BYTES: usize = 256;

fn template_alias(path: &str) -> Result<String, Status> {
    let encoded = path
        .strip_prefix(TEMPLATE_ALIAS_PREFIX)
        .ok_or_else(|| Status::not_found("route not found"))?;
    if encoded.is_empty() || encoded.contains('/') {
        return Err(Status::invalid_argument("invalid template alias path"));
    }
    let alias = percent_encoding::percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|_| Status::invalid_argument("invalid template alias path"))?
        .into_owned();
    if alias.is_empty()
        || alias.len() > MAX_TEMPLATE_ALIAS_BYTES
        || alias.contains('/')
        || alias.contains('%')
        || alias.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument("invalid template alias path"));
    }
    Ok(alias)
}

struct ListQuery {
    states: Vec<String>,
    metadata: BTreeMap<String, String>,
    cursor: String,
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
    let cursor = values.get("nextToken").cloned().unwrap_or_default();
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
        || Ok(BTreeMap::new()),
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
        cursor,
        limit,
    })
}

pub fn route(path: &str) -> bool {
    super::keys::route(path)
        || matches!(
            path,
            "/sandboxes" | "/v2/sandboxes" | "/snapshots" | "/v3/templates" | "/v2/templates"
        )
        || path.starts_with("/sandboxes/")
        || path.starts_with("/v2/sandboxes/")
        || path.starts_with("/templates/")
        || path.starts_with("/v2/templates/")
}
pub async fn handle(
    api: Arc<Api>,
    request: Request<Incoming>,
    caller: pb::CallerContext,
) -> Response<Body> {
    if api.clients.config.e2b.is_none() {
        return error(Status::unimplemented("E2B API is disabled"));
    }
    let raw_path = request.uri().path().to_owned();
    let path = raw_path.trim_end_matches('/').to_owned();
    let query = request.uri().query().unwrap_or("").to_owned();
    let result = if super::keys::route(&path) {
        super::keys::handle(&api, request, &path, &caller).await
    } else {
        match (request.method().as_str(), path.as_str()) {
            // Creating a sandbox builds a large lifecycle/RPC future. Keep that
            // polling chain off the default HTTP worker frame.
            ("POST", "/sandboxes" | "/v2/sandboxes") => Box::pin(create(&api, request, &caller))
                .await
                .map(|v| (201, v)),
            ("GET", _) if raw_path.starts_with(TEMPLATE_ALIAS_PREFIX) => {
                template_alias_lookup(&api, &raw_path, &caller)
                    .await
                    .map(|v| (200, v))
            }
            ("POST", "/v3/templates") => match decode(request).await {
                Ok(input) => super::template_api::register(&api, input, &caller)
                    .await
                    .map(|value| (202, value)),
                Err(error) => Err(error),
            },
            ("GET", "/v2/templates") => Err(validation(ValidationError::unsupported(
                "template history list is not supported; use the native Golden Snapshot list",
            ))),
            ("POST", _) if path.starts_with("/v2/templates/") => {
                let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
                if parts.len() == 5 && parts[3] == "builds" {
                    match decode(request).await {
                        Ok(input) => {
                            super::template_api::start(&api, parts[2], parts[4], input, &caller)
                                .await
                                .map(|value| (202, value))
                        }
                        Err(error) => Err(error),
                    }
                } else {
                    Err(Status::not_found("template route not found"))
                }
            }
            ("GET", _) if path.starts_with("/templates/") && path.contains("/files/") => {
                let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
                if parts.len() == 4 && parts[2] == "files" {
                    super::template_api::upload_link(&api, parts[1], parts[3], &caller)
                        .await
                        .map(|value| (201, value))
                } else {
                    Err(Status::not_found("template file route not found"))
                }
            }
            ("GET", _) if path.starts_with("/templates/") && path.ends_with("/status") => {
                let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
                if parts.len() == 5 && parts[2] == "builds" {
                    super::template_api::status(&api, parts[1], parts[3], &caller, &query)
                        .await
                        .map(|value| (200, value))
                } else {
                    Err(Status::not_found("template route not found"))
                }
            }
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
fn alias_resolution_request(
    reference: &str,
    caller: &pb::CallerContext,
) -> Result<pb::ResolveSnapshotTemplateRequest, Status> {
    super::template_api::resolution_request(reference, caller, true)
}

fn registered_alias(
    value: pb::SnapshotPreparationView,
    registration_only: bool,
) -> Result<Value, Status> {
    if value.template_id.is_empty() {
        return Err(Status::data_loss(
            "registered alias has no template identity",
        ));
    }
    if !registration_only
        && (value.phase != pb::SnapshotPreparationPhase::Ready as i32
            || !value.snapshot_available
            || value.snapshot_id.is_empty())
    {
        return Err(Status::not_found("template tag has no ready preparation"));
    }
    Ok(json!({"public": false, "templateID": value.template_id}))
}

fn alias_resolution_error(error: Status, registration_only: bool) -> Status {
    if !registration_only && error.code() == Code::FailedPrecondition {
        Status::not_found("template tag has no ready preparation")
    } else {
        error
    }
}

async fn template_alias_lookup(
    api: &Api,
    path: &str,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let alias = template_alias(path)?;
    let request = alias_resolution_request(&alias, caller)?;
    let registration_only = request.registration_only;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    let value = api
        .clients
        .rpc(
            "apiserver.template_alias_registration",
            client.resolve_snapshot_template(trace::inject(request)),
        )
        .await
        .map_err(|error| alias_resolution_error(error, registration_only))?;
    registered_alias(value, registration_only)
}

async fn create(
    api: &Api,
    request: Request<Incoming>,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let input: CreateRequest = decode(request).await?;
    input.validate().map_err(validation)?;
    let spec = Box::pin(super::templates::create_spec(api, &input, caller)).await?;
    let id = spec.id.clone();
    let request_id = operation_id("create");
    Box::pin(
        api.environment_service
            .create(spec, json!({"e2b":true}), &request_id, caller),
    )
    .await
    .map_err(create_execution_error)?;
    let control = Box::pin(api.clients.environment_control(&id, caller))
        .await
        .map_err(create_execution_error)?;
    Box::pin(sandbox_detail(api, &control, caller))
        .await
        .map_err(create_execution_error)
}
fn create_execution_error(status: Status) -> Status {
    // E2B SDKs may retry a create after 503 with a new sandbox identity.
    // Once creation is attempted, Unavailable cannot prove that nothing started.
    if status.code() == Code::Unavailable {
        Status::with_details_and_metadata(
            Code::Internal,
            status.message().to_owned(),
            Bytes::copy_from_slice(status.details()),
            status.metadata().clone(),
        )
    } else {
        status
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCursor {
    after: String,
    query_digest: String,
}

fn list_digest(query: &ListQuery, caller: &pb::CallerContext) -> Result<String, Status> {
    let states: BTreeSet<_> = query.states.iter().collect();
    let bytes = serde_json::to_vec(&(
        "e2b",
        &caller.tenant_id,
        caller.administrator,
        states,
        &query.metadata,
    ))
    .map_err(|_| Status::internal("list query serialization failed"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
fn list_after(token: &str, digest: &str) -> Result<Option<String>, Status> {
    if token.is_empty() {
        return Ok(None);
    }
    if token.len() > 8192 {
        return Err(Status::invalid_argument("invalid list cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| Status::invalid_argument("invalid list cursor"))?;
    let cursor: ListCursor = serde_json::from_slice(&bytes)
        .map_err(|_| Status::invalid_argument("invalid list cursor"))?;
    if cursor.after.is_empty() || cursor.query_digest != digest {
        return Err(Status::invalid_argument(
            "list cursor belongs to a different query",
        ));
    }
    Ok(Some(cursor.after))
}
fn list_token(after: Option<String>, digest: &str) -> Result<String, Status> {
    let Some(after) = after else {
        return Ok(String::new());
    };
    serde_json::to_vec(&ListCursor {
        after,
        query_digest: digest.into(),
    })
    .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
    .map_err(|_| Status::internal("list cursor serialization failed"))
}
fn list_summary(
    record: &pb::EnvironmentRecord,
    control: &pb::EnvironmentControlRecord,
) -> Result<SandboxSummary, Status> {
    let spec = record
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing environment spec"))?;
    let compatibility = spec
        .compatibility
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("not an E2B sandbox"))?;
    let envd = spec
        .envd
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing envd configuration"))?;
    let resources = spec
        .resources
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing resources"))?;
    Ok(SandboxSummary {
        template_id: compatibility.template_id.clone(),
        sandbox_id: spec.id.clone(),
        client_id: "adx",
        envd_version: envd.version.clone(),
        state: if record.state == pb::EnvironmentState::Paused as i32 {
            "paused"
        } else {
            "running"
        },
        started_at: rfc3339(control.started_at_unix_ms),
        end_at: sandbox_end_at(record, control),
        cpu_count: resources.cpu_millis / 1000,
        memory_mb: resources.memory_bytes / 1_048_576,
        disk_size_mb: resources.disk_bytes / 1_048_576,
        metadata: spec
            .scheduling
            .as_ref()
            .map(|policy| policy.labels.clone())
            .unwrap_or_default(),
    })
}
fn sandbox_end_at(
    record: &pb::EnvironmentRecord,
    control: &pb::EnvironmentControlRecord,
) -> String {
    if record.state == pb::EnvironmentState::Paused as i32 {
        // A paused VM has no running timer. Report its real restore-point retention,
        // using the SDK's representable far-future date for unbounded retention.
        let expiry = record
            .checkpoint
            .as_ref()
            .and_then(|point| point.expires_at_unix_seconds);
        return expiry
            .map(|seconds| rfc3339(seconds.saturating_mul(1000)))
            .unwrap_or_else(|| "9999-12-31T23:59:59Z".into());
    }
    rfc3339(control.expires_at_unix_ms)
}
async fn list(api: &Api, query: &str, caller: &pb::CallerContext) -> Result<Value, Status> {
    let query = list_query(query)?;
    let digest = list_digest(&query, caller)?;
    let after = list_after(&query.cursor, &digest)?;
    let states: BTreeSet<_> = query
        .states
        .iter()
        .map(|state| {
            if state == "paused" {
                pb::EnvironmentState::Paused as i32
            } else {
                pb::EnvironmentState::Running as i32
            }
        })
        .collect();
    let selector = adx_core::scheduling::LabelSelector {
        match_labels: query.metadata,
        ..Default::default()
    };
    selector.validate().map_err(adx_protocol::status)?;
    let limit =
        NonZeroUsize::new(query.limit).ok_or_else(|| Status::invalid_argument("invalid limit"))?;
    let page = api
        .clients
        .filtered_environments(caller, after.as_deref(), limit, |record| {
            let spec = record
                .spec
                .as_ref()
                .ok_or_else(|| Status::data_loss("missing environment spec"))?;
            let labels = spec
                .scheduling
                .as_ref()
                .map(|policy| policy.labels.clone())
                .unwrap_or_default();
            Ok(spec.compatibility.is_some()
                && states.contains(&record.state)
                && selector.matches(&labels.into_iter().collect()))
        })
        .await?;
    let ids = page
        .entries
        .iter()
        .filter_map(|owner| {
            owner
                .record
                .as_ref()?
                .spec
                .as_ref()
                .map(|spec| spec.id.clone())
        })
        .collect();
    let mut controls = BTreeMap::new();
    if !page.entries.is_empty() {
        for control in api.clients.batch_environment_control(caller, ids).await? {
            if controls
                .insert(control.environment_id.clone(), control)
                .is_some()
            {
                return Err(Status::data_loss("duplicate environment control response"));
            }
        }
    }
    let mut items = Vec::new();
    let mut response_bytes = 2usize;
    for owner in page.entries {
        let record = owner
            .record
            .as_ref()
            .ok_or_else(|| Status::data_loss("missing environment record"))?;
        let assignment = record
            .assignment
            .as_ref()
            .ok_or_else(|| Status::data_loss("missing assignment"))?;
        let Some(control) = controls.get(&assignment.environment_id) else {
            continue;
        };
        if control.generation != assignment.generation {
            continue;
        }
        let summary = list_summary(record, control)?;
        let item = serde_json::to_value(summary)
            .map_err(|_| Status::internal("list summary serialization failed"))?;
        response_bytes = response_bytes
            .saturating_add(
                serde_json::to_vec(&item)
                    .map_err(|_| Status::internal("list summary serialization failed"))?
                    .len(),
            )
            .saturating_add(1);
        if response_bytes > 1024 * 1024 {
            return Err(Status::resource_exhausted(
                "sandbox list response too large; reduce limit",
            ));
        }
        items.push(item);
    }
    Ok(json!({"items":items,"nextToken":list_token(page.next_after, &digest)?}))
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
            sandbox_owner(api, id, caller, true).await?;
            api.environment_service
                .execute(Kind::Delete, id, &operation_id("delete"), json!({}), caller)
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "timeout") => {
            let input: TimeoutRequest = decode(request).await?;
            input.validate().map_err(validation)?;
            sandbox_owner(api, id, caller, true).await?;
            api.clients
                .update_environment_control(id, caller, input.timeout, false)
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "pause") => {
            let input = pause_request(body(request.into_body(), BODY_LIMIT).await?)?;
            input.validate().map_err(validation)?;
            sandbox_owner(api, id, caller, true).await?;
            api.environment_service
                .execute(Kind::Pause, id, &operation_id("pause"), json!({}), caller)
                .await?;
            Ok((204, Value::Null))
        }
        ("POST", "sandbox", "snapshots") => {
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
            sandbox_owner(api, id, caller, true).await?;
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
        ("POST", "sandbox", "resume") => resume(api, request, id, caller).await,
        ("GET", "template", "") => {
            super::template_api::detail(api, id, request.uri().query().unwrap_or(""), caller)
                .await
                .map(|value| (200, value))
        }
        ("DELETE", "template", "") => {
            if !super::template_api::delete(api, id, caller).await? {
                delete_snapshot(api, id, caller).await?;
            }
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
    let owner = sandbox_owner(api, id, caller, true).await?;
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
        .update_network_at_revision(
            id,
            &operation_id("network"),
            Some(policy),
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
    let owner = sandbox_owner(api, id, caller, true).await?;
    let paused = owner
        .record
        .as_ref()
        .is_some_and(|r| r.state == pb::EnvironmentState::Paused as i32);
    if paused {
        api.environment_service
            .resume_with_run_timeout(id, caller, timeout)
            .await?;
    }
    api.clients
        .update_environment_control(id, caller, timeout, true)
        .await?;
    let control = api.clients.environment_control(id, caller).await?;
    Ok((
        if paused { 201 } else { 200 },
        sandbox_detail(api, &control, caller).await?,
    ))
}

async fn resume(
    api: &Api,
    request: Request<Incoming>,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<(u16, Value), Status> {
    let input: ResumeRequest = serde_json::from_value(body(request.into_body(), BODY_LIMIT).await?)
        .map_err(|_| Status::invalid_argument("invalid resume request"))?;
    input.validate().map_err(validation)?;
    let owner = sandbox_owner(api, id, caller, true).await?;
    let record = owner
        .record
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing environment record"))?;
    if let Some(auto_pause) = input.auto_pause {
        let action = record
            .spec
            .as_ref()
            .and_then(|spec| spec.run_policy.as_ref())
            .map(|policy| policy.action);
        if action
            != Some(if auto_pause {
                pb::RunExpiryAction::Pause
            } else {
                pb::RunExpiryAction::Delete
            } as i32)
        {
            return Err(validation(ValidationError::unsupported(
                "changing autoPause during resume is not supported",
            )));
        }
    }
    if record.state != pb::EnvironmentState::Paused as i32 {
        return Err(Status::failed_precondition("sandbox is not paused"));
    }
    let timeout = input.timeout.unwrap_or(15);
    // Override any staged paused-state expiry until response preparation. Zero
    // is applied below through the existing owner timer, after Ready is observed.
    let recovery_timeout = if timeout == 0 {
        DEFAULT_TIMEOUT_SECONDS
    } else {
        timeout
    };
    api.environment_service
        .resume_with_run_timeout(id, caller, recovery_timeout)
        .await?;
    // Prepare the response before zero expiry can delete the restored execution,
    // but do not let a response failure skip that requested expiry.
    let response = match async {
        let control = api.clients.environment_control(id, caller).await?;
        sandbox_detail(api, &control, caller).await
    }
    .await
    {
        Err(error) if timeout != 0 => return Err(error),
        response => response,
    };
    // The ID-based update has no run precondition. Do not retry an ambiguous
    // reply: expiry may have paused the sandbox, and another update would stage
    // zero for a later wake (or affect a concurrently resumed execution).
    let control = api
        .clients
        .update_environment_control(id, caller, timeout, false)
        .await?;
    let mut response = response?;
    response["endAt"] = json!(rfc3339(control.expires_at_unix_ms));
    Ok((201, response))
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
                kind: Some(pb::SnapshotKind::Regular as i32),
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
// Scope is an E2B adapter concern. A common run policy is not an E2B marker.
async fn sandbox_owner(
    api: &Api,
    id: &str,
    caller: &pb::CallerContext,
    refresh: bool,
) -> Result<pb::GetEnvironmentResponse, Status> {
    let owner = api.clients.owner(id, caller, refresh).await?;
    if owner
        .record
        .as_ref()
        .and_then(|record| record.spec.as_ref())
        .is_none_or(|spec| spec.compatibility.is_none())
    {
        return Err(Status::not_found("E2B sandbox not found"));
    }
    Ok(owner)
}
async fn sandbox_detail(
    api: &Api,
    control: &pb::EnvironmentControlRecord,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let owner = sandbox_owner(api, &control.environment_id, caller, false).await?;
    let record = owner
        .record
        .ok_or_else(|| Status::data_loss("missing environment record"))?;
    let resources = record
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .ok_or_else(|| Status::data_loss("missing environment resources"))?;
    let metadata = record
        .spec
        .as_ref()
        .and_then(|spec| spec.scheduling.as_ref())
        .map(|policy| &policy.labels);
    let state = if record.state == pb::EnvironmentState::Paused as i32 {
        "paused"
    } else if record.state == pb::EnvironmentState::Running as i32 {
        "running"
    } else {
        return Err(Status::failed_precondition("sandbox is not available"));
    };
    let compatibility = record
        .spec
        .as_ref()
        .and_then(|spec| spec.compatibility.as_ref())
        .ok_or_else(|| Status::not_found("E2B sandbox not found"))?;
    let envd = record
        .spec
        .as_ref()
        .and_then(|spec| spec.envd.as_ref())
        .ok_or_else(|| Status::data_loss("sandbox envd configuration missing"))?;
    let domain = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?
        .domain
        .clone();
    Ok(
        json!({"templateID":compatibility.template_id,"sandboxID":control.environment_id,"clientID":"adx","envdVersion":envd.version,"state":state,"startedAt":rfc3339(control.started_at_unix_ms),"endAt":sandbox_end_at(&record, control),"cpuCount":resources.cpu_millis / 1000,"memoryMB":resources.memory_bytes / 1_048_576,"diskSizeMB":resources.disk_bytes / 1_048_576,"metadata":metadata.cloned().unwrap_or_default(),"envdAccessToken":compatibility.envd_access_token,"trafficAccessToken":compatibility.traffic_access_token,"domain":domain}),
    )
}
pub(super) fn rfc3339(milliseconds: u64) -> String {
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
        .as_object_mut()
        .and_then(|object| object.remove("nextToken"))
        .and_then(|value| value.as_str().map(str::to_owned));
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
        Code::InvalidArgument | Code::OutOfRange => 400,
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
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            adx_protocol::unsupported_capability(&status).map(|_| "capability_not_supported")
        });
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
        assert!(route("/sandboxes"));
        assert!(route("/v2/sandboxes"));
        assert!(route("/sandboxes/sandbox-1/timeout"));
        assert!(route("/templates/snapshot-1"));
        assert!(route("/v3/templates"));
        assert!(!route("/api/sandbox/v1/sandboxes/sandbox-1"));
        assert!(!route("/sandboxes-old"));
        assert!(!route("/v3/templates-old"));
    }

    #[test]
    fn alias_path_requires_one_safely_decoded_segment() {
        assert_eq!(
            template_alias("/templates/aliases/harbor__123").unwrap(),
            "harbor__123"
        );
        assert_eq!(template_alias("/templates/aliases/a%2Db").unwrap(), "a-b");
        for path in [
            "/templates/aliases/",
            "/templates/aliases/a/b",
            "/templates/aliases/a%2Fb",
            "/templates/aliases/a%252Fb",
            "/templates/aliases/a%ZZ",
            "/templates/aliases/%FF",
            "/templates/aliases/%00",
            "/templates/aliases/alias/",
        ] {
            assert!(template_alias(path).is_err(), "{path}");
        }
        assert!(template_alias(&format!(
            "/templates/aliases/{}",
            "a".repeat(MAX_TEMPLATE_ALIAS_BYTES + 1)
        ))
        .is_err());
    }

    #[test]
    fn registered_bare_alias_exists_before_any_snapshot_is_ready() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        };
        assert!(
            alias_resolution_request("demo", &caller)
                .unwrap()
                .registration_only
        );
        assert!(
            !alias_resolution_request("demo:default", &caller)
                .unwrap()
                .registration_only
        );
        let waiting = pb::SnapshotPreparationView {
            template_id: "family-uuid".into(),
            phase: pb::SnapshotPreparationPhase::Registered as i32,
            ..Default::default()
        };
        assert_eq!(
            registered_alias(waiting.clone(), true).unwrap(),
            json!({"public":false,"templateID":"family-uuid"})
        );
        assert_eq!(
            registered_alias(waiting, false).unwrap_err().code(),
            Code::NotFound
        );
        let ready = pb::SnapshotPreparationView {
            template_id: "family-uuid".into(),
            phase: pb::SnapshotPreparationPhase::Ready as i32,
            snapshot_available: true,
            snapshot_id: "capture".into(),
            ..Default::default()
        };
        assert_eq!(
            registered_alias(ready, false).unwrap()["templateID"],
            "family-uuid"
        );
    }

    #[test]
    fn explicit_alias_unavailable_capture_is_not_found_without_hiding_dependency_failures() {
        assert_eq!(
            alias_resolution_error(Status::failed_precondition("capture unavailable"), false)
                .code(),
            Code::NotFound
        );
        assert_eq!(
            alias_resolution_error(Status::failed_precondition("capture unavailable"), true).code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            alias_resolution_error(Status::unavailable("directory unavailable"), false).code(),
            Code::Unavailable
        );
    }

    #[tokio::test]
    async fn template_list_wire_body_is_an_array_with_header_pagination() {
        let response = raw(
            200,
            json!({"items":[{"templateID":"family"}],"nextToken":"cursor"}),
        );
        assert_eq!(response.headers()["x-next-token"], "cursor");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!([{"templateID":"family"}])
        );
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
    fn directory_cursor_is_bound_to_tenant_filters_and_e2b_scope() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        };
        let query = list_query("metadata=team%3Dcore&state=running,paused").unwrap();
        let digest = list_digest(&query, &caller).unwrap();
        let token = list_token(Some("last-environment".into()), &digest).unwrap();
        assert_eq!(
            list_after(&token, &digest).unwrap(),
            Some("last-environment".into())
        );
        for value in [
            "metadata=team%3Dother&state=running,paused",
            "metadata=team%3Dcore&state=running",
        ] {
            assert!(list_after(
                &token,
                &list_digest(&list_query(value).unwrap(), &caller).unwrap()
            )
            .is_err());
        }
        let other = pb::CallerContext {
            tenant_id: "other".into(),
            administrator: false,
        };
        assert!(list_after(&token, &list_digest(&query, &other).unwrap()).is_err());
        let native = URL_SAFE_NO_PAD
            .encode(br#"{"after":"one","tenant_id":"tenant","administrator":false}"#);
        assert!(list_after(&native, &digest).is_err());
    }

    #[test]
    fn list_summary_never_exposes_credentials_and_uses_paused_retention() {
        let mut record = pb::EnvironmentRecord {
            state: pb::EnvironmentState::Paused as i32,
            spec: Some(pb::EnvironmentSpec {
                id: "box".into(),
                resources: Some(pb::Resources::default()),
                compatibility: Some(pb::EnvironmentCompatibility {
                    template_id: "template".into(),
                    envd_access_token: "secret-sdk-token".into(),
                    traffic_access_token: "secret-traffic-token".into(),
                    ..Default::default()
                }),
                envd: Some(pb::EnvdServiceConfig {
                    version: "0.9.0".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            checkpoint: Some(pb::RestorePoint {
                expires_at_unix_seconds: Some(42),
                ..Default::default()
            }),
            ..Default::default()
        };
        let control = pb::EnvironmentControlRecord {
            started_at_unix_ms: 1,
            ..Default::default()
        };
        let value = serde_json::to_value(list_summary(&record, &control).unwrap()).unwrap();
        assert_eq!(value["endAt"], rfc3339(42_000));
        assert!(value.get("envdAccessToken").is_none());
        assert!(value.get("trafficAccessToken").is_none());
        assert!(!value.to_string().contains("secret-"));
        record.checkpoint.as_mut().unwrap().expires_at_unix_seconds = None;
        assert_eq!(
            list_summary(&record, &control).unwrap().end_at,
            "9999-12-31T23:59:59Z"
        );
    }

    #[test]
    fn list_query_defaults_and_rejects_malformed_cursors() {
        let query = list_query(
            "state=running,paused&metadata=team%3Dcore&limit=2&nextToken=platform-cursor",
        )
        .unwrap();
        assert_eq!(query.states, ["running", "paused"]);
        assert_eq!(query.metadata["team"], "core");
        assert_eq!((query.limit, query.cursor.as_str()), (2, "platform-cursor"));
        for query in ["limit=0", "limit=101", "state=deleted", "metadata=bad"] {
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

    #[tokio::test]
    async fn runtime_unsupported_capability_uses_e2b_capability_error_code() {
        let response = error(adx_protocol::status(
            adx_core::Error::UnsupportedCapability(
                adx_core::UnsupportedCapability::CheckpointRestore,
            ),
        ));

        assert_eq!(response.status(), 400);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "code": 400,
                "message": "capability not supported: checkpoint_restore",
                "error_code": "capability_not_supported",
            })
        );
    }
}
