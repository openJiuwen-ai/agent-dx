use crate::{
    clients::Clients,
    contract,
    errors::ErrorDetail,
    operations::{snapshot_value, Kind},
    sandbox_service::SandboxService,
};
use adx_observability::trace;
use adx_protocol::control as pb;
use adx_transport::request::RequestContext;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    Request, Response, StatusCode,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tonic::{Code, Status};

type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Body = UnsyncBoxBody<Bytes, Error>;
pub struct Api {
    pub clients: Arc<Clients>,
    pub sandbox_service: Arc<SandboxService>,
    proxy: reqwest::Client,
}
impl Api {
    pub fn new(clients: Arc<Clients>) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            sandbox_service: SandboxService::new(clients.clone()),
            clients,
            proxy: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .build()?,
        }))
    }
    pub async fn serve(
        self: Arc<Self>,
        request: Request<Incoming>,
    ) -> Result<Response<Body>, Infallible> {
        let trace = trace::Trace::remote(
            "apiserver.http",
            header(&request, "traceparent"),
            header(&request, "tracestate"),
        );
        let started = Instant::now();
        let method = request.method().to_string();
        let route = route_name(request.uri().path());
        let request_id = header(&request, "x-request-id")
            .filter(|s| !s.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                trace
                    .scope(|| {
                        trace::traceparent().and_then(|p| p.split('-').nth(1).map(str::to_string))
                    })
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
            });
        let mut request = request;
        let request_context = RequestContext {
            request_id,
            operation_id: None,
        };
        let _ = request_context.inject(request.headers_mut());
        let request_id = request_context.request_id;
        let context = trace.scope(trace::traceparent).unwrap_or_default();
        let mut response = trace.run(Box::pin(self.handle(request))).await;
        if let Ok(value) = request_id.parse() {
            response.headers_mut().insert("x-request-id", value);
        }
        let (trace_id, span_id) = trace_identifiers(&context);
        adx_observability::info!(
            event = "http_request",
            trace_id,
            span_id,
            method = %method,
            route,
            status = response.status().as_u16(),
            duration_ms = started.elapsed().as_millis() as u64,
            "API request completed"
        );
        Ok(response)
    }
    async fn handle(self: Arc<Self>, request: Request<Incoming>) -> Response<Body> {
        let request_id = RequestContext::from_headers(request.headers()).request_id;
        let api_key = header(&request, "authorization")
            .and_then(|s| s.strip_prefix("Bearer "))
            .or_else(|| header(&request, "x-auth-token"))
            .or_else(|| header(&request, "x-auth"))
            .unwrap_or("")
            .trim();
        let caller = match Box::pin(self.clients.authenticate(api_key)).await {
            Ok(caller) => caller,
            Err(error) => {
                let status = if matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded) {
                    Status::unavailable("authentication service unavailable")
                } else {
                    Status::unauthenticated("authentication failed")
                };
                return error_response(status, &request_id, None, None, false);
            }
        };
        let path = match percent_encoding::percent_decode_str(request.uri().path()).decode_utf8() {
            Ok(path) => path.trim_end_matches('/').to_string(),
            Err(_) => {
                return error_response(
                    Status::invalid_argument("invalid path encoding"),
                    &request_id,
                    None,
                    None,
                    false,
                )
            }
        };
        let method = request.method().as_str().to_string();
        if agent_route(&method, &path) {
            return Box::pin(self.agent(request, &caller)).await;
        }
        let query: HashMap<String, String> =
            url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
                .into_owned()
                .collect();
        let stream = header(&request, "accept").is_some_and(|v| {
            v.split(',')
                .any(|v| v.trim().starts_with("text/event-stream"))
        });
        let lifecycle_id = header(&request, "x-adx-request-id")
            .unwrap_or("")
            .trim()
            .to_string();
        if path.starts_with("/api/admin/v1/keys") {
            if !caller.administrator {
                return error_response(
                    Status::permission_denied("administrator required"),
                    &request_id,
                    None,
                    None,
                    false,
                );
            }
            let body = match body(request.into_body(), 8192).await {
                Ok(v) => v,
                Err(error) => return error_response(error, &request_id, None, None, false),
            };
            return match Box::pin(self.keys(&method, &path, &query, body, &caller)).await {
                Ok((status, data)) => {
                    let mut response = plain(status, data);
                    response.headers_mut().insert(
                        "cache-control",
                        hyper::header::HeaderValue::from_static("no-store"),
                    );
                    response
                }
                Err(error) => error_response(error, &request_id, None, None, method != "GET"),
            };
        }
        if method == "GET" && path == "/api/sandbox/v1/resources" {
            return match self.clients.nodes().await {
                Ok(nodes) => plain(StatusCode::OK.as_u16(), resource_view(nodes)),
                Err(error) => error_response(error, &request_id, None, None, false),
            };
        }
        if method == "GET" && path == "/global-scheduler/resources" {
            return match self.clients.nodes().await {
                Ok(nodes) => plain(
                    StatusCode::OK.as_u16(),
                    legacy_resource_view(nodes, &request_id),
                ),
                Err(error) => error_response(error, &request_id, None, None, false),
            };
        }
        if method == "GET" && path == "/global-scheduler/scheduling_queue" {
            if !caller.administrator {
                return error_response(
                    Status::permission_denied("administrator required"),
                    &request_id,
                    None,
                    None,
                    false,
                );
            }
            return match self.clients.scheduling_queue().await {
                Ok(queue) => plain(StatusCode::OK.as_u16(), scheduling_queue_view(queue)),
                Err(error) => error_response(error, &request_id, None, None, false),
            };
        }
        if matches!(method.as_str(), "POST" | "DELETE")
            && path == "/global-scheduler/node/localschedulingstatus"
        {
            if !caller.administrator {
                return error_response(
                    Status::permission_denied("administrator required"),
                    &request_id,
                    None,
                    None,
                    true,
                );
            }
            let Some(node_id) = query
                .get("node_id")
                .filter(|node_id| !node_id.trim().is_empty())
                .cloned()
            else {
                return error_response(
                    Status::invalid_argument("node_id required"),
                    &request_id,
                    None,
                    None,
                    true,
                );
            };
            let accepting = method == "DELETE";
            return match self.clients.set_node_scheduling(node_id, accepting).await {
                Ok(_) => plain(
                    StatusCode::OK.as_u16(),
                    json!({
                        "status": if accepting { "normal" } else { "evicting" },
                        "message": "success",
                    }),
                ),
                Err(error) => error_response(error, &request_id, None, None, true),
            };
        }
        if method == "GET" && path == "/api/instances" {
            let Some(instance_id) = query
                .get("instance_id")
                .filter(|value| !value.trim().is_empty())
            else {
                return error_response(
                    Status::invalid_argument("instance_id required"),
                    &request_id,
                    None,
                    None,
                    false,
                );
            };
            return match Box::pin(self.clients.owner(instance_id, &caller, false)).await {
                Ok(owner) => {
                    let Some(record) = owner.record else {
                        return error_response(
                            Status::unavailable("environment directory returned no record"),
                            &request_id,
                            None,
                            Some(instance_id),
                            false,
                        );
                    };
                    if record.state == pb::EnvironmentState::Deleted as i32 {
                        return error_response(
                            Status::not_found("instance not found"),
                            &request_id,
                            None,
                            Some(instance_id),
                            false,
                        );
                    }
                    let Some(spec) = record.spec else {
                        return error_response(
                            Status::data_loss("environment record returned no spec"),
                            &request_id,
                            None,
                            Some(instance_id),
                            false,
                        );
                    };
                    let resource = spec.resources.unwrap_or_default();
                    plain(
                        200,
                        json!([{
                            "id": instance_id,
                            "status": state(record.state),
                            "required_cpu": resource.cpu_millis,
                            "required_mem": resource.memory_bytes / 1048576,
                            "image": spec.image,
                        }]),
                    )
                }
                Err(error) => error_response(error, &request_id, None, Some(instance_id), false),
            };
        }
        let mut input = match body(request.into_body(), 1048576).await {
            Ok(v) => v,
            Err(error) => return error_response(error, &request_id, None, None, false),
        };
        if method == "POST"
            && (path == "/api/sandbox/v1/sandboxes" || path == "/api/sandbox/create")
        {
            if path == "/api/sandbox/create"
                && input
                    .get("runtime")
                    .and_then(Value::as_str)
                    .is_some_and(|v| matches!(v, "rust" | "execd" | "adx-execd"))
            {
                if let Some(object) = input.as_object_mut() {
                    object.remove("runtime");
                }
            }
            let spec = match contract::create_spec_with_environment(
                input.clone(),
                &caller,
                self.clients.config.runtime_profile.as_ref(),
            ) {
                Ok(s) => s,
                Err(error) => return error_response(error, &request_id, None, None, false),
            };
            if stream && path.ends_with("sandboxes") {
                return self.create_stream(spec, input, request_id, caller);
            }
            let result = Box::pin(
                self.sandbox_service
                    .create(spec, input, &request_id, &caller),
            )
            .await;
            return response(
                if path == "/api/sandbox/create" {
                    result.map(|v| json!({"instance_id":v["instanceId"]}))
                } else {
                    result
                },
                &request_id,
                None,
                None,
                true,
            );
        }
        if let Some(id) = path.strip_prefix("/api/sandbox/v1/snapshots/") {
            if method == "DELETE" && lifecycle_id.is_empty() {
                return error_response(
                    Status::invalid_argument("snapshot request ID required"),
                    &request_id,
                    None,
                    None,
                    false,
                );
            }
            return response(
                Box::pin(self.snapshots(&method, Some(id), &query, &caller)).await,
                &request_id,
                (!lifecycle_id.is_empty()).then_some(lifecycle_id.as_str()),
                None,
                method != "GET",
            );
        }
        if path == "/api/sandbox/v1/snapshots" {
            return response(
                Box::pin(self.snapshots(&method, None, &query, &caller)).await,
                &request_id,
                None,
                None,
                method != "GET",
            );
        }
        let rest = path
            .strip_prefix("/api/sandbox/v1/sandboxes/")
            .or_else(|| path.strip_prefix("/api/sandbox/"));
        if let Some(rest) = rest {
            let (id, action) = rest.split_once('/').unwrap_or((rest, ""));
            let kind = match (method.as_str(), action) {
                ("DELETE", "") => Some(Kind::Delete),
                ("POST", "pause") => Some(Kind::Pause),
                ("POST", "resume") => Some(Kind::Resume),
                ("POST", "snapshots") => Some(Kind::Snapshot),
                ("PUT", "network") => Some(Kind::Network),
                ("POST", "reload") => Some(Kind::Reload),
                _ => None,
            };
            if let Some(kind) = kind {
                let op_id = if kind == Kind::Delete {
                    &request_id
                } else {
                    &lifecycle_id
                };
                if kind != Kind::Delete {
                    let prefix = if kind == Kind::Snapshot {
                        "snapshot"
                    } else {
                        action
                    };
                    if !operation_id(op_id, prefix) {
                        return error_response(
                            Status::invalid_argument("invalid operation request ID"),
                            &request_id,
                            Some(op_id),
                            Some(id),
                            false,
                        );
                    }
                }
                return response(
                    Box::pin(
                        self.sandbox_service
                            .execute(kind, id, op_id, input, &caller),
                    )
                    .await,
                    &request_id,
                    Some(op_id),
                    Some(id),
                    true,
                );
            }
            if matches!((method.as_str(), action), ("POST", "invoke")) {
                if let Err(e) = Box::pin(self.clients.owner(id, &caller, false)).await {
                    return response(Err(e), &request_id, None, Some(id), false);
                }
                return error_response(
                    Status::unimplemented("operation is not supported by this API"),
                    &request_id,
                    None,
                    Some(id),
                    false,
                );
            }
        }
        error_response(
            Status::not_found("route not found"),
            &request_id,
            None,
            None,
            false,
        )
    }

    fn create_stream(
        self: Arc<Self>,
        spec: pb::EnvironmentSpec,
        input: Value,
        request_id: String,
        caller: pb::CallerContext,
    ) -> Response<Body> {
        let (sender, receiver) = mpsc::channel::<Result<Frame<Bytes>, Error>>(4);
        let response = Response::builder()
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no")
            .body(
                StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(receiver))
                    .boxed_unsync(),
            )
            .expect("static event-stream response is valid");
        let trace = trace::Trace::child("apiserver.create_stream");
        tokio::spawn(trace.run(async move {
            let accepted = json!({"status":"creating", "requestId":request_id});
            if sender.send(Ok(sse("accepted", accepted))).await.is_err() {
                return;
            }

            let instance_id = spec.id.clone();
            let create = self
                .sandbox_service
                .create(spec, input, &request_id, &caller);
            tokio::pin!(create);
            let result = loop {
                tokio::select! {
                    result = &mut create => break result,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        let heartbeat = Frame::data(Bytes::from_static(b": heartbeat\n\n"));
                        let _ = sender.try_send(Ok(heartbeat));
                    }
                }
            };
            let final_event = match result {
                Ok(value) => value,
                Err(error) => {
                    let detail = ErrorDetail::from_status(
                        &error,
                        &request_id,
                        None,
                        Some(&instance_id),
                        true,
                    );
                    json!({
                        "status": if error.code() == Code::DeadlineExceeded {
                            "timeout"
                        } else {
                            "failed"
                        },
                        "requestId": request_id,
                        "errorCode": status_code(&error),
                        "message": error.message(),
                        "error": detail,
                    })
                }
            };
            let _ = sender.send(Ok(sse("final", final_event))).await;
        }));
        response
    }

    async fn snapshots(
        &self,
        method: &str,
        id: Option<&str>,
        query: &HashMap<String, String>,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
            self.clients.coordinator().await?,
        );
        match (method, id) {
            ("GET", Some(id)) => {
                let v = self
                    .clients
                    .rpc(
                        "apiserver.get_snapshot",
                        client.get_snapshot(trace::inject(pb::GetSnapshotRequest {
                            id: id.into(),
                            caller: Some(caller.clone()),
                            node_session_id: String::new(),
                        })),
                    )
                    .await?;
                if v.id != id {
                    return Err(Status::data_loss("snapshot identity mismatch"));
                }
                snapshot_value(&v, caller)
            }
            ("DELETE", Some(id)) => {
                let value = self
                    .clients
                    .rpc(
                        "apiserver.delete_snapshot",
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
                snapshot_value(&value, caller)?;
                Ok(json!({}))
            }
            ("GET", None) => {
                let page = self
                    .clients
                    .rpc(
                        "apiserver.list_snapshots",
                        client.list_snapshots(trace::inject(pb::ListSnapshotsRequest {
                            caller: Some(caller.clone()),
                            name: query.get("name").cloned().unwrap_or_default(),
                            page_token: query.get("pageToken").cloned().unwrap_or_default(),
                            page_size: page_size(query)?,
                        })),
                    )
                    .await?;
                let items = page
                    .snapshots
                    .iter()
                    .map(|s| snapshot_value(s, caller))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({"items":items,"nextPageToken":page.next_page_token}))
            }
            _ => Err(Status::unimplemented("unsupported snapshot method")),
        }
    }
    async fn keys(
        &self,
        method: &str,
        path: &str,
        query: &HashMap<String, String>,
        body: Value,
        caller: &pb::CallerContext,
    ) -> Result<(u16, Value), Status> {
        let mut client = pb::credential_service_client::CredentialServiceClient::new(
            self.clients.coordinator().await?,
        );
        let value = match (method, path.strip_prefix("/api/admin/v1/keys/")) {
            ("POST", None) => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields, rename_all = "camelCase")]
                struct Input {
                    tenant_id: String,
                    #[serde(default)]
                    expires_at_unix_seconds: u64,
                }
                let input: Input = serde_json::from_value(body)
                    .map_err(|_| Status::invalid_argument("invalid key request"))?;
                let result = self
                    .clients
                    .rpc(
                        "apiserver.create_key",
                        client.create_tenant_key(trace::inject(pb::CreateTenantKeyRequest {
                            caller: Some(caller.clone()),
                            tenant_id: input.tenant_id.clone(),
                            expires_at_unix_seconds: input.expires_at_unix_seconds,
                        })),
                    )
                    .await?;
                let key = result
                    .key
                    .filter(|k| !k.id.is_empty() && k.tenant_id == input.tenant_id)
                    .ok_or_else(|| Status::data_loss("invalid key response"))?;
                if result.api_key.len() < 32 {
                    return Err(Status::data_loss("invalid key response"));
                }
                return Ok((201, json!({"key":key_value(&key),"apiKey":result.api_key})));
            }
            ("GET", None) => {
                let tenant = query.get("tenantId").cloned().unwrap_or_default();
                let page = self
                    .clients
                    .rpc(
                        "apiserver.list_keys",
                        client.list_tenant_keys(trace::inject(pb::ListTenantKeysRequest {
                            caller: Some(caller.clone()),
                            tenant_id: tenant.clone(),
                            page_token: query.get("pageToken").cloned().unwrap_or_default(),
                            page_size: page_size(query)?,
                        })),
                    )
                    .await?;
                if page.keys.iter().any(|k| {
                    k.id.is_empty()
                        || k.tenant_id.is_empty()
                        || (!tenant.is_empty() && k.tenant_id != tenant)
                }) {
                    return Err(Status::data_loss("invalid key page"));
                }
                json!({"items":page.keys.iter().map(key_value).collect::<Vec<_>>(),"nextPageToken":page.next_page_token})
            }
            ("DELETE", Some(id)) => {
                self.clients
                    .rpc(
                        "apiserver.revoke_key",
                        client.revoke_tenant_key(trace::inject(pb::RevokeTenantKeyRequest {
                            caller: Some(caller.clone()),
                            id: id.into(),
                        })),
                    )
                    .await?;
                return Ok((204, Value::Null));
            }
            _ => return Err(Status::not_found("unknown key endpoint")),
        };
        Ok((200, value))
    }
    async fn agent(
        &self,
        request: Request<Incoming>,
        caller: &pb::CallerContext,
    ) -> Response<Body> {
        if self.clients.config.agent_address.is_empty() {
            return plain(503, json!({"error":"Agent service unavailable"}));
        }
        let (parts, body) = request.into_parts();
        let target = format!(
            "{}{}",
            self.clients.config.agent_address.trim_end_matches('/'),
            parts.uri.path_and_query().map_or("", |p| p.as_str())
        );
        let mut headers = parts.headers;
        strip_hop_headers(&mut headers);
        headers.remove("host");
        headers.remove("tenantid");
        headers.remove("x-tenant-id");
        headers.remove("x-adx-role");
        if let Ok(value) = caller.tenant_id.parse() {
            headers.insert("x-tenant-id", value);
        } else {
            return plain(502, json!({"error":"invalid identity"}));
        }
        trace::inject_headers(&mut headers);
        let response = self
            .proxy
            .request(parts.method, target)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .send()
            .await;
        match response {
            Err(_) => plain(502, json!({"error":"Agent service unavailable"})),
            Ok(response) => {
                let output = Response::builder().status(response.status());
                let mut headers = response.headers().clone();
                strip_hop_headers(&mut headers);
                let stream = response
                    .bytes_stream()
                    .map_ok(Frame::data)
                    .map_err(|e| -> Error { Box::new(e) });
                let mut output = output
                    .body(StreamBody::new(stream).boxed_unsync())
                    .expect("upstream status forms a valid HTTP response");
                *output.headers_mut() = headers;
                output
            }
        }
    }
}
fn trace_identifiers(context: &str) -> (&str, &str) {
    let mut fields = context.split('-');
    let _version = fields.next();
    let trace_id = fields.next().unwrap_or("");
    let span_id = fields.next().unwrap_or("");
    (trace_id, span_id)
}

fn strip_hop_headers(headers: &mut hyper::HeaderMap) {
    if let Some(connection) = headers
        .get("connection")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    {
        for name in connection.split(',') {
            headers.remove(name.trim());
        }
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
fn key_value(key: &pb::TenantKey) -> Value {
    json!({
        "id": key.id,
        "tenantId": key.tenant_id,
        "expiresAtUnixSeconds": key.expires_at_unix_seconds,
    })
}
fn page_size(query: &HashMap<String, String>) -> Result<u32, Status> {
    query
        .get("pageSize")
        .map(|value| {
            value
                .parse::<u32>()
                .ok()
                .filter(|size| *size <= 1000)
                .ok_or_else(|| Status::invalid_argument("invalid page size"))
        })
        .transpose()
        .map(|size| size.unwrap_or(0))
}
fn header<'a>(request: &'a Request<Incoming>, key: &str) -> Option<&'a str> {
    request.headers().get(key).and_then(|v| v.to_str().ok())
}
fn sse(name: &str, data: Value) -> Frame<Bytes> {
    Frame::data(Bytes::from(format!("event: {name}\ndata: {data}\n\n")))
}
async fn body(body: Incoming, limit: usize) -> Result<Value, Status> {
    let bytes = tokio::time::timeout(
        Duration::from_secs(10),
        http_body_util::Limited::new(body, limit).collect(),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("request body timeout"))?
    .map_err(|_| Status::out_of_range("request body too large"))?
    .to_bytes();
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&bytes).map_err(|_| Status::invalid_argument("invalid JSON request"))
}
fn response(
    value: Result<Value, Status>,
    request_id: &str,
    operation_id: Option<&str>,
    instance_id: Option<&str>,
    execution_may_have_started: bool,
) -> Response<Body> {
    match value {
        Ok(value) => envelope(200, Some(value), None),
        Err(error) => error_response(
            error,
            request_id,
            operation_id,
            instance_id,
            execution_may_have_started,
        ),
    }
}

fn error_response(
    error: Status,
    request_id: &str,
    operation_id: Option<&str>,
    instance_id: Option<&str>,
    execution_may_have_started: bool,
) -> Response<Body> {
    let status = status_code(&error);
    let detail = ErrorDetail::from_status(
        &error,
        request_id,
        operation_id,
        instance_id,
        execution_may_have_started,
    );
    plain(
        status,
        json!({
            "code": status,
            "message": error.message(),
            "data": null,
            "error": detail,
        }),
    )
}
fn envelope(status: u16, value: Option<Value>, error: Option<&str>) -> Response<Body> {
    let data = value.filter(|value| !value.is_null()).map(|value| {
        let bytes =
            serde_json::to_vec(&value).expect("serde_json::Value serialization is infallible");
        STANDARD.encode(bytes)
    });
    plain(
        status,
        json!({
            "code": status,
            "message": error.unwrap_or(""),
            "data": data,
        }),
    )
}
fn plain(status: u16, value: Value) -> Response<Body> {
    let data = if status == 204 {
        vec![]
    } else {
        serde_json::to_vec(&value).expect("serde_json::Value serialization is infallible")
    };
    Response::builder()
        .status(StatusCode::from_u16(status).expect("internal HTTP status is valid"))
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(data))
                .map_err(|e: Infallible| match e {})
                .boxed_unsync(),
        )
        .expect("static response headers are valid")
}
fn status_code(error: &Status) -> u16 {
    match error.code() {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => 409,
        Code::ResourceExhausted => 429,
        Code::OutOfRange => 413,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::DeadlineExceeded => 504,
        _ => 500,
    }
}
fn state(value: i32) -> &'static str {
    match pb::EnvironmentState::try_from(value) {
        Ok(pb::EnvironmentState::Running) => "running",
        Ok(pb::EnvironmentState::Paused) => "paused",
        Ok(pb::EnvironmentState::Deleted) => "deleted",
        Ok(pb::EnvironmentState::Failed) => "failed",
        Ok(pb::EnvironmentState::Pending) => "pending",
        Ok(pb::EnvironmentState::Starting) => "starting",
        Ok(pb::EnvironmentState::Pausing) => "pausing",
        Ok(pb::EnvironmentState::Resuming) => "resuming",
        Ok(pb::EnvironmentState::Deleting) => "deleting",
        _ => "unknown",
    }
}
fn operation_id(value: &str, prefix: &str) -> bool {
    value
        .strip_prefix(&format!("{prefix}-"))
        .is_some_and(|identifier| {
            identifier.len() <= 128
                && identifier
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        })
}
fn agent_route(method: &str, path: &str) -> bool {
    if path == "/api/agent" {
        return matches!(method, "GET" | "POST");
    }
    let Some(rest) = path.strip_prefix("/api/agent/") else {
        return false;
    };
    let Some((id, action)) = rest.split_once('/') else {
        return !rest.is_empty() && matches!(method, "GET" | "DELETE");
    };
    !id.is_empty()
        && matches!(
            (method, action),
            ("POST", "invoke" | "files/upload" | "files/mkdir")
                | ("GET", "files/download" | "files/list")
        )
}
fn route_name(path: &str) -> &'static str {
    if path.starts_with("/global-scheduler/") {
        "/global-scheduler/*"
    } else if path.starts_with("/api/admin/") {
        "/api/admin/*"
    } else if path.starts_with("/api/agent") {
        "/api/agent/*"
    } else if path.contains("/snapshots") {
        "/api/sandbox/v1/snapshots/*"
    } else if path.starts_with("/api/sandbox") {
        "/api/sandbox/*"
    } else {
        "other"
    }
}

fn resource_view(nodes: Vec<pb::NodeEndpoint>) -> Value {
    fn resources(resources: Option<pb::Resources>, devices: &[pb::Device]) -> Value {
        let resources = resources.unwrap_or_default();
        let mut values = serde_json::Map::from_iter([
            ("CPU".into(), json!(resources.cpu_millis)),
            ("Memory".into(), json!(resources.memory_bytes / 1_048_576)),
            ("Disk".into(), json!(resources.disk_bytes / 1_048_576)),
        ]);
        for (name, count) in device_counts(devices) {
            values.insert(name, json!(count));
        }
        Value::Object(values)
    }
    json!({
        "items": nodes.into_iter().map(|node| json!({
            "id": node.node_id,
            "status": if node.accepting_allocations { 0 } else { 1 },
            "capacity": resources(node.capacity, &node.capacity_devices),
            "allocatable": resources(node.allocatable, &node.allocatable_devices),
            "labels": node.labels,
        })).collect::<Vec<_>>()
    })
}

fn legacy_resource_view(nodes: Vec<pb::NodeEndpoint>, request_id: &str) -> Value {
    fn resources(resources: Option<pb::Resources>, devices: &[pb::Device]) -> Value {
        let resources = resources.unwrap_or_default();
        let mut values = serde_json::Map::from_iter([
            (
                "CPU".into(),
                json!({"scalar": {"value": resources.cpu_millis}}),
            ),
            (
                "Memory".into(),
                json!({"scalar": {"value": resources.memory_bytes / 1_048_576}}),
            ),
            (
                "Disk".into(),
                json!({"scalar": {"value": resources.disk_bytes / 1_048_576}}),
            ),
        ]);
        for (name, count) in device_counts(devices) {
            values.insert(
                name,
                json!({"vectors": {"values": {"count": {"vectors": {
                    "cards": {"values": [count]}
                }}}}}),
            );
        }
        json!({"resources": values})
    }
    fn add(total: &mut pb::Resources, value: &Option<pb::Resources>) {
        if let Some(value) = value {
            total.cpu_millis = total.cpu_millis.saturating_add(value.cpu_millis);
            total.memory_bytes = total.memory_bytes.saturating_add(value.memory_bytes);
            total.disk_bytes = total.disk_bytes.saturating_add(value.disk_bytes);
        }
    }
    let mut capacity = pb::Resources::default();
    let mut allocatable = pb::Resources::default();
    let mut capacity_devices = Vec::new();
    let mut allocatable_devices = Vec::new();
    let fragments = nodes
        .into_iter()
        .map(|node| {
            add(&mut capacity, &node.capacity);
            add(&mut allocatable, &node.allocatable);
            capacity_devices.extend(node.capacity_devices.clone());
            allocatable_devices.extend(node.allocatable_devices.clone());
            let labels = node
                .labels
                .into_iter()
                .map(|(name, value)| {
                    let items = serde_json::Map::from_iter([(value, json!(1))]);
                    (name, json!({"items": items}))
                })
                .collect::<serde_json::Map<_, _>>();
            let id = node.node_id;
            let unit = json!({
                "id": id.clone(),
                "capacity": resources(node.capacity, &node.capacity_devices),
                "allocatable": resources(node.allocatable, &node.allocatable_devices),
                "nodeLabels": labels,
                "status": if node.accepting_allocations { 0 } else { 1 },
            });
            (id, unit)
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "requestID": request_id,
        "resource": {
            "id": "adx-cluster",
            "capacity": resources(Some(capacity), &capacity_devices),
            "allocatable": resources(Some(allocatable), &allocatable_devices),
            "fragment": fragments,
            "status": 0,
        }
    })
}

fn device_counts(devices: &[pb::Device]) -> std::collections::BTreeMap<String, u64> {
    let mut counts = std::collections::BTreeMap::new();
    for device in devices {
        let kind = match pb::DeviceKind::try_from(device.kind) {
            Ok(pb::DeviceKind::Gpu) => "GPU",
            Ok(pb::DeviceKind::Npu) => "NPU",
            _ => continue,
        };
        let name = if device.model.is_empty() {
            kind.to_string()
        } else {
            format!("{kind}/{}", device.model)
        };
        *counts.entry(name).or_default() += 1;
    }
    counts
}

fn scheduling_queue_view(queue: pb::GetSchedulingQueueResponse) -> Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let instance_infos = queue
        .environments
        .into_iter()
        .filter_map(|pending| {
            let spec = pending.spec?;
            let resources = spec.resources.unwrap_or_default();
            let mut public_resources = serde_json::Map::from_iter([
                ("cpu".into(), json!(format!("{}m", resources.cpu_millis))),
                (
                    "memory".into(),
                    json!(format!("{}Mi", resources.memory_bytes / 1_048_576)),
                ),
                (
                    "Disk".into(),
                    json!(format!("{}Mi", resources.disk_bytes / 1_048_576)),
                ),
            ]);
            if let Some(scheduling) = spec.scheduling {
                let mut gpu = 0u64;
                let mut npu = 0u64;
                for device in scheduling.devices {
                    match pb::DeviceKind::try_from(device.kind).ok()? {
                        pb::DeviceKind::Gpu => gpu += u64::from(device.count),
                        pb::DeviceKind::Npu => npu += u64::from(device.count),
                        pb::DeviceKind::Unspecified => return None,
                    }
                }
                if gpu > 0 {
                    public_resources.insert("GPU".into(), json!(gpu.to_string()));
                }
                if npu > 0 {
                    public_resources.insert("NPU".into(), json!(npu.to_string()));
                }
            }
            Some(json!({
                "instanceID": spec.id,
                "requestID": spec.id,
                "resources": public_resources,
                "enqueueTimeMs": pending.enqueue_time_millis.to_string(),
                "waitDurationMs": now.saturating_sub(pending.enqueue_time_millis).to_string(),
            }))
        })
        .collect::<Vec<_>>();
    json!({"count": instance_infos.len(), "instanceInfos": instance_infos})
}

#[cfg(test)]
mod error_contract_tests {
    use super::*;

    async fn error_body(response: Response<Body>) -> Value {
        serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("static body")
                .to_bytes(),
        )
        .expect("JSON error body")
    }

    #[test]
    fn resource_view_uses_public_units_and_plain_labels() {
        let value = resource_view(vec![pb::NodeEndpoint {
            node_id: "node-a".into(),
            address: "node-a:17001".into(),
            session_id: "session-a".into(),
            capacity: Some(pb::Resources {
                cpu_millis: 4000,
                memory_bytes: 8 * 1_048_576,
                disk_bytes: 20 * 1_048_576,
            }),
            allocatable: Some(pb::Resources {
                cpu_millis: 3000,
                memory_bytes: 6 * 1_048_576,
                disk_bytes: 10 * 1_048_576,
            }),
            labels: [("arch".into(), "arm64".into())].into_iter().collect(),
            accepting_allocations: true,
            capacity_devices: vec![pb::Device {
                id: 0,
                kind: pb::DeviceKind::Gpu.into(),
                model: "A100".into(),
                healthy: true,
            }],
            allocatable_devices: vec![pb::Device {
                id: 0,
                kind: pb::DeviceKind::Gpu.into(),
                model: "A100".into(),
                healthy: true,
            }],
        }]);
        assert_eq!(value["items"][0]["id"], "node-a");
        assert_eq!(value["items"][0]["capacity"]["CPU"], 4000);
        assert_eq!(value["items"][0]["allocatable"]["Memory"], 6);
        assert_eq!(value["items"][0]["labels"]["arch"], "arm64");
        assert_eq!(value["items"][0]["status"], 0);
        assert_eq!(value["items"][0]["capacity"]["GPU/A100"], 1);
    }

    #[test]
    fn legacy_resource_view_exposes_fragment_units_and_aggregate_capacity() {
        let node = |id: &str, accepting_allocations: bool| pb::NodeEndpoint {
            node_id: id.into(),
            capacity: Some(pb::Resources {
                cpu_millis: 2000,
                memory_bytes: 4096 * 1_048_576,
                disk_bytes: 1024 * 1_048_576,
            }),
            allocatable: Some(pb::Resources {
                cpu_millis: 1000,
                memory_bytes: 2048 * 1_048_576,
                disk_bytes: 512 * 1_048_576,
            }),
            labels: [("HOST_IP".into(), format!("10.0.0.{id}"))]
                .into_iter()
                .collect(),
            accepting_allocations,
            capacity_devices: vec![pb::Device {
                id: id.parse().unwrap(),
                kind: pb::DeviceKind::Npu.into(),
                model: "910B".into(),
                healthy: true,
            }],
            allocatable_devices: vec![pb::Device {
                id: id.parse().unwrap(),
                kind: pb::DeviceKind::Npu.into(),
                model: "910B".into(),
                healthy: true,
            }],
            ..Default::default()
        };
        let value = legacy_resource_view(vec![node("1", true), node("2", false)], "req");
        assert_eq!(value["requestID"], "req");
        assert_eq!(
            value["resource"]["capacity"]["resources"]["CPU"]["scalar"]["value"],
            4000
        );
        assert_eq!(value["resource"]["fragment"]["1"]["status"], 0);
        assert_eq!(value["resource"]["fragment"]["2"]["status"], 1);
        assert_eq!(
            value["resource"]["allocatable"]["resources"]["NPU/910B"]["vectors"]["values"]["count"]
                ["vectors"]["cards"]["values"][0],
            2
        );
        assert_eq!(
            value["resource"]["fragment"]["1"]["nodeLabels"]["HOST_IP"]["items"]["10.0.0.1"],
            1
        );
    }

    #[test]
    fn scheduling_queue_view_preserves_legacy_shape_and_device_totals() {
        let value = scheduling_queue_view(pb::GetSchedulingQueueResponse {
            environments: vec![pb::PendingEnvironment {
                spec: Some(pb::EnvironmentSpec {
                    id: "environment-a".into(),
                    resources: Some(pb::Resources {
                        cpu_millis: 2000,
                        memory_bytes: 4096 * 1_048_576,
                        disk_bytes: 1024 * 1_048_576,
                    }),
                    scheduling: Some(pb::SchedulingPolicy {
                        devices: vec![pb::DeviceRequest {
                            kind: pb::DeviceKind::Gpu.into(),
                            model: None,
                            count: 2,
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                enqueue_time_millis: 1,
            }],
        });
        assert_eq!(value["count"], 1);
        assert_eq!(value["instanceInfos"][0]["instanceID"], "environment-a");
        assert_eq!(value["instanceInfos"][0]["requestID"], "environment-a");
        assert_eq!(value["instanceInfos"][0]["resources"]["cpu"], "2000m");
        assert_eq!(value["instanceInfos"][0]["resources"]["GPU"], "2");
    }

    #[tokio::test]
    async fn submitted_write_error_serializes_the_stable_contract() {
        let response = error_response(
            Status::unavailable("reply lost"),
            "request-a",
            Some("pause-a"),
            Some("instance-a"),
            true,
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let value = error_body(response).await;
        assert_eq!(value["code"], 503);
        assert_eq!(value["error"]["code"], "OUTCOME_UNKNOWN");
        assert_eq!(value["error"]["retry"], "same_operation");
        assert_eq!(value["error"]["outcome"], "unknown");
        assert_eq!(value["error"]["requestId"], "request-a");
        assert_eq!(value["error"]["operationId"], "pause-a");
        assert_eq!(value["error"]["instanceId"], "instance-a");
    }

    #[tokio::test]
    async fn documented_grpc_classes_keep_http_retry_and_outcome_semantics() {
        let cases = [
            (
                Code::InvalidArgument,
                false,
                400,
                "INVALID_ARGUMENT",
                "never",
                "not_started",
            ),
            (
                Code::OutOfRange,
                false,
                413,
                "INVALID_ARGUMENT",
                "never",
                "not_started",
            ),
            (
                Code::Unauthenticated,
                false,
                401,
                "UNAUTHENTICATED",
                "never",
                "not_started",
            ),
            (
                Code::PermissionDenied,
                false,
                403,
                "PERMISSION_DENIED",
                "never",
                "not_started",
            ),
            (Code::NotFound, false, 404, "NOT_FOUND", "never", "terminal"),
            (
                Code::AlreadyExists,
                false,
                409,
                "CONFLICT",
                "never",
                "not_started",
            ),
            (
                Code::ResourceExhausted,
                false,
                429,
                "RESOURCE_EXHAUSTED",
                "after_backoff",
                "not_started",
            ),
            (
                Code::Unimplemented,
                false,
                501,
                "UNSUPPORTED",
                "never",
                "not_started",
            ),
            (
                Code::Unavailable,
                false,
                503,
                "UNAVAILABLE",
                "after_backoff",
                "not_started",
            ),
            (
                Code::DeadlineExceeded,
                false,
                504,
                "DEADLINE_EXCEEDED",
                "same_operation",
                "not_started",
            ),
            (Code::DataLoss, false, 500, "DATA_LOSS", "never", "terminal"),
            (
                Code::Internal,
                false,
                500,
                "INTERNAL",
                "after_backoff",
                "not_started",
            ),
            (
                Code::Unavailable,
                true,
                503,
                "OUTCOME_UNKNOWN",
                "same_operation",
                "unknown",
            ),
            (
                Code::DeadlineExceeded,
                true,
                504,
                "OUTCOME_UNKNOWN",
                "same_operation",
                "unknown",
            ),
            (
                Code::Internal,
                true,
                500,
                "OUTCOME_UNKNOWN",
                "same_operation",
                "unknown",
            ),
        ];
        for (grpc, submitted, http, code, retry, outcome) in cases {
            let response = error_response(
                Status::new(grpc, "case"),
                "request-case",
                Some("operation-case"),
                Some("instance-case"),
                submitted,
            );
            assert_eq!(response.status().as_u16(), http, "gRPC {grpc:?}");
            assert_eq!(response.headers()["content-type"], "application/json");
            let value = error_body(response).await;
            assert_eq!(value["code"], http);
            assert_eq!(value["error"]["code"], code);
            assert_eq!(value["error"]["retry"], retry);
            assert_eq!(value["error"]["outcome"], outcome);
            assert_eq!(value["error"]["requestId"], "request-case");
            assert_eq!(value["error"]["operationId"], "operation-case");
            assert_eq!(value["error"]["instanceId"], "instance-case");
        }
    }
}
