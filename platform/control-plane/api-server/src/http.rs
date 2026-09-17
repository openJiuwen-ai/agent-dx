use crate::{
    clients::{authorize, Clients},
    contract,
    operations::{snapshot_value, Kind, Operations},
};
use adx_observability::trace;
use adx_protocol::control as pb;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    Request, Response, StatusCode,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};
use tonic::{Code, Status};

type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Body = UnsyncBoxBody<Bytes, Error>;
type CreateKey = (String, String);
struct CreateOperation {
    digest: Vec<u8>,
    spec: pb::InstanceSpec,
    result: Option<Result<Value, Status>>,
    touched: Instant,
}
pub struct Api {
    pub clients: Arc<Clients>,
    operations: Operations,
    creates: Mutex<HashMap<CreateKey, Arc<Mutex<CreateOperation>>>>,
    names: Mutex<HashMap<String, CreateKey>>,
    proxy: reqwest::Client,
}
impl Api {
    pub fn new(clients: Arc<Clients>) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            operations: Operations::new(clients.clone()),
            clients,
            creates: Mutex::default(),
            names: Mutex::default(),
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
            "api_server.http",
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
        if let Ok(value) = request_id.parse() {
            request.headers_mut().insert("x-request-id", value);
        }
        let context = trace.scope(trace::traceparent).unwrap_or_default();
        let mut response = trace.run(Box::pin(self.handle(request))).await;
        if let Ok(value) = request_id.parse() {
            response.headers_mut().insert("x-request-id", value);
        }
        adx_observability::info!(event="http_request",trace_id=context.split('-').nth(1).unwrap_or(""),span_id=context.split('-').nth(2).unwrap_or(""),method=%method,route,status=response.status().as_u16(),duration_ms=started.elapsed().as_millis() as u64,"API request completed");
        Ok(response)
    }
    async fn handle(self: Arc<Self>, request: Request<Incoming>) -> Response<Body> {
        let key = header(&request, "authorization")
            .and_then(|s| s.strip_prefix("Bearer "))
            .or_else(|| header(&request, "x-auth-token"))
            .or_else(|| header(&request, "x-auth"))
            .unwrap_or("")
            .trim();
        let caller = match Box::pin(self.clients.authenticate(key)).await {
            Ok(c) => c,
            Err(e) => {
                return envelope(
                    if matches!(e.code(), Code::Unavailable | Code::DeadlineExceeded) {
                        503
                    } else {
                        401
                    },
                    None,
                    Some("authentication failed"),
                )
            }
        };
        let path = match percent_encoding::percent_decode_str(request.uri().path()).decode_utf8() {
            Ok(path) => path.trim_end_matches('/').to_string(),
            Err(_) => return envelope(400, None, Some("invalid path encoding")),
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
        let request_id = header(&request, "x-request-id")
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let lifecycle_id = header(&request, "x-adx-request-id")
            .unwrap_or("")
            .trim()
            .to_string();
        if path.starts_with("/api/admin/v1/keys") {
            if !caller.administrator {
                return plain(403, json!({"error":"administrator required"}));
            }
            let body = match body(request.into_body(), 8192).await {
                Ok(v) => v,
                Err(e) => return plain(400, json!({"error":e.message()})),
            };
            return match Box::pin(self.keys(&method, &path, &query, body, &caller)).await {
                Ok((status, data)) => {
                    let mut r = plain(status, data);
                    r.headers_mut()
                        .insert("cache-control", "no-store".parse().unwrap());
                    r
                }
                Err(e) => plain(status_code(&e), json!({"error":e.message()})),
            };
        }
        if method == "GET" && path == "/api/instances" {
            let Some(id) = query.get("instance_id").filter(|v| !v.trim().is_empty()) else {
                return plain(400, json!({"error":"instance_id required"}));
            };
            return match Box::pin(self.clients.owner(id, &caller, false)).await {
                Ok(v) => {
                    let r = v.record.unwrap();
                    if r.state == pb::InstanceState::Deleted as i32 {
                        return plain(404, json!({"error":"Not Found"}));
                    }
                    let spec = r.spec.unwrap();
                    let resource = spec.resources.unwrap_or_default();
                    plain(
                        200,
                        json!([{"id":id,"status":state(r.state),"required_cpu":resource.cpu_millis,"required_mem":resource.memory_bytes/1048576,"image":spec.image}]),
                    )
                }
                Err(e) => plain(status_code(&e), json!({"error":e.message()})),
            };
        }
        let mut input = match body(request.into_body(), 1048576).await {
            Ok(v) => v,
            Err(e) => return envelope(status_code(&e), None, Some(e.message())),
        };
        if method == "POST"
            && (path == "/api/sandbox/v1/sandboxes" || path == "/api/sandbox/create")
        {
            if path == "/api/sandbox/create"
                && input
                    .get("runtime")
                    .and_then(Value::as_str)
                    .is_some_and(|v| matches!(v, "rust" | "rrt" | "rrt-runtime"))
            {
                input.as_object_mut().unwrap().remove("runtime");
            }
            let spec = match contract::create_spec_with_environment(
                input.clone(),
                &caller,
                self.clients.config.runtime_environment.as_ref(),
            ) {
                Ok(s) => s,
                Err(e) => return envelope(status_code(&e), None, Some(e.message())),
            };
            if stream && path.ends_with("sandboxes") {
                let (sender, receiver) = mpsc::channel::<Result<Frame<Bytes>, Error>>(4);
                let result = Response::builder()
                    .header("content-type", "text/event-stream")
                    .header("cache-control", "no-cache")
                    .header("x-accel-buffering", "no")
                    .body(
                        StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(receiver))
                            .boxed_unsync(),
                    )
                    .unwrap();
                let parent = trace::Trace::child("api_server.create_stream");
                tokio::spawn(parent.run(async move{
     if sender.send(Ok(sse("accepted",json!({"status":"creating","requestId":request_id})))).await.is_err(){return;}
     let future=self.create(spec,input,&request_id,&caller);tokio::pin!(future);
     let final_value=loop{tokio::select!{
      value=&mut future=>break value,
      _=tokio::time::sleep(Duration::from_secs(10))=>{let _=sender.try_send(Ok(Frame::data(Bytes::from_static(b": heartbeat\n\n"))));}
     }};
     let data=match final_value{Ok(v)=>v,Err(e)=>json!({"status":if e.code()==Code::DeadlineExceeded{"timeout"}else{"failed"},"requestId":request_id,"errorCode":status_code(&e),"message":e.message()})};
     let _=sender.send(Ok(sse("final",data))).await;
    }));
                return result;
            }
            let result = Box::pin(self.create(spec, input, &request_id, &caller)).await;
            return response(if path == "/api/sandbox/create" {
                result.map(|v| json!({"instance_id":v["instanceId"]}))
            } else {
                result
            });
        }
        if let Some(id) = path.strip_prefix("/api/sandbox/v1/snapshots/") {
            if method == "DELETE" && lifecycle_id.is_empty() {
                return envelope(400, None, Some("snapshot request ID required"));
            }
            return response(Box::pin(self.snapshots(&method, Some(id), &query, &caller)).await);
        }
        if path == "/api/sandbox/v1/snapshots" {
            return response(Box::pin(self.snapshots(&method, None, &query, &caller)).await);
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
                        return envelope(400, None, Some("invalid operation request ID"));
                    }
                }
                return response(
                    Box::pin(self.operations.execute(kind, id, op_id, input, &caller)).await,
                );
            }
            if matches!(
                (method.as_str(), action),
                ("POST", "reload" | "invoke") | ("PUT", "network")
            ) {
                if let Err(e) = Box::pin(self.clients.owner(id, &caller, false)).await {
                    return response(Err(e));
                }
                return envelope(501, None, Some("operation is not supported by this API"));
            }
        }
        plain(404, json!({"error":"Not Found"}))
    }
    async fn create(
        &self,
        spec: pb::InstanceSpec,
        input: Value,
        request_id: &str,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        if request_id.len() > 256 {
            return Err(Status::invalid_argument("request ID too long"));
        }
        let key = (caller.tenant_id.clone(), request_id.to_string());
        let digest = Sha256::digest(serde_json::to_vec(&input).unwrap()).to_vec();
        let op = {
            let mut creates = self.creates.lock().await;
            creates.retain(|_, v| {
                v.try_lock().map_or(true, |o| {
                    o.result.is_none() || o.touched.elapsed() < Duration::from_secs(600)
                })
            });
            if let Some(op) = creates.get(&key) {
                op.clone()
            } else {
                if creates.len() >= self.clients.config.cache_entries {
                    return Err(Status::resource_exhausted("create replay budget exhausted"));
                }
                let op = Arc::new(Mutex::new(CreateOperation {
                    digest: digest.clone(),
                    spec,
                    result: None,
                    touched: Instant::now(),
                }));
                creates.insert(key.clone(), op.clone());
                op
            }
        };
        let mut op = op.lock().await;
        if op.digest != digest {
            return Err(Status::already_exists(
                "request ID reused with different arguments",
            ));
        }
        if let Some(result) = &op.result {
            return result.clone();
        }
        {
            let mut names = self.names.lock().await;
            if let Some(existing) = names.get(&op.spec.id) {
                if existing != &key
                    && self.clients.config.create_mode == crate::config::CreateMode::Central
                {
                    return Err(Status::already_exists("instance create in progress"));
                }
            }
            names.insert(op.spec.id.clone(), key.clone());
        }
        let result=async{
   if self.clients.config.create_mode == crate::config::CreateMode::Central { match self.clients.owner(&op.spec.id,caller,true).await{
    Ok(_)=>return Err(Status::already_exists("instance already exists")),
    Err(e) if e.code()==Code::NotFound=>{},Err(e)=>return Err(e)
   }
   }
   let budget=Duration::from_secs(contract::create_timeout(&input)?).min(self.clients.config.timeout());
   let result=self.clients.create_instance(pb::CreateInstanceRequest{spec:Some(op.spec.clone()),caller:Some(caller.clone())},budget).await?;
   authorize(caller,result.record.as_ref())?;
   let r=result.record.unwrap();let got=r.spec.as_ref().unwrap();
   if r.state!=pb::InstanceState::Running as i32 || result.durability!=pb::Durability::Published as i32 || !matches_spec(&op.spec,got){return Err(Status::unavailable("create is not durably confirmed"));}
   let mut value=json!({"sandboxId":got.id,"instanceId":got.id,"status":"running","requestId":request_id});
   if input.pointer("/tunnel/enabled").and_then(Value::as_bool)==Some(true){
    let port=got.env.get("RRT_TUNNEL_HTTP_PORT").and_then(|p|p.parse::<u16>().ok()).unwrap_or(8766);let safe=got.id.replace('@',"-at-").replace(['/', '.', '_'],"-");let path=format!("/tunnel/{safe}");
    value["tunnel"]=json!({"url":path,"path":path,"wsPath":path,"proxyUrl":format!("http://127.0.0.1:{port}"),"proxyPort":port});
   }
   Ok(value)
  }.await;
        self.names.lock().await.remove(&op.spec.id);
        // An uncertain result is retryable only through this retained spec/ID.
        if !result
            .as_ref()
            .is_err_and(|e| matches!(e.code(), Code::Unavailable | Code::DeadlineExceeded))
        {
            op.result = Some(result.clone());
        }
        op.touched = Instant::now();
        result
    }
    async fn snapshots(
        &self,
        method: &str,
        id: Option<&str>,
        query: &HashMap<String, String>,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let mut client =
            pb::snapshot_service_client::SnapshotServiceClient::new(self.clients.master().await?);
        match (method, id) {
            ("GET", Some(id)) => {
                let v = self
                    .clients
                    .rpc(
                        "api_server.get_snapshot",
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
                        "api_server.delete_snapshot",
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
                        "api_server.list_snapshots",
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
            self.clients.master().await?,
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
                        "api_server.create_key",
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
                        "api_server.list_keys",
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
                        "api_server.revoke_key",
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
                let mut output = Response::builder().status(response.status());
                let mut headers = response.headers().clone();
                strip_hop_headers(&mut headers);
                *output.headers_mut().unwrap() = headers;
                let stream = response
                    .bytes_stream()
                    .map_ok(Frame::data)
                    .map_err(|e| -> Error { Box::new(e) });
                output.body(StreamBody::new(stream).boxed_unsync()).unwrap()
            }
        }
    }
}
fn matches_spec(want: &pb::InstanceSpec, got: &pb::InstanceSpec) -> bool {
    if want.snapshot_id.is_none() {
        return want == got;
    }
    want.id == got.id
        && want.tenant_id == got.tenant_id
        && want.snapshot_id == got.snapshot_id
        && (!got.image.is_empty() || got.runtime_environment.is_some())
        && !got.runtime.is_empty()
        && (want.image.is_empty() || want.image == got.image)
        && (want.runtime.is_empty() || want.runtime == got.runtime)
        && want.resources.as_ref().is_none_or(|w| {
            got.resources.as_ref().is_some_and(|g| {
                (w.cpu_millis == 0 || w.cpu_millis == g.cpu_millis)
                    && (w.memory_bytes == 0 || w.memory_bytes == g.memory_bytes)
                    && (w.disk_bytes == 0 || w.disk_bytes == g.disk_bytes)
            })
        })
        && want.env.iter().all(|(k, v)| got.env.get(k) == Some(v))
        && want.priority == got.priority
        && want.lifecycle == got.lifecycle
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
fn key_value(k: &pb::TenantKey) -> Value {
    json!({"id":k.id,"tenantId":k.tenant_id,"expiresAtUnixSeconds":k.expires_at_unix_seconds})
}
fn page_size(query: &HashMap<String, String>) -> Result<u32, Status> {
    query
        .get("pageSize")
        .map(|s| {
            s.parse::<u32>()
                .ok()
                .filter(|v| *v <= 1000)
                .ok_or_else(|| Status::invalid_argument("invalid page size"))
        })
        .transpose()
        .map(|v| v.unwrap_or(0))
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
fn response(value: Result<Value, Status>) -> Response<Body> {
    match value {
        Ok(v) => envelope(200, Some(v), None),
        Err(e) => envelope(status_code(&e), None, Some(e.message())),
    }
}
fn envelope(status: u16, value: Option<Value>, error: Option<&str>) -> Response<Body> {
    plain(
        status,
        json!({"code":status,"message":error.unwrap_or(""),"data":value.filter(|v|!v.is_null()).map(|v|STANDARD.encode(serde_json::to_vec(&v).unwrap()))}),
    )
}
fn plain(status: u16, value: Value) -> Response<Body> {
    let data = if status == 204 {
        vec![]
    } else {
        serde_json::to_vec(&value).unwrap()
    };
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(data))
                .map_err(|e: Infallible| match e {})
                .boxed_unsync(),
        )
        .unwrap()
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
fn state(s: i32) -> &'static str {
    match pb::InstanceState::try_from(s) {
        Ok(pb::InstanceState::Running) => "running",
        Ok(pb::InstanceState::Paused) => "paused",
        Ok(pb::InstanceState::Deleted) => "deleted",
        Ok(pb::InstanceState::Failed) => "failed",
        Ok(pb::InstanceState::Pending) => "pending",
        Ok(pb::InstanceState::Starting) => "starting",
        Ok(pb::InstanceState::Pausing) => "pausing",
        Ok(pb::InstanceState::Resuming) => "resuming",
        Ok(pb::InstanceState::Deleting) => "deleting",
        _ => "unknown",
    }
}
fn operation_id(value: &str, prefix: &str) -> bool {
    value.strip_prefix(&format!("{prefix}-")).is_some_and(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.as_bytes()[0].is_ascii_alphanumeric()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
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
    if path.starts_with("/api/admin/") {
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
