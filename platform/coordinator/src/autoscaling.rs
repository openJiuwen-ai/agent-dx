//! Coordinator HTTP operations for external node autoscaling.
use crate::rpc::CoordinatorRpc;
use adx_core::{Error, Result as DomainResult};
use adx_protocol::control::{self as pb, coordinator_service_server::CoordinatorService};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    convert::Infallible,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tonic::{Code, Status};

#[derive(Debug)]
pub(crate) struct InstanceQuery {
    pub node_id: Option<String>,
    pub tenant_id: Option<String>,
    pub instance_id: Option<String>,
    pub offset: usize,
    pub page_size: usize,
}

impl InstanceQuery {
    fn parse(query: &HashMap<String, String>) -> DomainResult<Self> {
        if query.keys().any(|key| {
            !matches!(
                key.as_str(),
                "node_id" | "tenant_id" | "instance_id" | "page" | "page_size"
            )
        }) {
            return Err(Error::Invalid("unknown instance query parameter".into()));
        }
        let page = number(query, "page", 1, usize::MAX)?;
        let page_size = number(query, "page_size", 100, 1000)?;
        Ok(Self {
            node_id: text(query, "node_id")?,
            tenant_id: text(query, "tenant_id")?,
            instance_id: text(query, "instance_id")?,
            offset: (page - 1)
                .checked_mul(page_size)
                .ok_or_else(|| Error::Invalid("page offset overflow".into()))?,
            page_size,
        })
    }
}

#[derive(Serialize)]
pub(crate) struct InstanceView {
    #[serde(rename = "instanceID")]
    pub instance_id: String,
    pub tenant_id: String,
    pub node_id: String,
    pub generation: u64,
    pub revision: u64,
    pub status: String,
    pub resources_held: bool,
    pub invalidated: bool,
    pub restart_pending: bool,
    pub resume_pending: bool,
    pub recovery_pending: bool,
}

#[derive(Serialize)]
pub(crate) struct InstancePage {
    pub count: usize,
    #[serde(rename = "instanceInfos")]
    pub instance_infos: Vec<InstanceView>,
    pub epoch: u64,
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeAdmission>,
}

#[derive(Serialize)]
pub(crate) struct NodeAdmission {
    pub node_id: String,
    pub scheduling_paused: bool,
    pub accepting_allocations: bool,
}

fn text(query: &HashMap<String, String>, key: &str) -> DomainResult<Option<String>> {
    query
        .get(key)
        .map(|value| {
            if value.trim().is_empty() {
                Err(Error::Invalid(format!("{key} must not be blank")))
            } else {
                Ok(value.clone())
            }
        })
        .transpose()
}

fn number(
    query: &HashMap<String, String>,
    key: &str,
    default: usize,
    max: usize,
) -> DomainResult<usize> {
    query.get(key).map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0 && *n <= max)
            .ok_or_else(|| Error::Invalid(format!("invalid {key}")))
    })
}

async fn dispatch(
    coordinator: &CoordinatorRpc,
    method: &Method,
    uri: &hyper::Uri,
) -> Result<Value, Status> {
    let mut query = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if query.insert(key.into_owned(), value.into_owned()).is_some() {
            return Err(Status::invalid_argument("duplicate query parameter"));
        }
    }
    match (method, uri.path()) {
        (&Method::GET, "/global-scheduler/scheduling_queue") => {
            let queue = coordinator
                .get_scheduling_queue(tonic::Request::new(pb::GetSchedulingQueueRequest {}))
                .await?
                .into_inner();
            Ok(queue_view(queue))
        }
        (&Method::POST | &Method::DELETE, "/global-scheduler/node/localschedulingstatus") => {
            let node_id = text(&query, "node_id")
                .map_err(adx_protocol::status)?
                .ok_or_else(|| Status::invalid_argument("node_id required"))?;
            let accepting = method == Method::DELETE;
            coordinator
                .set_node_scheduling(tonic::Request::new(pb::SetNodeSchedulingRequest {
                    node_id,
                    accepting_allocations: accepting,
                }))
                .await?;
            Ok(
                json!({"status": if accepting { "normal" } else { "evicting" }, "message": "success"}),
            )
        }
        (&Method::GET, "/instance-manager/query-tenant-instances") => {
            let page = coordinator
                .scaling_instances(InstanceQuery::parse(&query).map_err(adx_protocol::status)?)
                .await
                .map_err(adx_protocol::status)?;
            serde_json::to_value(page)
                .map_err(|_| Status::internal("instance query serialization failed"))
        }
        _ => Err(Status::not_found("autoscaling route not found")),
    }
}

fn queue_view(queue: pb::GetSchedulingQueueResponse) -> Value {
    let now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let instances = queue
        .environments
        .into_iter()
        .filter_map(|pending| {
            let spec = pending.spec?;
            let resources = spec.resources.unwrap_or_default();
            let mut values = serde_json::Map::from_iter([
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
            let mut gpu = 0u64;
            let mut npu = 0u64;
            if let Some(scheduling) = spec.scheduling {
                for device in scheduling.devices {
                    match pb::DeviceKind::try_from(device.kind).ok()? {
                        pb::DeviceKind::Gpu => gpu += u64::from(device.count),
                        pb::DeviceKind::Npu => npu += u64::from(device.count),
                        pb::DeviceKind::Unspecified => return None,
                    }
                }
            }
            if gpu > 0 {
                values.insert("GPU".into(), json!(gpu.to_string()));
            }
            if npu > 0 {
                values.insert("NPU".into(), json!(npu.to_string()));
            }
            Some(
                json!({"instanceID": spec.id, "requestID": spec.id, "resources": values,
            "schedulingRequirements": {"runtimeClass": spec.runtime_class},
            "enqueueTimeMs": pending.enqueue_time_millis.to_string(),
            "waitDurationMs": now.saturating_sub(pending.enqueue_time_millis).to_string()}),
            )
        })
        .collect::<Vec<_>>();
    json!({"count": instances.len(), "instanceInfos": instances})
}

fn response(result: Result<Value, Status>) -> Response<Full<Bytes>> {
    let (status, body) = match result {
        Ok(value) => (StatusCode::OK, value),
        Err(error) => {
            let status = match error.code() {
                Code::InvalidArgument => StatusCode::BAD_REQUEST,
                Code::NotFound => StatusCode::NOT_FOUND,
                Code::FailedPrecondition | Code::Aborted | Code::AlreadyExists => {
                    StatusCode::CONFLICT
                }
                Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
                Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                json!({"error": {"code": format!("{:?}", error.code()), "message": error.message()}}),
            )
        }
    };
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        hyper::header::CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response
}

/// Serve the Coordinator's unauthenticated management HTTP interface.
/// Returns when accepting a connection fails; individual connections have a bounded lifetime.
pub async fn serve(listener: TcpListener, coordinator: CoordinatorRpc) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let coordinator = coordinator.clone();
                async move {
                    let result = if request.uri().path().starts_with("/v1/node-upgrades/") {
                        let (parts, body) = request.into_parts();
                        match Limited::new(body, 65_536).collect().await {
                            Ok(body) => match serde_json::from_slice::<Value>(&body.to_bytes())
                                .or_else(|error| {
                                    if parts.method == Method::GET {
                                        Ok(Value::Null)
                                    } else {
                                        Err(error)
                                    }
                                }) {
                                Ok(value) => match tokio::spawn(async move {
                                    coordinator
                                        .upgrade_http(&parts.method, parts.uri.path(), value)
                                        .await
                                        .map_err(adx_protocol::status)
                                })
                                .await
                                {
                                    Ok(result) => result,
                                    Err(_) => Err(Status::internal(
                                        "upgrade operation interrupted; query task",
                                    )),
                                },
                                Err(_) => {
                                    Err(Status::invalid_argument("JSON upgrade request required"))
                                }
                            },
                            Err(_) => Err(Status::invalid_argument(
                                "upgrade body exceeds limit or cannot be read",
                            )),
                        }
                    } else {
                        dispatch(&coordinator, request.method(), request.uri()).await
                    };
                    Ok::<_, Infallible>(response(result))
                }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(900), connection).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn management_errors_preserve_status_payload_and_headers() {
        for (error, status, code) in [
            (
                adx_protocol::status(Error::NoCapacity),
                StatusCode::TOO_MANY_REQUESTS,
                "ResourceExhausted",
            ),
            (
                Status::unavailable("node RPC unavailable"),
                StatusCode::SERVICE_UNAVAILABLE,
                "Unavailable",
            ),
            (
                Status::internal("unexpected failure"),
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal",
            ),
        ] {
            let message = error.message().to_owned();
            let result = response(Err(error));
            assert_eq!(result.status(), status);
            assert_eq!(
                result.headers()[hyper::header::CONTENT_TYPE],
                "application/json"
            );
            assert_eq!(result.headers()[hyper::header::CACHE_CONTROL], "no-store");
            let bytes = result.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body, json!({"error": {"code": code, "message": message}}));
        }
    }
}
