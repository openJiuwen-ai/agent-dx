//! Node-local HTTP cooperation with the Capsule runtime.
use adx_core::{runtime::*, CapsuleRecord, Error, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{header::HeaderValue, Method, Request, StatusCode};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{net::SocketAddr, time::Duration};

pub struct RuntimeControlClient {
    client: Client<HttpConnector, Full<Bytes>>,
    port: u16,
    timeout: Duration,
    token: Option<HeaderValue>,
}
impl RuntimeControlClient {
    pub fn new(port: u16, timeout: Duration) -> Result<Self> {
        if port == 0 || timeout.is_zero() {
            return Err(Error::Invalid(
                "runtime HTTP port and timeout must be positive".into(),
            ));
        }
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(timeout));
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
            port,
            timeout,
            token: None,
        })
    }
    pub fn with_token(mut self, token: &str) -> Result<Self> {
        if token.is_empty() {
            return Err(Error::Invalid("runtime token must not be empty".into()));
        }
        let mut value = HeaderValue::from_str(token)
            .map_err(|_| Error::Invalid("invalid runtime HTTP token".into()))?;
        value.set_sensitive(true);
        self.token = Some(value);
        Ok(self)
    }
    pub fn identity(record: &CapsuleRecord) -> RuntimeIdentity {
        RuntimeIdentity {
            capsule_id: record.spec.id.clone(),
            runtime_id: record.runtime.id.clone(),
            ownership_generation: record.assignment.generation,
        }
    }
    async fn request(
        &self,
        record: &CapsuleRecord,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<RuntimeStatus> {
        let expected = Self::identity(record);
        expected.validate()?;
        let address = SocketAddr::new(
            record
                .runtime
                .ip
                .ok_or_else(|| Error::Invalid("runtime IP is required".into()))?,
            self.port,
        );
        let method = if body.is_some() {
            Method::POST
        } else {
            Method::GET
        };
        let body = body.unwrap_or_default();
        tokio::time::timeout(self.timeout, async {
            // Every runtime-control operation is fenced by identity and, for
            // mutations, an operation ID plus revision. Retrying the identical
            // request once is therefore safe and closes the result-unknown gap
            // where the runtime accepted an operation but the HTTP response was
            // lost while the connection was being retired.
            let response = {
                let mut last_error = None;
                let mut response = None;
                for _ in 0..2 {
                    let mut builder = Request::builder()
                        .method(method.clone())
                        .uri(format!("http://{address}/control/v1/{path}"));
                    if let Some(token) = &self.token {
                        builder = builder.header("X-Auth", token);
                    }
                    let request = builder
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::copy_from_slice(&body)))
                        .map_err(|_| Error::Invalid("invalid runtime HTTP request".into()))?;
                    match self.client.request(request).await {
                        Ok(value) => {
                            response = Some(value);
                            break;
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                response.ok_or_else(|| {
                    unavailable(last_error.expect("two failed HTTP attempts must retain an error"))
                })?
            };
            match response.status() {
                StatusCode::OK => {}
                StatusCode::CONFLICT => return Err(Error::Conflict),
                status => return Err(unavailable(format!("HTTP {status}"))),
            }
            let body = Limited::new(response.into_body(), 65_536)
                .collect()
                .await
                .map_err(unavailable)?
                .to_bytes();
            let status: RuntimeStatus = serde_json::from_slice(&body).map_err(unavailable)?;
            if status.identity != expected || status.revision == 0 {
                return Err(Error::Conflict);
            }
            Ok(status)
        })
        .await
        .map_err(|_| unavailable("deadline exceeded"))?
    }
    pub async fn status(&self, record: &CapsuleRecord) -> Result<RuntimeStatus> {
        self.request(record, "status", None).await
    }
    /// Prepared acknowledges the runtime barrier only. The caller must then invoke
    /// the execution backend and persist checkpoint artifacts/metadata separately.
    pub async fn prepare(
        &self,
        record: &CapsuleRecord,
        operation_id: &str,
        expected_revision: u64,
    ) -> Result<RuntimeStatus> {
        let request = PrepareCheckpoint {
            identity: Self::identity(record),
            operation_id: operation_id.into(),
            expected_revision,
        };
        let status = self
            .request(
                record,
                "checkpoint/prepare",
                Some(serde_json::to_vec(&request).map_err(unavailable)?),
            )
            .await?;
        if status.phase != RuntimePhase::Prepared
            || status.checkpoint.as_ref().is_none_or(|c| {
                c.operation_id != operation_id || c.phase != CheckpointPhase::Prepared
            })
        {
            return Err(unavailable(
                "runtime did not acknowledge checkpoint preparation",
            ));
        }
        Ok(status)
    }
    /// Only after the execution backend confirms it did not begin checkpointing.
    pub async fn abort_unstarted(
        &self,
        record: &CapsuleRecord,
        operation_id: &str,
        expected_revision: u64,
    ) -> Result<RuntimeStatus> {
        let request = AbortCheckpoint {
            identity: Self::identity(record),
            operation_id: operation_id.into(),
            expected_revision,
        };
        let status = self
            .request(
                record,
                "checkpoint/abort-unstarted",
                Some(serde_json::to_vec(&request).map_err(unavailable)?),
            )
            .await?;
        if status.phase != RuntimePhase::Running
            || status.checkpoint.as_ref().is_none_or(|c| {
                c.operation_id != operation_id || c.phase != CheckpointPhase::Aborted
            })
        {
            return Err(unavailable("runtime did not acknowledge checkpoint abort"));
        }
        Ok(status)
    }
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("runtime HTTP control: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::{
        lifecycle::LifecyclePolicy,
        runtime::{CheckpointPhase, CheckpointStatus},
        sandbox::SandboxOptions,
        scheduling::SchedulingPolicy,
        Assignment, CapsuleSpec, CapsuleState, Resources,
    };
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
        thread,
    };

    fn record(port: u16) -> (CapsuleRecord, RuntimeControlClient) {
        let record = CapsuleRecord {
            spec: CapsuleSpec {
                environment: None,
                snapshot_id: None,
                lifecycle: LifecyclePolicy::default(),
                env: BTreeMap::new(),
                id: "capsule-a".into(),
                tenant_id: "tenant-a".into(),
                image: "image-a".into(),
                runtime_class: "firecracker".into(),
                resources: Resources {
                    cpu_millis: 1,
                    memory_bytes: 1,
                    disk_bytes: 1,
                },
                priority: 0,
                scheduling: SchedulingPolicy::default(),
                sandbox: SandboxOptions::default(),
            },
            assignment: Assignment {
                capsule_id: "capsule-a".into(),
                node_id: "node-a".into(),
                shard_id: 0,
                generation: 7,
                devices: vec![],
            },
            state: CapsuleState::Running,
            revision: 2,
            runtime: adx_core::Runtime {
                id: "runtime-a".into(),
                ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            },
            resources_held: true,
            checkpoint: None,
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
        };
        (
            record,
            RuntimeControlClient::new(port, Duration::from_secs(5)).unwrap(),
        )
    }

    fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 4096];
        loop {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0, "client closed before sending a complete request");
            request.extend_from_slice(&chunk[..count]);
            let Some(header_end) = request.windows(4).position(|value| value == b"\r\n\r\n") else {
                continue;
            };
            let header_end = header_end + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if request.len() >= header_end + content_length {
                return request;
            }
        }
    }

    fn request_body(request: &[u8]) -> &[u8] {
        let start = request
            .windows(4)
            .position(|value| value == b"\r\n\r\n")
            .unwrap()
            + 4;
        &request[start..]
    }

    #[tokio::test]
    async fn prepare_retries_the_same_fenced_request_after_a_lost_response() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (record, client) = record(port);
        let expected = RuntimeStatus {
            identity: RuntimeControlClient::identity(&record),
            revision: 2,
            phase: RuntimePhase::Prepared,
            checkpoint: Some(CheckpointStatus {
                operation_id: "pause-a".into(),
                phase: CheckpointPhase::Prepared,
                error: None,
            }),
            active_requests: 0,
            active_commands: 0,
            activity_revision: 1,
        };
        let encoded = serde_json::to_vec(&expected).unwrap();
        let server = thread::spawn(move || {
            let (mut first_stream, _) = listener.accept().unwrap();
            let first = read_request(&mut first_stream);
            // Drop the connection after receiving the complete request. The
            // runtime operation may already have committed at this point.
            drop(first_stream);
            let (mut retry, _) = listener.accept().unwrap();
            let retry_request = read_request(&mut retry);
            assert_eq!(request_body(&first), request_body(&retry_request));
            write!(
                retry,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                encoded.len()
            )
            .unwrap();
            retry.write_all(&encoded).unwrap();
        });
        let result = client.prepare(&record, "pause-a", 1).await.unwrap();
        assert_eq!(result, expected);
        server.join().unwrap();
    }
}
