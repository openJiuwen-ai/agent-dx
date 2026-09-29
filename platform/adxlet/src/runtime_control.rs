//! Node-local HTTP cooperation with the Environment runtime.
use adx_core::{runtime::*, EnvironmentRecord, Error, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::Incoming,
    client::conn::http1::{self, SendRequest},
    header::{HeaderValue, HOST},
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{watch, Mutex as AsyncMutex},
    task::AbortHandle,
};

const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RuntimeControlKey {
    environment_id: String,
    runtime_id: String,
    ownership_generation: u64,
    address: SocketAddr,
}

struct RuntimeConnection {
    address: SocketAddr,
    connect_timeout: Duration,
    valid: AtomicBool,
    sender: AsyncMutex<Option<SendRequest<Full<Bytes>>>>,
    driver: Mutex<Option<AbortHandle>>,
    idle_reset: Mutex<Option<watch::Sender<bool>>>,
    idle_task: Mutex<Option<AbortHandle>>,
}

impl RuntimeConnection {
    fn new(address: SocketAddr, connect_timeout: Duration) -> Self {
        Self {
            address,
            connect_timeout,
            valid: AtomicBool::new(true),
            sender: AsyncMutex::new(None),
            driver: Mutex::new(None),
            idle_reset: Mutex::new(None),
            idle_task: Mutex::new(None),
        }
    }

    fn close_transport(&self) {
        self.idle_reset
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(task) = self
            .idle_task
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            task.abort();
        }
        if let Some(driver) = self
            .driver
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
        {
            driver.abort();
        }
    }

    fn retire(&self) {
        self.valid.store(false, Ordering::Release);
        if let Ok(mut sender) = self.sender.try_lock() {
            sender.take();
        }
        self.close_transport();
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>> {
        let stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(self.address))
            .await
            .map_err(|_| unavailable("connect deadline exceeded"))?
            .map_err(unavailable)?;
        stream.set_nodelay(true).map_err(unavailable)?;
        let (sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(unavailable)?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let driver_abort = driver.abort_handle();
        let (idle_reset, mut idle_state) = watch::channel(false);
        let idle_driver = driver_abort.clone();
        let idle_task = tokio::spawn(async move {
            loop {
                if *idle_state.borrow() {
                    tokio::select! {
                        _ = tokio::time::sleep(IDLE_CONNECTION_TIMEOUT) => {
                            idle_driver.abort();
                            break;
                        }
                        changed = idle_state.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                    }
                } else if idle_state.changed().await.is_err() {
                    break;
                }
            }
        });
        *self
            .driver
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(driver_abort);
        *self
            .idle_reset
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(idle_reset);
        *self
            .idle_task
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(idle_task.abort_handle());
        Ok(sender)
    }

    fn set_idle(&self, idle: bool) {
        if let Some(reset) = self
            .idle_reset
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
        {
            let _ = reset.send(idle);
        }
    }

    async fn send(&self, request: Request<Full<Bytes>>) -> Result<Response<Incoming>> {
        if !self.valid.load(Ordering::Acquire) {
            return Err(unavailable("runtime connection is retired"));
        }
        self.set_idle(false);
        let mut sender = self.sender.lock().await;
        let disconnected = match sender.as_mut() {
            Some(sender) => sender.ready().await.is_err(),
            None => true,
        };
        if disconnected {
            sender.take();
            self.close_transport();
            *sender = Some(self.connect().await?);
        }
        if !self.valid.load(Ordering::Acquire) {
            sender.take();
            self.close_transport();
            return Err(unavailable("runtime connection was retired"));
        }
        let Some(active) = sender.as_mut() else {
            return Err(unavailable("runtime sender was not initialized"));
        };
        let result = active.send_request(request).await;
        if result.is_err() || !self.valid.load(Ordering::Acquire) {
            sender.take();
            self.close_transport();
        }
        if !self.valid.load(Ordering::Acquire) {
            return Err(unavailable("runtime connection was retired"));
        }
        result.map_err(unavailable)
    }
}

#[derive(Clone)]
pub struct RuntimeControlClient {
    clients: Arc<Mutex<HashMap<RuntimeControlKey, Arc<RuntimeConnection>>>>,
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
        Ok(Self {
            clients: Arc::default(),
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
    pub fn identity(record: &EnvironmentRecord) -> RuntimeIdentity {
        RuntimeIdentity {
            environment_id: record.spec.id.clone(),
            runtime_id: record.runtime.id.clone(),
            ownership_generation: record.assignment.generation,
        }
    }
    fn key(&self, record: &EnvironmentRecord) -> Result<RuntimeControlKey> {
        let identity = Self::identity(record);
        identity.validate()?;
        let address = SocketAddr::new(
            record
                .runtime
                .ip
                .ok_or_else(|| Error::Invalid("runtime IP is required".into()))?,
            self.port,
        );
        Ok(RuntimeControlKey {
            environment_id: identity.environment_id,
            runtime_id: identity.runtime_id,
            ownership_generation: identity.ownership_generation,
            address,
        })
    }
    fn connection(&self, key: &RuntimeControlKey) -> Arc<RuntimeConnection> {
        self.clients
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RuntimeConnection::new(key.address, self.timeout)))
            .clone()
    }
    /// Retire transport state owned by exactly this Environment runtime.
    ///
    /// The ownership generation fences a late cleanup from closing connections
    /// belonging to a replacement runtime which reused the same endpoint.
    pub fn retire(&self, record: &EnvironmentRecord) {
        let Ok(key) = self.key(record) else {
            return;
        };
        let retired = self
            .clients
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&key);
        if let Some(connection) = retired {
            connection.retire();
        }
    }
    async fn request(
        &self,
        record: &EnvironmentRecord,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<RuntimeStatus> {
        let expected = Self::identity(record);
        let key = self.key(record)?;
        let connection = self.connection(&key);
        let method = if body.is_some() {
            Method::POST
        } else {
            Method::GET
        };
        let attempts = if body.is_some() { 2 } else { 1 };
        let body = body.unwrap_or_default();
        let result = tokio::time::timeout(self.timeout, async {
            // Mutations are fenced by identity, operation ID and revision. Retrying
            // the identical mutation once closes a lost-response gap. Status is
            // retried by its caller's readiness loop so one failed probe does not
            // double the connection storm while many runtimes are booting.
            let response = {
                let mut last_error = None;
                let mut response = None;
                for _ in 0..attempts {
                    let mut builder = Request::builder()
                        .method(method.clone())
                        .uri(format!("/control/v1/{path}"))
                        .header(HOST, key.address.to_string());
                    if let Some(token) = &self.token {
                        builder = builder.header("X-Auth", token);
                    }
                    let request = builder
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::copy_from_slice(&body)))
                        .map_err(|_| Error::Invalid("invalid runtime HTTP request".into()))?;
                    match connection.send(request).await {
                        Ok(value) => {
                            response = Some(value);
                            break;
                        }
                        Err(error) => last_error = Some(error),
                    }
                }
                response.ok_or_else(|| {
                    unavailable(
                        last_error
                            .map(|error| error.to_string())
                            .unwrap_or_else(|| "runtime request was not attempted".into()),
                    )
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
        .map_err(|_| unavailable("deadline exceeded"))?;
        connection.set_idle(true);
        result
    }
    pub async fn status(&self, record: &EnvironmentRecord) -> Result<RuntimeStatus> {
        self.request(record, "status", None).await
    }
    pub async fn finish_checkpoint(
        &self,
        record: &EnvironmentRecord,
        operation_id: &str,
        error: Option<String>,
    ) -> Result<RuntimeStatus> {
        let request = FinishWorkloadCheckpoint {
            identity: Self::identity(record),
            operation_id: operation_id.into(),
            error,
        };
        self.request(
            record,
            "checkpoint/finish",
            Some(serde_json::to_vec(&request).map_err(unavailable)?),
        )
        .await
    }
    /// Prepared acknowledges the runtime barrier only. The caller must then invoke
    /// the execution backend and persist checkpoint artifacts/metadata separately.
    pub async fn prepare(
        &self,
        record: &EnvironmentRecord,
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
        record: &EnvironmentRecord,
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
        Assignment, EnvironmentSpec, EnvironmentState, Resources,
    };
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
        thread,
        time::Instant,
    };

    fn record(port: u16) -> (EnvironmentRecord, RuntimeControlClient) {
        let record = EnvironmentRecord {
            spec: EnvironmentSpec {
                runtime_profile: None,
                snapshot_id: None,
                lifecycle: LifecyclePolicy::default(),
                env: BTreeMap::new(),
                id: "environment-a".into(),
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
                environment_id: "environment-a".into(),
                node_id: "node-a".into(),
                shard_id: 0,
                generation: 7,
                devices: vec![],
            },
            state: EnvironmentState::Running,
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
            requested_checkpoint: None,
            requested_checkpoint_deadline_unix_millis: None,
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

    #[tokio::test]
    async fn status_leaves_transport_retry_to_the_readiness_loop() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (record, client) = record(port);
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            drop(stream);
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_millis(100);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok(_) => panic!("status retried a failed transport request"),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("status retry observation failed: {error}"),
                }
            }
        });
        assert!(client.status(&record).await.is_err());
        server.join().unwrap();
    }

    #[test]
    fn retire_is_fenced_by_environment_runtime_and_generation() {
        let (old, client) = record(50090);
        let old_key = client.key(&old).unwrap();
        drop(client.connection(&old_key));

        let mut replacement = old.clone();
        replacement.assignment.generation += 1;
        replacement.runtime.id = "runtime-b".into();
        let replacement_key = client.key(&replacement).unwrap();
        drop(client.connection(&replacement_key));
        assert_eq!(client.clients.lock().unwrap().len(), 2);

        let cleanup = client.clone();
        cleanup.retire(&old);
        let clients = client.clients.lock().unwrap();
        assert_eq!(clients.len(), 1);
        assert!(clients.contains_key(&replacement_key));
    }

    #[tokio::test]
    async fn retire_closes_the_runtime_idle_connection() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (record, client) = record(port);
        let expected = RuntimeStatus {
            requested_checkpoint: None,
            requested_checkpoint_deadline_unix_millis: None,
            identity: RuntimeControlClient::identity(&record),
            revision: 2,
            phase: RuntimePhase::Running,
            checkpoint: None,
            active_requests: 0,
            active_commands: 0,
            activity_revision: 1,
        };
        let encoded = serde_json::to_vec(&expected).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                encoded.len()
            )
            .unwrap();
            stream.write_all(&encoded).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut byte = [0; 1];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });

        assert_eq!(client.status(&record).await.unwrap(), expected);
        client.retire(&record);
        // Let Tokio observe the aborted HTTP driver before blocking on the
        // synchronous server thread.
        tokio::task::yield_now().await;
        server.join().unwrap();
    }

    #[tokio::test]
    async fn repeated_runtime_retire_does_not_grow_the_pool() {
        const CYCLES: u64 = 32;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let (base, client) = record(port);
        let records = (1..=CYCLES)
            .map(|generation| {
                let mut record = base.clone();
                record.spec.id = format!("environment-{generation}");
                record.assignment.environment_id = record.spec.id.clone();
                record.assignment.generation = generation;
                record.runtime.id = format!("runtime-{generation}");
                record
            })
            .collect::<Vec<_>>();
        let responses = records
            .iter()
            .map(|record| {
                serde_json::to_vec(&RuntimeStatus {
                    requested_checkpoint: None,
                    requested_checkpoint_deadline_unix_millis: None,
                    identity: RuntimeControlClient::identity(record),
                    revision: 2,
                    phase: RuntimePhase::Running,
                    checkpoint: None,
                    active_requests: 0,
                    active_commands: 0,
                    activity_revision: 1,
                })
                .unwrap()
            })
            .collect::<Vec<_>>();
        let server = thread::spawn(move || {
            for encoded in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = read_request(&mut stream);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    encoded.len()
                )
                .unwrap();
                stream.write_all(&encoded).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut byte = [0; 1];
                assert_eq!(stream.read(&mut byte).unwrap(), 0);
            }
        });

        for record in records {
            client.status(&record).await.unwrap();
            client.retire(&record);
            tokio::task::yield_now().await;
        }
        server.join().unwrap();
        assert!(client.clients.lock().unwrap().is_empty());
    }
}
