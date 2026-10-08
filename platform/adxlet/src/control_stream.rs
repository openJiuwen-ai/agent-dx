//! Expected executions and their Execd-initiated control streams. Disconnects
//! invalidate observations, not executions; Adxlet's state machine owns cleanup.
use adx_core::{
    runtime::{RuntimeIdentity, RuntimePhase, RuntimeStatus},
    Error, Result,
};
use adx_protocol::{
    runtime as pb,
    runtime_stream::{verify_token, ControlOperation, MAX_CONTROL_MESSAGE_BYTES},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type Key = (String, String, u64);
fn key(identity: &RuntimeIdentity) -> Key {
    (
        identity.environment_id.clone(),
        identity.runtime_id.clone(),
        identity.ownership_generation,
    )
}
type Reply = oneshot::Sender<Result<RuntimeStatus>>;
struct Session {
    id: u64,
    commands: mpsc::Sender<std::result::Result<pb::RuntimeCommand, Status>>,
    pending: HashMap<u64, Reply>,
}
struct Pending {
    slot: Arc<Slot>,
    request_id: u64,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Ok(mut session) = self.slot.session.lock() {
            if let Some(current) = session.as_mut() {
                current.pending.remove(&self.request_id);
            }
        }
    }
}
struct Slot {
    session: Mutex<Option<Session>>,
    status: watch::Sender<Option<RuntimeStatus>>,
    retired: AtomicBool,
}
struct Inner {
    slots: Mutex<HashMap<Key, Arc<Slot>>>,
    secret: Vec<u8>,
    timeout: Duration,
    sequence: AtomicU64,
    events: mpsc::Sender<RuntimeIdentity>,
    receiver: Mutex<Option<mpsc::Receiver<RuntimeIdentity>>>,
}
#[derive(Clone)]
pub struct RuntimeControlHub(Arc<Inner>);
impl RuntimeControlHub {
    pub fn new(secret: Vec<u8>, timeout: Duration) -> Result<Self> {
        if secret.len() < 32 || timeout.is_zero() {
            return Err(Error::Invalid(
                "runtime control secret and positive timeout required".into(),
            ));
        }
        let (events, receiver) = mpsc::channel(1024);
        Ok(Self(Arc::new(Inner {
            slots: Mutex::default(),
            secret,
            timeout,
            sequence: AtomicU64::new(1),
            events,
            receiver: Mutex::new(Some(receiver)),
        })))
    }
    /// Arm before invoking Start: a fast Execd may register before Start returns.
    /// Reconciliation arms only records whose authoritative ownership is valid.
    pub fn expect(&self, identity: &RuntimeIdentity) -> Result<()> {
        identity.validate()?;
        let mut slots = self.0.slots.lock().map_err(poisoned)?;
        slots.entry(key(identity)).or_insert_with(|| {
            Arc::new(Slot {
                session: Mutex::default(),
                status: watch::channel(None).0,
                retired: AtomicBool::new(false),
            })
        });
        Ok(())
    }
    pub fn take_events(&self) -> Result<mpsc::Receiver<RuntimeIdentity>> {
        self.0
            .receiver
            .lock()
            .map_err(poisoned)?
            .take()
            .ok_or(Error::Conflict)
    }
    pub fn len(&self) -> usize {
        self.0.slots.lock().map(|s| s.len()).unwrap_or(0)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn slot(&self, identity: &RuntimeIdentity) -> Result<Arc<Slot>> {
        self.0
            .slots
            .lock()
            .map_err(poisoned)?
            .get(&key(identity))
            .cloned()
            .ok_or(Error::Conflict)
    }
    pub fn observed(&self, identity: &RuntimeIdentity) -> Result<Option<RuntimeStatus>> {
        Ok(self.slot(identity)?.status.borrow().clone())
    }
    pub async fn wait_connected(&self, identity: &RuntimeIdentity) -> Result<()> {
        let slot = self.slot(identity)?;
        let mut changes = slot.status.subscribe();
        tokio::time::timeout(self.0.timeout, async {
            loop {
                if slot.retired.load(Ordering::Acquire) {
                    return Err(Error::Conflict);
                }
                if changes.borrow_and_update().is_some() {
                    return Ok(());
                }
                changes
                    .changed()
                    .await
                    .map_err(|_| unavailable("control stream closed"))?;
            }
        })
        .await
        .map_err(|_| unavailable("runtime control reconnect timed out"))?
    }
    pub async fn wait_ready(&self, identity: &RuntimeIdentity) -> Result<()> {
        let slot = self.slot(identity)?;
        let mut changes = slot.status.subscribe();
        tokio::time::timeout(self.0.timeout, async {
            loop {
                if slot.retired.load(Ordering::Acquire) {
                    return Err(Error::Conflict);
                }
                if changes
                    .borrow_and_update()
                    .as_ref()
                    .is_some_and(|s| s.phase == RuntimePhase::Running)
                {
                    return Ok(());
                }
                changes
                    .changed()
                    .await
                    .map_err(|_| unavailable("control stream closed"))?;
            }
        })
        .await
        .map_err(|_| unavailable("runtime Ready notification timed out"))?
    }
    pub fn disconnect(&self, identity: &RuntimeIdentity) -> Result<()> {
        let slot = self.slot(identity)?;
        let mut session = slot.session.lock().map_err(poisoned)?;
        if let Some(old) = session.take() {
            close(old);
        }
        slot.status.send_replace(None);
        Ok(())
    }
    pub fn retire(&self, identity: &RuntimeIdentity) {
        let slot = self
            .0
            .slots
            .lock()
            .ok()
            .and_then(|mut s| s.remove(&key(identity)));
        if let Some(slot) = slot {
            slot.retired.store(true, Ordering::Release);
            if let Ok(mut session) = slot.session.lock() {
                if let Some(old) = session.take() {
                    close(old);
                }
            }
            slot.status.send_replace(None);
        }
    }
    pub async fn wait_resumed(&self, identity: &RuntimeIdentity, operation_id: &str) -> Result<()> {
        let slot = self.slot(identity)?;
        let mut changes = slot.status.subscribe();
        // The checkpoint caller supplies the operation deadline (up to 600s
        // by default); a per-RPC timeout must not shorten handoff waiting.
        async {
            loop {
                if slot.retired.load(Ordering::Acquire) {
                    return Err(Error::Conflict);
                }
                if let Some(status) = changes.borrow_and_update().as_ref() {
                    if status.phase == RuntimePhase::Failed {
                        return Err(unavailable("checkpoint handoff failed"));
                    }
                    if let Some(cp) = &status.checkpoint {
                        if cp.operation_id != operation_id {
                            return Err(Error::Conflict);
                        }
                        if cp.phase == adx_core::runtime::CheckpointPhase::Failed {
                            return Err(unavailable("checkpoint handoff failed"));
                        }
                        if status.phase == RuntimePhase::Running
                            && cp.phase == adx_core::runtime::CheckpointPhase::Resumed
                        {
                            return Ok(());
                        }
                    }
                }
                changes
                    .changed()
                    .await
                    .map_err(|_| unavailable("runtime control closed"))?;
            }
        }
        .await
    }
    pub async fn request(
        &self,
        identity: &RuntimeIdentity,
        operation: ControlOperation,
    ) -> Result<RuntimeStatus> {
        let slot = self.slot(identity)?;
        let request_id = self
            .0
            .sequence
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
            .map_err(|_| Error::Conflict)?;
        let operation_json =
            serde_json::to_vec(&operation).map_err(|e| Error::Invalid(e.to_string()))?;
        let (reply, result) = oneshot::channel();
        {
            let mut current = slot.session.lock().map_err(poisoned)?;
            let session = current
                .as_mut()
                .ok_or_else(|| unavailable("runtime control disconnected"))?;
            if slot.retired.load(Ordering::Acquire) || session.pending.len() >= 16 {
                return Err(unavailable("runtime control unavailable or saturated"));
            }
            session
                .commands
                .try_send(Ok(pb::RuntimeCommand {
                    request_id,
                    operation_json,
                }))
                .map_err(|_| unavailable("runtime control queue full or disconnected"))?;
            session.pending.insert(request_id, reply);
        }
        let _pending = Pending {
            slot: slot.clone(),
            request_id,
        };
        let result = tokio::time::timeout(self.0.timeout, result).await;
        if let Ok(mut current) = slot.session.lock() {
            if let Some(session) = current.as_mut() {
                session.pending.remove(&request_id);
            }
        }
        result
            .map_err(|_| unavailable("runtime control response timed out; outcome may be unknown"))?
            .map_err(|_| unavailable("runtime control disconnected"))?
    }
    fn observe(
        &self,
        slot: &Slot,
        session_id: u64,
        status: RuntimeStatus,
        identity: &RuntimeIdentity,
    ) -> Result<()> {
        if status.identity != *identity || status.revision == 0 {
            return Err(Error::Conflict);
        }
        let session = slot.session.lock().map_err(poisoned)?;
        if slot.retired.load(Ordering::Acquire)
            || session.as_ref().is_none_or(|s| s.id != session_id)
        {
            return Err(Error::Conflict);
        }
        let previous = slot.status.borrow().clone();
        if previous
            .as_ref()
            .is_some_and(|s| s.revision > status.revision)
        {
            return Err(Error::Conflict);
        }
        let pending = status.requested_checkpoint.is_some();
        slot.status.send_replace(Some(status));
        drop(session);
        if pending {
            // A bounded wakeup hint. Status remains in the slot if full; the
            // existing monitor recovers missed hints without losing requests.
            let _ = self.0.events.try_send(identity.clone());
        }
        Ok(())
    }
    async fn consume(
        &self,
        mut incoming: tonic::Streaming<pb::RuntimeEvent>,
        identity: RuntimeIdentity,
        slot: Arc<Slot>,
        session_id: u64,
    ) {
        let mut updates = slot.status.subscribe();
        loop {
            let next = tokio::select! {
                next = incoming.message() => next,
                changed = updates.changed() => {
                    if changed.is_err() || slot.retired.load(Ordering::Acquire)
                        || slot.session.lock().map(|s| s.as_ref().is_none_or(|s| s.id != session_id)).unwrap_or(true) { break; }
                    continue;
                }
            };
            let Ok(Some(event)) = next else {
                break;
            };
            let result = match event.event {
                Some(pb::runtime_event::Event::StatusJson(bytes)) => decode_status(&bytes)
                    .and_then(|s| self.observe(&slot, session_id, s, &identity)),
                Some(pb::runtime_event::Event::Reply(reply)) => {
                    let result = if reply.error_code.is_empty() {
                        decode_status(&reply.status_json)
                    } else {
                        Err(match reply.error_code.as_str() {
                            "INVALID" => Error::Invalid(reply.error_message),
                            "CONFLICT" => Error::Conflict,
                            _ => unavailable(reply.error_message),
                        })
                    };
                    let observed = match &result {
                        Ok(status) => self.observe(&slot, session_id, status.clone(), &identity),
                        Err(_) => Ok(()),
                    };
                    if observed.is_err() {
                        break;
                    }
                    let sender = slot.session.lock().ok().and_then(|mut current| {
                        current
                            .as_mut()
                            .filter(|s| s.id == session_id)
                            .and_then(|s| s.pending.remove(&reply.request_id))
                    });
                    if let Some(sender) = sender {
                        let _ = sender.send(result);
                    }
                    Ok(())
                }
                _ => Err(Error::Conflict),
            };
            if result.is_err() {
                break;
            }
        }
        // A superseded stream cannot clear its replacement or fail its requests.
        if let Ok(mut session) = slot.session.lock() {
            if session.as_ref().is_some_and(|s| s.id == session_id) {
                if let Some(old) = session.take() {
                    close(old);
                }
                slot.status.send_replace(None);
            }
        }
    }
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::Unavailable(message.into())
}
fn poisoned<T>(_: std::sync::PoisonError<T>) -> Error {
    unavailable("runtime control lock poisoned")
}
fn close(session: Session) {
    for (_, reply) in session.pending {
        let _ = reply.send(Err(unavailable(
            "runtime control connection replaced or disconnected",
        )));
    }
}
fn decode_status(bytes: &[u8]) -> Result<RuntimeStatus> {
    if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
        return Err(Error::Invalid("runtime status too large".into()));
    }
    serde_json::from_slice(bytes).map_err(|e| Error::Invalid(e.to_string()))
}
#[tonic::async_trait]
impl pb::runtime_control_service_server::RuntimeControlService for RuntimeControlHub {
    type OpenControlStream = ReceiverStream<std::result::Result<pb::RuntimeCommand, Status>>;
    async fn open_control(
        &self,
        request: Request<tonic::Streaming<pb::RuntimeEvent>>,
    ) -> std::result::Result<Response<Self::OpenControlStream>, Status> {
        let mut incoming = request.into_inner();
        let first = tokio::time::timeout(self.0.timeout, incoming.message())
            .await
            .map_err(|_| Status::deadline_exceeded("runtime hello timed out"))??;
        let Some(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(hello)),
        }) = first
        else {
            return Err(Status::invalid_argument("runtime hello required"));
        };
        let status = decode_status(&hello.status_json)
            .map_err(|_| Status::invalid_argument("invalid runtime status"))?;
        if status.revision == 0 {
            return Err(Status::invalid_argument(
                "positive runtime revision required",
            ));
        }
        verify_token(&self.0.secret, &status.identity, &hello.token)
            .map_err(|_| Status::unauthenticated("invalid execution credential"))?;
        let identity = status.identity.clone();
        let slot = self
            .slot(&identity)
            .map_err(|_| Status::failed_precondition("execution is not expected on this node"))?;
        let session_id = self
            .0
            .sequence
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
            .map_err(|_| Status::resource_exhausted("runtime stream sequence exhausted"))?;
        let (commands, receiver) = mpsc::channel(16);
        {
            let mut current = slot
                .session
                .lock()
                .map_err(|_| Status::unavailable("runtime control lock poisoned"))?;
            if slot.retired.load(Ordering::Acquire) {
                return Err(Status::failed_precondition("execution retired"));
            }
            if let Some(old) = current.replace(Session {
                id: session_id,
                commands,
                pending: HashMap::new(),
            }) {
                close(old);
            }
            slot.status.send_replace(None);
        }
        self.observe(&slot, session_id, status, &identity)
            .map_err(|_| Status::failed_precondition("invalid runtime hello"))?;
        let hub = self.clone();
        tokio::spawn(async move {
            hub.consume(incoming, identity, slot, session_id).await;
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

mod config;
pub use config::StreamConfig;

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> RuntimeIdentity {
        RuntimeIdentity {
            environment_id: "i".into(),
            runtime_id: "i-1".into(),
            ownership_generation: 1,
        }
    }
    fn status() -> RuntimeStatus {
        RuntimeStatus {
            identity: identity(),
            revision: 1,
            phase: RuntimePhase::Running,
            checkpoint: None,
            requested_checkpoint: None,
            requested_checkpoint_deadline_unix_millis: None,
            active_requests: 0,
            active_commands: 0,
            activity_revision: 1,
        }
    }
    #[tokio::test]
    async fn cancelled_request_releases_its_pending_reply_slot() {
        let hub = RuntimeControlHub::new(vec![1; 32], Duration::from_secs(60)).unwrap();
        hub.expect(&identity()).unwrap();
        let slot = hub.slot(&identity()).unwrap();
        let (commands, mut receiver) = mpsc::channel(16);
        *slot.session.lock().unwrap() = Some(Session {
            id: 1,
            commands,
            pending: HashMap::new(),
        });
        let client = hub.clone();
        let request =
            tokio::spawn(
                async move { client.request(&identity(), ControlOperation::Status).await },
            );
        receiver.recv().await.unwrap().unwrap();
        assert_eq!(
            slot.session.lock().unwrap().as_ref().unwrap().pending.len(),
            1
        );
        request.abort();
        let _ = request.await;
        assert!(slot
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .pending
            .is_empty());
    }
    #[tokio::test]
    async fn superseded_stream_cannot_publish_or_clear_replacement_status() {
        let hub = RuntimeControlHub::new(vec![1; 32], Duration::from_secs(1)).unwrap();
        hub.expect(&identity()).unwrap();
        let slot = hub.slot(&identity()).unwrap();
        let (commands, _receiver) = mpsc::channel(16);
        *slot.session.lock().unwrap() = Some(Session {
            id: 2,
            commands,
            pending: HashMap::new(),
        });
        hub.observe(&slot, 2, status(), &identity()).unwrap();
        assert!(hub.observe(&slot, 1, status(), &identity()).is_err());
        assert!(hub.observed(&identity()).unwrap().is_some());
        hub.retire(&identity());
        assert!(hub.observe(&slot, 2, status(), &identity()).is_err());
        assert!(hub.is_empty());
    }
    #[test]
    fn node_key_survives_process_configuration_reload() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let config = StreamConfig {
            listen: "127.0.0.1:19003".parse().unwrap(),
            advertised_address: "http://127.0.0.1:19003".into(),
            key_file: temp.path().join("secrets/key"),
        };
        let hostname = StreamConfig {
            listen: config.listen,
            advertised_address: "http://node.example:19003".into(),
            key_file: temp.path().join("hostname-key"),
        };
        assert!(hostname.load_key().is_err());
        assert!(!hostname.key_file.exists());
        let first = config.load_key().unwrap();
        assert_eq!(first, config.load_key().unwrap());
        assert_eq!(
            std::fs::metadata(&config.key_file)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
