//! E2A response projection. Connection ownership and push dispatch stay with the driver.
use super::protocol::{Method, ProtocolError, Request, MAX_FRAME_BYTES};
use serde::Deserialize;
use serde_json::{json, Map, Value};

type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Debug, Deserialize, PartialEq, Eq)]
enum Kind {
    #[serde(rename = "e2a.chunk")]
    Chunk,
    #[serde(rename = "e2a.complete")]
    Complete,
    #[serde(rename = "e2a.error")]
    Error,
    #[serde(rename = "plan.approval_required")]
    PlanApproval,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Status {
    InProgress,
    Succeeded,
    Failed,
}

#[derive(Debug, Deserialize)]
struct WireResponse {
    protocol_version: String,
    request_id: Option<String>,
    sequence: i64,
    is_final: bool,
    status: Status,
    response_kind: Kind,
    body: Map<String, Value>,
    session_id: Option<String>,
    channel: Option<String>,
    user_id: Option<String>,
    agent_ref: Option<Map<String, Value>>,
    #[serde(default)]
    metadata: Map<String, Value>,
}

#[derive(Debug)]
pub struct Response {
    wire: WireResponse,
}

#[derive(Debug)]
pub struct Projection {
    pub frame: Option<Value>,
    /// End of this E2A response, not proof that the business execution has finished.
    pub complete: bool,
}

impl Response {
    /// Reject malformed envelopes and inconsistent terminal/status combinations.
    /// Only E2A kinds used by the current beegent request surface are accepted.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(ProtocolError::TooLarge);
        }
        let response: WireResponse = serde_json::from_slice(bytes)
            .map_err(|_| ProtocolError::Invalid("invalid E2A response"))?;
        let valid_state = matches!(
            (&response.response_kind, &response.status, response.is_final),
            (Kind::Chunk, Status::InProgress, false)
                | (Kind::Complete, Status::Succeeded, true)
                | (Kind::Error, Status::Failed, true)
                | (Kind::PlanApproval, Status::Succeeded, true)
        );
        if response.protocol_version != "1.0" || !valid_state {
            return Err(ProtocolError::Invalid("invalid E2A response state"));
        }
        if response
            .metadata
            .get("_jiuwenswarm_server_push")
            .is_some_and(|v| !v.is_boolean())
        {
            return Err(ProtocolError::Invalid("invalid E2A push marker"));
        }
        let parsed = Self { wire: response };
        if parsed.sequence() < 0 && !(parsed.sequence() == -1 && parsed.is_keepalive()) {
            return Err(ProtocolError::Invalid("invalid E2A response sequence"));
        }
        Ok(parsed)
    }

    pub fn request_id(&self) -> Option<&str> {
        self.wire.request_id.as_deref()
    }
    pub fn session_id(&self) -> Option<&str> {
        self.wire.session_id.as_deref()
    }
    pub fn user_id(&self) -> Option<&str> {
        self.wire.user_id.as_deref()
    }
    pub fn is_final(&self) -> bool {
        self.wire.is_final
    }

    pub fn sequence(&self) -> i64 {
        self.wire.sequence
    }
    pub fn is_push(&self) -> bool {
        self.wire
            .metadata
            .get("_jiuwenswarm_server_push")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    fn is_keepalive(&self) -> bool {
        if self.wire.response_kind != Kind::Chunk {
            return false;
        }
        let event = self
            .wire
            .body
            .get("delta")
            .and_then(Value::as_object)
            .and_then(|payload| payload.get("event_type"))
            .or_else(|| self.wire.body.get("event_type"));
        event.and_then(Value::as_str) == Some("keepalive")
    }

    /// Project a response already associated with a pending client request.
    /// Rejects mismatched request/user/channel identities and server pushes. Absent
    /// backend user IDs rely on the driver's dedicated, authorized Sandbox connection.
    /// Metadata busy-state augmentation and other requests' turn IDs belong to the driver.
    pub fn project(
        &self,
        request: &Request,
        backend_id: &str,
        user_id: &str,
    ) -> Result<Projection> {
        let mut projection = self.project_unmapped(request, backend_id, user_id)?;
        remap_ids(&mut projection, |id| {
            (id == backend_id).then(|| request.id().to_owned())
        });
        Ok(projection)
    }

    pub(super) fn project_unmapped(
        &self,
        request: &Request,
        backend_id: &str,
        user_id: &str,
    ) -> Result<Projection> {
        self.validate_owner(user_id)?;
        if backend_id.is_empty() || self.request_id() != Some(backend_id) || self.is_push() {
            return Err(ProtocolError::Invalid("E2A response ownership mismatch"));
        }
        let failed = self.wire.response_kind == Kind::Error;
        if request.is_unary() {
            if !self.wire.is_final {
                return Err(ProtocolError::Invalid("stream response for unary request"));
            }
            return Ok(Projection {
                frame: Some(self.unary_frame(request, failed)?),
                complete: true,
            });
        }
        self.event_projection(request.session_id(), Some(request))
    }

    /// Project a side-channel event on a connection bound to this verified user's
    /// Sandbox. Explicit user/channel mismatches are rejected. A missing user ID
    /// is permitted only because the connection itself is dedicated and authorized.
    /// A business push must identify its session; it never completes a unary RPC.
    pub fn project_push(
        &self,
        user_id: &str,
        fallback_session: Option<&str>,
    ) -> Result<Projection> {
        self.validate_owner(user_id)?;
        if !self.is_push() {
            return Err(ProtocolError::Invalid("expected E2A server push"));
        }
        let projection = self.event_projection(self.session_id().or(fallback_session), None)?;
        if let Some(frame) = &projection.frame {
            if !frame["payload"]["session_id"]
                .as_str()
                .is_some_and(super::protocol::session_id)
            {
                return Err(ProtocolError::Invalid("server push has no valid session"));
            }
        }
        Ok(projection)
    }

    fn validate_owner(&self, user_id: &str) -> Result<()> {
        if user_id.trim().is_empty()
            || self
                .user_id()
                .is_some_and(|id| !id.is_empty() && id != user_id)
            || self
                .wire
                .channel
                .as_deref()
                .is_some_and(|channel| !channel.is_empty() && channel != "web")
        {
            return Err(ProtocolError::Invalid("E2A response ownership mismatch"));
        }
        Ok(())
    }

    fn event_projection(
        &self,
        fallback_session: Option<&str>,
        request: Option<&Request>,
    ) -> Result<Projection> {
        if self.is_keepalive() {
            return Ok(Projection {
                frame: None,
                complete: false,
            });
        }
        let failed = self.wire.response_kind == Kind::Error;
        let mut payload = match self.wire.response_kind {
            Kind::Complete => self.result()?,
            Kind::Error => self.error_payload(),
            Kind::Chunk => self.chunk_payload(),
            Kind::PlanApproval => {
                let mut payload = self.wire.body.clone();
                payload.insert("event_type".into(), json!("plan.approval_required"));
                payload
            }
        };
        if request.is_none_or(|r| matches!(r.method(), Method::Chat | Method::History))
            && self.wire.is_final
            && !failed
            && (payload.is_empty()
                || (payload.len() == 1 && payload.get("is_complete") == Some(&Value::Bool(true))))
        {
            return Ok(Projection {
                frame: None,
                complete: true,
            });
        }
        let event = payload
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if let Some(request) = request.filter(|r| r.method() == Method::Generic) {
            if failed || (self.wire.is_final && event.is_empty()) {
                return Ok(Projection {
                    frame: Some(self.unary_frame(request, failed)?),
                    complete: true,
                });
            }
            if event.is_empty() {
                return Ok(Projection {
                    frame: Some(json!({"type":"event","event":"e2a.chunk","payload":payload})),
                    complete: false,
                });
            }
        }
        // Internal control marker: the background team stream owns completion.
        if event == "chat.processing_status_deferred" {
            return Ok(Projection {
                frame: None,
                complete: self.wire.is_final,
            });
        }
        let chat_error = event == "runtime.error"
            || event == "chat.error"
            || failed
                && event.is_empty()
                && request.is_none_or(|r| r.method() != Method::Interrupt);
        if chat_error {
            payload.insert("event_type".into(), json!("chat.error"));
            payload.insert("is_complete".into(), json!(true));
        } else if let Some(request) = request.filter(|r| !r.is_stream() && event.is_empty()) {
            return Ok(Projection {
                frame: Some(self.unary_frame(request, failed)?),
                complete: self.wire.is_final,
            });
        }
        let event = if chat_error {
            "chat.error"
        } else if event.is_empty() {
            "chat.final"
        } else {
            &event
        };
        if !payload.contains_key("session_id") {
            if let Some(session) = fallback_session.or(self.session_id()) {
                payload.insert("session_id".into(), json!(session));
            }
        }
        if let Some(id) = self.request_id().filter(|id| !id.is_empty()) {
            payload.entry("request_id").or_insert_with(|| json!(id));
            if interaction(event) {
                payload
                    .entry("turn_request_id")
                    .or_insert_with(|| json!(id));
            }
        }
        if let Some(agent) = &self.wire.agent_ref {
            payload.insert("agent_ref".into(), json!(agent));
        }
        if let Some(automation) = self
            .wire
            .metadata
            .get("automation")
            .and_then(Value::as_object)
            .filter(|a| a.get("kind").and_then(Value::as_str) == Some("heartbeat"))
        {
            let metadata = payload.entry("metadata").or_insert_with(|| json!({}));
            if let Some(metadata) = metadata.as_object_mut() {
                metadata.insert("automation".into(), json!(automation));
            }
        }
        Ok(Projection {
            frame: Some(json!({"type":"event","event":event,"payload":payload})),
            complete: self.wire.is_final,
        })
    }

    fn result(&self) -> Result<Map<String, Value>> {
        self.wire
            .body
            .get("result")
            .and_then(Value::as_object)
            .cloned()
            .ok_or(ProtocolError::Invalid("invalid E2A completion payload"))
    }

    fn error_payload(&self) -> Map<String, Value> {
        if let Some(details) = self.wire.body.get("details").and_then(Value::as_object) {
            return details.clone();
        }
        let mut payload = self.wire.body.clone();
        if !payload.contains_key("error") {
            payload.insert(
                "error".into(),
                self.wire
                    .body
                    .get("message")
                    .cloned()
                    .unwrap_or_else(|| json!("Agent error")),
            );
        }
        payload
    }

    fn unary_frame(&self, request: &Request, failed: bool) -> Result<Value> {
        if !failed {
            return Ok(json!({"type":"res","id":request.id(),"ok":true,"payload":self.result()?}));
        }
        let payload = self.error_payload();
        let error = payload
            .get("error")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{} failed", request.method_name()));
        let default_code = if request.method() == Method::SessionCreate {
            "SESSION_CREATE_FAILED"
        } else {
            "BAD_REQUEST"
        };
        let code = payload
            .get("code")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(default_code);
        Ok(json!({"type":"res","id":request.id(),"ok":false,"error":error,"code":code}))
    }

    fn chunk_payload(&self) -> Map<String, Value> {
        let kind = self.wire.body.get("delta_kind").and_then(Value::as_str);
        let event = self.wire.body.get("event_type").and_then(Value::as_str);
        let delta = self.wire.body.get("delta").cloned().unwrap_or(Value::Null);
        if event == Some("chat.delta") || matches!(kind, Some("text" | "reasoning")) {
            let mut payload = Map::new();
            payload.insert("event_type".into(), json!("chat.delta"));
            payload.insert(
                "content".into(),
                if delta.is_null() { json!("") } else { delta },
            );
            if kind == Some("reasoning") {
                payload.insert("source_chunk_type".into(), json!("llm_reasoning"));
            } else if let Some(value) = self
                .wire
                .body
                .get("source_chunk_type")
                .filter(|v| !v.is_null())
            {
                payload.insert("source_chunk_type".into(), value.clone());
            }
            for key in [
                "rid",
                "role",
                "member_name",
                "agent_template_name",
                "execution_id",
                "output_phase_id",
                "output_suppressed",
                "output_order",
                "timestamp",
                "turn_request_id",
                "message_origin",
                "session_message_id",
                "cross_session",
            ] {
                if let Some(value) = self.wire.body.get(key).filter(|v| !v.is_null()) {
                    payload.insert(key.into(), value.clone());
                }
            }
            payload
        } else {
            let mut payload = match delta {
                Value::Object(payload) => payload,
                Value::Null => Map::new(),
                value => Map::from_iter([("content".into(), value)]),
            };
            if let Some(event) = event {
                payload.entry("event_type").or_insert_with(|| json!(event));
            }
            payload
        }
    }
}

/// One authorized backend connection's observed session state. It is not a
/// cross-replica runtime snapshot; unknown sessions match the original Gateway's false default.
pub struct SessionState {
    busy: std::collections::HashSet<String>,
    capacity: usize,
}
impl SessionState {
    /// Reject a zero capacity. Exhaustion is reported by observe without evicting active sessions.
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ProtocolError::Invalid(
                "session state capacity must be positive",
            ));
        }
        Ok(Self {
            busy: std::collections::HashSet::new(),
            capacity,
        })
    }

    /// Consume only processing/interrupt events from a validated projection.
    /// Terminal transport markers and chat.final segments do not change busy state.
    /// Returns Invalid for malformed session IDs or Capacity when tracking is full.
    pub fn observe(&mut self, projection: &Projection) -> Result<()> {
        let Some(frame) = &projection.frame else {
            return Ok(());
        };
        if frame.get("type").and_then(Value::as_str) != Some("event") {
            return Ok(());
        }
        let payload = &frame["payload"];
        let busy = match frame.get("event").and_then(Value::as_str) {
            Some("chat.processing_status") => payload
                .get("is_processing")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            Some("chat.interrupt_result") => matches!(
                payload.get("intent").and_then(Value::as_str),
                Some("pause" | "supplement" | "resume")
            ),
            _ => return Ok(()),
        };
        let Some(session) = payload.get("session_id").and_then(Value::as_str) else {
            return Ok(());
        };
        if !super::protocol::session_id(session) {
            return Err(ProtocolError::Invalid("invalid processing session_id"));
        }
        if busy {
            if self.busy.len() >= self.capacity && !self.busy.contains(session) {
                return Err(ProtocolError::Capacity("too many busy Jiuwen sessions"));
            }
            self.busy.insert(session.into());
        } else {
            self.busy.remove(session);
        }
        Ok(())
    }

    /// Apply the observed runtime state after a successful metadata response.
    /// Call in receive order with observe so a preceding status event is reflected.
    pub fn augment_metadata(&self, request: &Request, projection: &mut Projection) {
        if request.method() != Method::SessionMetadata {
            return;
        }
        let Some(frame) = &mut projection.frame else {
            return;
        };
        if frame.get("type").and_then(Value::as_str) != Some("res")
            || frame.get("ok") != Some(&Value::Bool(true))
        {
            return;
        }
        if let Some(payload) = frame.get_mut("payload").and_then(Value::as_object_mut) {
            let busy = request
                .session_id()
                .is_some_and(|id| self.busy.contains(id));
            payload.insert("is_processing".into(), json!(busy));
        }
    }
}

fn interaction(event: &str) -> bool {
    matches!(event, "chat.ask_user_question" | "plan.approval_required")
}

/// Only transport aliases are rewritten. Interaction IDs and nested history/answer
/// fields remain backend business identities even if they equal a transport ID.
pub(super) fn remap_ids(projection: &mut Projection, resolve: impl Fn(&str) -> Option<String>) {
    let Some(frame) = &mut projection.frame else {
        return;
    };
    if frame.get("type").and_then(Value::as_str) != Some("event") {
        return;
    }
    let preserve_request = frame
        .get("event")
        .and_then(Value::as_str)
        .is_some_and(interaction);
    let Some(payload) = frame.get_mut("payload").and_then(Value::as_object_mut) else {
        return;
    };
    for key in ["request_id", "turn_request_id", "input_request_id"] {
        if key == "request_id" && preserve_request {
            continue;
        }
        if let Some(mapped) = payload.get(key).and_then(Value::as_str).and_then(&resolve) {
            payload.insert(key.into(), json!(mapped));
        }
    }
}
