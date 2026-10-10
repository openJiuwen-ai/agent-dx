//! beegent request adaptation to the existing AgentServer E2A 1.0 contract.
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// AgentServer's existing maximum message size, including the E2A envelope.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Invalid(&'static str),
    Capacity(&'static str),
    TooLarge,
}
impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::Capacity(message) => f.write_str(message),
            Self::TooLarge => f.write_str("Jiuwen message exceeds AgentServer frame limit"),
        }
    }
}
impl std::error::Error for ProtocolError {}

type Result<T> = std::result::Result<T, ProtocolError>;

// Named variants select existing client compatibility behavior; Generic still uses the same E2A connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    SessionList,
    SessionCreate,
    SessionMetadata,
    History,
    Chat,
    Interrupt,
    UserAnswer,
    SwarmflowReply,
    Generic,
}
impl Method {
    fn parse(value: &str) -> Self {
        match value {
            "session.list" => Self::SessionList,
            "session.create" => Self::SessionCreate,
            "session.get_metadata" => Self::SessionMetadata,
            "history.get" => Self::History,
            "chat.send" => Self::Chat,
            "chat.interrupt" => Self::Interrupt,
            "chat.user_answer" => Self::UserAnswer,
            "chat.swarmflow_reply" => Self::SwarmflowReply,
            _ => Self::Generic,
        }
    }
}

#[derive(Deserialize)]
struct WireRequest {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    method: String,
    #[serde(default)]
    params: Map<String, Value>,
    #[serde(default)]
    is_stream: bool,
}

#[derive(Debug)]
pub struct Request {
    id: String,
    method: Method,
    method_name: String,
    session_id: Option<String>,
    params: Map<String, Value>,
    is_stream: bool,
}

/// Prepare once and retain across an unsent transport retry; do not regenerate create_token.
#[derive(Debug)]
pub struct PreparedRequest {
    pub envelope: Value,
    /// Gateway admission acknowledgement only; never an Agent execution receipt.
    pub acknowledgement: Option<Value>,
}

fn identifier(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
pub(super) fn session_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    let edge = |b: &u8| b.is_ascii_alphanumeric() || *b == b'_';
    !bytes.is_empty()
        && bytes.len() <= 80
        && bytes.first().is_some_and(edge)
        && bytes.last().is_some_and(edge)
        && bytes.iter().all(|b| edge(b) || matches!(b, b'.' | b'-'))
}

impl Request {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn method(&self) -> Method {
        self.method
    }

    pub fn method_name(&self) -> &str {
        &self.method_name
    }

    pub fn is_stream(&self) -> bool {
        self.is_stream
    }

    pub fn is_unary(&self) -> bool {
        matches!(
            self.method,
            Method::SessionList | Method::SessionCreate | Method::SessionMetadata
        ) || (self.method == Method::Generic && !self.is_stream)
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Reject malformed or oversized frames before storing any routing state.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(ProtocolError::TooLarge);
        }
        let wire: WireRequest = serde_json::from_slice(bytes)
            .map_err(|_| ProtocolError::Invalid("invalid Jiuwen request"))?;
        if wire.kind != "req" || !identifier(&wire.id) {
            return Err(ProtocolError::Invalid("invalid Jiuwen request identity"));
        }
        if !identifier(&wire.method) {
            return Err(ProtocolError::Invalid("invalid Jiuwen method"));
        }
        let method = Method::parse(&wire.method);
        let session = if method == Method::SessionCreate {
            None
        } else {
            match wire.params.get("session_id") {
                Some(Value::String(value)) if session_id(value) => Some(value.clone()),
                None if matches!(method, Method::SessionList | Method::Generic) => None,
                _ => return Err(ProtocolError::Invalid("invalid or missing session_id")),
            }
        };
        let is_stream = matches!(method, Method::Chat | Method::History)
            || (method == Method::Generic && wire.is_stream);
        Ok(Self {
            id: wire.id,
            method,
            method_name: wire.method,
            session_id: session,
            params: wire.params,
            is_stream,
        })
    }

    /// `user_id` must come from the caller's verified identity, never client params.
    /// `backend_request_id` is allocated by the connection owner to avoid client ID collisions.
    pub fn prepare(&self, user_id: &str, backend_request_id: &str) -> Result<PreparedRequest> {
        if !identifier(user_id) || !identifier(backend_request_id) {
            return Err(ProtocolError::Invalid(
                "verified user and backend request identity required",
            ));
        }
        let mut params = self.params.clone();
        params.remove("user_id");
        if self.method == Method::SessionCreate {
            params.remove("session_id");
            params
                .entry("create_token")
                .or_insert_with(|| Value::String(uuid::Uuid::new_v4().simple().to_string()));
        }
        if self.method == Method::Chat && !params.contains_key("query") {
            if let Some(content) = params.get("content").cloned() {
                params.insert("query".into(), content);
            }
        }
        let acknowledgement = if self.is_unary() || self.method == Method::Generic {
            None
        } else {
            let mut payload = json!({"accepted":true,"session_id":self.session_id});
            let fields: &[&str] = match self.method {
                Method::History => &["page_idx", "cursor", "limit"],
                Method::Interrupt => &["intent"],
                Method::UserAnswer => &["request_id"],
                _ => &[],
            };
            for field in fields {
                if let Some(value) = params.get(*field) {
                    if self.method == Method::History
                        || value.as_str().is_some_and(|value| !value.is_empty())
                    {
                        payload[*field] = value.clone();
                    }
                }
            }
            Some(json!({"type":"res","id":self.id,"ok":true,"payload":payload}))
        };
        let envelope = json!({
            "protocol_version":"1.0", "request_id":backend_request_id,
            "identity_origin":"user", "channel":"web", "user_id":user_id,
            "session_id":self.session_id, "method":self.method_name,
            "params":params, "is_stream":self.is_stream,
            "provenance":{"source_protocol":"e2a"}
        });
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|_| ProtocolError::Invalid("invalid E2A request"))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(ProtocolError::TooLarge);
        }
        Ok(PreparedRequest {
            envelope,
            acknowledgement,
        })
    }
}
