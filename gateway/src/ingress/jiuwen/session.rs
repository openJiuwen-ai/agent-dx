//! Per-authorized-connection request ownership and ordered response/push dispatch.
use super::protocol::{PreparedRequest, ProtocolError, Request};
use super::response::{remap_ids, Response, SessionState};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Clone, Copy)]
pub struct SessionLimits {
    pub pending: usize,
    pub recent: usize,
    pub busy: usize,
}

struct Alias {
    client_id: String,
    session_id: Option<String>,
}

/// Owned by one backend connection and one authenticated frontend attachment.
/// Never reuse it across users or reconnections. The transport driver supplies
/// the dedicated Sandbox connection and calls receive serially in wire order.
pub struct Session {
    user_id: String,
    limits: SessionLimits,
    pending: HashMap<String, Request>,
    recent: HashMap<String, Alias>,
    recent_order: VecDeque<String>,
    client_ids: HashSet<String>,
    state: SessionState,
}
impl Session {
    /// Validate identity and explicit positive capacities. Identity must already
    /// be authenticated and bound to the backend Sandbox by the caller.
    pub fn new(user_id: String, limits: SessionLimits) -> Result<Self> {
        if user_id.trim().is_empty()
            || user_id.len() > 256
            || user_id.chars().any(char::is_control)
            || limits.pending == 0
            || limits.recent == 0
        {
            return Err(ProtocolError::Invalid(
                "invalid Jiuwen session identity or capacity",
            ));
        }
        Ok(Self {
            user_id,
            limits,
            pending: HashMap::new(),
            recent: HashMap::new(),
            recent_order: VecDeque::new(),
            client_ids: HashSet::new(),
            state: SessionState::new(limits.busy)?,
        })
    }

    /// Reserve a fresh backend ID and prepare the E2A request exactly once.
    /// Returns Invalid for a live/recent client ID collision or Capacity when full.
    /// The driver must not replay the returned envelope after a possible write.
    pub fn submit(&mut self, request: Request) -> Result<PreparedRequest> {
        if self.client_ids.contains(request.id()) {
            return Err(ProtocolError::Invalid("duplicate Jiuwen request ID"));
        }
        if self.pending.len() >= self.limits.pending {
            return Err(ProtocolError::Capacity("too many pending Jiuwen requests"));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let prepared = request.prepare(&self.user_id, &id)?;
        self.client_ids.insert(request.id().into());
        self.pending.insert(id, request);
        Ok(prepared)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Dispatch a validated response on this connection. Unknown ordinary replies
    /// are discarded as stale; pushes never consume an ordinary response slot.
    /// Only a terminal push sentinel can retire its matching streaming request.
    /// Explicit owner mismatches and state exhaustion return structured errors.
    pub fn receive(&mut self, response: &Response) -> Result<Option<Value>> {
        let id = response.request_id().unwrap_or("");
        let request = self.pending.get(id);
        let mut projection = if response.is_push() {
            let session = request
                .and_then(Request::session_id)
                .or_else(|| self.recent.get(id).and_then(|a| a.session_id.as_deref()));
            response.project_push(&self.user_id, session)?
        } else {
            let Some(request) = request else {
                return Ok(None);
            };
            response.project_unmapped(request, id, &self.user_id)?
        };
        remap_ids(&mut projection, |backend_id| {
            self.pending
                .get(backend_id)
                .map(|r| r.id().to_owned())
                .or_else(|| self.recent.get(backend_id).map(|a| a.client_id.clone()))
        });
        self.state.observe(&projection)?;
        if !response.is_push() {
            if let Some(request) = request {
                self.state.augment_metadata(request, &mut projection);
            }
        }
        let retire = projection.complete
            && (!response.is_push()
                || projection.frame.is_none() && request.is_some_and(|r| r.is_stream()));
        if retire {
            self.retire(id);
        }
        Ok(projection.frame)
    }

    /// Stop waiting for one timed-out unary/control request. This does not cancel
    /// backend work or imply that a write was rejected; late ordinary replies are stale.
    pub(super) fn timeout(&mut self, id: &str) -> Option<Value> {
        let request = self.pending.get(id)?;
        let frame = if request.is_unary() {
            serde_json::json!({"type":"res","id":request.id(),"ok":false,"code":"AGENT_SERVER_TIMEOUT","error":"AgentServer request timed out"})
        } else {
            serde_json::json!({"type":"event","event":"chat.error","payload":{"request_id":request.id(),"session_id":request.session_id(),"code":"SESSION_INPUT_DELIVERY_UNKNOWN","error":"AgentServer request timed out; result is unknown","is_complete":true}})
        };
        self.retire(id);
        Some(frame)
    }

    fn retire(&mut self, id: &str) {
        let Some(request) = self.pending.remove(id) else {
            return;
        };
        self.recent.insert(
            id.into(),
            Alias {
                client_id: request.id().into(),
                session_id: request.session_id().map(str::to_owned),
            },
        );
        self.recent_order.push_back(id.into());
        while self.recent.len() > self.limits.recent {
            if let Some(oldest) = self.recent_order.pop_front() {
                if let Some(alias) = self.recent.remove(&oldest) {
                    self.client_ids.remove(&alias.client_id);
                }
            }
        }
    }
}
