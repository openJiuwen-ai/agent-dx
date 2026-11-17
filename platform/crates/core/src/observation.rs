//! Bounded execution evidence carried through the existing state journal.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

macro_rules! labels {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub enum $name { $(#[serde(rename = $label)] $variant),+ }
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub fn as_str(self) -> &'static str { match self { $(Self::$variant => $label),+ } }
            pub fn parse(value: &str) -> Result<Self> {
                match value { $($label => Ok(Self::$variant)),+, _ => Err(Error::Invalid(concat!("invalid ", stringify!($name)).into())) }
            }
        }
    };
}
labels!(CreateStage {
    Scheduling => "scheduling", RuntimePrepare => "runtime_prepare", RuntimeStart => "runtime_start",
    Readiness => "readiness", RouteActivation => "route_activation", StatePublish => "state_publish", Unknown => "unknown",
});
labels!(InterruptionReason {
    Oom => "oom", RuntimeExit => "runtime_exit", ProbeFailed => "probe_failed",
    ExecutionMissing => "execution_missing", NodeUnreachable => "node_unreachable",
    ControlUnhealthy => "control_unhealthy", Unknown => "unknown",
});
labels!(EvidenceSource {
    BackendExit => "backend_exit", Probe => "probe", Reconcile => "reconcile",
    Coordinator => "coordinator", Control => "control", Unknown => "unknown",
});
labels!(OperationIntent { User => "user", Idle => "idle", Policy => "policy", Pressure => "pressure" });

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterruptionEvidence {
    pub event_id: String,
    pub detected_at_unix_millis: u64,
    pub assignment_generation: u64,
    pub runtime_id: String,
    pub worker_id: String,
    pub reason: InterruptionReason,
    pub evidence_source: EvidenceSource,
    pub exit_code: Option<i32>,
    pub detail: String,
}
impl InterruptionEvidence {
    pub fn validate(&self) -> Result<()> {
        if self.event_id.is_empty()
            || self.event_id.len() > 2048
            || self.runtime_id.is_empty()
            || self.runtime_id.len() > 1024
            || self.worker_id.is_empty()
            || self.worker_id.len() > 1024
            || self.assignment_generation == 0
            || self.detected_at_unix_millis == 0
            || self.detail.len() > 4096
        {
            return Err(Error::Invalid(
                "invalid execution interruption evidence".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption: Option<InterruptionEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<OperationIntent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_stage: Option<CreateStage>,
}

pub fn now_millis() -> Result<u64> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Unavailable("system clock before epoch".into()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Unavailable("system clock out of range".into()))
}

pub fn bounded_detail(value: &str) -> String {
    let mut end = value.len().min(4096);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
