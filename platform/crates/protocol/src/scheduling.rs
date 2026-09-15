//! Strict typed transport conversion for placement policies.
use crate::control as wire;
use adx_core::{scheduling as model, Error, Result};
fn required<T>(v: Option<T>) -> Result<T> {
    v.ok_or_else(|| Error::Invalid("missing scheduling submessage".into()))
}
fn decode<T, U: TryFrom<T, Error = Error>>(v: Vec<T>) -> Result<Vec<U>> {
    v.into_iter().map(TryInto::try_into).collect()
}
fn encode<T, U: From<T>>(v: Vec<T>) -> Vec<U> {
    v.into_iter().map(Into::into).collect()
}
impl TryFrom<wire::Device> for model::Device {
    type Error = Error;
    fn try_from(v: wire::Device) -> Result<Self> {
        let value = Self {
            id: v.id,
            kind: kind(v.kind)?,
            model: v.model,
            healthy: v.healthy,
        };
        Ok(value)
    }
}
impl From<model::Device> for wire::Device {
    fn from(v: model::Device) -> Self {
        Self {
            id: v.id,
            kind: kind_wire(v.kind),
            model: v.model,
            healthy: v.healthy,
        }
    }
}
impl TryFrom<wire::DeviceAllocation> for model::DeviceAllocation {
    type Error = Error;
    fn try_from(v: wire::DeviceAllocation) -> Result<Self> {
        let value = Self {
            id: v.id,
            kind: kind(v.kind)?,
            model: v.model,
        };
        Ok(value)
    }
}
impl From<model::DeviceAllocation> for wire::DeviceAllocation {
    fn from(v: model::DeviceAllocation) -> Self {
        Self {
            id: v.id,
            kind: kind_wire(v.kind),
            model: v.model,
        }
    }
}
impl TryFrom<wire::DeviceRequest> for model::DeviceRequest {
    type Error = Error;
    fn try_from(v: wire::DeviceRequest) -> Result<Self> {
        let value = Self {
            kind: kind(v.kind)?,
            model: v.model,
            count: v.count,
        };
        Ok(value)
    }
}
impl From<model::DeviceRequest> for wire::DeviceRequest {
    fn from(v: model::DeviceRequest) -> Self {
        Self {
            kind: kind_wire(v.kind),
            model: v.model,
            count: v.count,
        }
    }
}
impl TryFrom<wire::LabelRequirement> for model::LabelRequirement {
    type Error = Error;
    fn try_from(v: wire::LabelRequirement) -> Result<Self> {
        let value = Self {
            key: v.key,
            op: op(v.op)?,
            values: v.values,
        };
        Ok(value)
    }
}
impl From<model::LabelRequirement> for wire::LabelRequirement {
    fn from(v: model::LabelRequirement) -> Self {
        Self {
            key: v.key,
            op: op_wire(v.op),
            values: v.values,
        }
    }
}
impl TryFrom<wire::LabelSelector> for model::LabelSelector {
    type Error = Error;
    fn try_from(v: wire::LabelSelector) -> Result<Self> {
        let value = Self {
            match_labels: v.match_labels.into_iter().collect(),
            expressions: decode(v.expressions)?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<model::LabelSelector> for wire::LabelSelector {
    fn from(v: model::LabelSelector) -> Self {
        Self {
            match_labels: v.match_labels.into_iter().collect(),
            expressions: encode(v.expressions),
        }
    }
}
impl TryFrom<wire::WeightedSelector> for model::WeightedSelector {
    type Error = Error;
    fn try_from(v: wire::WeightedSelector) -> Result<Self> {
        let value = Self {
            selector: required(v.selector)?.try_into()?,
            weight: v.weight,
        };
        Ok(value)
    }
}
impl From<model::WeightedSelector> for wire::WeightedSelector {
    fn from(v: model::WeightedSelector) -> Self {
        Self {
            selector: Some(v.selector.into()),
            weight: v.weight,
        }
    }
}
impl TryFrom<wire::PeerTerm> for model::PeerTerm {
    type Error = Error;
    fn try_from(v: wire::PeerTerm) -> Result<Self> {
        let value = Self {
            selector: required(v.selector)?.try_into()?,
            topology_key: v.topology_key,
            tenants: v.tenants,
        };
        Ok(value)
    }
}
impl From<model::PeerTerm> for wire::PeerTerm {
    fn from(v: model::PeerTerm) -> Self {
        Self {
            selector: Some(v.selector.into()),
            topology_key: v.topology_key,
            tenants: v.tenants,
        }
    }
}
impl TryFrom<wire::WeightedPeer> for model::WeightedPeer {
    type Error = Error;
    fn try_from(v: wire::WeightedPeer) -> Result<Self> {
        let value = Self {
            term: required(v.term)?.try_into()?,
            weight: v.weight,
        };
        Ok(value)
    }
}
impl From<model::WeightedPeer> for wire::WeightedPeer {
    fn from(v: model::WeightedPeer) -> Self {
        Self {
            term: Some(v.term.into()),
            weight: v.weight,
        }
    }
}
impl TryFrom<wire::TopologySpread> for model::TopologySpread {
    type Error = Error;
    fn try_from(v: wire::TopologySpread) -> Result<Self> {
        let value = Self {
            topology_key: v.topology_key,
            selector: required(v.selector)?.try_into()?,
            max_skew: v.max_skew,
            min_domains: v.min_domains,
            when_unsatisfiable: spread(v.when_unsatisfiable)?,
        };
        Ok(value)
    }
}
impl From<model::TopologySpread> for wire::TopologySpread {
    fn from(v: model::TopologySpread) -> Self {
        Self {
            topology_key: v.topology_key,
            selector: Some(v.selector.into()),
            max_skew: v.max_skew,
            min_domains: v.min_domains,
            when_unsatisfiable: spread_wire(v.when_unsatisfiable),
        }
    }
}
impl TryFrom<wire::SchedulingPolicy> for model::SchedulingPolicy {
    type Error = Error;
    fn try_from(v: wire::SchedulingPolicy) -> Result<Self> {
        let value = Self {
            labels: v.labels.into_iter().collect(),
            devices: decode(v.devices)?,
            required_node: decode(v.required_node)?,
            preferred_node: decode(v.preferred_node)?,
            required_affinity: decode(v.required_affinity)?,
            required_anti_affinity: decode(v.required_anti_affinity)?,
            preferred_affinity: decode(v.preferred_affinity)?,
            preferred_anti_affinity: decode(v.preferred_anti_affinity)?,
            topology_spread: decode(v.topology_spread)?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<model::SchedulingPolicy> for wire::SchedulingPolicy {
    fn from(v: model::SchedulingPolicy) -> Self {
        Self {
            labels: v.labels.into_iter().collect(),
            devices: encode(v.devices),
            required_node: encode(v.required_node),
            preferred_node: encode(v.preferred_node),
            required_affinity: encode(v.required_affinity),
            required_anti_affinity: encode(v.required_anti_affinity),
            preferred_affinity: encode(v.preferred_affinity),
            preferred_anti_affinity: encode(v.preferred_anti_affinity),
            topology_spread: encode(v.topology_spread),
        }
    }
}
fn kind(v: i32) -> Result<model::DeviceKind> {
    match v {
        1 => Ok(model::DeviceKind::Gpu),
        2 => Ok(model::DeviceKind::Npu),
        _ => Err(Error::Invalid("invalid scheduling enum".into())),
    }
}
fn kind_wire(v: model::DeviceKind) -> i32 {
    match v {
        model::DeviceKind::Gpu => 1,
        model::DeviceKind::Npu => 2,
    }
}
fn op(v: i32) -> Result<model::SelectorOp> {
    match v {
        1 => Ok(model::SelectorOp::In),
        2 => Ok(model::SelectorOp::NotIn),
        3 => Ok(model::SelectorOp::Exists),
        4 => Ok(model::SelectorOp::DoesNotExist),
        5 => Ok(model::SelectorOp::Gt),
        6 => Ok(model::SelectorOp::Lt),
        _ => Err(Error::Invalid("invalid scheduling enum".into())),
    }
}
fn op_wire(v: model::SelectorOp) -> i32 {
    match v {
        model::SelectorOp::In => 1,
        model::SelectorOp::NotIn => 2,
        model::SelectorOp::Exists => 3,
        model::SelectorOp::DoesNotExist => 4,
        model::SelectorOp::Gt => 5,
        model::SelectorOp::Lt => 6,
    }
}
fn spread(v: i32) -> Result<model::SpreadMode> {
    match v {
        1 => Ok(model::SpreadMode::DoNotSchedule),
        2 => Ok(model::SpreadMode::ScheduleAnyway),
        _ => Err(Error::Invalid("invalid scheduling enum".into())),
    }
}
fn spread_wire(v: model::SpreadMode) -> i32 {
    match v {
        model::SpreadMode::DoNotSchedule => 1,
        model::SpreadMode::ScheduleAnyway => 2,
    }
}
impl TryFrom<wire::RegisterNodeRequest> for model::Node {
    type Error = Error;
    fn try_from(v: wire::RegisterNodeRequest) -> Result<Self> {
        let cap = required(v.capacity)?;
        let node = Self {
            id: v.node_id,
            capacity: adx_core::Resources {
                cpu_millis: cap.cpu_millis,
                memory_bytes: cap.memory_bytes,
                disk_bytes: cap.disk_bytes,
            },
            available: v.accepting_allocations,
            labels: v.labels.into_iter().collect(),
            devices: decode(v.devices)?,
        };
        node.validate()?;
        Ok(node)
    }
}
