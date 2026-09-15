//! Typed placement policies and node-local whole-card accounting.
use crate::{Error, Resources, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    Gpu,
    Npu,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Device {
    pub id: u32,
    pub kind: DeviceKind,
    pub model: String,
    pub healthy: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceAllocation {
    pub id: u32,
    pub kind: DeviceKind,
    pub model: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRequest {
    pub kind: DeviceKind,
    pub model: Option<String>,
    pub count: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub capacity: Resources,
    pub available: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub devices: Vec<Device>,
}
impl Node {
    pub fn validate(&self) -> Result<()> {
        nonempty(&self.id)?;
        self.capacity.validate()?;
        validate_labels(&self.labels)?;
        validate_inventory(&self.devices)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectorOp {
    In,
    NotIn,
    Exists,
    DoesNotExist,
    Gt,
    Lt,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelRequirement {
    pub key: String,
    pub op: SelectorOp,
    pub values: Vec<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LabelSelector {
    pub match_labels: BTreeMap<String, String>,
    pub expressions: Vec<LabelRequirement>,
}
impl LabelSelector {
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.match_labels
            .iter()
            .all(|(k, v)| labels.get(k) == Some(v))
            && self.expressions.iter().all(|r| {
                let value = labels.get(&r.key);
                match r.op {
                    SelectorOp::In => value.is_some_and(|v| r.values.contains(v)),
                    SelectorOp::NotIn => value.is_none_or(|v| !r.values.contains(v)),
                    SelectorOp::Exists => value.is_some(),
                    SelectorOp::DoesNotExist => value.is_none(),
                    SelectorOp::Gt | SelectorOp::Lt => value
                        .and_then(|v| v.parse::<i64>().ok())
                        .zip(r.values.first().and_then(|v| v.parse::<i64>().ok()))
                        .is_some_and(|(a, b)| if r.op == SelectorOp::Gt { a > b } else { a < b }),
                }
            })
    }
    pub fn validate(&self) -> Result<()> {
        validate_labels(&self.match_labels)?;
        for r in &self.expressions {
            nonempty(&r.key)?;
            let valid = match r.op {
                SelectorOp::In | SelectorOp::NotIn => !r.values.is_empty(),
                SelectorOp::Exists | SelectorOp::DoesNotExist => r.values.is_empty(),
                SelectorOp::Gt | SelectorOp::Lt => {
                    r.values.len() == 1 && r.values[0].parse::<i64>().is_ok()
                }
            };
            if !valid {
                return Err(Error::Invalid("invalid label selector operands".into()));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedSelector {
    pub selector: LabelSelector,
    pub weight: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerTerm {
    pub selector: LabelSelector,
    pub topology_key: String,
    #[serde(default)]
    pub tenants: Vec<String>,
}
impl PeerTerm {
    pub fn matches(
        &self,
        owner_tenant: &str,
        peer_tenant: &str,
        labels: &BTreeMap<String, String>,
    ) -> bool {
        (if self.tenants.is_empty() {
            owner_tenant == peer_tenant
        } else {
            self.tenants.iter().any(|t| t == peer_tenant)
        }) && self.selector.matches(labels)
    }
    fn validate(&self) -> Result<()> {
        self.selector.validate()?;
        nonempty(&self.topology_key)?;
        for t in &self.tenants {
            nonempty(t)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedPeer {
    pub term: PeerTerm,
    pub weight: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpreadMode {
    DoNotSchedule,
    ScheduleAnyway,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySpread {
    pub topology_key: String,
    pub selector: LabelSelector,
    pub max_skew: u32,
    pub min_domains: u32,
    pub when_unsatisfiable: SpreadMode,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingPolicy {
    pub labels: BTreeMap<String, String>,
    pub devices: Vec<DeviceRequest>,
    /// Required node selectors are OR terms; each selector's expressions are AND.
    pub required_node: Vec<LabelSelector>,
    pub preferred_node: Vec<WeightedSelector>,
    pub required_affinity: Vec<PeerTerm>,
    pub required_anti_affinity: Vec<PeerTerm>,
    pub preferred_affinity: Vec<WeightedPeer>,
    pub preferred_anti_affinity: Vec<WeightedPeer>,
    pub topology_spread: Vec<TopologySpread>,
}
impl SchedulingPolicy {
    pub fn matches_node(&self, labels: &BTreeMap<String, String>) -> bool {
        self.required_node.is_empty() || self.required_node.iter().any(|s| s.matches(labels))
    }
    pub fn validate(&self) -> Result<()> {
        validate_labels(&self.labels)?;
        validate_requests(&self.devices)?;
        for s in &self.required_node {
            s.validate()?;
        }
        for s in &self.preferred_node {
            s.selector.validate()?;
            weight(s.weight)?;
        }
        for t in self
            .required_affinity
            .iter()
            .chain(&self.required_anti_affinity)
        {
            t.validate()?;
        }
        for t in self
            .preferred_affinity
            .iter()
            .chain(&self.preferred_anti_affinity)
        {
            t.term.validate()?;
            weight(t.weight)?;
        }
        for t in &self.topology_spread {
            nonempty(&t.topology_key)?;
            t.selector.validate()?;
            if t.max_skew == 0 || t.min_domains == 0 {
                return Err(Error::Invalid(
                    "topology max_skew and min_domains must be positive".into(),
                ));
            }
        }
        Ok(())
    }
}
fn nonempty(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::Invalid("empty identity or label key".into()))
    } else {
        Ok(())
    }
}
fn weight(value: u32) -> Result<()> {
    if value == 0 {
        Err(Error::Invalid("preference weight must be positive".into()))
    } else {
        Ok(())
    }
}
fn validate_labels(labels: &BTreeMap<String, String>) -> Result<()> {
    for key in labels.keys() {
        nonempty(key)?;
    }
    Ok(())
}
pub fn validate_requests(requests: &[DeviceRequest]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for r in requests {
        if r.count == 0
            || r.model.as_ref().is_some_and(|m| m.trim().is_empty())
            || !seen.insert((r.kind, r.model.as_deref()))
        {
            return Err(Error::Invalid("invalid or duplicate device request".into()));
        }
    }
    Ok(())
}
pub fn validate_inventory(devices: &[Device]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for d in devices {
        nonempty(&d.model)?;
        if !seen.insert((d.kind, d.id)) {
            return Err(Error::Invalid("duplicate physical device".into()));
        }
    }
    Ok(())
}
pub fn validate_allocations(devices: &[DeviceAllocation]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for d in devices {
        nonempty(&d.model)?;
        if !seen.insert((d.kind, d.id)) {
            return Err(Error::Invalid("duplicate allocated device".into()));
        }
    }
    Ok(())
}
/// Match model-specific requests before wildcard requests, so a wildcard cannot
/// consume the only card satisfying a specific request. Deterministic by kind/id.
pub fn select_devices(
    requests: &[DeviceRequest],
    devices: &[Device],
) -> Result<Vec<DeviceAllocation>> {
    validate_requests(requests)?;
    validate_inventory(devices)?;
    let mut requests: Vec<_> = requests.iter().collect();
    requests.sort_by_key(|r| (r.model.is_none(), r.kind, r.model.as_deref()));
    let mut devices: Vec<_> = devices.iter().filter(|d| d.healthy).collect();
    devices.sort_by_key(|d| (d.kind, d.id));
    let mut used = BTreeSet::new();
    let mut result = Vec::new();
    for r in requests {
        let mut count = 0;
        for d in &devices {
            if count == r.count {
                break;
            }
            if d.kind == r.kind
                && r.model.as_ref().is_none_or(|m| m == &d.model)
                && used.insert((d.kind, d.id))
            {
                result.push(DeviceAllocation {
                    id: d.id,
                    kind: d.kind,
                    model: d.model.clone(),
                });
                count += 1;
            }
        }
        if count != r.count {
            return Err(Error::NoCapacity);
        }
    }
    result.sort_by_key(|d| (d.kind, d.id));
    Ok(result)
}
pub fn validate_device_assignment(
    requests: &[DeviceRequest],
    allocated: &[DeviceAllocation],
) -> Result<()> {
    validate_allocations(allocated)?;
    let inventory: Vec<_> = allocated
        .iter()
        .map(|d| Device {
            id: d.id,
            kind: d.kind,
            model: d.model.clone(),
            healthy: true,
        })
        .collect();
    let selected = select_devices(requests, &inventory)?;
    if selected.len() != allocated.len() {
        return Err(Error::Invalid(
            "device allocation does not match request".into(),
        ));
    }
    Ok(())
}
#[derive(Debug, Clone, Default)]
pub struct DeviceLedger {
    inventory: Vec<Device>,
    held: BTreeMap<String, Vec<DeviceAllocation>>,
    busy: BTreeSet<(DeviceKind, u32)>,
}
impl DeviceLedger {
    pub fn update(&mut self, devices: Vec<Device>) -> Result<()> {
        validate_inventory(&devices)?;
        self.inventory = devices;
        Ok(())
    }
    pub fn available(&self) -> Vec<Device> {
        self.inventory
            .iter()
            .filter(|d| d.healthy && !self.busy.contains(&(d.kind, d.id)))
            .cloned()
            .collect()
    }
    pub fn reserve(
        &mut self,
        id: &str,
        requests: &[DeviceRequest],
        allocated: &[DeviceAllocation],
    ) -> Result<()> {
        validate_device_assignment(requests, allocated)?;
        if let Some(existing) = self.held.get(id) {
            return if existing == allocated {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        let available = if allocated.is_empty() {
            Vec::new()
        } else {
            self.available()
        };
        if allocated.iter().any(|a| {
            !available
                .iter()
                .any(|d| d.id == a.id && d.kind == a.kind && d.model == a.model)
        }) {
            return Err(Error::NoCapacity);
        }
        self.busy.extend(allocated.iter().map(|d| (d.kind, d.id)));
        self.held.insert(id.into(), allocated.to_vec());
        Ok(())
    }
    pub fn release(&mut self, id: &str) -> Result<()> {
        let allocated = self.held.remove(id).ok_or(Error::NotFound)?;
        for d in allocated {
            self.busy.remove(&(d.kind, d.id));
        }
        Ok(())
    }
    /// Restore persisted occupancy even if a card is missing or unhealthy now.
    /// Conflicting ownership remains an error; this is not new admission.
    pub fn restore(&mut self, id: &str, allocated: &[DeviceAllocation]) -> Result<()> {
        validate_allocations(allocated)?;
        if let Some(old) = self.held.get(id) {
            return if old == allocated {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if allocated
            .iter()
            .any(|d| self.busy.contains(&(d.kind, d.id)))
        {
            return Err(Error::Conflict);
        }
        self.busy.extend(allocated.iter().map(|d| (d.kind, d.id)));
        self.held.insert(id.into(), allocated.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selectors_cover_presence_set_and_numeric_operators() {
        let labels = BTreeMap::from([("zone".into(), "a".into()), ("version".into(), "7".into())]);
        for (key, op, values, expected) in [
            ("zone", SelectorOp::In, vec!["a"], true),
            ("zone", SelectorOp::NotIn, vec!["a"], false),
            ("missing", SelectorOp::NotIn, vec!["a"], true),
            ("missing", SelectorOp::Exists, vec![], false),
            ("missing", SelectorOp::DoesNotExist, vec![], true),
            ("version", SelectorOp::Gt, vec!["6"], true),
            ("version", SelectorOp::Lt, vec!["8"], true),
            ("zone", SelectorOp::Gt, vec!["6"], false),
        ] {
            let selector = LabelSelector {
                match_labels: Default::default(),
                expressions: vec![LabelRequirement {
                    key: key.into(),
                    op,
                    values: values.into_iter().map(str::to_owned).collect(),
                }],
            };
            selector.validate().unwrap();
            assert_eq!(selector.matches(&labels), expected);
        }
    }
    #[test]
    fn failed_device_reservation_is_atomic_and_inventory_refresh_preserves_busy_cards() {
        let cards = vec![Device {
            id: 0,
            kind: DeviceKind::Gpu,
            model: "a".into(),
            healthy: true,
        }];
        let requests = vec![DeviceRequest {
            kind: DeviceKind::Gpu,
            model: None,
            count: 1,
        }];
        let allocated = select_devices(&requests, &cards).unwrap();
        let mut ledger = DeviceLedger::default();
        ledger.update(cards.clone()).unwrap();
        ledger.reserve("a", &requests, &allocated).unwrap();
        assert!(ledger.reserve("b", &requests, &allocated).is_err());
        ledger.update(vec![]).unwrap();
        ledger.update(cards).unwrap();
        assert!(ledger.available().is_empty());
        ledger.release("a").unwrap();
        assert_eq!(ledger.available().len(), 1);
    }
}
