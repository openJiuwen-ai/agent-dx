//! Hard and soft placement rules over one coherent cluster snapshot.
use crate::query::{PeerMatches, Prepared};
use crate::{Candidate, Filter, Score, MAX_SCORE};
use adx_core::{scheduling::*, EnvironmentSpec, Result};
use std::borrow::Cow;
fn prepared<'a>(r: &EnvironmentSpec, c: &'a Candidate<'_>) -> Cow<'a, Prepared> {
    match c.prepared {
        Some(p) => Cow::Borrowed(p),
        None => Cow::Owned(Prepared::new(r, c.snapshot)),
    }
}
fn matches(c: &Candidate<'_>, term: &PeerTerm, peers: &PeerMatches) -> bool {
    c.node
        .labels
        .get(&term.topology_key)
        .is_some_and(|v| peers.values.contains(v))
}

pub struct DeviceFit;
impl Filter for DeviceFit {
    fn name(&self) -> &'static str {
        "device-fit"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        match select_devices(&r.scheduling.devices, c.devices) {
            Ok(_) => Ok(true),
            Err(adx_core::Error::NoCapacity) => Ok(false),
            Err(e) => Err(e),
        }
    }
}
pub struct RuntimeFit;
impl Filter for RuntimeFit {
    fn name(&self) -> &'static str {
        "runtime-fit"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        Ok(c.node.runtime_classes.contains(&r.runtime_class))
    }
}
pub struct NodeAffinity;
impl Filter for NodeAffinity {
    fn name(&self) -> &'static str {
        "node-affinity"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        Ok(r.scheduling.matches_node(&c.node.labels))
    }
}
pub struct EnvironmentAffinity;
impl Filter for EnvironmentAffinity {
    fn name(&self) -> &'static str {
        "environment-affinity"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        let q = prepared(r, c);
        for (term, peers) in r.scheduling.required_affinity.iter().zip(&q.required) {
            if !c.node.labels.contains_key(&term.topology_key) {
                return Ok(false);
            }
            if !matches(c, term, peers)
                && (peers.any || !term.matches(&r.tenant_id, &r.tenant_id, &r.scheduling.labels))
            {
                return Ok(false);
            }
        }
        for (term, peers) in r.scheduling.required_anti_affinity.iter().zip(&q.anti) {
            if !c.node.labels.contains_key(&term.topology_key) || matches(c, term, peers) {
                return Ok(false);
            }
        }
        if q.reverse_missing_node {
            return Ok(false);
        }
        for (key, value) in &q.reverse {
            if value.is_none()
                || !c.node.labels.contains_key(key)
                || c.node.labels.get(key) == value.as_ref()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
pub struct Topology;
impl Filter for Topology {
    fn name(&self) -> &'static str {
        "topology-spread"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        let q = prepared(r, c);
        for (t, counts) in r
            .scheduling
            .topology_spread
            .iter()
            .zip(&q.spread)
            .filter(|(t, _)| t.when_unsatisfiable == SpreadMode::DoNotSchedule)
        {
            let Some(count) = c
                .node
                .labels
                .get(&t.topology_key)
                .and_then(|v| counts.get(v))
            else {
                return Ok(false);
            };
            let min = if counts.len() < t.min_domains as usize {
                0
            } else {
                *counts.values().min().unwrap_or(&0)
            };
            let incoming = u64::from(t.selector.matches(&r.scheduling.labels));
            if count + incoming - min > u64::from(t.max_skew) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn normalized(terms: impl Iterator<Item = (u32, bool)>) -> u32 {
    let (value, total) = terms.fold((0u128, 0u128), |(value, total), (weight, matched)| {
        (
            value + if matched { u128::from(weight) } else { 0 },
            total + u128::from(weight),
        )
    });
    (value * u128::from(MAX_SCORE))
        .checked_div(total)
        .unwrap_or(0) as u32
}
pub struct NodePreference;
impl Score for NodePreference {
    fn name(&self) -> &'static str {
        "node-preference"
    }
    fn score(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<u32> {
        Ok(normalized(
            r.scheduling
                .preferred_node
                .iter()
                .map(|t| (t.weight, t.selector.matches(&c.node.labels))),
        ))
    }
}
pub struct EnvironmentPreference;
impl Score for EnvironmentPreference {
    fn name(&self) -> &'static str {
        "environment-preference"
    }
    fn score(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<u32> {
        let q = prepared(r, c);
        Ok(normalized(
            r.scheduling
                .preferred_affinity
                .iter()
                .zip(&q.preferred)
                .map(|(t, peers)| (t.weight, matches(c, &t.term, peers)))
                .chain(
                    r.scheduling
                        .preferred_anti_affinity
                        .iter()
                        .zip(&q.preferred_anti)
                        .map(|(t, peers)| {
                            (
                                t.weight,
                                c.node.labels.contains_key(&t.term.topology_key)
                                    && !matches(c, &t.term, peers),
                            )
                        }),
                ),
        ))
    }
}
pub struct TopologyPreference;
impl Score for TopologyPreference {
    fn name(&self) -> &'static str {
        "topology-preference"
    }
    fn score(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<u32> {
        let q = prepared(r, c);
        let mut total = 0u128;
        let mut terms = 0u128;
        for (t, counts) in r
            .scheduling
            .topology_spread
            .iter()
            .zip(&q.spread)
            .filter(|(t, _)| t.when_unsatisfiable == SpreadMode::ScheduleAnyway)
        {
            terms += 1;
            if let Some(count) = c
                .node
                .labels
                .get(&t.topology_key)
                .and_then(|v| counts.get(v))
            {
                total += u128::from(MAX_SCORE) / (u128::from(*count) + 1);
            }
        }
        Ok(total.checked_div(terms).unwrap_or(0) as u32)
    }
}
