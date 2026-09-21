//! Request-scoped precomputation, shared by every Filter/Score candidate.
use crate::Snapshot;
use adx_core::{scheduling::*, CapsuleSpec};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Default)]
pub struct PeerMatches {
    pub any: bool,
    pub values: BTreeSet<String>,
}
#[derive(Clone, Default)]
pub struct Prepared {
    pub required: Vec<PeerMatches>,
    pub anti: Vec<PeerMatches>,
    pub preferred: Vec<PeerMatches>,
    pub preferred_anti: Vec<PeerMatches>,
    pub reverse: BTreeSet<(String, Option<String>)>,
    pub reverse_missing_node: bool,
    pub groups: Vec<Vec<BTreeSet<String>>>,
    pub reverse_group_nodes: BTreeSet<String>,
    pub spread: Vec<BTreeMap<String, u64>>,
}
impl Prepared {
    pub fn new(r: &CapsuleSpec, snapshot: &Snapshot) -> Self {
        let peers = |term: &PeerTerm| {
            let mut result = PeerMatches::default();
            let tenants = if term.tenants.is_empty() {
                vec![r.tenant_id.as_str()]
            } else {
                term.tenants.iter().map(String::as_str).collect()
            };
            for tenant in tenants {
                for p in snapshot.matching(tenant, &term.selector) {
                    result.any = true;
                    if let Some(value) = snapshot
                        .node(&p.node_id)
                        .and_then(|n| n.labels.get(&term.topology_key))
                    {
                        result.values.insert(value.clone());
                    }
                }
            }
            result
        };
        let mut result = Self {
            required: r.scheduling.required_affinity.iter().map(peers).collect(),
            anti: r
                .scheduling
                .required_anti_affinity
                .iter()
                .map(peers)
                .collect(),
            preferred: r
                .scheduling
                .preferred_affinity
                .iter()
                .map(|t| peers(&t.term))
                .collect(),
            preferred_anti: r
                .scheduling
                .preferred_anti_affinity
                .iter()
                .map(|t| peers(&t.term))
                .collect(),
            ..Default::default()
        };
        result.groups = r
            .scheduling
            .placement_groups
            .iter()
            .map(|g| {
                g.terms
                    .iter()
                    .map(|term| {
                        if g.target == PlacementTarget::Node {
                            BTreeSet::new()
                        } else {
                            snapshot
                                .matching(&r.tenant_id, &term.selector)
                                .map(|p| p.node_id.clone())
                                .collect()
                        }
                    })
                    .collect()
            })
            .collect();
        for id in &snapshot.reverse_anti {
            let p = &snapshot.capsules[id];
            if p.spec.tenant_id == r.tenant_id
                && p.spec.scheduling.placement_groups.iter().any(|g| {
                    g.target == PlacementTarget::Capsule
                        && g.required
                        && g.anti
                        && g.terms
                            .iter()
                            .any(|t| t.selector.matches(&r.scheduling.labels))
                })
            {
                result.reverse_group_nodes.insert(p.node_id.clone());
            }
            for t in &p.spec.scheduling.required_anti_affinity {
                if t.matches(&p.spec.tenant_id, &r.tenant_id, &r.scheduling.labels) {
                    match snapshot.node(&p.node_id) {
                        None => result.reverse_missing_node = true,
                        Some(n) => {
                            result.reverse.insert((
                                t.topology_key.clone(),
                                n.labels.get(&t.topology_key).cloned(),
                            ));
                        }
                    }
                }
            }
        }
        for t in &r.scheduling.topology_spread {
            let mut counts: BTreeMap<String, u64> = snapshot
                .nodes
                .values()
                .filter(|n| n.available && r.scheduling.matches_node(&n.labels))
                .filter_map(|n| n.labels.get(&t.topology_key).map(|v| (v.clone(), 0)))
                .collect();
            for p in snapshot.matching(&r.tenant_id, &t.selector) {
                if let Some(count) = snapshot
                    .node(&p.node_id)
                    .and_then(|n| n.labels.get(&t.topology_key))
                    .and_then(|v| counts.get_mut(v))
                {
                    *count += 1;
                }
            }
            result.spread.push(counts);
        }
        result
    }
}
