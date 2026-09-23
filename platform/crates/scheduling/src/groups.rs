use crate::query::Prepared;
use crate::{Candidate, Filter, Score, MAX_SCORE};
use adx_core::{scheduling::PlacementTarget, EnvironmentSpec, Result};
use std::borrow::Cow;
fn prepared<'a>(r: &EnvironmentSpec, c: &'a Candidate<'_>) -> Cow<'a, Prepared> {
    c.prepared
        .map(Cow::Borrowed)
        .unwrap_or_else(|| Cow::Owned(Prepared::new(r, c.snapshot)))
}
fn matching(r: &EnvironmentSpec, c: &Candidate<'_>, q: &Prepared, index: usize) -> Vec<bool> {
    let group = &r.scheduling.placement_groups[index];
    group
        .terms
        .iter()
        .enumerate()
        .map(|(i, term)| match group.target {
            PlacementTarget::Node => term.selector.matches(&c.node.labels),
            PlacementTarget::Environment => q.groups[index][i].contains(&c.node.id),
        })
        .collect()
}
pub struct Required;
impl Filter for Required {
    fn name(&self) -> &'static str {
        "placement-groups"
    }
    fn filter(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<bool> {
        let q = prepared(r, c);
        if q.reverse_group_nodes.contains(&c.node.id) {
            return Ok(false);
        }
        for (i, g) in r
            .scheduling
            .placement_groups
            .iter()
            .enumerate()
            .filter(|(_, g)| g.required)
        {
            let found = matching(r, c, &q, i).into_iter().any(|v| v);
            if g.anti {
                if found {
                    return Ok(false);
                }
            } else if !found {
                let bootstrap = g.target == PlacementTarget::Environment
                    && q.groups[i].iter().all(|peers| peers.is_empty())
                    && g.terms
                        .iter()
                        .any(|t| t.selector.matches(&r.scheduling.labels));
                if !bootstrap {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}
pub struct Preference;
impl Score for Preference {
    fn name(&self) -> &'static str {
        "placement-group-preference"
    }
    fn score(&self, r: &EnvironmentSpec, c: &Candidate<'_>) -> Result<u32> {
        let q = prepared(r, c);
        let mut sum = 0u64;
        let mut count = 0u64;
        for (i, g) in r
            .scheduling
            .placement_groups
            .iter()
            .enumerate()
            .filter(|(_, g)| !g.required || g.ordered)
        {
            let values = matching(r, c, &q, i);
            count += 1;
            let desired = |matched: bool| if g.anti { !matched } else { matched };
            let score = if g.ordered {
                values.iter().position(|v| desired(*v)).map_or(0, |index| {
                    u64::from(MAX_SCORE) * (values.len() - index) as u64 / values.len() as u64
                })
            } else {
                let total: u64 = g.terms.iter().map(|t| u64::from(t.weight)).sum();
                let hit: u64 = g
                    .terms
                    .iter()
                    .zip(values)
                    .filter(|(_, v)| desired(*v))
                    .map(|(t, _)| u64::from(t.weight))
                    .sum();
                u64::from(MAX_SCORE) * hit / total
            };
            sum += score;
        }
        Ok(sum.checked_div(count).unwrap_or(0) as u32)
    }
}
