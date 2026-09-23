//! Synchronous, read-only placement rules shared by scheduling consumers.
//! Domain owns queues and reservations; plugins only evaluate candidate snapshots.
pub mod constraints;
mod groups;
pub mod plugins;
pub mod query;
pub mod snapshot;
use adx_core::scheduling::Device;
pub use adx_core::scheduling::Node;
pub use snapshot::{PlacedEnvironment, Snapshot};

use adx_core::{EnvironmentSpec, Error, Resources, Result};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Debug, Clone, Copy)]
pub enum Placement {
    Pack,
    Spread,
}

/// Capacity and free resources are from the same Domain scheduling round.
/// Free resources already subtract pending reservations.
pub struct Candidate<'a> {
    pub node: &'a Node,
    pub available: Resources,
    pub devices: &'a [Device],
    pub snapshot: &'a Snapshot,
    pub prepared: Option<&'a query::Prepared>,
}

pub trait Filter: Send + Sync {
    fn name(&self) -> &'static str;
    /// false rejects this candidate; an error aborts this scheduling attempt.
    fn filter(&self, request: &EnvironmentSpec, candidate: &Candidate<'_>) -> Result<bool>;
}
pub trait Score: Send + Sync {
    fn name(&self) -> &'static str;
    /// Higher is better. Only filtered candidates are scored, in [0, MAX_SCORE].
    fn score(&self, request: &EnvironmentSpec, candidate: &Candidate<'_>) -> Result<u32>;
}
/// Three scalar resource dimensions, each normalized to one million.
pub const MAX_SCORE: u32 = 3_000_000;

pub struct WeightedScore {
    plugin: Arc<dyn Score>,
    weight: u32,
}
impl WeightedScore {
    pub fn new(plugin: Arc<dyn Score>, weight: u32) -> Result<Self> {
        if weight == 0 {
            return Err(Error::Invalid("score weight must be positive".into()));
        }
        Ok(Self { plugin, weight })
    }
}

pub struct Framework {
    filters: Vec<Arc<dyn Filter>>,
    scores: Vec<WeightedScore>,
    builtin_profile: bool,
}
impl Framework {
    /// Static built-in registration. Deployment selects Pack or Spread.
    pub fn builtin(placement: Placement) -> Self {
        let mut framework = Self::new(
            vec![],
            vec![
                WeightedScore::new(Arc::new(groups::Preference), 1).expect("static weight"),
                WeightedScore::new(Arc::new(plugins::ResourceBalance(placement)), 1)
                    .expect("static weight"),
                WeightedScore::new(Arc::new(constraints::NodePreference), 1)
                    .expect("static weight"),
                WeightedScore::new(Arc::new(constraints::EnvironmentPreference), 1)
                    .expect("static weight"),
                WeightedScore::new(Arc::new(constraints::TopologyPreference), 1)
                    .expect("static weight"),
            ],
        )
        .expect("static plugin profile");
        framework.builtin_profile = true;
        framework
    }
    /// Additional rules are composed in registration order. Node availability
    /// and scalar capacity are mandatory guards in every profile.
    /// An empty score list uses stable node-ID ordering among eligible nodes.
    pub fn new(
        additional_filters: Vec<Arc<dyn Filter>>,
        scores: Vec<WeightedScore>,
    ) -> Result<Self> {
        let mut filters: Vec<Arc<dyn Filter>> = vec![
            Arc::new(plugins::NodeAvailable),
            Arc::new(plugins::ResourceFit),
            Arc::new(constraints::DeviceFit),
            Arc::new(constraints::NodeAffinity),
            Arc::new(groups::Required),
            Arc::new(constraints::EnvironmentAffinity),
            Arc::new(constraints::Topology),
        ];
        filters.extend(additional_filters);
        validate_names(filters.iter().map(|p| p.name()))?;
        validate_names(scores.iter().map(|p| p.plugin.name()))?;
        let max_total = scores.iter().try_fold(0u64, |total, score| {
            total.checked_add(u64::from(MAX_SCORE) * u64::from(score.weight))
        });
        if max_total.is_none() {
            return Err(Error::Invalid("total score weight overflow".into()));
        }
        Ok(Self {
            filters,
            scores,
            builtin_profile: false,
        })
    }
    pub fn filter_names(&self) -> Vec<&'static str> {
        self.filters.iter().map(|p| p.name()).collect()
    }
    pub fn score_weights(&self) -> Vec<(&'static str, u32)> {
        self.scores
            .iter()
            .map(|p| (p.plugin.name(), p.weight))
            .collect()
    }

    /// Unknown/custom profiles are deliberately ineligible, even if they use
    /// builtin-looking names. Reverse hard anti-affinity also disables reuse.
    pub fn supports_aggregation(&self, request: &EnvironmentSpec, snapshot: &Snapshot) -> bool {
        self.builtin_profile
            && request.resources.disk_bytes == 0
            && request.scheduling == Default::default()
            && !snapshot.has_reverse_anti_affinity()
    }
    /// Hard rules only; local-first admission intentionally skips all scoring.
    pub fn allows(&self, request: &EnvironmentSpec, candidate: &Candidate<'_>) -> Result<bool> {
        request.validate()?;
        for filter in &self.filters {
            if !filter.filter(request, candidate)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub fn evaluate(
        &self,
        request: &EnvironmentSpec,
        candidate: &Candidate<'_>,
    ) -> Result<Option<u64>> {
        if !self.allows(request, candidate)? {
            return Ok(None);
        }
        let mut total = 0;
        for score in &self.scores {
            let value = score.plugin.score(request, candidate)?;
            if value > MAX_SCORE {
                return Err(Error::Invalid(format!(
                    "score plugin {} returned {value}, maximum {MAX_SCORE}",
                    score.plugin.name()
                )));
            }
            total += u64::from(value) * u64::from(score.weight);
        }
        Ok(Some(total))
    }
    pub fn select<'a>(
        &self,
        request: &EnvironmentSpec,
        candidates: impl IntoIterator<Item = Candidate<'a>>,
    ) -> Result<Option<&'a str>> {
        request.validate()?;
        let mut candidates = candidates.into_iter().peekable();
        let Some(first) = candidates.peek() else {
            return Ok(None);
        };
        let prepared = query::Prepared::new(request, first.snapshot);
        let mut best: Option<(&'a str, u64)> = None;
        for candidate in candidates {
            let evaluated = Candidate {
                node: candidate.node,
                available: candidate.available,
                devices: candidate.devices,
                snapshot: candidate.snapshot,
                prepared: Some(&prepared),
            };
            let Some(total) = self.evaluate(request, &evaluated)? else {
                continue;
            };
            let id = candidate.node.id.as_str();
            if best.is_none_or(|(best_id, best_score)| {
                total > best_score || (total == best_score && id < best_id)
            }) {
                best = Some((id, total));
            }
        }
        Ok(best.map(|(id, _)| id))
    }
}
fn validate_names<'a>(names: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for name in names {
        if name.trim().is_empty() || !seen.insert(name) {
            return Err(Error::Invalid(format!(
                "empty or duplicate plugin name: {name}"
            )));
        }
    }
    Ok(())
}
