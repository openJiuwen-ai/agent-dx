use super::*;

impl CoordinatorRpc {
    /// Current per-Environment inventory for the optional short-retention store.
    pub async fn environment_metrics(&self) -> Result<String> {
        let state = self
            .0
            .state
            .try_lock()
            .map_err(|_| Error::Unavailable("Coordinator state busy".into()))?;
        state.healthy()?;
        let mut out = adx_observability::metrics::Text::default();
        for stored in state.environments.values() {
            let name = if stored.invalidated {
                "Invalidated".to_owned()
            } else if stored
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.pending)
            {
                "Recovering".to_owned()
            } else {
                stored
                    .result
                    .as_ref()
                    .map_or("Reserved".to_owned(), |record| {
                        format!("{:?}", record.state)
                    })
            };
            out.gauge(
                "adx_environment_info",
                &[
                    ("environment_id", stored.spec.id.clone()),
                    ("node_id", stored.assignment.node_id.clone()),
                    ("state", name),
                ],
                1,
            );
        }
        Ok(out.finish())
    }

    /// Read the current in-memory committed directory and scheduler ledgers.
    /// Never query Redis or block lifecycle RPCs waiting for a metrics scrape.
    pub async fn metrics(&self) -> Result<String> {
        let state = self
            .0
            .state
            .try_lock()
            .map_err(|_| Error::Unavailable("Coordinator state busy".into()))?;
        state.healthy()?;
        let unavailable = state
            .nodes
            .keys()
            .filter(|id| {
                state.live.get(*id).is_none_or(|live| live.expired)
                    || state.overdue(id, self.0.heartbeat_timeout)
            })
            .cloned()
            .collect();
        let mut out = state.scheduler.metrics_excluding(&unavailable);
        let states = [
            "Pending",
            "Reserved",
            "Starting",
            "Running",
            "Pausing",
            "Paused",
            "Resuming",
            "Deleting",
            "Failed",
            "Invalidated",
            "Recovering",
        ];
        let mut counts: BTreeMap<(usize, String, String), u64> = BTreeMap::new();
        for (id, node) in &state.nodes {
            for name in states {
                counts.insert((node.shard_id, id.clone(), name.into()), 0);
            }
            let labels = [
                ("shard_id", node.shard_id.to_string()),
                ("node_id", id.clone()),
            ];
            out.gauge(
                "adx_coordinator_node_reachable",
                &labels,
                u64::from(!unavailable.contains(id)),
            );
            if let Some(live) = state.live.get(id) {
                out.gauge(
                    "adx_coordinator_node_heartbeat_timeout",
                    &labels,
                    u64::from(live.expired || state.overdue(id, self.0.heartbeat_timeout)),
                );
                out.gauge(
                    "adx_coordinator_node_heartbeat_age_seconds",
                    &labels,
                    live.last_seen.elapsed().as_secs(),
                );
            }
        }
        for environment in state.environments.values() {
            let name = if environment.invalidated {
                "Invalidated".into()
            } else if environment.recovery.as_ref().is_some_and(|r| r.pending) {
                "Recovering".into()
            } else {
                environment
                    .result
                    .as_ref()
                    .map_or("Reserved".into(), |r| format!("{:?}", r.state))
            };
            *counts
                .entry((
                    environment.assignment.shard_id,
                    environment.assignment.node_id.clone(),
                    name,
                ))
                .or_default() += 1;
        }
        for ((shard, node, state), count) in counts {
            out.gauge(
                "adx_coordinator_environments",
                &[
                    ("shard_id", shard.to_string()),
                    ("node_id", node),
                    ("state", state),
                ],
                count,
            );
        }
        let mut output = out.finish();
        output.push_str(&format!(
            "# TYPE adx_environment_created_total counter\nadx_environment_created_total {}\n",
            self.0.created.load(std::sync::atomic::Ordering::Relaxed)
        ));
        Ok(output)
    }
}
