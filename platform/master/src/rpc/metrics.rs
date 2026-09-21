use super::*;

impl MasterRpc {
    /// Read the current in-memory committed directory and scheduler ledgers.
    /// Never query Redis or block lifecycle RPCs waiting for a metrics scrape.
    pub async fn metrics(&self) -> Result<String> {
        let state = self
            .0
            .state
            .try_lock()
            .map_err(|_| Error::Unavailable("Master state busy".into()))?;
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
                "adx_master_node_reachable",
                &labels,
                u64::from(!unavailable.contains(id)),
            );
            if let Some(live) = state.live.get(id) {
                out.gauge(
                    "adx_master_node_heartbeat_age_seconds",
                    &labels,
                    live.last_seen.elapsed().as_secs(),
                );
            }
        }
        let mut deleted = 0;
        for capsule in state.capsules.values() {
            if capsule
                .result
                .as_ref()
                .is_some_and(|r| r.state == CapsuleState::Deleted)
            {
                deleted += 1;
                continue;
            }
            let name = if capsule.invalidated {
                "Invalidated".into()
            } else if capsule.recovery.as_ref().is_some_and(|r| r.pending) {
                "Recovering".into()
            } else {
                capsule
                    .result
                    .as_ref()
                    .map_or("Reserved".into(), |r| format!("{:?}", r.state))
            };
            *counts
                .entry((
                    capsule.assignment.shard_id,
                    capsule.assignment.node_id.clone(),
                    name,
                ))
                .or_default() += 1;
        }
        for ((shard, node, state), count) in counts {
            out.gauge(
                "adx_master_capsules",
                &[
                    ("shard_id", shard.to_string()),
                    ("node_id", node),
                    ("state", state),
                ],
                count,
            );
        }
        // Retained terminal directory entries are not live capsules or an
        // all-time event counter: future retention cleanup can decrease this.
        out.gauge("adx_master_deleted_records", &[], deleted);
        Ok(out.finish())
    }
}
