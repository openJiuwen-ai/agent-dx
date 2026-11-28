//! Authoritative, bounded Environment queries for node scaling operations.
use super::*;
use crate::autoscaling::{InstancePage, InstanceQuery, InstanceView, NodeAdmission};

impl CoordinatorRpc {
    pub(crate) async fn scaling_instances(&self, query: InstanceQuery) -> Result<InstancePage> {
        let state = self.0.state.lock().await;
        state.healthy()?;
        let (snapshot, _) = state.session.publication_snapshot()?;
        if query
            .node_id
            .as_ref()
            .is_some_and(|node| !snapshot.nodes.contains_key(node))
        {
            return Err(Error::NotFound);
        }
        let node = query
            .node_id
            .as_ref()
            .and_then(|id| snapshot.nodes.get(id))
            .map(|node| NodeAdmission {
                node_id: node.node.id.clone(),
                scheduling_paused: node.scheduling_paused,
                accepting_allocations: node.node.available
                    && !node.scheduling_paused
                    && node.session.as_ref().is_some_and(|s| s.routable)
                    && state.live.get(&node.node.id).is_some_and(|live| {
                        !live.expired
                            && live.inspected
                            && !live.report.reconciling
                            && live.last_seen.elapsed() < self.0.heartbeat_timeout
                    }),
            });
        let epoch = state.session.epoch();
        drop(state);
        let mut count = 0usize;
        let mut instances = Vec::new();
        for stored in snapshot.environments.values() {
            let record = stored.effective_record();
            if (record.state == EnvironmentState::Deleted && !stored.resources_held())
                || query
                    .node_id
                    .as_ref()
                    .is_some_and(|id| id != &stored.assignment.node_id)
                || query
                    .tenant_id
                    .as_ref()
                    .is_some_and(|id| id != &stored.spec.tenant_id)
                || query
                    .instance_id
                    .as_ref()
                    .is_some_and(|id| id != &stored.spec.id)
            {
                continue;
            }
            if count >= query.offset && instances.len() < query.page_size {
                instances.push(InstanceView {
                    instance_id: stored.spec.id.clone(),
                    tenant_id: stored.spec.tenant_id.clone(),
                    node_id: stored.assignment.node_id.clone(),
                    generation: stored.assignment.generation,
                    revision: record.revision,
                    status: format!("{:?}", record.state).to_ascii_lowercase(),
                    resources_held: stored.resources_held(),
                    invalidated: stored.invalidated,
                    restart_pending: record.restart_pending,
                    resume_pending: stored.resume.is_some(),
                    recovery_pending: stored.recovery.as_ref().is_some_and(|r| r.pending),
                });
            }
            count += 1;
        }
        Ok(InstancePage {
            count,
            instance_infos: instances,
            epoch,
            revision: snapshot.revision,
            node,
        })
    }
}
