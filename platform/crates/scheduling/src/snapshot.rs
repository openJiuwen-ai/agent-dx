//! Persistent indexes: publication clones tree roots; writes copy only changed paths.
use crate::Node;
use adx_core::{scheduling::LabelSelector, EnvironmentSpec};
use im::{OrdMap, OrdSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct PlacedEnvironment {
    pub spec: EnvironmentSpec,
    pub node_id: String,
}

#[derive(Clone, Default)]
pub struct Snapshot {
    pub revision: u64,
    pub(crate) nodes: OrdMap<String, Arc<Node>>,
    pub(crate) environments: OrdMap<String, Arc<PlacedEnvironment>>,
    tenants: OrdMap<String, OrdSet<String>>,
    labels: OrdMap<(String, String, String), OrdSet<String>>,
    pub(crate) reverse_anti: OrdSet<String>,
}
impl Snapshot {
    pub fn nodes(&self) -> &OrdMap<String, Arc<Node>> {
        &self.nodes
    }
    pub fn environments(&self) -> &OrdMap<String, Arc<PlacedEnvironment>> {
        &self.environments
    }
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id).map(AsRef::as_ref)
    }
    pub fn update_node(&mut self, node: Node) {
        self.nodes.insert(node.id.clone(), Arc::new(node));
        self.revision += 1;
    }
    pub fn place(&mut self, placement: PlacedEnvironment) {
        let id = placement.spec.id.clone();
        self.remove_indexes(&id);
        let spec = &placement.spec;
        self.tenants
            .entry(spec.tenant_id.clone())
            .or_default()
            .insert(id.clone());
        for (key, value) in &spec.scheduling.labels {
            self.labels
                .entry((spec.tenant_id.clone(), key.clone(), value.clone()))
                .or_default()
                .insert(id.clone());
        }
        if !spec.scheduling.required_anti_affinity.is_empty()
            || spec.scheduling.placement_groups.iter().any(|g| {
                g.target == adx_core::scheduling::PlacementTarget::Environment
                    && g.required
                    && g.anti
            })
        {
            self.reverse_anti.insert(id.clone());
        }
        self.environments.insert(id, Arc::new(placement));
        self.revision += 1;
    }
    fn remove_indexes(&mut self, id: &str) {
        if let Some(p) = self.environments.remove(id) {
            let tenant = &p.spec.tenant_id;
            if let Some(ids) = self.tenants.get_mut(tenant) {
                ids.remove(id);
                if ids.is_empty() {
                    self.tenants.remove(tenant);
                }
            }
            for (key, value) in &p.spec.scheduling.labels {
                let index = (tenant.clone(), key.clone(), value.clone());
                if let Some(ids) = self.labels.get_mut(&index) {
                    ids.remove(id);
                    if ids.is_empty() {
                        self.labels.remove(&index);
                    }
                }
            }
            self.reverse_anti.remove(id);
        }
    }
    pub fn remove(&mut self, id: &str) {
        self.remove_indexes(id);
        self.revision += 1;
    }
    /// Pick the narrowest exact-label/tenant index, then evaluate all expressions.
    /// NotIn/DoesNotExist retain missing-label semantics; they never become an
    /// incorrect positive-label index lookup.
    pub fn matching<'a>(
        &'a self,
        tenant: &str,
        selector: &'a LabelSelector,
    ) -> impl Iterator<Item = &'a PlacedEnvironment> {
        let mut ids = self.tenants.get(tenant);
        for (key, value) in &selector.match_labels {
            let Some(index) = self
                .labels
                .get(&(tenant.to_owned(), key.clone(), value.clone()))
            else {
                ids = None;
                break;
            };
            if ids.is_some_and(|current| index.len() < current.len()) {
                ids = Some(index);
            }
        }
        ids.into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.environments.get(id))
            .filter(move |p| selector.matches(&p.spec.scheduling.labels))
            .map(AsRef::as_ref)
    }
    pub fn has_reverse_anti_affinity(&self) -> bool {
        !self.reverse_anti.is_empty()
    }
}
