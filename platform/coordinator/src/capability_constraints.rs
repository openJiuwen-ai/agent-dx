//! Read capability constraints from the reserved node-affinity labels.
use adx_core::scheduling::{
    LabelSelector, SchedulingPolicy, SelectorOp, NODE_CAPABILITY_LABEL_VALUE,
};

pub(crate) fn required(policy: &SchedulingPolicy) -> bool {
    policy.required_node_capability_labels().next().is_some()
}

fn requires_key(selector: &LabelSelector, key: &str) -> bool {
    selector
        .match_labels
        .get(key)
        .is_some_and(|value| value == NODE_CAPABILITY_LABEL_VALUE)
        || selector.expressions.iter().any(|expression| {
            expression.key == key
                && expression.op == SelectorOp::In
                && expression.values.as_slice() == [NODE_CAPABILITY_LABEL_VALUE]
        })
}

pub(crate) fn retained(requested: &SchedulingPolicy, retained: &SchedulingPolicy) -> bool {
    requested.required_node_capability_labels().all(|key| {
        !retained.required_node.is_empty()
            && retained
                .required_node
                .iter()
                .all(|selector| requires_key(selector, key))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::scheduling::{node_capability_label, LabelRequirement, CHECKPOINT_RESTORE};
    use std::collections::BTreeMap;

    #[test]
    fn named_replay_requires_every_retained_selector_and_current_label() {
        let key = node_capability_label("runsc", CHECKPOINT_RESTORE);
        let capability = LabelSelector {
            expressions: vec![LabelRequirement {
                key: key.clone(),
                op: SelectorOp::In,
                values: vec![NODE_CAPABILITY_LABEL_VALUE.into()],
            }],
            ..Default::default()
        };
        let requested = SchedulingPolicy {
            required_node: vec![capability.clone()],
            ..Default::default()
        };
        let mut existing = SchedulingPolicy::default();
        assert!(!retained(&requested, &existing));
        existing.required_node = vec![capability.clone(), LabelSelector::default()];
        assert!(!retained(&requested, &existing));
        existing.required_node[1] = capability;
        assert!(retained(&requested, &existing));

        let mut worker = BTreeMap::new();
        assert!(!requested.matches_node_capabilities(&worker));
        worker.insert(key.clone(), NODE_CAPABILITY_LABEL_VALUE.into());
        assert!(requested.matches_node_capabilities(&worker));
        worker.remove(&key);
        assert!(!requested.matches_node_capabilities(&worker));
    }
}
