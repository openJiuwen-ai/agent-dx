use crate::Node;
use adx_core::{
    scheduling::{
        node_capability_label, valid_node_capability_runtime, CHECKPOINT_RESTORE_V1,
        NODE_CAPABILITY_LABEL_PREFIX, NODE_CAPABILITY_LABEL_VALUE,
    },
    Result,
};
use adx_protocol::control as pb;

/// Derive scheduler labels from a worker's authenticated registration.
pub(super) fn registered_node(report: &pb::RegisterNodeRequest) -> Result<Node> {
    let mut node = Node::try_from(report.clone())?;
    node.labels
        .retain(|key, _| !key.starts_with(NODE_CAPABILITY_LABEL_PREFIX));
    for (runtime, capabilities) in &report.runtime_capabilities {
        if valid_node_capability_runtime(runtime)
            && node.runtime_classes.contains(runtime)
            && capabilities
                .capability_ids
                .iter()
                .any(|capability| capability == CHECKPOINT_RESTORE_V1)
        {
            node.labels.insert(
                node_capability_label(runtime, CHECKPOINT_RESTORE_V1),
                NODE_CAPABILITY_LABEL_VALUE.into(),
            );
        }
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_derives_labels_only_from_current_runtime_observations() {
        let capability_label = node_capability_label("runsc", CHECKPOINT_RESTORE_V1);
        let mut report = pb::RegisterNodeRequest {
            node_id: "node".into(),
            capacity: Some(pb::Resources::default()),
            runtime_classes: vec!["runsc".into(), "runc".into()],
            labels: [
                (capability_label.clone(), NODE_CAPABILITY_LABEL_VALUE.into()),
                ("zone".into(), "a".into()),
            ]
            .into(),
            ..Default::default()
        };
        let node = registered_node(&report).unwrap();
        assert!(!node.labels.contains_key(&capability_label));
        assert_eq!(node.labels["zone"], "a");

        report.runtime_capabilities = [
            (
                "runsc".into(),
                pb::RuntimeCapabilities {
                    capability_ids: vec![CHECKPOINT_RESTORE_V1.into(), "unknown.v1".into()],
                },
            ),
            (
                "kata".into(),
                pb::RuntimeCapabilities {
                    capability_ids: vec![CHECKPOINT_RESTORE_V1.into()],
                },
            ),
        ]
        .into();
        let node = registered_node(&report).unwrap();
        assert_eq!(node.labels[&capability_label], NODE_CAPABILITY_LABEL_VALUE);
        assert!(!node
            .labels
            .contains_key(&node_capability_label("runsc", "unknown.v1")));
        assert!(!node
            .labels
            .contains_key(&node_capability_label("kata", CHECKPOINT_RESTORE_V1)));

        report.runtime_capabilities.clear();
        assert!(!registered_node(&report)
            .unwrap()
            .labels
            .contains_key(&capability_label));
    }
}
