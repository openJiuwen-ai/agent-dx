use adx_core::{scheduling::*, InstanceSpec, Resources};
use adx_scheduling::{PlacedInstance, Snapshot};
fn placed(id: &str, tenant: &str, labels: &[(&str, &str)]) -> PlacedInstance {
    PlacedInstance {
        node_id: "n".into(),
        spec: InstanceSpec {
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            id: id.into(),
            tenant_id: tenant.into(),
            image: "i".into(),
            runtime: "r".into(),
            priority: 0,
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 0,
            },
            scheduling: SchedulingPolicy {
                labels: labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                ..Default::default()
            },
        },
    }
}
#[test]
fn indexes_match_full_scan_across_replacement_release_and_negative_selectors() {
    let mut s = Snapshot::default();
    for i in 0..200 {
        s.place(placed(
            &format!("i{i}"),
            if i % 2 == 0 { "a" } else { "b" },
            if i % 3 == 0 {
                &[("app", "worker")]
            } else {
                &[]
            },
        ));
    }
    let old = s.clone();
    for i in 0..100 {
        s.remove(&format!("i{i}"));
    }
    s.place(placed("i150", "b", &[("app", "other")]));
    for snapshot in [&old, &s] {
        for tenant in ["a", "b", "missing"] {
            for selector in [
                LabelSelector::default(),
                LabelSelector {
                    match_labels: [("app".into(), "worker".into())].into(),
                    ..Default::default()
                },
                LabelSelector {
                    expressions: vec![LabelRequirement {
                        key: "app".into(),
                        op: SelectorOp::NotIn,
                        values: vec!["worker".into()],
                    }],
                    ..Default::default()
                },
            ] {
                let indexed: Vec<_> = snapshot
                    .matching(tenant, &selector)
                    .map(|p| p.spec.id.clone())
                    .collect();
                let scan: Vec<_> = snapshot
                    .instances()
                    .values()
                    .filter(|p| {
                        p.spec.tenant_id == tenant && selector.matches(&p.spec.scheduling.labels)
                    })
                    .map(|p| p.spec.id.clone())
                    .collect();
                assert_eq!(indexed, scan);
            }
        }
    }
    assert_eq!(old.instances().len(), 200);
    assert_eq!(s.instances().len(), 100);
}
